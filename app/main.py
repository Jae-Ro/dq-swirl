import json
import os
from typing import Annotated, AsyncGenerator, Optional

from dotenv import load_dotenv
from litellm import ModelResponse, acompletion
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pydantic.alias_generators import to_camel
from quart import Quart, Response, make_response, request
from quart_cors import cors

from dq_swirl.utils.log_utils import get_custom_logger

logger = get_custom_logger()

load_dotenv("secrets.env")


app = Quart(__name__)
app = cors(app, allow_origin="*")


class ChatRequest(BaseModel):
    """
    BaseModel class to dictate chat request parameters.

    Attributes:
        prompt: user's input message -- must not be empty string (required)
        model: name of model to be run (required)
        user_id: id of user (required)
        conversation_id: id of chat conversation from provided user (required)
    """

    # for converting camelCase to snake_case
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
    )
    # attributes
    prompt: str = Annotated[str, Field(min_length=1)]
    model: str
    user_id: str
    conversation_id: str


@app.route("/api/chat", methods=["POST"])
async def chat() -> Response:
    """Function to handle /api/chat route

    :return: Response with async generator for LLM/Agent request call
    :yield: SSE response chunk string
    """
    body = await request.get_json()
    logger.info(f"Handling Chat Request:\n{json.dumps(body, indent=4)}")

    # handling request body validation
    try:
        body = ChatRequest.model_validate(body)
    except ValidationError as e:
        logger.exception(e)
        return await make_response({"error": e.errors()}, 400)

    # get parsed fields
    user_prompt = body.prompt
    model = body.model

    async def generate() -> AsyncGenerator[str, None]:
        # heartbeat token to open resp stream pipe
        yield ": heartbeat\n\n"

        messages = [
            {
                "role": "user",
                "content": user_prompt,
            }
        ]
        llm_api_base = f"{os.getenv('OPENAI_API_BASE_URL')}/v1"

        logger.debug(
            f"Sending Request to {llm_api_base} for model: {model}:\n{json.dumps(messages, indent=4)}"
        )

        try:
            response = await acompletion(
                model=model,
                messages=messages,
                api_base=llm_api_base,
                stream=True,
                api_key=os.getenv("OPENAI_API_KEY"),
            )

            async for chunk in response:
                chunk: ModelResponse
                content: Optional[str] = chunk.choices[0].delta.content
                if content:
                    payload = json.dumps(
                        {
                            "data": {
                                "content": content,
                            }
                        }
                    )
                    yield f"data: {payload}\n\n"

            yield "data: [DONE]\n\n"

        except Exception as e:
            logger.exception(e)
            error_payload = json.dumps(
                {
                    "error": f"{str(e)}",
                }
            )
            yield f"data: {error_payload}\n\n"
            yield "data: [DONE]\n\n"

    # create response stream object
    response = await make_response(generate())

    # set headers
    response.mimetype = "text/event-stream"
    response.headers["X-Accel-Buffering"] = "no"
    response.headers["Connection"] = "keep-alive"
    response.headers["Cache-Control"] = "no-cache"
    response.headers["Transfer-Encoding"] = "chunked"

    # set timeout to be 5 min
    response.timeout = 300

    return response


if __name__ == "__main__":
    app.run(
        port=5000,
        debug=True,
        keep_alive_timeout=300,
    )
