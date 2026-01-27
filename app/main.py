import json
import os
from typing import AsyncGenerator, Optional

from dotenv import load_dotenv
from litellm import ModelResponse, acompletion
from quart import Quart, Response, make_response, request
from quart_cors import cors

from dq_swirl.utils.log_utils import get_custom_logger

logger = get_custom_logger()

load_dotenv("secrets.env")


app = Quart(__name__)
app = cors(app, allow_origin="*")


@app.route("/api/chat", methods=["POST"])
async def chat() -> Response:
    """Function to handle /api/chat route

    :return: Response with async generator for LLM/Agent request call
    :yield: SSE response chunk string
    """
    body = await request.get_json()
    logger.debug(f"Handling Chat Request:\n{json.dumps(body, indent=4)}")
    user_prompt = body.get("prompt")
    # Using the correct model name now!
    model = body.get("model", "openai/google/gemma-3-27b-it")

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
            error_payload = json.dumps({"error": f"{str(e)}"})
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
