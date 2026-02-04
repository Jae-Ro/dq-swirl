from typing import Any, Dict, Optional

import httpx

from dq_swirl.utils.log_utils import get_custom_logger

logger = get_custom_logger()


async def log_response_info(response: httpx.Response) -> None:
    """Async function to log the client connection id as a callback event hook for connection pool.

    :param response: The HTTPX Response object intercepted by the event hook.
    """
    extensions = response.extensions

    # Get the actual network stream object
    stream = extensions.get("network_stream")
    # Use the object's memory ID as a unique identifier for the connection
    conn_id = id(stream) if stream else "NoStream"

    # We'll use a local cache (or just look at the logs) to see if the ID repeats
    http_version = extensions.get("http_version", b"unknown").decode()
    logger.debug(
        f"[HTTPX Pool] ConnID: {conn_id} | {http_version} | {response.request.method} {response.url}"
    )
    return


async def create_async_httpx_client_pool(
    max_connections: int = 20,
    max_keepalive_connections: int = 10,
    timeout_connect: float = 5.0,
    timeout_read: float = 10.0,
) -> httpx.AsyncClient:
    """Async function to create a pre-configured HTTPX async client pool with event hooks.

    :param max_connections: max number of concurrent total connections allowed, defaults to 20
    :param max_keepalive_connections: max number of idle connections to keep "hot" in the pool for reuse, defaults to 10
    :param timeout_connect: max time (seconds) to wait for a successful TCP/TLS handshake, defaults to 5.0
    :param timeout_read: max time (seconds) to wait for a chunk of data from the server, defaults to 10.0
    :return: configured instance of httpx.AsyncClient instance ready for use in async tasks as a connection pool
    """
    pool = httpx.AsyncClient(
        limits=httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
        ),
        timeout=httpx.Timeout(
            60.0,
            connect=timeout_connect,
            read=timeout_read,
        ),
        event_hooks={
            "response": [log_response_info],
        },
        follow_redirects=True,
    )
    return pool


class AsyncHttpxClient:
    def __init__(
        self,
        pool: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self._client = pool

    async def request(
        self,
        url: str,
        method: str = "GET",
        request_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any] | str | Any:
        """Async method to get data from an http api endpoint

        :param url: api url
        :param method: HTTP verb method, defaults to "GET"
        :param request_body: dictionary of request body (converted to query params for GET and JSON body for POST), defaults to None
        :return: _description_
        """

        http_client = self._client
        client_created = False

        # create client if does not exist
        if not http_client:
            http_client = await create_async_httpx_client_pool(
                max_connections=3,
            )
            client_created = True

        req_url = url
        is_get = method.upper() == "GET"

        try:
            response = await http_client.request(
                method=method.upper(),
                url=req_url,
                params=request_body if is_get else None,
                json=request_body if not is_get else None,
            )

            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "")
            if "application/json" in content_type:
                return response.json()

            # return the text or raw bytes
            logger.warning(f"Expected JSON but got {content_type}. Returning raw text.")
            return response.text

        except httpx.HTTPStatusError as exc:
            logger.error(
                f"Error response {exc.response.status_code} while requesting {exc.request.url!r}"
            )
            raise
        except Exception as e:
            logger.error(f"Unexpected error calling {req_url}: {str(e)}")
            raise
        finally:
            # closing client if created in method
            if client_created:
                await http_client.aclose()
