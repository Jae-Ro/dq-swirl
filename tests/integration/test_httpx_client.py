from swirl.clients.async_httpx_client import AsyncHttpxClient


class TestAsyncHTTPXClient:
    async def test_get_healthcheck(self):
        client = AsyncHttpxClient()
        res = await client.request("http://localhost:5000/health")

        assert isinstance(res, dict)
        assert "status" in res
        assert res["status"] == "ok"
