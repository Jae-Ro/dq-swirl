from swirl.clients.pg_duckdb_client import PGConfig, PGDuckDBClient


class TestPGDuckDBClient:
    def test_connection(self, pg_config: PGConfig):
        client = PGDuckDBClient(config=pg_config)
        healthy = client.is_healthy()
        assert healthy
