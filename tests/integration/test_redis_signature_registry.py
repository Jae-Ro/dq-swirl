from dq_swirl.persistence.signature_registry import SignatureRegistry
from dq_swirl.utils.log_utils import get_custom_logger

logger = get_custom_logger()


class TestRedisSignatureRegistry:
    async def test_signature_registry_create(
        self,
        redis_url,
        etl_lookup_map,
        cluster_sets,
    ) -> None:
        """_summary_

        :param redis_url: _description_
        :param etl_lookup_map: _description_
        :param cluster_sets: _description_
        """
        registry = SignatureRegistry(redis_url=redis_url)
        # store them
        await registry.store_etl_lookup(etl_lookup_map, cluster_sets)
        # read and validate
        cand_hash = "50eb97a85647221ecc7f65f74d68d156"
        res = await registry.lookup_hash_signature(cand_hash)
        logger.debug(res)
