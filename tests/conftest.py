import os
from typing import List

import litellm
import pytest
from dotenv import load_dotenv

from dq_swirl.clients.async_llm_client import LLMConfig

load_dotenv("secrets.env")
load_dotenv(".env")

litellm.num_retries = 5

DEFAULT_MODEL = os.getenv("LLM_MODEL")
DEFAULT_LLM_URL = os.getenv("LLM_BASE_URL")
DEFAULT_LLM_API_KEY = os.getenv("LLM_API_KEY", "123")


REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_PW = os.getenv("REDIS_PW")
REDIS_URL = f"redis://:{REDIS_PW}@{REDIS_HOST}:{REDIS_PORT}"


LLM_CONFIGS = [
    pytest.param(
        LLMConfig(
            model=DEFAULT_MODEL,
            base_url=DEFAULT_LLM_URL,
            api_key=DEFAULT_LLM_API_KEY,
        )
    )
]

MESSY_SAMPLE_DATA = [
    "Order 1001: Buyer=John Davis, Location=Columbus, OH, Total=$742.10, Items: laptop, hdmi cable",
    "Order 1004:   Buyer=  AMANDA SMITH ,Location=Seattle, WA,Total=$50.00, Items: desk lamp",
    "Order 1005: Buyer=Raj Patel, Total=1,200.50, Items: monitor, stand, cable",
    "Order 1006: total=$89.99, location=Miami, FL, buyer=Elena Rossi, Items: keyboard",
    "Order 1007: Buyer=Chris P., Location=Denver, CO, Total=$12.00, Items: stickers -- [DISCOUNT APPLIED]",
    "Order 1008: Buyer=O'Connor, S., Location=Portland, OR, Total=$0.00, Items: ",
    "Order 1011: Buyer=John Davis, Location=Columbus, OH, Total=$742.10, Items: laptop, hdmi cable",
    "Order 1012: Buyer=Sarah Liu, Location=Austin, TX, Total=$156.55, Items: headphones",
    "Order 1013: Buyer=Mike Turner, Location=Cleveland, OH, Total=$1299.99, Items: gaming pc, mouse",
    "Order 1014: Buyer=Rachel Kim, Locadtion=Seattle, WA, Total=$89.50, Items: coffee maker",
    "Order 1015: Buyer=Chris Myers, Location=Cincinnati, OH, Total=$512.00, Items: monitor, desk lamp",
    "Order=1016, Buyer=Jake Myers, Total=$1,512.00, Items: monitor,",
    '{"id": "usr_001", "name": "Alex Johnson", "role": "admin", "isActive": true, "createdAt": "2025-11-02T09:14:23Z"}',
    '{"id": "usr_002", "name": "Maria Lopez", "email": "maria.lopez@example.com", "role": "editor", "isActive": null, "createdAt": "2025-12-18T16:47:10Z", "lastLoginIp": "192.168.1.42"}',
    '{"id": "usr_003", "email": "samir.patel@example.com", "role": "viewer", "isActive": false, "createdAt": "08/05/2024"}',
    '{"id": 4, "name": "Chen Wei", "email": "chen.wei@example.com", "isActive": true, "createdAt": null}',
    '{"id": "usr_005", "name": "Broken Record", "email": "broken@example.com"}',
]

ETL_LOOKUP_MAP = {
    "fd116cd512d5ecd2e59edf12fc258b32": {
        "semantic_cluster_id": "0",
        "structure_cluster_id": "0",
        "base_model_fpath": "data/pipeline_runs/run_20260203-215528/sem_0-order/order_base_model.py",
        "parser_fpath": "data/pipeline_runs/run_20260203-215528/sem_0-order/order_parser-struct_0.py",
        "fields": ["order", "buyer", "location", "total", "items"],
    },
    "50eb97a85647221ecc7f65f74d68d156": {
        "semantic_cluster_id": "0",
        "structure_cluster_id": "0",
        "base_model_fpath": "data/pipeline_runs/run_20260203-215528/sem_0-order/order_base_model.py",
        "parser_fpath": "data/pipeline_runs/run_20260203-215528/sem_0-order/order_parser-struct_0.py",
        "fields": ["order", "buyer", "total", "items"],
    },
    "28d9f3b14d0e5516a186062212502d0c": {
        "semantic_cluster_id": "0",
        "structure_cluster_id": "0",
        "base_model_fpath": "data/pipeline_runs/run_20260203-215528/sem_0-order/order_base_model.py",
        "parser_fpath": "data/pipeline_runs/run_20260203-215528/sem_0-order/order_parser-struct_0.py",
        "fields": ["order", "buyer", "locadtion", "total", "items"],
    },
    "b441fd0cc9071e4311f67a792a309a9c": {
        "semantic_cluster_id": "1",
        "structure_cluster_id": "1",
        "base_model_fpath": "data/pipeline_runs/run_20260203-215528/sem_1-user/user_base_model.py",
        "parser_fpath": "data/pipeline_runs/run_20260203-215528/sem_1-user/user_parser-struct_1.py",
        "fields": ["createdat", "id", "isactive", "name", "role"],
    },
    "fb00b9735a7c3887cb459047473c541c": {
        "semantic_cluster_id": "1",
        "structure_cluster_id": "1",
        "base_model_fpath": "data/pipeline_runs/run_20260203-215528/sem_1-user/user_base_model.py",
        "parser_fpath": "data/pipeline_runs/run_20260203-215528/sem_1-user/user_parser-struct_1.py",
        "fields": [
            "createdat",
            "email",
            "id",
            "isactive",
            "lastloginip",
            "name",
            "role",
        ],
    },
    "4727360ea96dc2ded6d762f08cb6fbc1": {
        "semantic_cluster_id": "1",
        "structure_cluster_id": "1",
        "base_model_fpath": "data/pipeline_runs/run_20260203-215528/sem_1-user/user_base_model.py",
        "parser_fpath": "data/pipeline_runs/run_20260203-215528/sem_1-user/user_parser-struct_1.py",
        "fields": ["createdat", "email", "id", "isactive", "role"],
    },
    "d6b88dcbee3611198395069946d9f5fe": {
        "semantic_cluster_id": "1",
        "structure_cluster_id": "1",
        "base_model_fpath": "data/pipeline_runs/run_20260203-215528/sem_1-user/user_base_model.py",
        "parser_fpath": "data/pipeline_runs/run_20260203-215528/sem_1-user/user_parser-struct_1.py",
        "fields": ["createdat", "email", "id", "isactive", "name"],
    },
    "d2d16f7c3698c6195ddaeb6205139150": {
        "semantic_cluster_id": "1",
        "structure_cluster_id": "1",
        "base_model_fpath": "data/pipeline_runs/run_20260203-215528/sem_1-user/user_base_model.py",
        "parser_fpath": "data/pipeline_runs/run_20260203-215528/sem_1-user/user_parser-struct_1.py",
        "fields": ["email", "id", "name"],
    },
}


CLUSTER_SETS = {
    "0": [
        "fd116cd512d5ecd2e59edf12fc258b32",
        "50eb97a85647221ecc7f65f74d68d156",
        "28d9f3b14d0e5516a186062212502d0c",
    ],
    "1": [
        "b441fd0cc9071e4311f67a792a309a9c",
        "fb00b9735a7c3887cb459047473c541c",
        "4727360ea96dc2ded6d762f08cb6fbc1",
        "d6b88dcbee3611198395069946d9f5fe",
        "d2d16f7c3698c6195ddaeb6205139150",
    ],
}


@pytest.fixture(scope="class")
def redis_url() -> str:
    return REDIS_URL


@pytest.fixture(scope="class")
def messy_data() -> List[str]:
    return MESSY_SAMPLE_DATA


@pytest.fixture(scope="class")
def etl_lookup_map():
    return ETL_LOOKUP_MAP


@pytest.fixture(scope="class")
def cluster_sets():
    return CLUSTER_SETS
