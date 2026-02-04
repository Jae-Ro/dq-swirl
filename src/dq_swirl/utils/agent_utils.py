import asyncio
import importlib.util
import inspect
import random
import re
from functools import wraps
from pathlib import Path
from typing import Dict, List, Optional, Type

from pydantic import BaseModel, Field

from dq_swirl.utils.log_utils import get_custom_logger

logger = get_custom_logger()


def extract_python_code(text: str) -> str:
    """Helper function to extract python code block from a string.

    :param text: input text string
    :return: python code string
    """
    block_pattern = r"```(?:python)?\s*(.*?)\s*```"
    match = re.search(block_pattern, text, re.DOTALL)

    return match.group(1).strip() if match else ""


def extract_sql_code(text: str) -> str:
    """Helper function to extract sql code block from a string.

    :param text: input text string
    :return: sql code string
    """
    block_pattern = r"```(?:sql)?\s*(.*?)\s*```"
    match = re.search(block_pattern, text, re.DOTALL)

    return match.group(1).strip() if match else ""


def prepause(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        await asyncio.sleep(random.uniform(0.2, 0.5))
        return await func(*args, **kwargs)

    return wrapper


def load_function(file_path: str, function_name: str) -> callable:
    """_summary_

    :param file_path: _description_
    :param function_name: _description_
    :return: _description_
    """
    path = Path(file_path)
    module_name = path.stem
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    func = getattr(module, function_name)
    return func


def load_pydantic_base_models(file_path: str) -> List[Type[BaseModel]]:
    """_summary_

    :param file_path: _description_
    :return: _description_
    """
    path = Path(file_path)
    module_name = path.stem

    spec = importlib.util.spec_from_file_location(module_name, str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    models = []
    for name, obj in inspect.getmembers(module, inspect.isclass):
        if issubclass(obj, BaseModel) and obj is not BaseModel:
            if obj.__module__ == module_name:
                logger.debug(f"Loading BaseModel: {name}")
                models.append(obj)

    for model in models:
        try:
            model.model_rebuild()
        except Exception as e:
            logger.error(f"Note: Could not rebuild {model.__name__} yet: {e}")

    return models


# for hash_signature, records in new_analyzer.signature_map.items():
#     hash_dict = signature_map[hash_signature]
#     function_fpath = hash_dict["parser_fpath"]
#     transform_func = load_function(function_fpath, "transform_to_models")
#     raw_recrods = [r["raw"] for r in records["records"]]
#     parsed_records = [r["parsed"] for r in records["records"]]
#     logger.debug(f"BEFORE: {json.dumps(raw_recrods, indent=2)}")
#     result = transform_func(parsed_records)
#     logger.debug(f"AFTER: {json.dumps(result, indent=2)}")
