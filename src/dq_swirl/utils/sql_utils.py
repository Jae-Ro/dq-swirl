from typing import Any, Dict, Type

from pydantic import BaseModel
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
)


def generate_sqlalchemy_table(
    model_class: Type[BaseModel], metadata: MetaData
) -> Table:
    """_summary_

    :param model_class: _description_
    :param metadata: _description_
    :return: _description_
    """
    type_map = {
        str: String,
        int: Integer,
        float: Float,
        bool: Boolean,
    }

    columns = []

    for field_name, field_info in model_class.model_fields.items():
        python_type = field_info.annotation

        actual_type = getattr(python_type, "__args__", [python_type])[0]
        sql_type = type_map.get(actual_type, String)

        is_pk = (
            field_name.lower() == "id"
            or field_name == list(model_class.model_fields.keys())[0]
        )

        columns.append(Column(field_name, sql_type, primary_key=is_pk))

    return Table(
        model_class.__name__.lower(),
        metadata,
        *columns,
    )
