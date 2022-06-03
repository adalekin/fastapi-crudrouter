import json
from typing import Type, Any, Optional

from fastapi import HTTPException
from pydantic import create_model

from ._types import T, PYDANTIC_SCHEMA


class AttrDict(dict):  # type: ignore
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def get_pk_type(schema: Type[PYDANTIC_SCHEMA], pk_field: str) -> Any:
    try:
        return schema.__fields__[pk_field].type_
    except KeyError:
        return int


def schema_factory(schema_cls: Type[T], pk_field_name: str = "id", name: str = "Create") -> Type[T]:
    """
    Is used to create a CreateSchema which does not contain pk
    """

    fields = {f.name: (f.type_, ...) for f in schema_cls.__fields__.values() if f.name != pk_field_name}

    name = schema_cls.__name__ + name
    schema: Type[T] = create_model(__model_name=name, **fields)  # type: ignore
    return schema


def create_query_validation_exception(field: str, msg: str) -> HTTPException:
    return HTTPException(
        422,
        detail={"detail": [{"loc": ["query", field], "msg": msg, "type": "type_error.integer"}]},
    )


def filter_spec(filter: Optional[str] = None):
    if filter:
        return json.loads(filter)


def sort_spec(sort: Optional[str] = None):
    if sort:
        return json.loads(sort)
