from typing import Any, Callable, List, Optional, Type, Union, Coroutine

from fastapi import HTTPException, Path
from fastapi_pagination import Page

from . import NOT_FOUND, CRUDGenerator, _utils
from ._types import DEPENDENCIES
from ._types import PYDANTIC_SCHEMA as SCHEMA

try:
    from asyncpg.exceptions import UniqueViolationError
    from gino import Gino
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.ext.declarative import DeclarativeMeta as Model
except ImportError:
    Model = None
    IntegrityError = None
    UniqueViolationError = None
    Gino = None
    gino_installed = False
else:
    gino_installed = True

CALLABLE = Callable[..., Coroutine[Any, Any, Model]]
CALLABLE_LIST = Callable[..., Coroutine[Any, Any, List[Model]]]


class GinoCRUDRouter(CRUDGenerator[SCHEMA]):
    def __init__(
        self,
        schema: Type[SCHEMA],
        db_model: Model,
        db: "Gino",
        create_schema: Optional[Type[SCHEMA]] = None,
        update_schema: Optional[Type[SCHEMA]] = None,
        prefix: Optional[str] = None,
        tags: Optional[List[str]] = None,
        pagination: bool = False,
        get_all_route: Union[bool, DEPENDENCIES] = True,
        get_one_route: Union[bool, DEPENDENCIES] = True,
        create_route: Union[bool, DEPENDENCIES] = True,
        update_route: Union[bool, DEPENDENCIES] = True,
        delete_one_route: Union[bool, DEPENDENCIES] = True,
        delete_all_route: Union[bool, DEPENDENCIES] = True,
        **kwargs: Any
    ) -> None:
        assert gino_installed, "Gino must be installed to use the GinoCRUDRouter."

        self.db_model = db_model
        self.db = db
        self._pk: str = db_model.__table__.primary_key.columns.keys()[0]
        self._pk_type: type = _utils.get_pk_type(schema, self._pk)

        super().__init__(
            schema=schema,
            create_schema=create_schema,
            update_schema=update_schema,
            prefix=prefix or db_model.__tablename__,
            tags=tags,
            pagination=pagination,
            get_all_route=get_all_route,
            get_one_route=get_one_route,
            create_route=create_route,
            update_route=update_route,
            delete_one_route=delete_one_route,
            delete_all_route=delete_all_route,
            **kwargs
        )

    def _get_all(self, *args: Any, **kwargs: Any) -> CALLABLE_LIST:
        if self.pagination:
            from fastapi_pagination.ext.gino import paginate

            async def route() -> Page[Model]:
                return await paginate(query=self.db_model.query)  # type: ignore

        else:

            async def route() -> List[Model]:

                return await self.db_model.query.gino.all()

        return route

    def _get_one(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(item_id: self._pk_type = Path(..., alias=self.path_param_name)) -> Model:  # type: ignore
            model: Model = await self.db_model.get(item_id)

            if model:
                return model
            else:
                raise NOT_FOUND

        return route

    def _create(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(
            model: self.create_schema,  # type: ignore
        ) -> Model:
            try:
                async with self.db.transaction():
                    db_model: Model = await self.db_model.create(**model.dict())
                    return db_model
            except (IntegrityError, UniqueViolationError):
                raise HTTPException(422, "Key already exists") from None

        return route

    def _update(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(
            model: self.update_schema,  # type: ignore
            item_id: self._pk_type = Path(..., alias=self.path_param_name),  # type: ignore
        ) -> Model:
            try:
                db_model: Model = await self._get_one()(item_id)
                async with self.db.transaction():
                    model = model.dict(exclude={self._pk})
                    await db_model.update(**model).apply()

                return db_model
            except (IntegrityError, UniqueViolationError) as e:
                self._raise(e)

        return route

    def _delete_all(self, *args: Any, **kwargs: Any) -> CALLABLE_LIST:
        async def route() -> None:
            await self.db_model.delete.gino.status()

        return route

    def _delete_one(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(item_id: self._pk_type = Path(..., alias=self.path_param_name)) -> Model:  # type: ignore
            db_model: Model = await self._get_one()(item_id)
            await db_model.delete()

            return db_model

        return route
