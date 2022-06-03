from typing import Any, Callable, List, Type, Coroutine, Optional, Union

from fastapi import Path
from fastapi_pagination import Page

from . import CRUDGenerator, NOT_FOUND
from ._types import DEPENDENCIES, PYDANTIC_SCHEMA as SCHEMA

try:
    from tortoise.models import Model
except ImportError:
    Model = None  # type: ignore
    tortoise_installed = False
else:
    tortoise_installed = True


CALLABLE = Callable[..., Coroutine[Any, Any, Model]]
CALLABLE_LIST = Callable[..., Coroutine[Any, Any, List[Model]]]


class TortoiseCRUDRouter(CRUDGenerator[SCHEMA]):
    def __init__(
        self,
        schema: Type[SCHEMA],
        db_model: Type[Model],
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
        assert tortoise_installed, "Tortoise ORM must be installed to use the TortoiseCRUDRouter."

        self.db_model = db_model
        self._pk: str = db_model.describe()["pk_field"]["db_column"]

        super().__init__(
            schema=schema,
            create_schema=create_schema,
            update_schema=update_schema,
            prefix=prefix or db_model.describe()["name"].replace("None.", ""),
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
            from fastapi_pagination.ext.tortoise import paginate

            async def route() -> Page[Model]:
                return await paginate(self.db_model.all())  # type: ignore

        else:

            async def route() -> List[Model]:
                return await self.db_model.all()

        return route

    def _get_one(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(item_id: int = Path(..., alias=self.path_param_name)) -> Model:
            model = await self.db_model.filter(id=item_id).first()

            if model:
                return model
            else:
                raise NOT_FOUND

        return route

    def _create(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(model: self.create_schema) -> Model:  # type: ignore
            db_model = self.db_model(**model.dict())
            await db_model.save()

            return db_model

        return route

    def _update(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(
            model: self.update_schema,
            item_id: int = Path(..., alias=self.path_param_name),
        ) -> Model:  # type: ignore
            await self.db_model.filter(id=item_id).update(**model.dict(exclude_unset=True))
            return await self._get_one()(item_id)

        return route

    def _delete_all(self, *args: Any, **kwargs: Any) -> CALLABLE_LIST:
        async def route() -> None:
            await self.db_model.all().delete()

        return route

    def _delete_one(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(item_id: int = Path(..., alias=self.path_param_name)) -> Model:
            model: Model = await self._get_one()(item_id)
            await self.db_model.filter(id=item_id).delete()

            return model

        return route
