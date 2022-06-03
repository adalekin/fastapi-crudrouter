from typing import Any, Callable, List, Optional, Type, Union

from fastapi import Path
from fastapi_pagination import Page, resolve_params, create_page

from . import NOT_FOUND, CRUDGenerator
from ._types import DEPENDENCIES
from ._types import PYDANTIC_SCHEMA as SCHEMA

CALLABLE = Callable[..., SCHEMA]
CALLABLE_LIST = Callable[..., List[SCHEMA]]


class MemoryCRUDRouter(CRUDGenerator[SCHEMA]):
    def __init__(
        self,
        schema: Type[SCHEMA],
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
        super().__init__(
            schema=schema,
            create_schema=create_schema,
            update_schema=update_schema,
            prefix=prefix,
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

        self.models: List[SCHEMA] = []
        self._id = 1

    def _get_all(self, *args: Any, **kwargs: Any) -> CALLABLE_LIST:
        if self.pagination:

            def route() -> Page[SCHEMA]:
                params = resolve_params(params=None)
                raw_params = params.to_raw_params()

                if raw_params.limit is None:
                    items = self.models[raw_params.offset :]
                else:
                    items = self.models[raw_params.offset : raw_params.offset + raw_params.limit]

                return create_page(items, len(self.models), params)  # type: ignore

        else:

            def route() -> List[SCHEMA]:
                return self.models

        return route

    def _get_one(self, *args: Any, **kwargs: Any) -> CALLABLE:
        def route(item_id: int = Path(..., alias=self.path_param_name)) -> SCHEMA:
            for model in self.models:
                if model.id == item_id:  # type: ignore
                    return model

            raise NOT_FOUND

        return route

    def _create(self, *args: Any, **kwargs: Any) -> CALLABLE:
        def route(model: self.create_schema) -> SCHEMA:  # type: ignore
            model_dict = model.dict()
            model_dict["id"] = self._get_next_id()
            ready_model = self.schema(**model_dict)
            self.models.append(ready_model)
            return ready_model

        return route

    def _update(self, *args: Any, **kwargs: Any) -> CALLABLE:
        def route(model: self.update_schema, item_id: int = Path(..., alias=self.path_param_name)) -> SCHEMA:  # type: ignore
            for ind, model_ in enumerate(self.models):
                if model_.id == item_id:  # type: ignore
                    self.models[ind] = self.schema(**model.dict(), id=model_.id)  # type: ignore
                    return self.models[ind]

            raise NOT_FOUND

        return route

    def _delete_all(self, *args: Any, **kwargs: Any) -> CALLABLE_LIST:
        def route() -> None:
            self.models = []

        return route

    def _delete_one(self, *args: Any, **kwargs: Any) -> CALLABLE:
        def route(item_id: int = Path(..., alias=self.path_param_name)) -> SCHEMA:
            for ind, model in enumerate(self.models):
                if model.id == item_id:  # type: ignore
                    del self.models[ind]
                    return model

            raise NOT_FOUND

        return route

    def _get_next_id(self) -> int:
        id_ = self._id
        self._id += 1

        return id_
