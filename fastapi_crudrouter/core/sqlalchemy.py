from typing import Any, Callable, Generator, List, Optional, Type, Union

from fastapi import Depends, HTTPException, Path
from fastapi_pagination import Page

from . import NOT_FOUND, CRUDGenerator, _utils
from ._types import DEPENDENCIES
from ._types import PYDANTIC_SCHEMA as SCHEMA
from ._utils import sort_spec, filter_spec

try:
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.ext.declarative import DeclarativeMeta as Model
    from sqlalchemy.orm import Session
except ImportError:
    Model = None
    Session = None
    IntegrityError = None
    sqlalchemy_installed = False
else:
    sqlalchemy_installed = True
    Session = Callable[..., Generator[Session, Any, None]]

CALLABLE = Callable[..., Model]
CALLABLE_LIST = Callable[..., List[Model]]


class SQLAlchemyCRUDRouter(CRUDGenerator[SCHEMA]):
    def __init__(
        self,
        schema: Type[SCHEMA],
        db_model: Model,
        db: "Session",
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
        assert sqlalchemy_installed, "SQLAlchemy must be installed to use the SQLAlchemyCRUDRouter."

        self.db_model = db_model
        self.db_func = db
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
        from sqlalchemy_filters import apply_filters, apply_sort

        if self.pagination:
            from fastapi_pagination.ext.sqlalchemy import paginate

            def route(
                db: Session = Depends(self.db_func),
                filtering: dict = Depends(filter_spec),
                sorting: dict = Depends(sort_spec),
            ) -> Page[Model]:
                query = db.query(self.db_model)

                if filtering:
                    query = apply_filters(query, filtering)

                if sorting:
                    query = apply_sort(query, sorting)

                return paginate(query)  # type: ignore

        else:

            def route(
                db: Session = Depends(self.db_func),
                filtering: dict = Depends(filter_spec),
                sorting: dict = Depends(sort_spec),
            ) -> List[Model]:
                query = db.query(self.db_model)

                if filtering:
                    query = apply_filters(query, filtering)

                if sorting:
                    query = apply_sort(query, sorting)

                return query.all()

        return route

    def _get_one(self, *args: Any, **kwargs: Any) -> CALLABLE:
        def route(db: Session = Depends(self.db_func), item_id: self._pk_type = Path(..., alias=self.path_param_name)) -> Model:  # type: ignore
            model: Model = db.query(self.db_model).get(item_id)

            if model:
                return model
            else:
                raise NOT_FOUND from None

        return route

    def _create(self, *args: Any, **kwargs: Any) -> CALLABLE:
        def route(
            model: self.create_schema,  # type: ignore
            db: Session = Depends(self.db_func),
        ) -> Model:
            try:
                db_model: Model = self.db_model(**model.dict())
                db.add(db_model)
                db.commit()
                db.refresh(db_model)
                return db_model
            except IntegrityError:
                db.rollback()
                raise HTTPException(422, "Key already exists") from None

        return route

    def _update(self, *args: Any, **kwargs: Any) -> CALLABLE:
        def route(
            model: self.update_schema,  # type: ignore
            item_id: self._pk_type = Path(..., alias=self.path_param_name),  # type: ignore
            db: Session = Depends(self.db_func),
        ) -> Model:
            try:
                db_model: Model = self._get_one()(db=db, item_id=item_id)

                for key, value in model.dict(exclude={self._pk}).items():
                    if hasattr(db_model, key):
                        setattr(db_model, key, value)

                db.commit()
                db.refresh(db_model)

                return db_model
            except IntegrityError as e:
                db.rollback()
                self._raise(e)

        return route

    def _delete_all(self, *args: Any, **kwargs: Any) -> CALLABLE_LIST:
        def route(db: Session = Depends(self.db_func)) -> None:
            db.query(self.db_model).delete()
            db.commit()

        return route

    def _delete_one(self, *args: Any, **kwargs: Any) -> CALLABLE:
        def route(item_id: self._pk_type = Path(..., alias=self.path_param_name), db: Session = Depends(self.db_func)) -> Model:  # type: ignore
            db_model: Model = self._get_one()(db=db, item_id=item_id)
            db.delete(db_model)
            db.commit()

            return db_model

        return route
