from fastapi import FastAPI

from fastapi_crudrouter import MemoryCRUDRouter
from tests import Potato, Carrot, CarrotUpdate, CUSTOM_TAGS


def memory_implementation(**kwargs):
    app = FastAPI()

    router_settings = [
        dict(schema=Potato, pagination=True),
        dict(schema=Carrot, update_schema=CarrotUpdate, tags=CUSTOM_TAGS),
    ]

    return app, MemoryCRUDRouter, router_settings


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(memory_implementation(), port=5000)
