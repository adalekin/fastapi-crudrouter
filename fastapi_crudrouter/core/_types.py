from typing import Dict, TypeVar, Optional, Sequence, Union

from fastapi.params import Depends
from pydantic import BaseModel

PYDANTIC_SCHEMA = BaseModel

T = TypeVar("T", bound=BaseModel)
DEPENDENCIES = Optional[Sequence[Depends]]
