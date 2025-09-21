from typing import Any

from pydantic import BaseModel


class Message(BaseModel):
    id: str
    type: str
    payload: dict[str, Any]
