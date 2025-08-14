from typing import Annotated
from uuid import UUID

import jwt
from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from digestify_topics.settings import get_settings


class Auth(BaseModel):
    id: UUID
    is_anonymous: bool


_security = HTTPBearer()


def decode_token(token: str) -> dict:
    settings = get_settings()
    decoded_token = jwt.decode(
        token,
        settings.jwt_key,
        algorithms=["HS256"],
        audience="authenticated",
    )
    return decoded_token


def get_auth(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(_security)],
) -> Auth:
    token = credentials.credentials
    decoded_token = decode_token(token)
    user_id = UUID(decoded_token["sub"])
    is_anonymous = bool(decoded_token.get("is_anonymous", False))
    auth = Auth(id=user_id, is_anonymous=is_anonymous)
    return auth
