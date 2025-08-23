from abc import ABC, abstractmethod
from typing import override
from uuid import UUID

import aiohttp


class Queries(ABC):
    @abstractmethod
    async def check_user_subscription(self, user_id: UUID) -> bool: ...

    @abstractmethod
    async def validate_topic_creation(self, name: str, description: str) -> bool: ...


class MockQueries(Queries):
    @override
    async def check_user_subscription(self, user_id: UUID) -> bool:
        return True

    @override
    async def validate_topic_creation(self, name: str, description: str) -> bool:
        return True


class HTTPQueries(Queries):
    @override
    async def check_user_subscription(self, user_id: UUID) -> bool:
        url = f"/subscriptions/{user_id}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()

    @override
    async def validate_topic_creation(self, name: str, description: str) -> bool:
        return True
