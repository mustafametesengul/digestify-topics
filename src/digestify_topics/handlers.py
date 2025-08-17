from sqlmodel.ext.asyncio.session import AsyncSession

from digestify_topics.outbox import MessageHandler
from digestify_topics.schemas import TopicCreated

handlers = MessageHandler()


@handlers.register()
async def index_topic(payload: TopicCreated, session: AsyncSession):
    pass
