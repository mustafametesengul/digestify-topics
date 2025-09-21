from sqlmodel.ext.asyncio.session import AsyncSession

from digestify_topics.messaging import MessageDispatcher
from digestify_topics.schemas import TopicCreated

STREAM_NAME = "digestify_topics"

dispatcher = MessageDispatcher(stream=STREAM_NAME)


@dispatcher.register()
async def index_topic(payload: TopicCreated, session: AsyncSession):
    print(f"This is a message: {payload}")
