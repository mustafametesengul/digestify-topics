from sqlmodel.ext.asyncio.session import AsyncSession

from digestify_topics.message_dispatcher import MessageDispatcher
from digestify_topics.messages import TopicCreated

dispatcher = MessageDispatcher(stream="digestify_topics")


@dispatcher.register()
async def index_topic(payload: TopicCreated, session: AsyncSession):
    print(f"This is a message: {payload}")
