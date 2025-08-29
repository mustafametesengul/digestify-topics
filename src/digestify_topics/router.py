from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import select

from digestify_topics.auth import Auth, get_auth
from digestify_topics.db import AsyncSession, get_session
from digestify_topics.messages import TopicCreated, TopicDeleted
from digestify_topics.models import OutboxMessage, Topic, User
from digestify_topics.queries import HTTPQueries, Queries
from digestify_topics.schemas import TopicRespone, TopicsResponse, UserResponse

router = APIRouter()


@router.post("/topics", status_code=201)
async def create_topic(
    name: str,
    description: str,
    is_public: bool,
    locale: str,
    image_uri: str | None,
    auth: Annotated[Auth, Depends(get_auth)],
    session: Annotated[AsyncSession, Depends(get_session)],
    queries: Annotated[Queries, Depends(HTTPQueries)],
) -> TopicRespone:
    user = (
        await session.exec(select(User).where(User.id == auth.id).with_for_update())
    ).one_or_none()
    if user is None or user.discarded:
        raise HTTPException(status_code=404, detail="User not found")

    user.created_topic_count += 1
    user.increment_version()

    used_subscribed = await queries.check_user_subscription(user.id)
    if not used_subscribed and user.created_topic_count >= 5:
        raise HTTPException(
            status_code=403,
            detail="User is not subscribed and has already created 5 topics",
        )

    is_safe = await queries.validate_topic_creation(name, description)
    if not is_safe:
        raise HTTPException(status_code=400, detail="Topic creation is not safe")

    topic = Topic(
        name=name,
        description=description,
        is_public=is_public,
        locale=str(locale),
        image_uri=image_uri,
        user_id=user.id,
    )
    topic.increment_version()
    session.add(topic)

    message = OutboxMessage.from_payload(
        TopicCreated(topic_id=topic.id, user_id=user.id),
        entity="topic",
        version=topic.version,
    )
    session.add(message)

    await session.commit()

    await session.refresh(topic)
    return TopicRespone.model_validate(topic.model_dump())


@router.get("/topics/{topic_id}")
async def get_topic_by_id(
    topic_id: UUID,
    auth: Annotated[Auth, Depends(get_auth)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> TopicRespone:
    topic = (
        await session.exec(select(Topic).where(Topic.id == topic_id))
    ).one_or_none()
    if (
        topic is None
        or topic.discarded
        or (topic.user_id != auth.id and not topic.is_public)
    ):
        raise HTTPException(status_code=404, detail="Topic not found")

    return TopicRespone.model_validate(topic.model_dump())


@router.delete("/topics/{topic_id}", status_code=204)
async def delete_topic(
    topic_id: UUID,
    auth: Annotated[Auth, Depends(get_auth)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> None:
    topic = (
        await session.exec(select(Topic).where(Topic.id == topic_id).with_for_update())
    ).one_or_none()
    if topic is None or topic.discarded or topic.user_id != auth.id:
        raise HTTPException(status_code=404, detail="Topic not found")

    user = (
        await session.exec(
            select(User).where(User.id == topic.user_id).with_for_update()
        )
    ).one_or_none()
    if user is None or user.discarded:
        raise HTTPException(status_code=404, detail="User not found")

    user.created_topic_count -= 1
    user.increment_version()

    topic.discarded = True
    topic.increment_version()

    message = OutboxMessage.from_payload(
        TopicDeleted(topic_id=topic.id, user_id=user.id),
        entity="topic",
        version=topic.version,
    )
    session.add(message)

    await session.commit()


@router.get("/my_topics")
async def get_my_topics(
    auth: Annotated[Auth, Depends(get_auth)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> TopicsResponse:
    user = (await session.exec(select(User).where(User.id == auth.id))).one_or_none()
    if user is None or user.discarded:
        raise HTTPException(status_code=404, detail="User not found")
    topics = (await session.exec(select(Topic).where(Topic.user_id == auth.id))).all()
    topics = list(filter(lambda t: not t.discarded, topics))
    topic_responses = [
        TopicRespone.model_validate(topic.model_dump()) for topic in topics
    ]
    return TopicsResponse(topics=topic_responses)


@router.get("/me")
async def get_my_user(
    auth: Annotated[Auth, Depends(get_auth)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> UserResponse:
    user = (await session.exec(select(User).where(User.id == auth.id))).one_or_none()
    if user is None or user.discarded:
        raise HTTPException(status_code=404, detail="User not found")
    return UserResponse.model_validate(user.model_dump())


@router.post("/me")
async def create_my_user(
    auth: Annotated[Auth, Depends(get_auth)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> UserResponse:
    user = (
        await session.exec(select(User).where(User.id == auth.id).with_for_update())
    ).one_or_none()
    if user is not None and not user.discarded:
        raise HTTPException(status_code=400, detail="User already exists")

    if user is None:
        user = User(id=auth.id)
        session.add(user)
    else:
        user.discarded = False
    user.increment_version()

    await session.commit()
    await session.refresh(user)

    return UserResponse.model_validate(user.model_dump())
