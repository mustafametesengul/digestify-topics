from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import select

from digestify_topics.auth import Auth
from digestify_topics.auth import fake_get_auth as get_auth
from digestify_topics.db import AsyncSession, get_session
from digestify_topics.models import Outbox, Topic, User
from digestify_topics.queries import MockQueries as HTTPQueries
from digestify_topics.queries import Queries
from digestify_topics.schemas import TopicCreated, TopicDeleted

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
) -> None:
    user = (
        await session.exec(select(User).where(User.id == auth.id).with_for_update())
    ).one_or_none()
    if user is None:
        user = User(id=auth.id)
        session.add(user)

    if user.discarded:
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
    session.add(topic)

    topic.increment_version()

    message = Outbox.from_payload(
        TopicCreated(topic_id=topic.id, user_id=user.id),
        entity="topic",
        version=topic.version,
    )
    session.add(message)

    await session.commit()


@router.get("/topics/{topic_id}", response_model=Topic)
async def get_topic_by_id(
    topic_id: UUID,
    auth: Annotated[Auth, Depends(get_auth)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> Topic:
    topic = (
        await session.exec(select(Topic).where(Topic.id == topic_id))
    ).one_or_none()
    if (
        topic is None
        or topic.discarded
        or (topic.user_id != auth.id and not topic.is_public)
    ):
        raise HTTPException(status_code=404, detail="Topic not found")

    return topic


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

    message = Outbox.from_payload(
        TopicDeleted(topic_id=topic.id, user_id=user.id),
        entity="topic",
        version=topic.version,
    )
    session.add(message)

    await session.commit()


@router.get("/users/{user_id}", response_model=User)
async def get_user_by_id(
    user_id: UUID,
    auth: Annotated[Auth, Depends(get_auth)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> User:
    user = (await session.exec(select(User).where(User.id == user_id))).one_or_none()
    if user is None or user.discarded or user.id != auth.id:
        raise HTTPException(status_code=404, detail="User not found")

    return user
