# Development

Install dependencies:
```
uv sync
```

Sort imports:
```
uv run ruff check --select I --fix
```

Format code:
```
uv run ruff format
```

Generate migrations:
```
uv run alembic revision --autogenerate -m "Create initial tables"

docker compose run --rm alembic revision --autogenerate -m "Create initial tables"

```

Note: When using Docker, migration scripts are written to your local repository under `./alembic/versions` (bind-mounted into the container). You can edit them locally and commit as usual.

Common Docker-based Alembic commands:
```
# Create a new revision
docker compose run --rm alembic revision --autogenerate -m "message"

# Apply all migrations
docker compose run --rm alembic upgrade head

# Roll back one migration
docker compose run --rm alembic downgrade -1

# See current heads/history
docker compose run --rm alembic heads
docker compose run --rm alembic history
```

Apply all migrations:
```
uv run alembic upgrade head
```

Reset DB:
```
uv run alembic downgrade base
```

Run tests:
```
uv run pytest
```

Run the web API:
```
uv run fastapi dev src/digestify_topics/app.py
```

Environment variables:
```
POSTGRES_HOST="db"
POSTGRES_PORT="5432"
POSTGRES_USER="user"
POSTGRES_PASSWORD="password"
POSTGRES_DB="db"
REDIS_HOST="redis"
REDIS_PORT="6379"
REDIS_PASSWORD="password"
JWKS_URL="https://project.supabase.co/auth/v1/.well-known/jwks.json"
OPENAI_API_KEY="your_openai_api_key"
DEBUG="True"
```
