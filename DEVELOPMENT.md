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
