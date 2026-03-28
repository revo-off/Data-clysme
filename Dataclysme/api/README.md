# Dataclysme API (JWT + Pagination)

## Start with Docker Compose

From the Dataclysme folder:

```bash
docker compose up -d --build api
```

The API is exposed on:

- http://localhost:8000
- Swagger docs: http://localhost:8000/docs

## Authentication

### 1) Login

POST /auth/login

Body:

```json
{
  "username": "admin",
  "password": "admin123"
}
```

You will receive:

```json
{
  "access_token": "...",
  "token_type": "bearer",
  "expires_in_minutes": 60
}
```

### 2) Use token

In Postman, set header:

- Authorization: Bearer <access_token>

## Endpoints

- GET /health
- GET /api/v1/datamarts (JWT required)
- GET /api/v1/datamarts/{datamart_name}?page=1&page_size=50 (JWT required)

Allowed datamarts:

- risks
- tourism
- agriculture

## Pagination

Parameters:

- page starts at 1
- page_size between 1 and 500

Response includes:

- current page
- page size
- total rows
- total pages

## Environment variables

The defaults are defined in docker-compose.yml, but you should override them in production:

- API_JWT_SECRET
- API_JWT_ALGORITHM
- API_TOKEN_EXPIRE_MINUTES
- API_USER
- API_PASSWORD
- DATABASE_URL

See .env.example for reference.
