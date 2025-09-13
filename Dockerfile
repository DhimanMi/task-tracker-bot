FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml poetry.lock* /app/
# если не используешь poetry — меняй под pip
RUN pip install --no-cache-dir aiogram aiosqlite rapidfuzz google-api-python-client google-auth-httplib2 google-auth-oauthlib

COPY . /app

ENV PYTHONUNBUFFERED=1

CMD ["python", "bot.py"]