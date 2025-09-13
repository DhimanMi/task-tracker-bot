# Telegram Task Tracker Bot

<p align="center">
  <img src="https://i.ibb.co/sV7MszH/task-bot-banner.png" alt="Telegram Task Tracker Banner" style="max-width: 100%; height: auto; border-radius: 8px;" />
</p>

## Установка

1. Клонируем репозиторий:
```bash
git clone https://github.com/yourusername/task-tracker-bot.git
cd task-tracker-bot

## Установка

1. Клонируем репозиторий:
```bash
git clone https://github.com/yourusername/task-tracker-bot.git
cd task-tracker-bot
````

2. Создаём `.env` на основе примера:

```bash
cp env.example .env
```

В `.env` указываем:

```
TELEGRAM_TOKEN=your_bot_token_here
ADMIN_ID=123456789
GOOGLE_SHEET_ID=your_google_sheet_id
```

3. Добавляем Google Service Account JSON:

```
./secrets/google_sa.json
```

⚠️ Не забудьте дать этому сервисному аккаунту доступ на редактирование таблицы в Google Sheets.

4. Запуск через Docker:

```bash
docker-compose up --build
```

После сборки бот автоматически запустится.

---

### Важная информация по экспорту в Google Sheets

Экспорт задач выполняется **асинхронно**, чтобы не блокировать основной поток обработки сообщений.
Если администратор отключил экспорт через `/toggle_export`, команда `/export` для пользователей будет недоступна.

Google Sheets API требует:

* сервисный аккаунт Google Cloud,
* JSON-ключ в папке `./secrets/`,
* доступ этого аккаунта к таблице по `GOOGLE_SHEET_ID`.

---

## Стек технологий

* Python 3.11
* aiogram
* SQLite
* Google Sheets API
* Docker / Docker Compose

---

## Основные команды

* `/start` – регистрация пользователя
* `/add` – добавить задачу (с выбором категории)
* `/list` – список задач
* `/stats` – статистика по категориям
* `/search <название>` – поиск задачи с учётом опечаток
* `/export` – выгрузка в Google Sheets
* `/toggle_export` – включить/выключить экспорт (только админ)

---

## Безопасность

* `.env` и `./secrets/` не должны попадать в Git.
* SQLite база (`tasks.db`) хранится в `./data/` и игнорируется в репозитории.
* Все ключи и токены подгружаются только через переменные окружения.
