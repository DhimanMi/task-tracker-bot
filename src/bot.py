import asyncio
import logging
import datetime
import traceback
from typing import Optional, Any
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from config import BOT_TOKEN, DB_PATH, GOOGLE_SA_FILE, SHEET_ID, ADMIN_IDS
from db import DB
from search import find_similar_titles
from google_sheets import export_tasks_to_sheet  # должен вернуть либо URL, либо {'url':..., 'tab':...}

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
db = DB(DB_PATH)

CATEGORIES = ["development", "testing", "analytics", "other"]
CATEGORY_RU = {
    "development": "Разработка",
    "testing": "Тестирование",
    "analytics": "Аналитика",
    "other": "Другое"
}

# ===== FSM =====
class AddTaskStates(StatesGroup):
    waiting_for_category = State()
    waiting_for_title = State()
    waiting_for_description = State()

# ===== Helpers =====
def is_back(text: str) -> bool:
    return text.strip() == "⬅️ Назад"

def main_menu(user_id=None):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("➕ Добавить задачу")
    kb.add("📋 Мои задачи", "📊 Статистика")
    kb.add("🔍 Поиск", "📤 Экспорт в Google Sheets")
    if user_id in ADMIN_IDS:
        kb.add("⚙️ Админка")
    return kb

def categories_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    for c in CATEGORIES:
        kb.add(CATEGORY_RU[c])
    kb.add("⬅️ Назад")
    return kb

async def safe_send(user_id: int, text: str, **kwargs: Any):
    """Отправка сообщения пользователю с логированием ошибок — безопаснее вызывать внутри фоновых тасков."""
    try:
        await bot.send_message(user_id, text, **kwargs)
    except Exception:
        logging.exception("Не удалось отправить сообщение пользователю %s", user_id)

# ===== Start =====
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await db.init()
    await db.ensure_user(message.from_user.id, message.from_user.username)
    await message.reply(
        "Привет! Выберите действие:",
        reply_markup=main_menu(message.from_user.id)
    )

# ===== Add Task =====
@dp.message_handler(lambda m: m.text == "➕ Добавить задачу")
async def add_task_start(message: types.Message):
    await message.reply("Выберите категорию:", reply_markup=categories_keyboard())
    await AddTaskStates.waiting_for_category.set()

@dp.message_handler(state=AddTaskStates.waiting_for_category)
async def add_task_category(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if is_back(text):
        await state.finish()
        await message.reply("Возврат в главное меню.", reply_markup=main_menu(message.from_user.id))
        return

    # Проверяем выбранную категорию по русскому названию
    category = next((k for k, v in CATEGORY_RU.items() if v == text), None)
    if not category:
        await message.reply("Неизвестная категория. Выберите из списка.")
        return

    await state.update_data(category=category)

    # Кнопка назад к выбору категории при вводе заголовка
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("⬅️ Назад")
    await message.reply("Введите название задачи:", reply_markup=kb)
    await AddTaskStates.waiting_for_title.set()

@dp.message_handler(state=AddTaskStates.waiting_for_title)
async def add_task_title(message: types.Message, state: FSMContext):
    if is_back(message.text):
        # вернуться к самому началу добавления
        await add_task_start(message)
        return

    await state.update_data(title=message.text.strip())

    # Кнопка оставить пустым под сообщением
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Оставить пустым", callback_data="desc_empty"))
    await message.reply("Введите описание (можно оставить пустым):", reply_markup=kb)
    await AddTaskStates.waiting_for_description.set()

@dp.callback_query_handler(lambda c: c.data == "desc_empty", state=AddTaskStates.waiting_for_description)
async def desc_empty_callback(callback: types.CallbackQuery, state: FSMContext):
    try:
        data = await state.get_data()
        await db.add_task(callback.from_user.id, data["title"], data["category"], "")
        await bot.edit_message_reply_markup(callback.from_user.id, callback.message.message_id, reply_markup=None)
        await bot.send_message(
            callback.from_user.id,
            f"Задача '{data['title']}' добавлена в категорию '{CATEGORY_RU.get(data['category'], data['category'])}'.",
            reply_markup=main_menu(callback.from_user.id)
        )
    except Exception:
        logging.exception("Ошибка при добавлении задачи через callback desc_empty")
        await bot.send_message(callback.from_user.id, "Ошибка при добавлении задачи. Попробуйте ещё раз.", reply_markup=main_menu(callback.from_user.id))
    finally:
        await state.finish()
        await callback.answer()

@dp.message_handler(state=AddTaskStates.waiting_for_description)
async def add_task_desc(message: types.Message, state: FSMContext):
    if is_back(message.text):
        # вернуться к вводу заголовка
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add("⬅️ Назад")
        await message.reply("Введите название задачи:", reply_markup=kb)
        await AddTaskStates.waiting_for_title.set()
        return

    try:
        data = await state.get_data()
        desc = message.text.strip()
        await db.add_task(message.from_user.id, data["title"], data["category"], desc)
        await message.reply(
            f"Задача '{data['title']}' добавлена в категорию '{CATEGORY_RU.get(data['category'], data['category'])}'.",
            reply_markup=main_menu(message.from_user.id)
        )
    except Exception:
        logging.exception("Ошибка при добавлении задачи")
        await message.reply("Ошибка при добавлении задачи. Попробуйте ещё раз.", reply_markup=main_menu(message.from_user.id))
    finally:
        await state.finish()

# ===== List Tasks =====
@dp.message_handler(lambda m: m.text == "📋 Мои задачи")
async def list_tasks(message: types.Message):
    try:
        rows = await db.list_tasks(message.from_user.id)
    except Exception:
        logging.exception("Ошибка при получении задач пользователя %s", message.from_user.id)
        await message.reply("Не удалось получить список задач. Попробуйте позже.", reply_markup=main_menu(message.from_user.id))
        return

    if not rows:
        await message.reply("У вас пока нет задач.", reply_markup=main_menu(message.from_user.id))
        return
    for r in rows:
        task_id, title, desc, category, status, created = r
        created_str = datetime.datetime.fromisoformat(created).strftime("%m/%d/%Y")
        text = (
            f"#{task_id} — {title}\n"
            f"Категория: {CATEGORY_RU.get(category, category)}\n"
            f"Статус: {'Готово' if status=='done' else 'Открыто'}\n"
            f"Создано: {created_str}\n"
            f"{desc or ''}"
        )
        kb = types.InlineKeyboardMarkup()
        if status != "done":
            kb.add(types.InlineKeyboardButton("✅ Закрыть", callback_data=f"close_{task_id}"))
        kb.add(types.InlineKeyboardButton("❌ Удалить", callback_data=f"delete_{task_id}"))
        await message.reply(text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("close_") or c.data.startswith("delete_"))
async def task_action(callback: types.CallbackQuery):
    try:
        action, task_id = callback.data.split("_", 1)
        task_id = int(task_id)
        if action == "close":
            await db.close_task(task_id, callback.from_user.id)
            await callback.message.edit_text(f"✅ Задача #{task_id} закрыта.")
        elif action == "delete":
            await db.delete_task(task_id, callback.from_user.id)
            await callback.message.edit_text(f"🗑️ Задача #{task_id} удалена.")
    except Exception:
        logging.exception("Ошибка при действии над задачей %s", callback.data)
        await callback.message.edit_text("Ошибка при обработке операции с задачей.")
    finally:
        await callback.answer()

# ===== Stats =====
@dp.message_handler(lambda m: m.text == "📊 Статистика")
async def stats(message: types.Message):
    try:
        rows = await db.stats_by_category(message.from_user.id)
    except Exception:
        logging.exception("Ошибка при получении статистики для %s", message.from_user.id)
        await message.reply("Не удалось получить статистику. Попробуйте позже.", reply_markup=main_menu(message.from_user.id))
        return

    if not rows:
        await message.reply("Нет задач для статистики.", reply_markup=main_menu(message.from_user.id))
        return
    text = "Статистика по категориям:\n"
    for r in rows:
        text += f"{CATEGORY_RU.get(r[0], r[0])}: {r[1]}\n"
    await message.reply(text, reply_markup=main_menu(message.from_user.id))

# ===== Admin =====
@dp.message_handler(lambda m: m.text == "⚙️ Админка")
async def admin_menu(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("Только админ может видеть это меню.")
        return
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("Отключить экспорт", "Включить экспорт")
    kb.add("⬅️ Назад")
    await message.reply("Меню администратора:", reply_markup=kb)

@dp.message_handler(lambda m: m.text in ["Отключить экспорт", "Включить экспорт"])
async def admin_toggle_export(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        if message.text == "Отключить экспорт":
            await db.set_setting("export_enabled", "0")
            await message.reply("Экспорт отключён для всех пользователей.", reply_markup=main_menu(message.from_user.id))
        else:
            await db.set_setting("export_enabled", "1")
            await message.reply("Экспорт включён для всех пользователей.", reply_markup=main_menu(message.from_user.id))
    except Exception:
        logging.exception("Ошибка при переключении настройки экспорта")
        await message.reply("Не удалось изменить настройку. Проверьте логи.", reply_markup=main_menu(message.from_user.id))

# ===== Search =====
@dp.message_handler(lambda m: m.text == "🔍 Поиск")
async def search_request(message: types.Message):
    await message.reply("Введите текст для поиска:", reply_markup=types.ReplyKeyboardRemove())

@dp.message_handler(lambda m: m.text not in ["➕ Добавить задачу","📋 Мои задачи","📊 Статистика","🔍","📤 Экспорт в Google Sheets"])
async def search_process(message: types.Message):
    q = message.text.strip()
    if not q:
        return
    try:
        tasks = await db.search_tasks(message.from_user.id, q)
    except Exception:
        logging.exception("Ошибка при поиске в БД для пользователя %s", message.from_user.id)
        await message.reply("Ошибка при поиске. Попробуйте позже.", reply_markup=main_menu(message.from_user.id))
        return

    exact = [r for r in tasks if r[1].lower() == q.lower()]
    if exact:
        r = exact[0]
        await message.reply(
            f"Найдена задача: #{r[0]} — {r[1]}\n{r[2]}",
            reply_markup=main_menu(message.from_user.id)
        )
        return

    try:
        # Для простого ТЗ достаточно fuzzy поиска через find_similar_titles.
        # В проде лучше использовать полнотекстовый поиск (pg_trgm / elastic).
        matches = find_similar_titles(q, tasks, limit=5, score_cutoff=60)
    except Exception:
        logging.exception("Ошибка при работе с find_similar_titles")
        await message.reply("Ошибка при обработке запроса поиска.", reply_markup=main_menu(message.from_user.id))
        return

    if not matches:
        await message.reply("Ничего не найдено.", reply_markup=main_menu(message.from_user.id))
        return

    text = "Похожие варианты:\n"
    for row, score in matches:
        text += f"#{row[0]} — {row[1]} (score {score})\n"
    await message.reply(text, reply_markup=main_menu(message.from_user.id))

# ===== Export =====
async def export_worker(user_id: int, username: str, tasks: list):
    """
    Фоновая задача экспорта.
    В проде: вынести в отдельный воркер (Celery / RQ) с Redis/Broker — чтобы не блокировать процесс и не терять задания при рестарте.
    Здесь: запускаем в event loop и используем to_thread для вызова blocking-IO кода.
    """
    try:
        # Предполагаем, что export_tasks_to_sheet возвращает:
        # - строку с URL, или
        # - dict {'url': ..., 'tab': ...}, или
        # - {'gid': ...} / {'tab_name': ...}
        result = await asyncio.to_thread(export_tasks_to_sheet, GOOGLE_SA_FILE, SHEET_ID, tasks, username)
        sheet_url: Optional[str] = None
        extra_info = None

        if isinstance(result, str):
            sheet_url = result
        elif isinstance(result, dict):
            # попытка извлечь URL или построить ссылку
            sheet_url = result.get("url")
            extra_info = result.get("tab") or result.get("gid") or result.get("tab_name")
            if not sheet_url and SHEET_ID:
                # если вернули gid или tab_name, соберём базовую ссылку на SHEET_ID
                sheet_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit"
                if extra_info and str(extra_info).isdigit():
                    sheet_url += f"#gid={extra_info}"

        # уведомляем пользователя о результате
        if sheet_url:
            text = f"✅ Экспорт завершён. Открыть таблицу: {sheet_url}"
            if extra_info:
                text += f"\nВкладка: {extra_info}"
            await safe_send(user_id, text, reply_markup=main_menu(user_id))
        else:
            # если ничего не удалось извлечь — просто уведомим
            await safe_send(user_id, "✅ Экспорт завершён, но ссылка не получена. Проверьте Google Sheets.", reply_markup=main_menu(user_id))

    except Exception as e:
        logging.exception("Ошибка при экспорте задач для пользователя %s: %s", user_id, traceback.format_exc())
        await safe_send(user_id, f"❌ Ошибка при экспорте: {e}. Проверьте логи.", reply_markup=main_menu(user_id))

@dp.message_handler(lambda m: m.text == "📤 Экспорт в Google Sheets")
async def export_tasks(message: types.Message):
    try:
        export_enabled = await db.get_setting("export_enabled")
    except Exception:
        logging.exception("Ошибка при чтении настройки export_enabled")
        await message.reply("Не удалось прочитать настройку экспорта. Попробуйте позже.", reply_markup=main_menu(message.from_user.id))
        return

    if export_enabled == "0":
        await message.reply("Экспорт отключён администратором.", reply_markup=main_menu(message.from_user.id))
        return

    try:
        tasks = await db.get_all_tasks_for_user(message.from_user.id)
    except Exception:
        logging.exception("Ошибка при получении задач для экспорта пользователя %s", message.from_user.id)
        await message.reply("Не удалось получить задачи для экспорта. Попробуйте позже.", reply_markup=main_menu(message.from_user.id))
        return

    if not tasks:
        await message.reply("Нет задач для экспорта.", reply_markup=main_menu(message.from_user.id))
        return

    # запустим экспорт в фоне — пользователь получит уведомление, когда экспорт завершится с ссылкой
    await message.reply("Экспорт задач запущен в фоне. Я пришлю ссылку, когда всё будет готово.", reply_markup=main_menu(message.from_user.id))

    # в проде: запустить задание в очереди (Celery/RQ). Здесь — легковесный фон внутри процесса:
    asyncio.create_task(export_worker(message.from_user.id, message.from_user.username or str(message.from_user.id), tasks))

# ===== Main =====
if __name__ == "__main__":
    async def on_startup(_):
        await db.init()
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)