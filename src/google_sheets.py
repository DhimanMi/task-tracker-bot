import gspread
import datetime

CATEGORY_RU = {
    "development": "Разработка",
    "testing": "Тестирование",
    "analytics": "Аналитика",
    "other": "Другое"
}

def export_tasks_to_sheet(sa_file, sheet_id, tasks, username):
    """
    Экспорт задач пользователя в Google Sheet.
    Создаёт отдельную вкладку для username (если уже есть — использует её).
    Возвращает dict: {'url': <URL на таблицу>, 'tab': <имя вкладки>}
    """
    # Авторизация через современный gspread
    client = gspread.service_account(filename=sa_file)

    # Открываем таблицу
    sheet_doc = client.open_by_key(sheet_id)

    # Вкладка для пользователя
    tab_name = f"{username[:25]}"  # max 25 символов
    try:
        sheet = sheet_doc.worksheet(tab_name)
        sheet.clear()
    except gspread.WorksheetNotFound:
        sheet = sheet_doc.add_worksheet(title=tab_name, rows="100", cols="20")

    # Заголовки
    sheet.append_row(["id", "Заголовок", "Категория", "Описание", "Статус", "Создано"])

    # Данные
    for t in tasks:
        task_id, title, desc, category, status, created = t
        status_str = "Открыто" if status == "open" else "Готово"
        category_ru = CATEGORY_RU.get(category, category)
        created_str = datetime.datetime.fromisoformat(created).strftime("%m/%d/%Y")
        sheet.append_row([task_id, title, category_ru, desc, status_str, created_str])

    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
    return {"url": sheet_url, "tab": tab_name}