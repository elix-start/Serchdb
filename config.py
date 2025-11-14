# config.py

# Полная строка Cookie (скопируйте из браузера DevTools → Headers → cookie)
COOKIE = "__ym"

# База данных SQLite (можно заменить на путь к MySQL/Postgres, если перепишете коннектор)
DB_PATH = "zin_cdz.db"

# Директория для сохранения HTML файлов
HTML_STORAGE_DIR = "html_files"

# Диапазон ID тестов
START_ID = 0
END_ID = 89999   # полный диапазон для скачивания всех тестов

# Задержка между запросами (секунды)
SLEEP_BETWEEN = 0.5

# User-Agent (можно заменить на свой, чтобы меньше палиться как бот)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"

# Токен Telegram бота (получите у @BotFather)
TELEGRAM_BOT_TOKEN = "xxxxxx"
