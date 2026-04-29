import asyncio
import html
import logging
import os
import re
import signal
import sys
import hashlib
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler

import aiosqlite
import feedparser
import httpx
import trafilatura
from aiogram import Bot, Dispatcher, Router, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

load_dotenv()

# ═══════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ═══════════════════════════════════════════════════════════

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "").strip()
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "google/gemma-4-26b-a4b:free").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
ADMIN_USER_IDS = list(map(int, os.getenv("ADMIN_USER_IDS", "").split(","))) if os.getenv("ADMIN_USER_IDS") else []

_raw_rss = os.getenv("RSS_URLS", "")
if _raw_rss:
    RSS_URLS = [u.strip() for u in _raw_rss.split(",") if u.strip()]
else:
    RSS_URLS = [
        "https://lenta.ru/rss/news",
        "https://ria.ru/export/rss2/archive/index.xml",
        "https://www.gazeta.ru/export/rss/index.xml",
        "https://www.rbc.ru/rss/all",
        "https://www.kommersant.ru/RSS/news.xml",
        "https://www.mk.ru/rss/index.xml",
        "https://vkurse.pro/feed/",
        "https://varlamov.ru/feed/",
        "https://habr.com/ru/rss/news/?fl=ru",
        "https://tjournal.ru/rss",
        "https://vc.ru/rss",
        "https://hightech.plus/rss.xml",
        "https://3dnews.ru/news/rss/",
        "https://kod.ru/feed/",
        "https://www.ixbt.com/export/news.rss",
        "https://www.theverge.com/rss/index.xml",
        "https://techcrunch.com/feed/",
        "https://www.wired.com/feed/rss",
        "https://feeds.bbci.co.uk/news/rss.xml",
        "https://www.reutersagency.com/feed/?best-topics=bus",
        "https://nypost.com/feed/",
        "https://www.thesun.co.uk/feed/",
        "https://nplus1.ru/rss",
        "https://naked-science.ru/feed/",
        "https://rss.stopgame.ru/rss_news.xml",
        "https://dtf.ru/rss/all",
        "https://www.popmech.ru/rss/",
        "https://forklog.com/feed/",
        "https://bits.media/rss/news/",
        "https://cointelegraph.com/rss",
    ]

DB_PATH = os.getenv("DB_PATH", "data/bot.db")
LOGS_DIR = os.getenv("LOGS_DIR", "logs")
CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", "10"))
MAX_POSTS_PER_CHECK = int(os.getenv("MAX_POSTS_PER_CHECK", "5"))
MAX_DB_AGE_DAYS = int(os.getenv("MAX_DB_AGE_DAYS", "90"))

SYSTEM_INSTRUCTION = (
    "Ты — дерзкий автор Telegram-канала 'Открытый мир: Новости'. Твоя цель — виральность.\n"
    "ЭТАП 1 (Фильтрация): Проанализируй текст. Если новость скучная (отчеты, официальные совещания, мелкие события без хайпа) — выведи только одно слово 'SKIP'.\n"
    "ЭТАП 2 (Рерайт): Если новость 'заряженная' (деньги, запреты, ИИ, шок-наука, абсурд), напиши пост:\n"
    "Стиль: Саркастичный, с использованием сленга (база, кринж, жесть).\n"
    "Структура: Яркий заголовок КАПСОМ -> Суть в 2-3 хлестких предложениях -> В конце оригинальный авторский панч или вопрос.\n"
    "Ограничения: СТРОГО до 4-5 предложений. Никаких ссылок на источники.\n"
    "Язык: Если оригинал на английском — сделай адаптированный перевод в том же стиле."
)


# ═══════════════════════════════════════════════════════════
# ВАЛИДАЦИЯ КОНФИГУРАЦИИ
# ═══════════════════════════════════════════════════════════

def validate_config():
    """Проверяет обязательные переменные окружения перед запуском."""
    errors = []
    if not TELEGRAM_TOKEN:
        errors.append("TELEGRAM_TOKEN не задан")
    if not OPENROUTER_API_KEY:
        errors.append("OPENROUTER_API_KEY не задан")
    if not CHANNEL_ID:
        errors.append("CHANNEL_ID не задан")
    if not RSS_URLS:
        errors.append("RSS_URLS не задан и список по умолчанию пуст")

    if errors:
        for err in errors:
            print(f"[CRITICAL] {err}", file=sys.stderr)
        sys.exit(1)


# ═══════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ═══════════════════════════════════════════════════════════

def setup_logging() -> logging.Logger:
    """Настраивает логирование в stdout и ротируемый файл."""
    os.makedirs(LOGS_DIR, exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    # stdout handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    # file handler с ротацией (10 MB, max 5 файлов)
    file_handler = RotatingFileHandler(
        os.path.join(LOGS_DIR, "bot.log"),
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    # Логируем и ошибки apscheduler
    logging.getLogger("apscheduler").setLevel(logging.WARNING)

    return logger


logger = setup_logging()

# ═══════════════════════════════════════════════════════════
# ИНИЦИАЛИЗАЦИЯ TELEGRAM
# ═══════════════════════════════════════════════════════════

try:
    bot = Bot(
        token=TELEGRAM_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
except Exception as e:
    logger.critical(f"Не удалось инициализировать Telegram Bot: {e}")
    sys.exit(1)

dp = Dispatcher()
router = Router()
dp.include_router(router)

scheduler = AsyncIOScheduler()

# ═══════════════════════════════════════════════════════════
# БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════

async def init_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS published_news (
                url TEXT PRIMARY KEY,
                published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        # Индекс для ускорения cleanup
        await db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_published_at
            ON published_news(published_at)
            """
        )
        await db.commit()
    logger.info("База данных инициализирована.")


def get_title_hash(title: str) -> str:
    """Возвращает хеш заголовка для дедупликации новостей."""
    normalized = re.sub(r'[^\w\s]', '', title.lower()).strip()
    return hashlib.md5(normalized.encode('utf-8')).hexdigest()


async def is_url_published(url: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM published_news WHERE url = ?", (url,)
        )
        row = await cursor.fetchone()
        return row is not None


async def mark_url_published(url: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO published_news (url) VALUES (?)", (url,)
        )
        await db.commit()


async def cleanup_old_records():
    """Удаляет записи старше MAX_DB_AGE_DAYS дней."""
    cutoff = datetime.now() - timedelta(days=MAX_DB_AGE_DAYS)
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "DELETE FROM published_news WHERE published_at < ?",
            (cutoff.isoformat(),),
        )
        await db.commit()
        deleted = cursor.rowcount
    if deleted > 0:
        logger.info(f"Очистка БД: удалено {deleted} старых записей.")


async def get_stats() -> tuple:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM published_news")
        total = (await cursor.fetchone())[0]

        cursor = await db.execute(
            "SELECT COUNT(*) FROM published_news WHERE published_at >= date('now', 'start of day')"
        )
        today = (await cursor.fetchone())[0]
        return total, today


# ═══════════════════════════════════════════════════════════
# РАБОТА С ТЕКСТОМ И RSS
# ═══════════════════════════════════════════════════════════

def extract_full_text(url: str) -> str:
    """Извлекает полный текст статьи через trafilatura."""
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            text = trafilatura.extract(
                downloaded, include_comments=False, include_tables=False
            )
            if text:
                return text.strip()
    except Exception as e:
        logger.warning(f"Ошибка trafilatura для {url}: {e}")
    return ""


def get_image_url(entry) -> str | None:
    """Пытается найти URL картинки в RSS-записи."""
    # media:content
    if "media_content" in entry:
        for media in entry.media_content:
            if media.get("medium") == "image" or media.get("type", "").startswith("image"):
                return media.get("url")
            if "url" in media:
                return media["url"]

    # media:thumbnail
    if "media_thumbnail" in entry:
        thumbs = entry.media_thumbnail
        if thumbs:
            return thumbs[0].get("url")

    # enclosures
    if "enclosures" in entry and entry.enclosures:
        for enc in entry.enclosures:
            if enc.get("type", "").startswith("image"):
                return enc.get("href")

    # summary / description — ищем img src
    html_content = entry.get("summary", "") + " " + entry.get("description", "")
    if html_content:
        match = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', html_content, re.IGNORECASE)
        if match:
            return match.group(1)

    return None


async def process_with_seed(text: str) -> str | None:
    """Отправляет текст в Seed-2.0-Pro через OpenRouter и возвращает результат."""
    if not OPENROUTER_API_KEY:
        return None
    try:
        max_chars = 24000
        if len(text) > max_chars:
            text = text[:max_chars] + "\n... [текст обрезан]"

        async with httpx.AsyncClient(timeout=90.0) as client:
            response = await client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://github.com/news_bot",
                    "X-Title": "News Bot",
                },
                json={
                    "model": OPENROUTER_MODEL,
                    "messages": [
                        {"role": "system", "content": SYSTEM_INSTRUCTION},
                        {"role": "user", "content": text},
                    ],
                    "temperature": 0.92,
                    "max_tokens": 3072,
                    "top_p": 0.95,
                    "frequency_penalty": 0.1,
                    "presence_penalty": 0.1,
                },
            )
            response.raise_for_status()
            data = response.json()
            result = data["choices"][0]["message"]["content"].strip()
            return result
    except Exception as e:
        logger.error(f"Ошибка Seed-2.0-Pro: {e}")
        return None


# ═══════════════════════════════════════════════════════════
# ОТПРАВКА В КАНАЛ
# ═══════════════════════════════════════════════════════════

async def send_news(url: str, text: str, image_url: str | None):
    """Отправляет новость в канал с картинкой или без."""
    MAX_CAPTION = 1024
    caption = text
    if len(caption) > MAX_CAPTION:
        caption = caption[: (MAX_CAPTION - 3)] + "..."

    try:
        if image_url:
            await bot.send_photo(
                chat_id=CHANNEL_ID,
                photo=image_url,
                caption=caption,
            )
            logger.info(f"Отправлено с фото: {url}")
        else:
            await bot.send_message(
                chat_id=CHANNEL_ID,
                text=text,
            )
            logger.info(f"Отправлено текстом: {url}")
    except Exception as e:
        logger.error(f"Ошибка отправки в Telegram для {url}: {e}")


# ═══════════════════════════════════════════════════════════
# ОСНОВНАЯ ЛОГИКА ПАРСИНГА
# ═══════════════════════════════════════════════════════════

async def check_rss():
    """Главная задача парсинга RSS, фильтрации и публикации."""
    logger.info("=== Запуск проверки RSS ===")
    posts_sent = 0
    processed_urls = set()

    # Параллельный парсинг всех RSS фидов
    async def process_single_feed(rss_url):
        try:
            feed = await asyncio.to_thread(feedparser.parse, rss_url)
            return feed.entries if feed.entries else []
        except Exception as e:
            logger.warning(f"Ошибка парсинга {rss_url}: {e}")
            return []

    # Запускаем парсинг всех источников одновременно
    tasks = [process_single_feed(rss_url) for rss_url in RSS_URLS]
    all_entries = await asyncio.gather(*tasks)
    
    # Объединяем все записи
    entries = []
    for feed_entries in all_entries:
        entries.extend(feed_entries)
    
    # Сортируем по дате (новые сверху)
    entries.sort(key=lambda e: e.get('published_parsed', (0,0,0)), reverse=True)

    for entry in entries:
        if posts_sent >= MAX_POSTS_PER_CHECK:
            break

        url = entry.get("link", "").strip()
        title = entry.get("title", "").strip()
        if not url:
            continue

        if await is_url_published(url):
            continue
        
        # Дедупликация новостей по заголовку
        title_hash = get_title_hash(title)
        if await is_url_published(title_hash):
            logger.info(f"Дубликат новости по заголовку, пропускаем: {url}")
            continue

        logger.info(f"Обрабатываем: {url}")

        article_text = extract_full_text(url)

        if not article_text:
            article_text = entry.get("summary", "") + "\n" + entry.get("description", "")
            article_text = html.unescape(article_text)
            article_text = article_text.replace("<br>", "\n").replace("<br/>", "\n")
            article_text = re.sub(r"<[^>]+>", "", article_text).strip()

        if not article_text or len(article_text) < 100:
            logger.info(f"Слишком короткий текст, пропускаем: {url}")
            await mark_url_published(url)
            await mark_url_published(title_hash)
            continue

        ai_result = await process_with_seed(article_text)

        if ai_result is None:
            # Fallback: если AI недоступен, отправляем оригинал
            logger.warning(f"AI недоступен для {url}, отправляем оригинал.")
            title = entry.get("title", "Без заголовка")
            fallback_text = f"<b>{html.escape(title)}</b>\n\n{url}"
            image_url = get_image_url(entry)
            await send_news(url, fallback_text, image_url)
            await mark_url_published(url)
            await mark_url_published(title_hash)
            posts_sent += 1
            await asyncio.sleep(2)
            continue

        if ai_result.upper() == "SKIP":
            logger.info(f"AI отфильтровал (SKIP): {url}")
            await mark_url_published(url)
            await mark_url_published(title_hash)
            continue

        image_url = get_image_url(entry)
        await send_news(url, ai_result, image_url)
        await mark_url_published(url)
        await mark_url_published(title_hash)
        posts_sent += 1

        await asyncio.sleep(2)

    logger.info(f"=== Проверка RSS завершена. Отправлено постов: {posts_sent} ===")
