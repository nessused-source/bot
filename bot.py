import asyncio
import hashlib
import logging
import os
import random
import re
import urllib.parse
from datetime import datetime

import aiosqlite
import feedparser
import httpx
from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

load_dotenv()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# КОНФИГУРАЦИЯ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "").strip()
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "mistralai/mistral-7b-instruct:free").strip()
OPENROUTER_MODEL_FALLBACK = os.getenv("OPENROUTER_MODEL_FALLBACK", "").strip()
OPENROUTER_MODEL_FALLBACKS_ENV = os.getenv("OPENROUTER_MODEL_FALLBACKS", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
CHANNEL_LINK = os.getenv("CHANNEL_LINK", "").strip()
DB_PATH = os.getenv("DB_PATH", "news_bot.db").strip()
CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", "30").strip())
MAX_POSTS_PER_CHECK = int(os.getenv("MAX_POSTS_PER_CHECK", "5").strip())
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID", "").strip()

LOGS_DIR = os.getenv("LOGS_DIR", "").strip()

# RSS-ленты. Reddit через RSSHub для обхода блокировок.
RSS_URLS = [
    "https://lenta.ru/rss/news",
    "https://ria.ru/export/rss2/archive/index.xml",
    "https://www.gazeta.ru/rss/news.xml",
    "http://feeds.bbci.co.uk/news/world/rss.xml",
    # RSSHub — обходим 403 от Reddit
    "https://rsshub.app/reddit/r/worldnews",
    "https://rsshub.app/reddit/r/Unexpected",
    "https://naked-science.ru/rss.xml",
    "https://habr.com/ru/rss/all/top50/",
]

# ── подпись канала ─────────────────────────────────────────
_CHANNEL_LINK_TEXT = '<a href="https://t.me/world_news_gov">🌐 Мир без границ: Новости</a>'


def footer() -> str:
    if CHANNEL_LINK:
        return f'\n\n<a href="{CHANNEL_LINK}">🌐 Мир без границ: Новости</a>'
    return f"\n\n{_CHANNEL_LINK_TEXT}"


# Логирование
_handlers: list[logging.Handler] = [logging.StreamHandler()]
if LOGS_DIR:
    os.makedirs(LOGS_DIR, exist_ok=True)
    _handlers.append(logging.FileHandler(os.path.join(LOGS_DIR, "bot.log"), encoding="utf-8"))
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s", handlers=_handlers)


def _parse_model_list(raw: str) -> list[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    return [m.strip() for m in raw.split(",") if m.strip()]


def _validate_config() -> None:
    if not TELEGRAM_TOKEN:
        raise ValueError("TELEGRAM_TOKEN is not set")
    if not OPENROUTER_API_KEY:
        raise ValueError("OPENROUTER_API_KEY is not set")
    if not CHANNEL_ID:
        raise ValueError("CHANNEL_ID is not set")
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# БАЗА ДАННЫХ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS sent_news (hash TEXT PRIMARY KEY, dt DATETIME)")
        await db.commit()


async def is_new(news_hash):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM sent_news WHERE hash = ?", (news_hash,)) as cur:
            return await cur.fetchone() is None


async def mark_as_sent(news_hash):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO sent_news VALUES (?, ?)", (news_hash, datetime.now()))
        await db.commit()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ИЗВЛЕЧЕНИЕ КОНТЕНТА: Newspaper3k (онлайн, через newspaper3k)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def extract_article_text(url: str) -> str:
    """Синхронно запускает newspaper3k и возвращает чистый текст."""
    try:
        import newspaper
        from newspaper import Article
        article = Article(url, language="ru", fetch_images=False)
        article.download()
        article.parse()
        text = (article.text or "").strip()
        # Fallback на summary если текст слишком короткий
        if len(text) < 100:
            article.nlp()
            text = (article.summary or text).strip()
        return text
    except Exception as e:
        logging.warning(f"Newspaper3k failed for {url}: {e}")
        return ""


async def extract_article_text_async(url: str) -> str:
    """Асинхронная обёртка для newspaper3k."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, extract_article_text, url)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# OPENROUTER: генерация текста
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def _openrouter_chat(prompt: str, temperature: float = 0.8, max_tokens: int = 1024, timeout: float = 60.0) -> str:
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com",
    }
    fallback_models = _parse_model_list(OPENROUTER_MODEL_FALLBACKS_ENV)
    if not fallback_models and OPENROUTER_MODEL_FALLBACK:
        fallback_models = [OPENROUTER_MODEL_FALLBACK]
    hardcoded_free_models = [
        "openrouter/quasar-alpha",
        "openrouter/optimus-alpha",
    ]
    models_to_try: list[str] = []
    for m in [OPENROUTER_MODEL] + fallback_models:
        if m and m not in models_to_try:
            models_to_try.append(m)
    for m in hardcoded_free_models:
        if m and m not in models_to_try:
            models_to_try.append(m)

    async with httpx.AsyncClient() as client:
        last_err: Exception | None = None
        for model in models_to_try:
            try:
                resp = await client.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers=headers,
                    json={
                        "model": model,
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": temperature,
                        "max_tokens": max_tokens,
                    },
                    timeout=timeout,
                )
                resp.raise_for_status()
                return resp.json()["choices"][0]["message"]["content"].strip()
            except Exception as e:
                last_err = e
                logging.error(f"OpenRouter error (model={model}): {e}")
        raise RuntimeError(f"AI generation failed: {last_err}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AI-ВЫБОР ЛУЧШИХ НОВОСТЕЙ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def get_ai_best_candidates(candidates: list, target_count: int) -> list:
    if not candidates or len(candidates) <= target_count:
        return candidates
    lines = []
    for i, c in enumerate(candidates[:60], 1):
        domain = c["link"].split("/")[2] if "/" in c["link"] else "site"
        title = c["title"]
        lines.append(f"{i}. [{domain}] {title}")
    news_list_text = "\n".join(lines)
    prompt = (
        "Ты — профессиональный редактор Telegram-каналов со скандальной и социальной тематикой. "
        "Из списка ниже выбери самые \"мясистые\" новости — скандалы, курьёзы, криминал, "
        "необычные фобии, жёсткие соцситуации, технологические провалы. "
        "Ответь ТОЛЬКО номерами через запятую, без комментариев.\n\n"
        f"Нужно выбрать {target_count} лучших:\n{news_list_text}\n\n"
        f"Выбери {target_count} лучших номеров через запятую:"
    )
    try:
        raw = await _openrouter_chat(prompt, temperature=0.3, max_tokens=256, timeout=45.0)
        nums = [int(n) for n in re.findall(r"\d+", raw) if 1 <= int(n) <= len(candidates[:60])]
        if len(nums) >= target_count:
            return [candidates[int(n)-1] for n in nums[:target_count]]
    except Exception as e:
        logging.warning(f"AI pick failed: {e}")
    logging.warning("AI выбор не удался, использую случайный выбор.")
    return random.sample(candidates, target_count)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AI-РЕРАЙТ ПОСТА (стиль Топор / Топор+)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def get_ai_summary(text: str, title: str) -> str:
    if not OPENROUTER_API_KEY:
        raise RuntimeError("OPENROUTER_API_KEY is missing")
    text = (text or "").strip()[:12000]
    prompt = (
        "Ты — профессиональный грамотный редактор крупнейших Telegram-каналов с криминальной и социальной тематикой. "
        "Твоя задача — превращать сухие статьи в посты в стиле «Топор» и «Топор+».\n\n"
        "Инструкция:\n"
        "- Выбор инфоповода: Ищи самое «мясо» — скандалы, курьезы, криминал, необычные фобии, "
        "жесткие социальные ситуации или громкие технологические провалы.\n"
        "- Лид (первое предложение): Максимально емкое и цепляющее. Должно сразу бить в суть (кто, что сделал, какой итог).\n"
        "- Тело поста: 2-3 коротких абзаца. Простые предложения. Минимум прилагательных, максимум фактов и глаголов действия.\n"
        "- Тон: Прямой, циничный, разговорный, но без мата.\n"
        "- Никаких вводных слов: Убирай «согласно данным», «как стало известно», «эксперты полагают». Сразу к делу.\n"
        "- Жирный шрифт для ключевых имен или действий через <b>...</b>.\n\n"
        f"Заголовок оригинала (для ориентира): {title}\n\n"
        f"Исходный текст:\n{text}\n\n"
        "Жёсткие требования:\n"
        "- Объём: до 500 символов (включая пробелы).\n"
        "- Формат: только HTML-текст поста, без Markdown.\n"
        "- Не добавляй ссылок, URL, теги <a>, фразу «Источник».\n"
        "- Эмодзи только по делу, 1-3 штуки.\n"
        "- Следи за грамматикой: правильные окончания, падежи, спряжения.\n"
        "- Выводи ТОЛЬКО готовый текст поста, без дополнительных комментариев."
    )
    return await _openrouter_chat(prompt, temperature=0.8, max_tokens=1024)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AI-ПРОВЕРКА ОРФОГРАФИИ И ГРАММАТИКИ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def ai_proofread(text: str) -> str:
    """Отправляет текст в AI на корректуру орфографии и грамматики."""
    prompt = (
        "Отредактируй текст ниже: исправь орфографические ошибки, ошибки в окончаниях, падежах, спряжениях глаголов. "
        "Сохрани стиль, тон и форматирование. Не меняй структуру поста, только исправь ошибки.\n\n"
        f"Текст:\n{text}\n\n"
        "Выведи только исправленный текст, без пояснений."
    )
    try:
        corrected = await _openrouter_chat(prompt, temperature=0.2, max_tokens=1024)
        return corrected or text
    except Exception as e:
        logging.warning(f"AI proofread failed: {e}")
        return text


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ГЕНЕРАЦИЯ AI-ИЗОБРАЖЕНИЯ (Pollinations.ai)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ВАЛИДАЦИЯ HTML (Telegram parse_mode)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _fix_telegram_html(text: str) -> str:
    """Исправляет парсинг HTML: закрывает незакрытые теги, удаляет пустые."""
    # Заменяем HTML-переносы
    text = text.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    # Удаляем неподдерживаемые теги
    text = re.sub(r"</(?!b|i|u|s|a|code|pre|blockquote|span|strong|em)\w+[^>]*?>", "", text, flags=re.IGNORECASE)
    # Закрываем незакрытые <b> и <i> ... простая валидация
    for tag in ["b", "i", "u", "s"]:
        opens = len(re.findall(fr"<{tag}(?: [^>]*)?>", text, flags=re.IGNORECASE))
        closes = len(re.findall(fr"</{tag}>", text, flags=re.IGNORECASE))
        if opens > closes:
            text += f"</{tag}>" * (opens - closes)
        if closes > opens:
            # Удаляем лишние закрывающие
            text = re.sub(fr"(</{tag}>)(?!.*<(?:/|[^/]){tag})", "", text, count=closes - opens, flags=re.IGNORECASE)
    return text.strip()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ПАРСИНГ RSS И ПУБЛИКАЦИЯ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def check_news(bot: Bot):
    logging.info("🔍 Начинаю проверку RSS лент")
    candidates = []

    for url in RSS_URLS:
        try:
            feed = feedparser.parse(url)
            if not feed.entries:
                logging.warning(f"⚠️ Нет записей для {url}")
                continue
            for entry in feed.entries:
                news_link = entry.link.strip()
                news_hash = hashlib.md5(news_link.encode()).hexdigest()
                if not await is_new(news_hash):
                    continue

                # Извлекаем текст через Newspaper3k
                article_text = await extract_article_text_async(news_link)
                if not article_text or len(article_text) < 100:
                    article_text = (entry.get("summary") or entry.get("description") or "").strip()
                if not article_text or len(article_text) < 100:
                    continue

                # Источник из домена
                domain = news_link.split("/")[2] if "/" in news_link else ""

                candidates.append({
                    "link": news_link,
                    "hash": news_hash,
                    "title": (entry.title or "Новость").strip(),
                    "text": article_text,
                })
        except Exception as e:
            logging.error(f"❌ Ошибка при обработке {url}: {e}")

    logging.info(f"📊 Собрано {len(candidates)} новых новостей со всех RSS")

    if not candidates:
        logging.info("✅ Проверка завершена (нет новых новостей)")
        return

    # Выбираем ровно 5 лучших (или меньше если нет)
    target_count = min(MAX_POSTS_PER_CHECK, len(candidates))
    selected = await get_ai_best_candidates(candidates, target_count)

    posts_sent = 0
    for item in selected:
        try:
            title = item["title"]
            news_link = item["link"]
            news_hash = item["hash"]
            article_text = item["text"]

            # 1. AI-рерайт в стиле Топор
            raw_post = await get_ai_summary(article_text, title)
            # 2. AI-проверка грамматики
            final_post = await ai_proofread(raw_post)
            # 3. Валидация HTML
            final_post = _fix_telegram_html(final_post)
            final_post += footer()

            await bot.send_message(
                CHANNEL_ID,
                final_post,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            await mark_as_sent(news_hash)
            posts_sent += 1
            logging.info(f"✅ Опубликовано ({posts_sent}/{target_count}): {title[:50]}...")

            # Пауза между постами: 3–5 минут
            if posts_sent < target_count:
                delay = random.randint(180, 300)
                logging.info(f"⏳ Пауза {delay} сек перед следующим постом...")
                await asyncio.sleep(delay)
        except Exception as e:
            logging.error(f"❌ Ошибка при публикации: {e}")

    logging.info("✅ Проверка завершена. Опубликовано %s постов.", posts_sent)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# КОМАНДЫ ДЛЯ АДМИНА
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def is_admin(user_id: int | str | None) -> bool:
    if not ADMIN_USER_ID or user_id is None:
        return False
    return str(user_id) == ADMIN_USER_ID


async def cmd_start(message: types.Message):
    await message.answer(
        "Привет! Я бот для автоматической публикации новостей в канал.\n"
        f"Канал: {CHANNEL_LINK or _CHANNEL_LINK_TEXT}\n"
        "Используй /status для проверки состояния."
    )


async def cmd_status(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ У вас нет доступа к этой команде.")
        return
    await message.answer(
        f"✅ Бот работает.\n"
        f"Канал: {CHANNEL_ID}\n"
        f"Интервал проверки: {CHECK_INTERVAL_MINUTES} мин\n"
        f"Модель OpenRouter: {OPENROUTER_MODEL}\n"
        f"RSS-лент: {len(RSS_URLS)}"
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ЗАПУСК БОТА
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def main():
    _validate_config()
    await init_db()

    bot = Bot(
        token=TELEGRAM_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher()

    dp.message.register(cmd_start, F.text == "/start")
    dp.message.register(cmd_status, F.text == "/status")

    scheduler = AsyncIOScheduler()
    scheduler.add_job(check_news, "interval", minutes=CHECK_INTERVAL_MINUTES, args=[bot])
    scheduler.start()

    asyncio.create_task(check_news(bot))

    logging.info("🚀 Бот запущен")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("⏹️ Бот остановлен")
