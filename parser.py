import os
import re
import asyncio
import random
import sqlite3
import json
import time
import gc 
from datetime import datetime, timezone, timedelta
from pathlib import Path

from telethon import TelegramClient, events, Button
from telethon.errors import FloodWaitError, UserAlreadyParticipantError
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import User, UserStatusEmpty, UserStatusOffline
import socks
from openai import AsyncOpenAI

# ══════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════════════

API_ID = 6
API_HASH = "eb06d4abfb49dc3eeb1aeb98ae0f581e"
BOT_TOKEN = "8177768255:AAFJUXEx0jynaJz9frqGJvRJRwpcULDVRNw"
ADMIN_ID = 1568924415
OPENAI_API_KEY = "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"

DB_PATH = Path("leads.db")
SESSIONS_DIR = Path("sessions")
PROXIES_FILE = Path("proxies.txt")
BATCH_SIZE = 150  

openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

db_lock = asyncio.Lock()
ram_semaphore = asyncio.Semaphore(10) 

# ══════════════════════════════════════════════════════════════
# СЛОВАРИ (БЕЗ ИЗМЕНЕНИЙ)
# ══════════════════════════════════════════════════════════════

MINUS_WORDS = ["эстет", "эстетист", "эстетика", "аппаратная", "аппаратный", "аппаратка", "массаж", "чистка лица", "smas", "lpg", "rf-лифтинг", "лазер", "эпиляция", "маникюр", "ногти", "брови", "ресницы", "парикмахер", "визажист", "тату", "перманент", "шугаринг", "смм", "маркетолог", "таргетолог", "заработок", "крипта"]
PLUS_WORDS = ["косметолог", "cosmetolog", "врач", "dr.", "doctor", "инъекционист", "губы", "увеличение", "клиника", "filler", "ботокс", "токсин", "препарат", "биоревитализация", "мезотерапия", "прайс", "закупка"]

def hard_filter(text: str, username: str, bio: str) -> tuple:
    clean_text = text.lower().replace('a', 'а').replace('o', 'о').replace('e', 'е').replace('c', 'с').replace('p', 'р').replace('x', 'х')
    clean_bio = bio.lower()
    clean_user = username.lower()
    text_check = clean_text + " " + clean_user
    if any(m in text_check for m in MINUS_WORDS):
        return "TRASH", "Мусор по словарю"
    words = re.findall(r'\b\w+\b', clean_text)
    full_profile = f"{clean_text} {clean_user} {clean_bio}"
    if len(words) <= 5 or (len(words) <= 1 and any(char in text for char in ['+', '👍', '🔥', '❤️'])):
        if not any(p in full_profile for p in PLUS_WORDS):
            return "TRASH", "Флуд без признаков ЦА"
    return None, None

AI_PROMPT = """Ты — аналитик B2B-продаж. Ищи косметологов-инъекционистов.
Категории: HOT (прямой запрос), WARM (проф. обсуждение), TRASH (пациенты/мусор).
Ответь строго в JSON: {"thought_process": "...", "category": "HOT"}"""

async def get_ai_category(profile: dict) -> dict:
    try:
        prompt = AI_PROMPT + f"\nName: {profile['name']}, Bio: {profile['bio']}, Msg: {profile['messages']}"
        response = await openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.1
        )
        return json.loads(response.choices[0].message.content)
    except Exception: return {"category": "TRASH", "thought_process": "error"}

# ══════════════════════════════════════════════════════════════
# ЯДРО
# ══════════════════════════════════════════════════════════════

class State:
    queue = asyncio.Queue()
    is_running = False
    bot = None
    stop_event = asyncio.Event()
    waiting_for_links = False
    leads_session_total = 0
    leads_hot = 0
    leads_warm = 0

S = State()

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS leads (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER UNIQUE, username TEXT, real_name TEXT, bio TEXT, trigger_text TEXT, category TEXT, group_src TEXT, created_at INTEGER);
        CREATE TABLE IF NOT EXISTS seen (user_id INTEGER PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS bookmarks (link TEXT PRIMARY KEY, last_id INTEGER);
        CREATE TABLE IF NOT EXISTS user_bios (user_id INTEGER PRIMARY KEY, bio TEXT);
        """)

async def check_pulse():
    if S.leads_session_total > 0 and S.leads_session_total % 100 == 0:
        msg = f"💓 **ПУЛЬС ПАРСЕРА**\nСобрано: {S.leads_session_total}\nHOT: {S.leads_hot}, WARM: {S.leads_warm}"
        try: await S.bot.send_message(ADMIN_ID, msg)
        except: pass

async def process_user(client, user_obj, messages, group_link, acc_name):
    if not isinstance(user_obj, User) or user_obj.bot: return 
    uid = user_obj.id
    username = user_obj.username or ''
    name = f"{user_obj.first_name or ''} {user_obj.last_name or ''}".strip()
    trigger_text = messages[0] if messages else ""
    bio = ""
    async with db_lock:
        with sqlite3.connect(DB_PATH, timeout=30) as conn:
            cached = conn.execute("SELECT bio FROM user_bios WHERE user_id=?", (uid,)).fetchone()
    if cached: bio = cached[0]
    else:
        await asyncio.sleep(random.uniform(1, 2))
        try:
            full = await client(GetFullUserRequest(uid))
            bio = full.full_user.about or ""
            async with db_lock:
                with sqlite3.connect(DB_PATH, timeout=30) as conn:
                    conn.execute("INSERT OR REPLACE INTO user_bios VALUES (?, ?)", (uid, bio))
                    conn.commit()
        except: pass
    
    category, reason = hard_filter(trigger_text, username, bio)
    if category is None:
        res = await get_ai_category({"name": name, "username": username, "bio": bio, "messages": messages})
        category = res.get('category', 'TRASH').upper()
        reason = f"ИИ: {res.get('thought_process', '')[:100]}"

    if category in ['HOT', 'WARM']:
        async with db_lock:
            with sqlite3.connect(DB_PATH, timeout=30) as conn:
                try:
                    conn.execute("INSERT INTO leads VALUES (NULL,?,?,?,?,?,?,?,?)", (uid, username, name, bio, trigger_text, category, group_link, int(time.time())))
                    conn.commit()
                    S.leads_session_total += 1
                    if category == 'HOT': S.leads_hot += 1
                    else: S.leads_warm += 1
                except: pass 
        await check_pulse()

async def account_worker(name, session_path, proxy):
    client = TelegramClient(str(session_path), API_ID, API_HASH, proxy=proxy)
    await client.connect()
    if not await client.is_user_authorized(): return
    while not S.stop_event.is_set():
        try:
            link = await S.queue.get()
            async with ram_semaphore:
                last_id = 0
                async with db_lock:
                    with sqlite3.connect(DB_PATH, timeout=30) as conn:
                        row = conn.execute("SELECT last_id FROM bookmarks WHERE link=?", (link,)).fetchone()
                        if row: last_id = row[0]
                try:
                    entity = await client.get_entity(link)
                    if hasattr(entity, 'left') and entity.left:
                        await client(JoinChannelRequest(entity))
                except: entity = None
                if entity:
                    curr_id = last_id
                    async for msg in client.iter_messages(entity, limit=BATCH_SIZE, offset_id=last_id):
                        if S.stop_event.is_set(): break
                        curr_id = msg.id
                        if not msg.sender_id or not msg.text: continue
                        async with db_lock:
                            with sqlite3.connect(DB_PATH, timeout=30) as conn:
                                if conn.execute("SELECT 1 FROM seen WHERE user_id=?", (msg.sender_id,)).fetchone(): continue
                                conn.execute("INSERT OR IGNORE INTO seen VALUES (?)", (msg.sender_id,))
                                conn.commit()
                        if msg.sender: await process_user(client, msg.sender, [msg.text], link, name)
                    async with db_lock:
                        with sqlite3.connect(DB_PATH, timeout=30) as conn:
                            conn.execute("INSERT OR REPLACE INTO bookmarks VALUES (?, ?)", (link, curr_id))
                            conn.commit()
            gc.collect()
            await asyncio.sleep(60)
            S.queue.task_done()
            S.queue.put_nowait(link)
        except: await asyncio.sleep(10)

# ══════════════════════════════════════════════════════════════
# ИНТЕРФЕЙС
# ══════════════════════════════════════════════════════════════

def get_keyboard():
    return [[Button.text('🚀 Запуск'), Button.text('🛑 Стоп')], [Button.text('📦 Выгрузка'), Button.text('📊 Статистика')], [Button.text('➕ Добавить группы'), Button.text('♻️ Очистить базу')]]

async def export_txt(event):
    async with db_lock:
        with sqlite3.connect(DB_PATH, timeout=30) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM leads ORDER BY category ASC").fetchall()
    if not rows: return await event.reply("База пуста.", buttons=get_keyboard())
    path = "export_leads.txt"
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            contact = f"@{r['username']}" if r['username'] else str(r['user_id'])
            # ИСПРАВЛЕНО: Нет слэшей в f-строке
            clean_msg = str(r['trigger_text']).replace('\n', ' ')
            f.write(f"[{r['category']}] {contact} | {r['real_name']} | {clean_msg}\n")
    # ИСПРАВЛЕНО: Ровные отступы
    await event.reply(f"📦 Собрано {len(rows)} лидов", file=path, buttons=get_keyboard())
    if os.path.exists(path): os.remove(path)

def register_handlers(bot):
    @bot.on(events.NewMessage(pattern=re.compile(r'^(🚀 Запуск|/start)$', re.I)))
    async def _(e):
        if e.sender_id != ADMIN_ID: return
        S.waiting_for_links = False
        if S.is_running: return await e.reply("Работаю!", buttons=get_keyboard())
        S.stop_event.clear() 
        asyncio.create_task(run_main())
        await e.reply("🚀 ЗАПУЩЕНО!", buttons=get_keyboard())

    @bot.on(events.NewMessage(pattern=re.compile(r'^(🛑 Стоп|/stop)$', re.I)))
    async def _(e):
        if e.sender_id != ADMIN_ID: return
        S.stop_event.set()
        S.is_running = False
        await e.reply("🛑 ОСТАНОВЛЕНО.", buttons=get_keyboard())

    @bot.on(events.NewMessage(pattern=re.compile(r'^(📦 Выгрузка|/export)$', re.I)))
    async def _(e): await export_txt(e)
    
    @bot.on(events.NewMessage(pattern=re.compile(r'^(📊 Статистика|/stats)$', re.I)))
    async def _(e):
        async with db_lock:
            with sqlite3.connect(DB_PATH, timeout=30) as conn:
                hot = conn.execute("SELECT COUNT(*) FROM leads WHERE category='HOT'").fetchone()[0]
                seen = conn.execute("SELECT COUNT(*) FROM seen").fetchone()[0]
        await e.reply(f"📊 HOT: {hot}\n👀 Проверено: {seen}", buttons=get_keyboard())

    @bot.on(events.NewMessage(pattern=re.compile(r'^(♻️ Очистить базу|/clear_yes)$', re.I)))
    async def _(e):
        async with db_lock:
            with sqlite3.connect(DB_PATH) as conn: conn.executescript("DELETE FROM leads; DELETE FROM seen; DELETE FROM bookmarks;")
        await e.reply("♻️ ОЧИЩЕНО.", buttons=get_keyboard())

    @bot.on(events.NewMessage(pattern=re.compile(r'^(➕ Добавить группы)$', re.I)))
    async def _(e):
        S.waiting_for_links = True
        await e.reply("👇 Ссылки в столбик:", buttons=get_keyboard())

    @bot.on(events.NewMessage())
    async def _(e):
        if e.sender_id != ADMIN_ID: return
        if S.waiting_for_links and not e.text.startswith('/'):
            for link in e.text.split('\n'):
                if link.strip(): S.queue.put_nowait(link.strip())
            S.waiting_for_links = False
            await e.reply("✅ Добавлено!", buttons=get_keyboard())

async def run_main():
    S.is_running = True
    init_db()
    sessions = list(SESSIONS_DIR.glob("*.session"))
    tasks = [account_worker(s.stem, s, None) for s in sessions]
    await asyncio.gather(*tasks)

async def main():
    init_db()
    S.bot = TelegramClient('bot', API_ID, API_HASH)
    await S.bot.start(bot_token=BOT_TOKEN)
    register_handlers(S.bot)
    print("🤖 БОТ ЗАПУЩЕН")
    await S.bot.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())
