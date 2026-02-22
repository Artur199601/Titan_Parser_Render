import os
import re
import asyncio
import random
import sqlite3
import json
import time
from datetime import datetime
from pathlib import Path

from telethon import TelegramClient, events, Button
from telethon.tl.functions.users import GetFullUserRequest
import socks
from openai import AsyncOpenAI

# ══════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════════════

API_ID = 6
API_HASH = "eb06d4abfb49dc3eeb1aeb98ae0f581e"
BOT_TOKEN = "8177768255:AAECx4EjWSadym2FAjXEZ7yguP57VI8Cmx0"
ADMIN_ID = 1568924415
OPENAI_API_KEY = "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" # <--- НЕ ЗАБУДЬ КЛЮЧ!

DB_PATH = Path("leads.db")
SESSIONS_DIR = Path("sessions")
PROXIES_FILE = Path("proxies.txt")
BATCH_SIZE = 150  

openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# --- ТВОИ СЛОВАРИ (БАЙПАС ИИ) ---
PLUS_WORDS = [
    "aquashine", "celosome", "dermalax", "elasty", "medeyra", "refinex", "rejeunesse", "revolax", 
    "sardenya", "hyamax", "hyaron", "kiara", "ireju", "gemvous", "dermaheal", "tesoro", "biorepeel", 
    "ares", "jalupro", "lipo lab", "lipoone", "lipolab", "light fit", "meso-xanthin", "mesoxanthin", 
    "meso", "mesoeye", "revi", "реви", "novacutan", "новакутан", "juvederm", "ювидерм", "stylage", 
    "стилейдж", "teosyal", "теосиаль", "radiesse", "радиесс", "profhilo", "профайло", "belotero", 
    "белотеро", "neauvia", "rejuplla", "karisma", "каришма", "olidia", "aesthefill", "collost", 
    "liporase", "botox", "ботокс", "botulax", "ботулакс", "dysport", "диспорт", "nabota", "набота", 
    "rentox", "рентокс", "relatox", "релатокс", "филлер", "филлеры", "инъекционист", "контурная пластика",
    "гиалурон", "мезотерапия", "биоревитализация", "липолитик", "коллаген", "лидокаин", "канюля", "игла",
    "опт", "закупка", "прайс", "поставщик", "дистрибьютор", "cosmetolog", "injector", "dr.", "врач", 
    "💉", "💋", "🩺", "🧪", "🔬"
]

MINUS_WORDS = [
    "маникюр", "педикюр", "ногти", "nail", "бровист", "брови", "lash", "лэшмейкер", "ресницы", 
    "парикмахер", "стилист", "визажист", "makeup", "тату мастер", "татуаж", "перманент", 
    "шугаринг", "крипто", "инвестиции", "заработок", "игры", "посоветуйте мастера", "ищу мастера"
]

CONTEXT_PATTERNS = [
    r"(?:филлер|филлеры|губы|ботокс|ботулин|rev[iy]|juvederm|profhilo|revolax).{0,40}\b\d{3,6}\s*(?:руб|р|₽)\b",
    r"\b\d{3,6}\s*(?:руб|р|₽)\b",
    r"\b\d{1,2}(?:x)?\s*\d{1,2}[.,]?\d*\s*ml\b",
    r"^dr[\._]?", r"(?:cosmetolog|kosmetolog|injector)"
]

# ══════════════════════════════════════════════════════════════
# УМНЫЙ МОЗГ ИИ (БИЗНЕС-ЛОГИКА)
# ══════════════════════════════════════════════════════════════

AI_PROMPT = """Ты — опытный аналитик-следователь в сфере косметологического B2B бизнеса.
Твоя задача: отличить ПРОФЕССИОНАЛА (наш клиент) от ПАЦИЕНТА (мусор). Никаких баллов и оценок. Только жесткий бизнес-статус.

КАТЕГОРИИ КЛИЕНТОВ (БЕРЕМ):
1. HOT (Горячий): Клиника, врач, крупный косметолог. Ищет поставщиков, запрашивает прайсы, закупает препараты.
2. WARM (Теплый / Под вопросом): Частный мастер. Обсуждает техники, делится опытом, спрашивает советы у коллег ("коллеги, как убрать рубец?").

МУСОР (НЕ БЕРЕМ):
- TRASH (Мусор): Обычные пациенты ("хочу сделать губы", "как убрать прыщи"), мастера маникюра, спамеры.

Дано: Имя, Юзернейм, Bio, Текст сообщения.
КАК ТЫ ДОЛЖЕН ДУМАТЬ:
1. Ищи маркеры профессии: обращения "коллеги", смайлы 💉 в нике, обсуждение плотности филлеров.
2. Пациент ноет о проблеме. Мастер обсуждает, как эту проблему решить клиенту.
3. Помни: ЛУЧШЕ ВЗЯТЬ СОМНИТЕЛЬНОГО (WARM), ЧЕМ ПОТЕРЯТЬ КЛИЕНТА.

Ответь строго в JSON:
{
  "thought_process": "Твои мысли: вижу вопрос про рубец, но обращение 'коллеги' говорит, что это мастер. Берем как WARM.",
  "category": "HOT"
}"""

async def get_ai_category(profile: dict) -> dict:
    try:
        prompt = AI_PROMPT.format(
            name=profile['name'], username=profile['username'],
            bio=profile['bio'], messages="\n".join(profile['messages'])
        )
        response = await openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.1
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e: 
        print(f"Ошибка ИИ: {e}")
        return {"category": "TRASH", "thought_process": "error"}

def hard_filter(text: str, username: str) -> tuple:
    t = (text + " " + username).lower()
    if any(m in t for m in MINUS_WORDS) and not any(re.search(p, t) for p in CONTEXT_PATTERNS):
        return "TRASH", None
    hits = [w for w in PLUS_WORDS if w in t]
    if hits: return "HOT", f"Маркер: {', '.join(hits[:3])}"
    for p in CONTEXT_PATTERNS:
        if re.search(p, t): return "HOT", "Контекст (цена/бренд)"
    return None, None 

# ══════════════════════════════════════════════════════════════
# ЯДРО ПАРСЕРА С ЗАКЛАДКАМИ (КОПАЕМ ДО ДНА)
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
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER UNIQUE, username TEXT, real_name TEXT, 
            bio TEXT, trigger_text TEXT, category TEXT, group_src TEXT, created_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS seen (user_id INTEGER PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS bookmarks (link TEXT PRIMARY KEY, last_id INTEGER);
        """)

async def check_pulse():
    if S.leads_session_total > 0 and S.leads_session_total % 20 == 0:
        msg = (
            f"💓 **ПУЛЬС ПАРСЕРА** 💓\n\n"
            f"Собрано новых лидов: **{S.leads_session_total}**\n"
            f"🔥 Горячие (HOT): {S.leads_hot}\n"
            f"🤔 Под вопросом (WARM): {S.leads_warm}\n\n"
            f"Машина работает стабильно. ⚙️"
        )
        try:
            await S.bot.send_message(ADMIN_ID, msg)
        except: pass

async def process_user(client, user_obj, messages, group_link, acc_name):
    uid = user_obj.id
    if getattr(user_obj, 'bot', False): return
    
    username = getattr(user_obj, 'username', '') or ''
    name = f"{getattr(user_obj, 'first_name', '')} {getattr(user_obj, 'last_name', '')}".strip()
    trigger_text = messages[0] if messages else ""
    
    bio = ""
    try:
        full = await client(GetFullUserRequest(uid))
        bio = getattr(full.full_user, 'about', '') or ""
    except: pass
    
    full_content = f"{name} {username} {bio} {' '.join(messages)}"
    
    category, reason = hard_filter(full_content, username)
    
    if category is None:
        res = await get_ai_category({"name": name, "username": username, "bio": bio, "messages": messages})
        category = res.get('category', 'TRASH').upper()
        reason = f"ИИ: {res.get('thought_process', '')[:100]}..."

    if category in ['HOT', 'WARM']:
        with sqlite3.connect(DB_PATH) as conn:
            try:
                conn.execute("INSERT INTO leads VALUES (NULL,?,?,?,?,?,?,?,?)",
                            (uid, username, name, bio, trigger_text, category, group_link, int(time.time())))
                
                S.leads_session_total += 1
                if category == 'HOT': S.leads_hot += 1
                elif category == 'WARM': S.leads_warm += 1
                
                print(f"💎 [{acc_name}] {category}: {name} | {reason}", flush=True)
                await check_pulse()
            except sqlite3.IntegrityError:
                pass 

async def account_worker(name, session_path, proxy):
    client = TelegramClient(str(session_path), API_ID, API_HASH, proxy=proxy)
    await client.connect()
    if not await client.is_user_authorized():
        print(f"❌ {name} не авторизован"); return
    
    while not S.stop_event.is_set():
        try:
            link = await S.queue.get()
            
            # ЧИТАЕМ ЗАКЛАДКУ (чтобы копать глубже)
            last_id = 0
            with sqlite3.connect(DB_PATH) as conn:
                row = conn.execute("SELECT last_id FROM bookmarks WHERE link=?", (link,)).fetchone()
                if row: last_id = row[0]

            entity = await client.get_entity(link)
            
            messages_processed = 0
            oldest_msg_id = last_id

            # ПАРСИНГ С УЧЕТОМ ЗАКЛАДКИ
            async for msg in client.iter_messages(entity, limit=BATCH_SIZE, offset_id=last_id):
                if S.stop_event.is_set(): break
                
                messages_processed += 1
                oldest_msg_id = msg.id # Запоминаем ID самого старого прочитанного сообщения

                if not msg.sender_id or not msg.text: continue
                
                with sqlite3.connect(DB_PATH) as conn:
                    if conn.execute("SELECT 1 FROM seen WHERE user_id=?", (msg.sender_id,)).fetchone(): continue
                    conn.execute("INSERT OR IGNORE INTO seen VALUES (?)", (msg.sender_id,))
                
                if msg.sender:
                    await process_user(client, msg.sender, [msg.text], link, name)
                await asyncio.sleep(random.uniform(1, 3))

            # СОХРАНЯЕМ НОВУЮ ЗАКЛАДКУ (дно пробито еще глубже)
            if messages_processed > 0 and oldest_msg_id != last_id:
                with sqlite3.connect(DB_PATH) as conn:
                    conn.execute("INSERT OR REPLACE INTO bookmarks (link, last_id) VALUES (?, ?)", (link, oldest_msg_id))
                    print(f"📌 [{name}] Закладка в {link} обновлена. Копаем дальше с сообщения ID:{oldest_msg_id}")

            S.queue.put_nowait(link)
            await asyncio.sleep(600)
            S.queue.task_done()
        except Exception as e:
            await asyncio.sleep(30)

# ══════════════════════════════════════════════════════════════
# БОТ-ИНТЕРФЕЙС И КНОПКИ
# ══════════════════════════════════════════════════════════════

def get_keyboard():
    return [
        [Button.text('🚀 Запуск', resize=True), Button.text('📦 Выгрузка')],
        [Button.text('➕ Добавить группы'), Button.text('📊 Статистика')],
        [Button.text('♻️ Очистить базу')]
    ]

async def export_txt(event):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM leads ORDER BY category ASC").fetchall()
    
    if not rows: return await event.reply("База пуста. Парсер еще ничего не нашел.", buttons=get_keyboard())
    
    path = "export_leads.txt"
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            contact = f"@{r['username']}" if r['username'] else str(r['user_id'])
            f.write(f"[{r['category']}] {contact} | {r['real_name']} | {r['trigger_text'].replace('\n', ' ')}\n")
    
    await event.reply(f"📦 Собрано {len(rows)} лидов (B2B)", file=path, buttons=get_keyboard())
    os.remove(path)

def register_handlers(bot):
    @bot.on(events.NewMessage(pattern=re.compile(r'^(🚀 Запуск|/start)$', re.I)))
    async def _(e):
        if e.sender_id != ADMIN_ID: return
        S.waiting_for_links = False
        if S.is_running: 
            return await e.reply("Парсер уже работает! ⚙️", buttons=get_keyboard())
        asyncio.create_task(run_main())
        await e.reply("🚀 МНОГОПОТОК ЗАПУЩЕН! Аккаунты пошли выкачивать историю до дна.", buttons=get_keyboard())

    @bot.on(events.NewMessage(pattern=re.compile(r'^(📦 Выгрузка|/export)$', re.I)))
    async def _(e): 
        S.waiting_for_links = False
        await export_txt(e)
    
    @bot.on(events.NewMessage(pattern=re.compile(r'^(📊 Статистика|/stats)$', re.I)))
    async def _(e):
        S.waiting_for_links = False
        with sqlite3.connect(DB_PATH) as conn:
            hot = conn.execute("SELECT COUNT(*) FROM leads WHERE category='HOT'").fetchone()[0]
            warm = conn.execute("SELECT COUNT(*) FROM leads WHERE category='WARM'").fetchone()[0]
            seen = conn.execute("SELECT COUNT(*) FROM seen").fetchone()[0]
            bookmarks = conn.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0]
        await e.reply(f"📊 **АБСОЛЮТНАЯ СТАТИСТИКА:**\n\n🔥 Горячие (HOT): {hot}\n🤔 Под вопросом (WARM): {warm}\n👀 Проверено сообщений: {seen}\n📌 Активных закладок: {bookmarks}\n🔄 В очереди групп: {S.queue.qsize()}", buttons=get_keyboard())

    @bot.on(events.NewMessage(pattern=re.compile(r'^(♻️ Очистить базу|/clear_yes)$', re.I)))
    async def _(e):
        S.waiting_for_links = False
        with sqlite3.connect(DB_PATH) as conn:
            conn.executescript("DELETE FROM leads; DELETE FROM seen; DELETE FROM bookmarks;")
        while not S.queue.empty():
            S.queue.get_nowait()
            S.queue.task_done()
        S.leads_session_total = 0
        S.leads_hot = 0
        S.leads_warm = 0
        await e.reply("♻️ БАЗА И ЗАКЛАДКИ ОБНУЛЕНЫ.", buttons=get_keyboard())

    @bot.on(events.NewMessage(pattern=re.compile(r'^(➕ Добавить группы)$', re.I)))
    async def _(e):
        if e.sender_id != ADMIN_ID: return
        S.waiting_for_links = True
        await e.reply("👇 Отправь мне ссылки на чаты в столбик:", buttons=get_keyboard())

    @bot.on(events.NewMessage())
    async def _(e):
        if e.sender_id != ADMIN_ID: return
        
        text = e.text.strip()
        buttons_text = ['🚀 Запуск', '📦 Выгрузка', '📊 Статистика', '♻️ Очистить базу', '➕ Добавить группы', '/start', '/export', '/stats', '/clear_yes']
        
        if S.waiting_for_links and text not in buttons_text:
            links = text.split('\n')
            added_count = 0
            for link in links:
                link = link.strip()
                if link:
                    S.queue.put_nowait(link)
                    added_count += 1
            
            S.waiting_for_links = False
            await e.reply(f"✅ Успешно добавлено {added_count} групп в очередь!", buttons=get_keyboard())
        
        elif text not in buttons_text:
            await e.reply("Главное меню Fillers_Beauty:", buttons=get_keyboard())

async def run_main():
    S.is_running = True
    init_db()
    
    proxies = []
    if PROXIES_FILE.exists():
        for line in PROXIES_FILE.read_text().splitlines():
            p = line.split(':')
            if len(p) == 4: proxies.append((socks.HTTP, p[0], int(p[1]), True, p[2], p[3]))
            
    sessions = list(SESSIONS_DIR.glob("*.session"))
    tasks = []
    for i, sess in enumerate(sessions):
        proxy = proxies[i % len(proxies)] if proxies else None
        tasks.append(account_worker(sess.stem, sess, proxy))
        await asyncio.sleep(2)
        
    await asyncio.gather(*tasks)

async def main():
    init_db()
    S.bot = TelegramClient('bot', API_ID, API_HASH)
    await S.bot.start(bot_token=BOT_TOKEN)
    register_handlers(S.bot)
    
    try:
        await S.bot.send_message(ADMIN_ID, "🔧 Терминал парсера запущен. Готов к работе.", buttons=get_keyboard())
    except:
        pass
        
    print("🤖 БОТ ЗАПУЩЕН"); await S.bot.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())
