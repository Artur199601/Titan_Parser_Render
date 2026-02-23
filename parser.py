import os
import re
import asyncio
import random
import sqlite3
import json
import time
import gc # 👇 ДОБАВЛЕНО: Сборщик мусора
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
# Ключ исправлен (маленькая sk-)
OPENAI_API_KEY = "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"

# 👇 ДОБАВЛЕНО: Путь к защищенному диску (если он есть)
DB_PATH = Path("/data/leads.db") if os.path.exists("/data") else Path("leads.db")

SESSIONS_DIR = Path("sessions")
PROXIES_FILE = Path("proxies.txt")
BATCH_SIZE = 150  

openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# 🛑 ЗАЩИТА СЕРВЕРА И БАЗЫ (ОБНОВЛЕНИЕ)
db_lock = asyncio.Lock()
# Запускаем 10 аккаунтов одновременно для скорости
ram_semaphore = asyncio.Semaphore(10) 

# ══════════════════════════════════════════════════════════════
# БРОНЕБОЙНЫЙ ФИЛЬТР МУСОРА (Убивает 100% шлак до ИИ)
# ══════════════════════════════════════════════════════════════

MINUS_WORDS = [
    # ЭСТЕТИКА, АППАРАТКА И ТЕЛО
    "эстет", "эстетист", "эстетика", "аппаратная", "аппаратный", "аппаратка", "аппаратный массаж",
    "массаж", "массажист", "чистка лица", "чистки", "чистку", "smas", "смас", "lpg", "rf-лифтинг", "рф-лифтинг", 
    "микротоки", "уходовая", "лазер", "лазерная", "эпиляция", "депиляция", "электроэпиляция",
    "косметик", "карбокситерапия", "гидропилинг", "кавитация", "прессотерапия", "криолиполиз", 
    "вакуумный", "миостимуляция", "эндосфера", "b-flexy", "воск", "восковая", "фейсфитнес", 
    "тейпирование", "тейпы", "пигментация", "пигментации", "bbl",

    # ЧИСТАЯ ДЕРМАТОЛОГИЯ И БОЛЕЗНИ
    "псориаз", "экзема", "дерматит", "розацеа", "акне", "постакне", "лечение акне", "подолог", 
    "бородавки", "папилломы", "грибок", "трихолог", "выпадение волос", "алопеция", "себорея", 
    "лишай", "купероз", "витилиго", "меланома", "невус", "родинки", "удаление родинок", 
    "удаление бородавок", "дерматоскоп", "дерматоскопия", "соскоб", "криодеструкция", 
    "венеролог", "миколог", "роаккутан", "акнекутан", "изотретиноин", "базирон", "скинорен", 
    "эффезел", "зеркалин", "демодекс",

    # БЫТОВЫЕ ПАЦИЕНТЫ И МАМАШИ
    "прыщи", "прыщ", "прыщей", "сыпь", "аллергия", "аллергическая", "ребенок", "ребенка", "дочь", "сын", 
    "муж", "подросток", "домашний уход", "умывалка", "крем", "bb крем", "вв крем", "сыворотка", "пенка",

    # БЬЮТИ-СПАМЕРЫ: ОБОРУДОВАНИЕ И КУРСЫ
    "аппарат", "аппараты", "оборудование", "оборудования", "поставляем", "обучение", "обучения", 
    "видео лекции", "курс", "курсы", "соцконтракт", "соц.контракт", "государства", "аренда", "помещение",

    # ДРУГОЙ БЬЮТИ-МУСОР
    "маникюр", "педикюр", "ногти", "nail", "бровист", "брови", "lash", "лэшмейкер", "ресницы", 
    "парикмахер", "стилист", "визажист", "makeup", "тату", "тату-мастер", "татуаж", "перманент", 
    "шугаринг", "колорист", "барбер", "лами", "ламинирование", "наращивание",

    # АДМИНЫ И СПАМ
    "крипто", "инвестиции", "заработок", "игры", "посоветуйте мастера", "ищу мастера", 
    "подскажите косметолога", "менеджер", "админ", "запись по телефону", "запись в директ",
    "прайс-лист в шапке"
]

def hard_filter(text: str, username: str) -> tuple:
    t = (text + " " + username).lower()
    if any(m in t for m in MINUS_WORDS):
        return "TRASH", "Мусор по словарю"
    return None, None 

# ══════════════════════════════════════════════════════════════
# УМНЫЙ МОЗГ ИИ 
# ══════════════════════════════════════════════════════════════

AI_PROMPT = """Ты — опытный, безжалостный и вдумчивый аналитик B2B-продаж. 
Твоя компания: Fillers_Beauty (оптовая и розничная продажа косметологических препаратов: филлеры, токсины, липолитики).

Твоя задача: анализировать каждое входящее сообщение как ЖИВОЙ ЧЕЛОВЕК. У тебя нет лимита на токены, думай глубоко. Сопоставляй Имя, Юзернейм, Bio и сам текст.
Ищи РЕАЛЬНЫХ КЛИЕНТОВ (косметологов-инъекционистов, врачей, клиники), которые колют препараты и потенциально могут закупать их у нас.

КАТЕГОРИИ КЛИЕНТОВ:
1. HOT (Прямой клиент): Врач, инъекционист, клиника. Ищет закупку, спрашивает прайсы, ищет поставщиков ботокса/филлеров.
2. WARM (Потенциальный клиент): Врач/мастер. Обсуждает техники инъекций, разведение препаратов, осложнения после филлеров, делится профессиональным опытом с коллегами.

3. TRASH (Мусор - БЕЗЖАЛОСТНО УДАЛЯТЬ):
   - Обычные пациенты (жалобы на прыщи, аллергию, сыпь у детей, ищут крем, спрашивают "чем мазать", "посоветуйте мастера").
   - Конкуренты (те, кто сами спамят: "продам филлеры оптом", "лучшие цены на токсины").
   - Инфоцыгане, продавцы курсов, арендодатели кабинетов, продавцы оборудования и аппаратов.

ГЛАВНОЕ ПРАВИЛО АНАЛИЗА:
Смотри на картину целиком. Если сообщение короткое (например, "Цена?", "Спасибо", "Где купить?"), НЕ СПЕШИ кидать в TRASH. Внимательно изучи Bio и Имя! 
- Если человек пишет "Спасибо", а в Bio написано "Врач-косметолог, контурная пластика" — это наш человек (WARM/HOT). 
- Если пишет "Где купить?", а профиль пустой или бытовой — это может быть пациент, анализируй контекст беседы.
Будь безжалостен к пациентам и конкурентам! Пропускай только тех, кто реально держит в руках шприц.

Ответь строго в JSON:
{{
  "thought_process": "Глубокий анализ: вижу короткий текст 'Спасибо', но в Bio указано 'инъекционист, Москва', значит это реальный врач, берем в WARM...",
  "category": "HOT" 
}}"""

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

# ══════════════════════════════════════════════════════════════
# ЯДРО ПАРСЕРА
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
        CREATE TABLE IF NOT EXISTS user_bios (user_id INTEGER PRIMARY KEY, bio TEXT);
        """)

async def check_pulse():
    if S.leads_session_total > 0 and S.leads_session_total % 100 == 0:
        msg = (
            f"💓 **ПУЛЬС ПАРСЕРА** 💓\n\n"
            f"Собрано новых лидов: **{S.leads_session_total}**\n"
            f"🔥 Горячие (HOT): {S.leads_hot}\n"
            f"🤔 Под вопросом (WARM): {S.leads_warm}\n\n"
            f"Машина работает стабильно, ИИ думает над каждым. ⚙️"
        )
        try:
            await S.bot.send_message(ADMIN_ID, msg)
        except: pass

async def process_user(client, user_obj, messages, group_link, acc_name):
    if not isinstance(user_obj, User): return 
    if getattr(user_obj, 'bot', False): return 
    if getattr(user_obj, 'is_self', False): return 
    
    status = getattr(user_obj, 'status', None)
    if isinstance(status, UserStatusEmpty): return 
    if isinstance(status, UserStatusOffline):
        now = datetime.now(timezone.utc)
        if now - status.was_online > timedelta(days=30): return 
            
    uid = user_obj.id
    username = getattr(user_obj, 'username', '') or ''
    name = f"{getattr(user_obj, 'first_name', '')} {getattr(user_obj, 'last_name', '')}".strip()
    trigger_text = messages[0] if messages else ""
    
    # === УМНЫЙ КЭШ BIO И БЕЗОПАСНОСТЬ БАЗЫ ===
    bio = ""
    async with db_lock:
        with sqlite3.connect(DB_PATH, timeout=30) as conn:
            cached = conn.execute("SELECT bio FROM user_bios WHERE user_id=?", (uid,)).fetchone()
            
    if cached is not None:
        bio = cached[0]
    else:
        await asyncio.sleep(random.uniform(1.5, 3))
        try:
            full = await client(GetFullUserRequest(uid))
            bio = getattr(full.full_user, 'about', '') or ""
            async with db_lock:
                with sqlite3.connect(DB_PATH, timeout=30) as conn:
                    conn.execute("INSERT OR REPLACE INTO user_bios VALUES (?, ?)", (uid, bio))
                    conn.commit()
        except FloodWaitError as e:
            print(f"⚠️ FloodWait. Спим {e.seconds} сек.")
            await asyncio.sleep(e.seconds)
        except Exception:
            async with db_lock:
                with sqlite3.connect(DB_PATH, timeout=30) as conn:
                    conn.execute("INSERT OR REPLACE INTO user_bios VALUES (?, ?)", (uid, bio))
                    conn.commit()
    
    full_content = f"{name} {username} {bio} {' '.join(messages)}"
    
    category, reason = hard_filter(full_content, username)
    
    if category is None:
        res = await get_ai_category({"name": name, "username": username, "bio": bio, "messages": messages})
        category = res.get('category', 'TRASH').upper()
        reason = f"ИИ: {res.get('thought_process', '')[:150]}..."

    if category in ['HOT', 'WARM']:
        async with db_lock:
            with sqlite3.connect(DB_PATH, timeout=30) as conn:
                try:
                    conn.execute("INSERT INTO leads VALUES (NULL,?,?,?,?,?,?,?,?)",
                                (uid, username, name, bio, trigger_text, category, group_link, int(time.time())))
                    conn.commit()
                    S.leads_session_total += 1
                    if category == 'HOT': S.leads_hot += 1
                    elif category == 'WARM': S.leads_warm += 1
                    print(f"💎 [{acc_name}] {category}: {name} | {reason}", flush=True)
                except sqlite3.IntegrityError:
                    pass 
        await check_pulse()

async def account_worker(name, session_path, proxy):
    client = TelegramClient(str(session_path), API_ID, API_HASH, proxy=proxy)
    await client.connect()
    if not await client.is_user_authorized():
        print(f"❌ {name} не авторизован"); return
    
    while not S.stop_event.is_set():
        try:
            link = await S.queue.get()
            
            messages_processed = 0
            oldest_msg_id = 0
            last_id = 0
            
            # 👇 ВАЖНО: Семафор держит память только во время АКТИВНОГО парсинга
            async with ram_semaphore:
                async with db_lock:
                    with sqlite3.connect(DB_PATH, timeout=30) as conn:
                        row = conn.execute("SELECT last_id FROM bookmarks WHERE link=?", (link,)).fetchone()
                        if row: last_id = row[0]

                try:
                    entity = await client.get_entity(link)
                    # БЕЗОПАСНЫЙ ВХОД В ГРУППУ
                    if hasattr(entity, 'left') and entity.left:
                        await asyncio.sleep(random.uniform(5, 10))
                        try: await client(JoinChannelRequest(entity))
                        except UserAlreadyParticipantError: pass
                        except FloodWaitError as e: await asyncio.sleep(e.seconds)
                        except: pass
                except Exception:
                    entity = None

                if entity:
                    oldest_msg_id = last_id
                    async for msg in client.iter_messages(entity, limit=BATCH_SIZE, offset_id=last_id):
                        if S.stop_event.is_set(): break 
                        
                        messages_processed += 1
                        oldest_msg_id = msg.id 

                        if not msg.sender_id or not msg.text: continue
                        
                        is_seen = False
                        async with db_lock:
                            with sqlite3.connect(DB_PATH, timeout=30) as conn:
                                if conn.execute("SELECT 1 FROM seen WHERE user_id=?", (msg.sender_id,)).fetchone():
                                    is_seen = True
                                else:
                                    conn.execute("INSERT OR IGNORE INTO seen VALUES (?)", (msg.sender_id,))
                                    conn.commit()
                        if is_seen: continue
                        
                        if msg.sender:
                            await process_user(client, msg.sender, [msg.text], link, name)
                        await asyncio.sleep(random.uniform(1, 3))

                    if messages_processed > 0 and oldest_msg_id != last_id:
                        async with db_lock:
                            with sqlite3.connect(DB_PATH, timeout=30) as conn:
                                conn.execute("INSERT OR REPLACE INTO bookmarks (link, last_id) VALUES (?, ?)", (link, oldest_msg_id))
                                conn.commit()

            # 👇 ТУТ СЕМАФОР ОТПУСКАЕТСЯ. Аккаунт курит, сервер отдыхает.
            if not S.stop_event.is_set():
                gc.collect() # 👇 ДОБАВЛЕНО: Жесткая очистка мусора из памяти
                if messages_processed == BATCH_SIZE:
                    await asyncio.sleep(random.uniform(480, 780)) # ~10 минут
                    S.queue.put_nowait(link)
                elif messages_processed > 0:
                    await asyncio.sleep(random.uniform(1500, 2100)) # Дошли до дна ~30 мин
                    S.queue.put_nowait(link)
                else:
                    await asyncio.sleep(1800)
                    S.queue.put_nowait(link)
                
            S.queue.task_done()
        except Exception as e:
            await asyncio.sleep(30)

# ══════════════════════════════════════════════════════════════
# БОТ-ИНТЕРФЕЙС И КНОПКИ (СТРОГО ОРИГИНАЛ ОТ ПОЛЬЗОВАТЕЛЯ)
# ══════════════════════════════════════════════════════════════

def get_keyboard():
    return [
        [Button.text('🚀 Запуск', resize=True), Button.text('🛑 Стоп')],
        [Button.text('📦 Выгрузка'), Button.text('📊 Статистика')],
        [Button.text('➕ Добавить группы'), Button.text('♻️ Очистить базу')]
    ]

async def export_txt(event):
    async with db_lock:
        with sqlite3.connect(DB_PATH, timeout=30) as conn:
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
        
        S.stop_event.clear() 
        asyncio.create_task(run_main())
        await e.reply("🚀 МОЗГОВОЙ ШТУРМ ЗАПУЩЕН! ИИ анализирует каждого.", buttons=get_keyboard())

    @bot.on(events.NewMessage(pattern=re.compile(r'^(🛑 Стоп|/stop)$', re.I)))
    async def _(e):
        if e.sender_id != ADMIN_ID: return
        S.waiting_for_links = False
        if not S.is_running:
            return await e.reply("Парсер и так стоит на паузе 💤", buttons=get_keyboard())
        
        S.stop_event.set() 
        S.is_running = False
        
        while not S.queue.empty():
            S.queue.get_nowait()
            S.queue.task_done()
            
        await e.reply("🛑 ПАРСЕР ОСТАНОВЛЕН. База и закладки сохранены.", buttons=get_keyboard())

    @bot.on(events.NewMessage(pattern=re.compile(r'^(📦 Выгрузка|/export)$', re.I)))
    async def _(e): 
        S.waiting_for_links = False
        await export_txt(e)
    
    @bot.on(events.NewMessage(pattern=re.compile(r'^(📊 Статистика|/stats)$', re.I)))
    async def _(e):
        S.waiting_for_links = False
        async with db_lock:
            with sqlite3.connect(DB_PATH, timeout=30) as conn:
                hot = conn.execute("SELECT COUNT(*) FROM leads WHERE category='HOT'").fetchone()[0]
                warm = conn.execute("SELECT COUNT(*) FROM leads WHERE category='WARM'").fetchone()[0]
                seen = conn.execute("SELECT COUNT(*) FROM seen").fetchone()[0]
                bookmarks = conn.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0]
        await e.reply(f"📊 **АБСОЛЮТНАЯ СТАТИСТИКА:**\n\n🔥 Горячие (HOT): {hot}\n🤔 Под вопросом (WARM): {warm}\n👀 Проверено сообщений: {seen}\n📌 Активных закладок: {bookmarks}\n🔄 В очереди групп: {S.queue.qsize()}", buttons=get_keyboard())

    @bot.on(events.NewMessage(pattern=re.compile(r'^(♻️ Очистить базу|/clear_yes)$', re.I)))
    async def _(e):
        S.waiting_for_links = False
        async with db_lock:
            with sqlite3.connect(DB_PATH, timeout=30) as conn:
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
        buttons_text = ['🚀 Запуск', '🛑 Стоп', '📦 Выгрузка', '📊 Статистика', '♻️ Очистить базу', '➕ Добавить группы', '/start', '/stop', '/export', '/stats', '/clear_yes']
        
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
        await S.bot.send_message(ADMIN_ID, "🔧 Терминал парсера обновлен (Семафор на 10 акк + Кэш Bio). Готов к работе.", buttons=get_keyboard())
    except:
        pass
        
    print("🤖 БОТ ЗАПУЩЕН"); await S.bot.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())
