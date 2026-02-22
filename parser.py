import os, re, asyncio, random, sqlite3, json, time
from telethon import TelegramClient, events, Button
from telethon.errors import FloodWaitError, UserAlreadyParticipantError
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.channels import JoinChannelRequest
import socks
from openai import AsyncOpenAI
from pathlib import Path

# === КОНФИГУРАЦИЯ ===
API_ID = 6
API_HASH = "eb06d4abfb49dc3eeb1aeb98ae0f581e"
BOT_TOKEN = "8177768255:AAECx4EjWSadym2FAjXEZ7yguP57VI8Cmx0"
ADMIN_ID = 1568924415
OPENAI_API_KEY = "Sk-proj-xshkzyA-CoAp-sqSYP68CJkbkoDQlwe_O24YhFM3cPHcCZIF19au8Gl4QYgWuGnyYL2cKkdcXyT3BlbkFJfkGcb32wVMsxtzErRGgLo-NpgKAxjdUawKLKLl5iORBic_pPqNmeUOG0Cqy5RaKpzVuBV2DY8A"

openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)
DB_PATH = "leads.db"

# 🛑 СВЕТОФОР ДЛЯ БАЗЫ ДАННЫХ (Защита от Locked)
db_lock = asyncio.Lock()

MINUS_WORDS = ["прыщи", "сыпь", "ребенок", "аппарат", "оборудование", "маникюр", "курсы", "аренда", "сдам кабинет"]

def is_trash(text: str) -> bool:
    return any(m in text.lower() for m in MINUS_WORDS)

AI_PROMPT = """Ты аналитик Fillers_Beauty. Ищи косметологов. Если мастер/врач - HOT/WARM. Если пациент/спам - TRASH. JSON: {"category": "HOT/WARM/TRASH"}"""

async def get_ai_category(text, bio):
    try:
        res = await openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": f"{AI_PROMPT}\nТекст: {text}\nBio: {bio}"}],
            response_format={"type": "json_object"}
        )
        return json.loads(res.choices[0].message.content).get('category', 'TRASH')
    except: return "TRASH"

class State:
    queue = asyncio.Queue()
    is_running = False
    bot = None
    stop_event = asyncio.Event()
    leads_total = 0
    seen_total = 0

S = State()

# === БЕЗОПАСНАЯ РАБОТА С БАЗОЙ ===
async def db_execute(query, params=(), fetchone=False):
    async with db_lock:
        with sqlite3.connect(DB_PATH, timeout=30) as conn:
            cur = conn.execute(query, params)
            if fetchone: return cur.fetchone()
            conn.commit()

# === ЛОГИКА АККАУНТА ===
async def account_worker(name, session_path):
    client = TelegramClient(str(session_path), API_ID, API_HASH)
    try:
        await client.connect()
        if not await client.is_user_authorized(): return
    except Exception: return

    while not S.stop_event.is_set():
        link = await S.queue.get()
        try:
            # 1. БЕЗОПАСНЫЙ ВХОД В ГРУППУ
            entity = await client.get_entity(link)
            if hasattr(entity, 'left') and entity.left:
                await asyncio.sleep(random.uniform(5, 15)) 
                try: 
                    await client(JoinChannelRequest(entity))
                except UserAlreadyParticipantError: pass
                except FloodWaitError as e: await asyncio.sleep(e.seconds)
                except Exception: pass

            # Получаем закладку (глубину)
            row = await db_execute("SELECT last_id FROM bookmarks WHERE link=?", (link,), fetchone=True)
            last_id = row[0] if row else 0

            msgs_found = 0
            
            # 2. ПАРСИМ ГЛУБОКО ВНИЗ (ПО 200 СООБЩЕНИЙ)
            async for msg in client.iter_messages(entity, limit=200, offset_id=last_id):
                if S.stop_event.is_set(): break
                last_id = msg.id
                msgs_found += 1

                if not msg.sender_id or not msg.text or is_trash(msg.text): continue

                is_seen = await db_execute("SELECT 1 FROM seen WHERE user_id=?", (msg.sender_id,), fetchone=True)
                if is_seen: continue
                
                await db_execute("INSERT OR IGNORE INTO seen VALUES (?)", (msg.sender_id,))
                S.seen_total += 1

                # 3. УМНЫЙ КЭШ BIO (ЗАЩИТА ОТ БАНА)
                cached_bio = await db_execute("SELECT bio FROM user_bios WHERE user_id=?", (msg.sender_id,), fetchone=True)
                if cached_bio is not None:
                    bio = cached_bio[0]
                else:
                    await asyncio.sleep(random.uniform(1.5, 3)) # Легкая антифлуд пауза
                    try:
                        full = await client(GetFullUserRequest(msg.sender_id))
                        bio = full.full_user.about or ""
                        await db_execute("INSERT INTO user_bios VALUES (?, ?)", (msg.sender_id, bio))
                    except FloodWaitError as e:
                        print(f"⚠️ {name} поймал FloodWait. Спим {e.seconds} сек.")
                        await asyncio.sleep(e.seconds)
                        bio = ""
                    except Exception:
                        bio = ""
                        await db_execute("INSERT INTO user_bios VALUES (?, ?)", (msg.sender_id, bio))

                # 4. ОТПРАВКА В ИИ
                cat = await get_ai_category(msg.text, bio)
                if cat in ['HOT', 'WARM']:
                    await db_execute("INSERT OR IGNORE INTO leads (user_id, bio, trigger_text, category, group_src, created_at) VALUES (?,?,?,?,?,?)",
                                    (msg.sender_id, bio, msg.text[:100], cat, link, int(time.time())))
                    S.leads_total += 1
                    
                    # 5. ПУЛЬС КАЖДЫЕ 100 ЛИДОВ
                    if S.leads_total % 100 == 0:
                        await S.bot.send_message(ADMIN_ID, f"📈 ПУЛЬС: Найдено {S.leads_total} лидов.\nПроверено: {S.seen_total}")

            # Сохраняем закладку
            await db_execute("INSERT OR REPLACE INTO bookmarks (link, last_id) VALUES (?, ?)", (link, last_id))

            # 6. УМНЫЙ ОТДЫХ: ПЛАВАЮЩИЕ ТАЙМИНГИ (АНТИ-БАН)
            if msgs_found == 200:
                # Пачка полная -> идем вглубь после рандомного перекура (от 8 до 13 минут)
                sleep_time = random.uniform(480, 780)
                await asyncio.sleep(sleep_time)
                S.queue.put_nowait(link)
            else:
                # Дошли до дна -> ждем от 25 до 35 минут
                sleep_time = random.uniform(1500, 2100)
                await asyncio.sleep(sleep_time)
                S.queue.put_nowait(link)

        except FloodWaitError as e:
            await asyncio.sleep(e.seconds)
        except ConnectionError:
            await asyncio.sleep(15)
        except Exception:
            await asyncio.sleep(60)
        finally:
            S.queue.task_done()

# === ИНТЕРФЕЙС БОТА ===
def get_keyboard():
    return [[Button.text('🚀 Запуск'), Button.text('📊 Статистика')], [Button.text('♻️ Очистить базу'), Button.text('➕ Добавить группы')]]

def register_handlers(bot):
    @bot.on(events.NewMessage(pattern='/start'))
    async def _(e): await e.reply("Защищенный терминал активен.", buttons=get_keyboard())

    @bot.on(events.NewMessage(pattern='📊 Статистика'))
    async def _(e):
        leads = (await db_execute("SELECT COUNT(*) FROM leads", fetchone=True))[0]
        seen = (await db_execute("SELECT COUNT(*) FROM seen", fetchone=True))[0]
        bios = (await db_execute("SELECT COUNT(*) FROM user_bios", fetchone=True))[0]
        await e.reply(f"📊 Статус:\nЛидов: {leads}\nПроверено людей: {seen}\nБиографий в кэше: {bios}\nВ очереди: {S.queue.qsize()}")

    @bot.on(events.NewMessage(pattern='🚀 Запуск'))
    async def _(e):
        if S.is_running: return
        S.is_running = True
        S.stop_event.clear()
        asyncio.create_task(run_main())
        await e.reply("🚀 БРОНЕБОЙНЫЙ РЕЖИМ ЗАПУЩЕН! (Плавающие перекуры включены).")

    @bot.on(events.NewMessage(pattern='♻️ Очистить базу'))
    async def _(e):
        await db_execute("DELETE FROM leads")
        await db_execute("DELETE FROM seen")
        await db_execute("DELETE FROM bookmarks")
        # Таблицу user_bios НЕ чистим! Это наш золотой запас.
        S.leads_total = 0
        S.seen_total = 0
        await e.reply("♻️ Очередь и лиды очищены. Кэш профилей сохранен.")

    @bot.on(events.NewMessage(pattern='➕ Добавить группы'))
    async def _(e): await e.reply("Пришли ссылки в столбик:")

    @bot.on(events.NewMessage())
    async def _(e):
        if e.text.startswith('http'):
            for l in e.text.strip().split('\n'): S.queue.put_nowait(l.strip())
            await e.reply("✅ Группы добавлены.")

async def run_main():
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS leads (id INTEGER PRIMARY KEY, user_id INTEGER, bio TEXT, trigger_text TEXT, category TEXT, group_src TEXT, created_at INTEGER); 
        CREATE TABLE IF NOT EXISTS seen (user_id INTEGER PRIMARY KEY); 
        CREATE TABLE IF NOT EXISTS bookmarks (link TEXT PRIMARY KEY, last_id INTEGER);
        CREATE TABLE IF NOT EXISTS user_bios (user_id INTEGER PRIMARY KEY, bio TEXT);
        """)
    
    sessions = list(Path("sessions").glob("*.session"))
    for s in sessions: asyncio.create_task(account_worker(s.stem, s))

async def main():
    S.bot = TelegramClient('bot', API_ID, API_HASH)
    await S.bot.start(bot_token=BOT_TOKEN)
    register_handlers(S.bot)
    await S.bot.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())
