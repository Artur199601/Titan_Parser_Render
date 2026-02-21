import os
import re
import asyncio
import logging
import random
import sqlite3
import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from telethon import TelegramClient, events
from telethon.errors import (
    FloodWaitError, ChatAdminRequiredError, ChannelPrivateError
)
from telethon.tl.types import (
    UserStatusOnline, UserStatusRecently,
    UserStatusLastWeek, UserStatusLastMonth,
    UserStatusOffline, Channel
)
import socks

# ══════════════════════════════════════════════════════════════
# НАСТРОЙКИ (ВШИТЫЕ ДАННЫЕ)
# ══════════════════════════════════════════════════════════════

# Публичные ключи от официального Android (Неубиваемые)
API_ID = 6
API_HASH = "eb06d4abfb49dc3eeb1aeb98ae0f581e"

# Твой управляющий бот и твой личный Telegram ID
BOT_TOKEN = "8177768255:AAELNFdbH9BHcohyOB4pbLJEh3GuEKX05s4"
ADMIN_ID = 1568924415

# Нейросети (Используем Groq как основной мотор)
GROQ_API_KEY = "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"
OPENAI_API_KEY = ""
GEMINI_API_KEY = ""

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
SESSIONS_DIR = BASE_DIR / "sessions"
DB_PATH = Path(BASE_DIR / "leads.db")
PROXIES_FILE = BASE_DIR / "proxies.txt"

OUTPUT_DIR.mkdir(exist_ok=True)
SESSIONS_DIR.mkdir(exist_ok=True)

SCORE_THRESHOLD = 6
EXPORT_CHUNK = 1000
BATCH_SIZE = 200
ACCOUNT_REST_MIN = 300
ACCOUNT_REST_MAX = 900

# ══════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(BASE_DIR / "parser.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("titan")

# ══════════════════════════════════════════════════════════════
# ГЛОБАЛЬНОЕ СОСТОЯНИЕ
# ══════════════════════════════════════════════════════════════

class State:
    found_count: int = 0
    processed_count: int = 0
    session_start: float = 0.0
    stop_event = None
    is_running: bool = False
    current_group: str = ""
    queue: list = []
    bot_client = None
    pending_clear: dict = {}

S = State()

# ══════════════════════════════════════════════════════════════
# БАЗА ДАННЫХ
# ══════════════════════════════════════════════════════════════

def init_db():
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER UNIQUE,
            username TEXT,
            real_name TEXT,
            bio TEXT,
            messages TEXT,
            score INTEGER DEFAULT 0,
            ai_reason TEXT,
            activity TEXT,
            group_src TEXT,
            exported INTEGER DEFAULT 0,
            created_at INTEGER DEFAULT (strftime('%s','now'))
        );
        CREATE TABLE IF NOT EXISTS seen_users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_seen INTEGER DEFAULT (strftime('%s','now'))
        );
        CREATE TABLE IF NOT EXISTS group_bookmarks (
            group_link TEXT PRIMARY KEY,
            last_msg_id INTEGER DEFAULT 0,
            updated_at INTEGER DEFAULT (strftime('%s','now'))
        );
        CREATE TABLE IF NOT EXISTS parsed_groups (
            link TEXT PRIMARY KEY,
            done INTEGER DEFAULT 0,
            parsed_at INTEGER DEFAULT (strftime('%s','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_leads_score ON leads(score);
        CREATE INDEX IF NOT EXISTS idx_leads_activity ON leads(activity);
        CREATE INDEX IF NOT EXISTS idx_leads_exported ON leads(exported);
        """)

def is_seen(user_id: int) -> bool:
    with sqlite3.connect(str(DB_PATH)) as conn:
        return bool(conn.execute("SELECT 1 FROM seen_users WHERE user_id=?", (user_id,)).fetchone())

def mark_seen(user_id: int, username: str = ""):
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute("INSERT OR IGNORE INTO seen_users(user_id, username) VALUES(?,?)", (user_id, username or ""))

def save_lead(data: dict):
    with sqlite3.connect(str(DB_PATH)) as conn:
        existing = conn.execute("SELECT messages, score FROM leads WHERE user_id=?", (data["user_id"],)).fetchone()
        if existing:
            old_msgs = json.loads(existing[0] or "[]")
            new_msgs = json.loads(data.get("messages", "[]"))
            merged = list({m: None for m in old_msgs + new_msgs})[:20]
            new_score = max(existing[1], data.get("score", 0))
            conn.execute(
                "UPDATE leads SET messages=?, score=?, ai_reason=? WHERE user_id=?",
                (json.dumps(merged, ensure_ascii=False), new_score, data.get("ai_reason", ""), data["user_id"])
            )
        else:
            conn.execute("""
                INSERT INTO leads
                (user_id, username, real_name, bio, messages, score, ai_reason, activity, group_src)
                VALUES
                (:user_id, :username, :real_name, :bio, :messages, :score, :ai_reason, :activity, :group_src)
            """, data)

def get_bookmark(group_link: str) -> int:
    with sqlite3.connect(str(DB_PATH)) as conn:
        row = conn.execute("SELECT last_msg_id FROM group_bookmarks WHERE group_link=?", (group_link,)).fetchone()
        return row[0] if row else 0

def save_bookmark(group_link: str, last_msg_id: int):
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute("""
            INSERT INTO group_bookmarks(group_link, last_msg_id, updated_at)
            VALUES(?,?, strftime('%s','now'))
            ON CONFLICT(group_link) DO UPDATE SET
            last_msg_id=excluded.last_msg_id,
            updated_at=excluded.updated_at
        """, (group_link, last_msg_id))

def get_stats() -> dict:
    with sqlite3.connect(str(DB_PATH)) as conn:
        total = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
        quality = conn.execute("SELECT COUNT(*) FROM leads WHERE score>=?", (SCORE_THRESHOLD,)).fetchone()[0]
        hot = conn.execute("SELECT COUNT(*) FROM leads WHERE activity='hot'").fetchone()[0]
        warm = conn.execute("SELECT COUNT(*) FROM leads WHERE activity='warm'").fetchone()[0]
        exported = conn.execute("SELECT COUNT(*) FROM leads WHERE exported=1").fetchone()[0]
        return {"total": total, "quality": quality, "hot": hot, "warm": warm, "exported": exported}

# ══════════════════════════════════════════════════════════════
# ПРОКСИ
# ══════════════════════════════════════════════════════════════

def load_proxies() -> list:
    if not PROXIES_FILE.exists():
        log.warning("proxies.txt не найден — работаем без прокси")
        return [None]
    proxies = []
    for line in PROXIES_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"): continue
        
        # Поддержка формата user:pass@ip:port
        if '@' in line:
            auth, ip_port = line.split('@')
            user, passwd = auth.split(':')
            host, port = ip_port.split(':')
        else:
            parts = line.split(":")
            if len(parts) < 2: continue
            host, port = parts[0], parts[1]
            user = parts[2] if len(parts) > 2 else None
            passwd = parts[3] if len(parts) > 3 else None
            
        try:
            port = int(port)
            proxies.append((socks.HTTP, host, port, True, user, passwd))
        except ValueError:
            continue
            
    log.info("Загружено прокси: %d", len(proxies))
    return proxies if proxies else [None]

# ══════════════════════════════════════════════════════════════
# ОПРЕДЕЛЕНИЕ АКТИВНОСТИ И ИИ-ФИЛЬТРЫ
# ══════════════════════════════════════════════════════════════

def classify_activity(status) -> str:
    if isinstance(status, (UserStatusOnline, UserStatusRecently, UserStatusLastWeek)): return "hot"
    if isinstance(status, UserStatusLastMonth): return "warm"
    if isinstance(status, UserStatusOffline):
        try:
            days = (datetime.now(timezone.utc) - status.was_online).days
            if days <= 7: return "hot"
            if days <= 30: return "warm"
        except Exception: pass
    return "cold"

AI_PROMPT = """Ты — снайпер B2B продаж инъекционных косметологических препаратов.
Продукт: корейские филлеры и ботулотоксины.

БЕРЁМ (Score 5-10): Косметологи, хирурги, дерматологи, владельцы клиник закупающие препараты.
НЕ БЕРЁМ (Score 0-3): Обычные клиенты, бровисты, мастера ногтевого сервиса, блогеры, продавцы CRM.

Профиль:
Имя: {name}
Bio: {bio}
Сообщения:
{messages}

Ответь ТОЛЬКО JSON:
{{"score": 8, "reason": "краткая причина"}}"""

PLUS_WORDS = ["косметолог", "косметология", "инъекционист", "эстетист", "дерматолог", "филлер", "ботокс", "биоревитализация", "мезотерапия", "уколы красоты"]
MINUS_WORDS = ["мастер маникюра", "бровист", "лэшмейкер", "тату мастер", "продаю аппараты", "посоветуйте косметолога", "клиент"]

def quick_filter(bio: str, messages: list) -> tuple:
    import re
    text = " ".join([bio or ""] + (messages or [])).lower()
    bio_lower = (bio or "").lower()
    
    price_hit = re.search(r"(ботокс|филлер|губы|биоревит)[^\n.]{0,30}\d{3,6}[\s]*(руб|р\b)", text)
    if price_hit: return True, "прайс мастера -> score 8+"
    
    for word in MINUS_WORDS:
        if word in text: return False, f"минус: {word}"
        
    is_ip = any(w in bio_lower for w in ["ип ", "самозанятая"])
    has_cosm = any(w in text for w in ["косметолог", "филлер", "ботокс"])
    if is_ip and not has_cosm: return False, "ИП без косм. контекста"
    
    hits = [w for w in PLUS_WORDS if w in text]
    if hits: return True, f"плюс: {', '.join(hits[:3])}"
    
    return True, "нет маркеров -> ИИ"

async def _call_groq(profile: dict) -> Optional[dict]:
    if not GROQ_API_KEY: return None
    try:
        from groq import AsyncGroq
        client = AsyncGroq(api_key=GROQ_API_KEY)
        prompt = AI_PROMPT.format(
            name=profile.get("name", ""),
            bio=profile.get("bio", "") or "не указан",
            messages="\n".join(profile.get("messages", [])[:10]) or "нет сообщений"
        )
        r = await client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="llama-3.3-70b-versatile",
            temperature=0.1,
            max_tokens=120
        )
        text = re.sub(r"```json|```", "", r.choices[0].message.content).strip()
        return json.loads(text)
    except Exception as e:
        log.warning("Groq error: %s", e)
        return None

def _heuristic(profile: dict) -> dict:
    text = " ".join([profile.get("name", ""), profile.get("bio", "") or "", *profile.get("messages", [])]).lower()
    score = 0
    hits = []
    for w in ["косметолог", "филлер", "ботокс", "инъекции", "мезотерапия"]:
        if w in text: score += 2; hits.append(w)
    return {"score": min(score, 10), "reason": f"эвристика: {', '.join(hits[:4])}" if hits else "нет маркеров"}

async def analyze_profile(profile: dict) -> dict:
    result = await _call_groq(profile)
    if result and "score" in result:
        result["score"] = max(0, min(10, int(result["score"])))
        return result
    return _heuristic(profile)

# ══════════════════════════════════════════════════════════════
# АНТИБАН И ПАРСИНГ
# ══════════════════════════════════════════════════════════════

async def smart_sleep(base_min: float = 2.0, base_max: float = 6.0):
    roll = random.random()
    if roll < 0.70: delay = random.uniform(base_min, base_max) + random.uniform(0, 1.5)
    elif roll < 0.90: delay = random.uniform(20, 60)
    else: delay = random.uniform(90, 180)
    await asyncio.sleep(delay)

async def safe_request(coro, label: str = "", retries: int = 3):
    for attempt in range(retries):
        try: return await coro
        except FloodWaitError as e:
            wait = e.seconds + random.randint(10, 30)
            log.warning("FloodWait [%s]: ждём %d сек", label, wait)
            await asyncio.sleep(wait)
        except (ChatAdminRequiredError, ChannelPrivateError) as e:
            log.warning("Нет доступа [%s]: %s", label, e)
            return None
        except Exception as e:
            if attempt < retries - 1: await asyncio.sleep(random.uniform(5, 15))
            else: return None

async def notify(text: str):
    if S.bot_client and ADMIN_ID:
        try: await S.bot_client.send_message(ADMIN_ID, text)
        except Exception: pass

async def process_user(client: TelegramClient, user_obj, messages: list, group_link: str, account_name: str) -> bool:
    if not user_obj or getattr(user_obj, "bot", False) or getattr(user_obj, "deleted", False): return False
    
    user_id = user_obj.id
    activity = classify_activity(getattr(user_obj, "status", None))
    
    # ИСПРАВЛЕНИЕ: Если статус скрыт настройками приватности, не выкидываем, а считаем "теплым"
    if activity == "cold": activity = "warm" 
    
    username = getattr(user_obj, "username", "") or ""
    real_name = " ".join(filter(None, [getattr(user_obj, "first_name", ""), getattr(user_obj, "last_name", "")])).strip() or "Без имени"
    
    bio = getattr(user_obj, "about", "") or ""
    if not bio:
        try:
            full = await safe_request(client.get_entity(user_id), label=f"bio {user_id}")
            bio = getattr(full, "about", "") or "" if full else ""
        except Exception: bio = ""
        await asyncio.sleep(random.uniform(1.0, 2.5))
        
    should, reason = quick_filter(bio, messages)
    
    # НОВЫЙ ЛОГ: Теперь ты будешь видеть в Render каждого человека, которого он проверяет!
    print(f"[{account_name}] Анализ: {real_name} | Итог фильтра: {reason}", flush=True)
    
    if not should: return False
    
    result = await analyze_profile({"name": real_name, "bio": bio, "messages": messages})
    score = result.get("score", 0)
    ai_reason = result.get("reason", "")
    
    save_lead({
        "user_id": user_id, "username": username, "real_name": real_name,
        "bio": bio, "messages": json.dumps(messages, ensure_ascii=False),
        "score": score, "ai_reason": ai_reason, "activity": activity, "group_src": group_link,
    })
    
    is_quality = score >= SCORE_THRESHOLD
    if is_quality:
        S.found_count += 1
        if S.found_count % 20 == 0:
            elapsed = time.time() - S.session_start
            speed = S.found_count / (elapsed / 3600) if elapsed > 0 else 0
            await notify(f"💓 Ритм: {S.found_count} лидов\n⚡️ Скорость: {speed:.0f} л/ч\n📍 {group_link}\n👀 Обработано: {S.processed_count}")
    
    S.processed_count += 1
    return is_quality
    
    user_id = user_obj.id
    activity = classify_activity(getattr(user_obj, "status", None))
    if activity == "cold": return False
    
    username = getattr(user_obj, "username", "") or ""
    real_name = " ".join(filter(None, [getattr(user_obj, "first_name", ""), getattr(user_obj, "last_name", "")])).strip() or "Без имени"
    
    bio = getattr(user_obj, "about", "") or ""
    if not bio:
        try:
            full = await safe_request(client.get_entity(user_id), label=f"bio {user_id}")
            bio = getattr(full, "about", "") or "" if full else ""
        except Exception: bio = ""
        await asyncio.sleep(random.uniform(1.0, 2.5))
        
    should, reason = quick_filter(bio, messages)
    if not should: return False
    
    result = await analyze_profile({"name": real_name, "bio": bio, "messages": messages})
    score = result.get("score", 0)
    ai_reason = result.get("reason", "")
    
    save_lead({
        "user_id": user_id, "username": username, "real_name": real_name,
        "bio": bio, "messages": json.dumps(messages, ensure_ascii=False),
        "score": score, "ai_reason": ai_reason, "activity": activity, "group_src": group_link,
    })
    
    is_quality = score >= SCORE_THRESHOLD
    if is_quality:
        S.found_count += 1
        if S.found_count % 20 == 0:
            elapsed = time.time() - S.session_start
            speed = S.found_count / (elapsed / 3600) if elapsed > 0 else 0
            await notify(f"💓 Ритм: {S.found_count} лидов\n⚡️ Скорость: {speed:.0f} л/ч\n📍 {group_link}\n👀 Обработано: {S.processed_count}")
    
    S.processed_count += 1
    return is_quality

async def parse_group_batch(client: TelegramClient, account_name: str, group_link: str) -> bool:
    entity = await safe_request(client.get_entity(group_link), label=group_link)
    if not entity: return False
    
    bookmark = get_bookmark(group_link)
    user_msgs, user_objects = defaultdict(list), {}
    oldest_id, has_more, count = None, False, 0
    
    try:
        print(f"🚀 [{account_name}] ЗАШЕЛ В ГРУППУ И КАЧАЕТ СООБЩЕНИЯ!", flush=True)
        async for msg in client.iter_messages(entity, limit=BATCH_SIZE + 1, max_id=bookmark if bookmark > 0 else 0):
            if S.stop_event.is_set(): break
            count += 1
            if count > BATCH_SIZE:
                has_more = True
                break
            if oldest_id is None or msg.id < oldest_id: oldest_id = msg.id
            if not msg.sender_id or not msg.text: continue
            
            uid = msg.sender_id
            if is_seen(uid): continue
            
            user_msgs[uid].append(msg.text[:300])
            if uid not in user_objects and msg.sender: user_objects[uid] = msg.sender
            await smart_sleep(0.2, 0.6)
            
    except Exception as e:
        log.warning("[%s] Ошибка %s: %s", account_name, group_link, e)
        if oldest_id: save_bookmark(group_link, oldest_id)
        return True

    for uid, msgs in user_msgs.items():
        if S.stop_event.is_set(): break
        if is_seen(uid): continue
        mark_seen(uid, getattr(user_objects.get(uid), "username", "") or "")
        await process_user(client, user_objects.get(uid), msgs, group_link, account_name)
        await smart_sleep(1.5, 4.0)
        
    if oldest_id: save_bookmark(group_link, oldest_id)
    return has_more

async def parse_channel_comments_batch(client: TelegramClient, account_name: str, channel_link: str) -> bool:
    entity = await safe_request(client.get_entity(channel_link), label=channel_link)
    if not entity: return False
    
    bookmark = get_bookmark(f"comments:{channel_link}")
    posts_read, has_more, oldest_post_id = 0, False, None
    
    try:
        async for post in client.iter_messages(entity, limit=30, max_id=bookmark if bookmark > 0 else 0):
            if S.stop_event.is_set(): break
            posts_read += 1
            if oldest_post_id is None or post.id < oldest_post_id: oldest_post_id = post.id
            if not getattr(post, "replies", None): continue
            
            user_msgs, user_objects = defaultdict(list), {}
            async for comment in client.iter_messages(entity, reply_to=post.id, limit=50):
                if not comment.sender_id or not comment.text: continue
                uid = comment.sender_id
                if is_seen(uid): continue
                user_msgs[uid].append(comment.text[:300])
                if uid not in user_objects and comment.sender: user_objects[uid] = comment.sender
                
            for uid, msgs in user_msgs.items():
                if S.stop_event.is_set(): break
                if is_seen(uid): continue
                mark_seen(uid, getattr(user_objects.get(uid), "username", "") or "")
                await process_user(client, user_objects.get(uid), msgs, channel_link, account_name)
                await smart_sleep(1.0, 3.0)
                
        has_more = posts_read >= 30
    except Exception as e:
        log.warning("[%s] Ошибка комментов %s: %s", account_name, channel_link, e)
        
    if oldest_post_id: save_bookmark(f"comments:{channel_link}", oldest_post_id)
    return has_more

async def parse_target_batch(client: TelegramClient, account_name: str, link: str) -> bool:
    entity = await safe_request(client.get_entity(link), label=link)
    if not entity: return False
    if isinstance(entity, Channel) and getattr(entity, "broadcast", False):
        return await parse_channel_comments_batch(client, account_name, link)
    else:
        return await parse_group_batch(client, account_name, link)

async def run_relay(clients: list, names: list):
    if not clients: return
    resting_until, client_idx = {}, 0
    
    while S.queue and not S.stop_event.is_set():
        now = time.time()
        free_client, free_name, free_idx = None, None, None
        checked = 0
        
        while checked < len(clients):
            idx = client_idx % len(clients)
            client_idx += 1; checked += 1
            if resting_until.get(idx, 0) > now: continue
            free_client, free_name, free_idx = clients[idx], names[idx], idx
            break
            
        if free_client is None:
            wake_times = [t for t in resting_until.values() if t > now]
            await asyncio.sleep((min(wake_times) - now + 1) if wake_times else 10)
            continue
            
        link = S.queue.pop(0)
        S.current_group = link
        has_more = await parse_target_batch(free_client, free_name, link)
        
        if has_more: S.queue.append(link)
        else:
            await notify(f"✅ Группа прочитана:\n{link}")
            with sqlite3.connect(str(DB_PATH)) as conn:
                conn.execute("INSERT OR REPLACE INTO parsed_groups(link,done) VALUES(?,1)", (link,))
                
        rest = random.uniform(ACCOUNT_REST_MIN, ACCOUNT_REST_MAX)
        resting_until[free_idx] = time.time() + rest
        S.current_group = ""

# ══════════════════════════════════════════════════════════════
# ЭКСПОРТ И БОТ
# ══════════════════════════════════════════════════════════════

async def export_leads(event, limit: int = None, only_new_hours: int = None):
    msg = await event.reply("⏳ Формирую базу...")
    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            q = "SELECT id, username, real_name FROM leads WHERE score>=? AND exported=0 ORDER BY score DESC, activity DESC"
            params = [SCORE_THRESHOLD]
            if only_new_hours:
                q = q.replace("WHERE", f"WHERE created_at>={int(time.time()) - only_new_hours * 3600} AND")
            if limit: q += f" LIMIT {limit}"
            rows = conn.execute(q, params).fetchall()

        valid = [(r["id"], ("@" + r["username"] if not r["username"].startswith("@") else r["username"]), r["real_name"] or "") for r in rows if r["username"] and not r["username"].startswith("id")]
        if not valid:
            return await msg.edit("❌ Нет новых лидов с username.")

        chunks = [valid[i:i+EXPORT_CHUNK] for i in range(0, len(valid), EXPORT_CHUNK)]
        await msg.edit(f"📦 Выгружаю {len(valid)} лидов...")
        
        exported_ids = []
        for idx, chunk in enumerate(chunks, 1):
            file_path = OUTPUT_DIR / f"leads_{idx}.txt"
            with open(file_path, "w", encoding="utf-8") as f:
                for _, uname, name in chunk: f.write(f"{uname}:{name}\n")
            await S.bot_client.send_file(event.chat_id, file_path, caption=f"✅ Часть {idx}/{len(chunks)}")
            exported_ids.extend([r[0] for r in chunk])
            file_path.unlink(missing_ok=True)
            
        if exported_ids:
            with sqlite3.connect(str(DB_PATH)) as conn:
                conn.execute(f"UPDATE leads SET exported=1 WHERE id IN ({','.join('?'*len(exported_ids))})", exported_ids)
    except Exception as e:
        await msg.edit(f"❌ Ошибка: {e}")

def register_handlers(bot: TelegramClient):
    @bot.on(events.NewMessage(pattern=r"(?i)^/help$"))
    async def _help(event):
        if event.sender_id != ADMIN_ID: return
        await event.reply("📖 TITAN PARSER\n/add [ссылки] — добавить в очередь\n/start — пуск\n/stop — пауза\n/status — стата\n/export — выгрузить\n/top10 — лучшие лиды\n/clear — сброс")

    @bot.on(events.NewMessage(pattern=r"(?i)^/add(.*)"))
    async def _add(event):
        if event.sender_id != ADMIN_ID: return
        text = event.pattern_match.group(1).strip()
        links = list(dict.fromkeys(re.findall(r"https?://t.me/\S+|t.me/[^\s\n]+", text) or [text.split()[0]] if text else []))
        added = [l.strip().rstrip("/") for l in links if l.strip().rstrip("/") not in S.queue]
        S.queue.extend(added)
        await event.reply(f"✅ Добавлено {len(added)}. В очереди: {len(S.queue)}") if added else await event.reply("⚠️ Уже в очереди.")

    @bot.on(events.NewMessage(pattern=r"(?i)^/start$"))
    async def _start(event):
        if event.sender_id != ADMIN_ID: return
        if S.is_running: return await event.reply("⚠️ Уже работает.")
        if not S.queue: return await event.reply("❌ Очередь пуста.")
        S.stop_event.clear()
        asyncio.create_task(_run_parser())
        await event.reply("🚀 Запускаю!")

    @bot.on(events.NewMessage(pattern=r"(?i)^/stop$"))
    async def _stop(event):
        if event.sender_id != ADMIN_ID: return
        S.stop_event.set()
        await event.reply("⛔ Остановка...")

    @bot.on(events.NewMessage(pattern=r"(?i)^/status$"))
    async def _status(event):
        if event.sender_id != ADMIN_ID: return
        stats = get_stats()
        speed = (S.found_count / ((time.time() - S.session_start) / 3600)) if (time.time() - S.session_start) > 60 else 0
        await event.reply(f"📊 STATUS: {'🟢' if S.is_running else '🔴'}\n📍 {S.current_group or '—'}\n📋 Очередь: {len(S.queue)}\n🔥 Горячих: {stats['hot']}\n💎 Качественных: {stats['quality']}\n⚡️ {speed:.0f} л/ч")

    @bot.on(events.NewMessage(pattern=r"(?i)^/export$"))
    async def _export_cmd(event):
        if event.sender_id == ADMIN_ID: await export_leads(event)

    @bot.on(events.NewMessage(pattern=r"(?i)^/top10$"))
    async def _top10(event):
        if event.sender_id != ADMIN_ID: return
        with sqlite3.connect(str(DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT username, real_name, score, ai_reason FROM leads WHERE score>=? ORDER BY score DESC LIMIT 10", (SCORE_THRESHOLD,)).fetchall()
        if not rows: return await event.reply("❌ Пусто.")
        await event.reply("\n".join([f"🏆 {r['real_name']} | @{r['username']}\n{r['score']}/10 — {r['ai_reason'][:50]}" for r in rows]))

    @bot.on(events.NewMessage(pattern=r"(?i)^/clear$"))
    async def _clear(event):
        if event.sender_id == ADMIN_ID: S.pending_clear[event.sender_id] = time.time(); await event.reply("⚠️ Отправь /clear_yes")

    @bot.on(events.NewMessage(pattern=r"(?i)^/clear_yes$"))
    async def _clear_yes(event):
        if event.sender_id == ADMIN_ID and (time.time() - S.pending_clear.get(event.sender_id, 0) < 30):
            with sqlite3.connect(str(DB_PATH)) as conn: conn.executescript("DELETE FROM leads; DELETE FROM seen_users; DELETE FROM group_bookmarks;")
            S.found_count = S.processed_count = 0
            await event.reply("♻️ Очищено.")

async def _run_parser():
    S.is_running, S.session_start, S.found_count, S.processed_count = True, time.time(), 0, 0
    proxies, sessions = load_proxies(), list(SESSIONS_DIR.glob("*.session"))
    if not sessions:
        S.is_running = False
        return await notify("❌ Нет .session файлов")

    clients, names = [], []
    for i, sess in enumerate(sessions):
        c = TelegramClient(str(sess), API_ID, API_HASH, proxy=proxies[i % len(proxies)], connection_retries=2, timeout=15)
        try:
            await c.connect()
            if await c.is_user_authorized(): clients.append(c); names.append(sess.stem)
            else: await c.disconnect()
        except Exception: pass

    if not clients:
        S.is_running = False
        return await notify("❌ Нет авторизованных сессий!")

    await notify(f"🚀 В работе: {len(clients)} акк")
    try: await run_relay(clients, names)
    except Exception: pass
    finally:
        for c in clients:
            try: await c.disconnect()
            except Exception: pass
        S.is_running = False
        await notify("🏁 Парсинг завершён! Отправь /export")

async def main():
    S.stop_event = asyncio.Event()
    init_db()
    bot = TelegramClient("control_bot", API_ID, API_HASH)
    await bot.start(bot_token=BOT_TOKEN)
    S.bot_client = bot
    register_handlers(bot)
    log.info("Бот готов.")
    await notify("🟢 TITAN PARSER готов к работе. Жду /add и /start")
    await bot.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())



