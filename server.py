import asyncio
import base64
import hashlib
import json
import os
import random
import secrets
import string
import struct
import time
import zlib
import gzip
from datetime import date, datetime, timedelta
try:
    from zoneinfo import ZoneInfo
    EASTERN_TZ = ZoneInfo("America/New_York")
except Exception:
    EASTERN_TZ = None
from aiohttp import web
import aiohttp
import motor.motor_asyncio

MAX_LOBBIES_PER_USER = 5
MAX_DM_HISTORY = 100
VALID_SIZES = [(256, 256), (512, 512), (1024, 1024)]
PUBLIC_LOBBIES = [
    {"name": "OFFICIAL 256x256", "cooldown": 0, "width": 256, "height": 256},
    {"name": "OFFICIAL 512x512", "cooldown": 0, "width": 512, "height": 512},
    {"name": "OFFICIAL 1024x1024", "cooldown": 0, "width": 1024, "height": 1024},
]
DEFAULT_COOLDOWN = 0.5
MAX_COOLDOWN = 60
ADMIN_USER = "toothpaste"
LOBBY_TIMEOUT_PUBLIC = 48 * 60 * 60                                        
LOBBY_TIMEOUT_PRIVATE = 168 * 60 * 60                                    
                                                                               
LOBBY_TIMEOUT = LOBBY_TIMEOUT_PRIVATE

def lobby_timeout_for(lobby):
    return LOBBY_TIMEOUT_PUBLIC if lobby.get("public") else LOBBY_TIMEOUT_PRIVATE

MONGO_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017/ezplace")
mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI, compressors='zlib', zlibCompressionLevel=6)
db = mongo_client.get_default_database() if "mongodb.net" in MONGO_URI else mongo_client["ezplace"]

accounts = {}
sessions = {}
captchas = {}
friends_data = {}
dms = {}
dm_last_seen = {}                                                 
bans = []
ip_bans = []
device_bans = []
user_devices = {}
moderators = []
mod_ban_log = []
MOD_BAN_LOG_MAX = 500
vips = []
ranks = {}
name_colors = {}
presence = {}
last_online = {}
reports = []
REPORTS_MAX = 500
user_ips = {}
fake_admins = []
brush_perms = {}                                                     
clans = {}                                                                                                      
groups = {}                                                          
group_messages = {}                                                          

                                                                                  
place_bucks = {}
lifetime_pixels = {}
purchases = {}
streaks = {}                                                                   
stay_seconds = {}
IDLE_REWARD_SECONDS = 900
economy_history = []
ECONOMY_SNAPSHOT_INTERVAL = 300
ECONOMY_HISTORY_MAX = 2016
PB_PIXELS_PER_BUCK = 100
SHOP_PRICES = {"custom_wheel": 5, "vip": 45, "name_color": 30, "custom_rank": 70, "streak_48hr": 1200, "streak_pass": 800}
SHOP_SELL_PRICES = {"streak_pass": 400}
PALETTE_SIZE = 81
STACKABLE_ITEMS = {"streak_pass"}
MAX_CASINO_BET = 5000
PB_TRANSFER_AMOUNT_PER_MIN = 100000
pb_transfer_amount_window = {}
LOBBY_PRICES = {(256, 256): 50, (512, 512): 100, (1024, 1024): 200}

ALLOWED_IMAGE_HOSTS = frozenset({"files.catbox.moe", "litter.catbox.moe", "i.imgur.com", "imgur.com"})
def is_safe_image_url(url):
    if not isinstance(url, str) or not url or len(url) > 500: return False
    if any(c in url for c in "\r\n\t\0 "): return False
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
        if p.scheme != "https": return False
        if p.username or p.password: return False
        return p.hostname is not None and p.hostname.lower() in ALLOWED_IMAGE_HOSTS
    except: return False
lobbies = {}
clients = {}
social_clients = {}
social_ips = {}

def is_fake_admin(user):
    return user and user.lower() in [f.lower() for f in fake_admins]

def is_admin(user):
    return user and user.lower() == ADMIN_USER

def get_friend_data(user):
    if user not in friends_data:
        friends_data[user] = {"friends": [], "incoming": [], "outgoing": []}
    return friends_data[user]

def dm_key(a, b):
    return ":".join(sorted([a.lower(), b.lower()]))

def is_online(username):
    ulow = username.lower()
    for info in clients.values():
        if info and not info.get("guest") and info.get("username", "").lower() == ulow:
            return True
    for u in social_clients.values():
        if u and u.lower() == ulow:
            return True
    return False

def is_banned(username):
    return username.lower() in [b.lower() for b in bans]

def is_ip_banned(request):
    ip = get_client_ip(request)
    return ip in ip_bans

def is_vip(username):
    return username and username.lower() in vips

def get_rank(username):
    if not username:
        return None
    if is_admin(username):
        return {"label": "CREATOR", "color": "rainbow"}
    if is_moderator(username):
        return {"label": "MOD", "color": "mod"}
    return ranks.get(username.lower())

def get_auth_user(request):
    return sessions.get(request.headers.get("Authorization", ""))

def get_client_ip(request):
    return request.headers.get("X-Forwarded-For", "").split(",")[0].strip() or request.remote or "unknown"

def lobby_info(lobby, include_code=False):
    info = {
        "id": lobby["id"], "name": lobby["name"], "owner": lobby["owner"],
        "public": lobby["public"], "whitelist_enabled": lobby["whitelist_enabled"],
        "online": sum(1 for c in clients.values() if c and c.get("lobby_id") == lobby["id"]),
        "cooldown": lobby.get("cooldown", DEFAULT_COOLDOWN),
        "width": lobby.get("width", 256), "height": lobby.get("height", 256),
        "last_activity": lobby.get("last_activity", time.time()),
        "expires_in": max(0, lobby_timeout_for(lobby) - (time.time() - lobby.get("last_activity", time.time()))) if not lobby["id"].startswith("public_") else None,
    }
    if include_code and lobby.get("code"):
        info["code"] = lobby["code"]
    if include_code:
        info["lobby_bans"] = lobby.get("lobby_bans", [])
    if lobby["whitelist_enabled"]:
        info["whitelist"] = lobby["whitelist"]
    return info

def user_lobby_count(username):
    return sum(1 for l in lobbies.values() if l["owner"] and l["owner"].lower() == username.lower() and not l["id"].startswith("public_"))

def get_leaderboard_top10(lobby):
    pc = lobby.get("pixel_counts", {})
    top = sorted(pc.items(), key=lambda x: x[1], reverse=True)[:10]
    return {
        "entries": [{"name": n, "pixels": c, "online": is_online(n)} for n, c in top],
        "original_owner": lobby.get("original_owner"),
    }

def hash_password(password, salt=None):
    if salt is None:
        salt = secrets.token_hex(16)
    return hashlib.sha256((salt + password).encode()).hexdigest(), salt

def clean_captchas():
    now = time.time()
    for k in [k for k, v in captchas.items() if v["expires"] < now]:
        del captchas[k]

async def db_save(collection, key, data):
    await db[collection].update_one({"_id": key}, {"$set": {"data": data}}, upsert=True)

async def db_load(collection, key):
    doc = await db[collection].find_one({"_id": key})
    return doc["data"] if doc else None

async def save_accounts(): await db_save("store", "accounts", accounts)
async def save_friends(): await db_save("store", "friends", friends_data)
async def save_bans(): await db_save("store", "bans", bans)
async def save_ip_bans(): await db_save("store", "ip_bans", ip_bans)
async def save_device_bans(): await db_save("store", "device_bans", device_bans)
async def save_user_devices(): await db_save("store", "user_devices", user_devices)
async def save_moderators(): await db_save("store", "moderators", moderators)
async def save_mod_ban_log(): await db_save("store", "mod_ban_log", mod_ban_log[-MOD_BAN_LOG_MAX:])

def is_device_banned(device_id):
    return bool(device_id) and device_id in device_bans

def is_moderator(user):
    return bool(user) and user.lower() in moderators

def track_user_device(user, device_id):
    if not user or not device_id: return
    user_devices[user.lower()] = device_id

async def save_vips(): await db_save("store", "vips", vips)
async def save_fake_admins(): await db_save("store", "fake_admins", fake_admins)
async def save_brush_perms(): await db_save("store", "brush_perms", brush_perms)
async def save_clans(): await db_save("store", "clans", clans)
async def save_groups(): await db_save("store", "groups", groups)
async def save_group_messages(gid): await db_save("group_messages", gid, group_messages.get(gid, []))

def find_clan_by_member(username):
    if not username: return None
    ulow = username.lower()
    for clan in clans.values():
        if clan.get("status") != "approved": continue
        if clan.get("owner", "").lower() == ulow: return clan
        if any(m.lower() == ulow for m in clan.get("members", [])): return clan
    return None

def get_clan_tag(username):
    """The clan chip shown in chat. Computed live from membership so it's
    independent of the `ranks` dict - a clan member who ALSO has an admin
    rank shows BOTH: [Clan] [AdminRank]."""
    if not username or is_admin(username):
        return None
    clan = find_clan_by_member(username)
    if not clan:
        return None
    ulow = username.lower()
    override = (clan.get("member_ranks") or {}).get(ulow) or {}
    return {"label": clan.get("name") or "", "color": override.get("color") or clan.get("color") or "#7c5cfc"}

async def apply_clan_rank(username, clan):

                                                       
    return

async def remove_user_clan_rank(username):
    """Strips clan-applied rank tags but preserves anything the user actually paid for.
    custom_rank and VIP are persistent purchases and must survive clan changes."""
    if not username or is_admin(username): return
    ulow = username.lower()
    pu = purchases.get(ulow) or {}
    has_custom_rank = bool(pu.get("custom_rank"))
    has_vip = bool(pu.get("vip"))
    if has_custom_rank:
        pass
    elif has_vip:
        ranks[ulow] = {"label": "VIP", "color": "#daa520"}
    else:
        ranks.pop(ulow, None)
    if has_vip and ulow not in vips:
        vips.append(ulow)
        await save_vips()
    elif not has_vip and ulow in vips:
        vips.remove(ulow)
        await save_vips()
    await save_ranks()

def get_brush_perm(username):
    if not username:
        return {"size": 1, "drag": False}
    if is_admin(username):
        return {"size": 25, "drag": True}
    return brush_perms.get(username.lower(), {"size": 1, "drag": False})

async def save_ranks(): await db_save("store", "ranks", ranks)

async def save_name_colors(): await db_save("store", "name_colors", name_colors)
def get_name_color(username): return name_colors.get((username or "").lower())
async def save_last_online(): await db_save("store", "last_online", last_online)
async def save_reports(): await db_save("store", "reports", reports[-REPORTS_MAX:])
def get_presence(username): return presence.get((username or "").lower()) or None

async def push_friend_presence(user):
    """Push presence state to everyone who has this user as a friend."""
    ulow = user.lower()
    p = presence.get(ulow) or {"afk": False, "since": time.time()}
    payload = {"type": "friend_presence", "user": user, "afk": bool(p.get("afk")), "since": p.get("since", time.time()), "online": is_online(user)}
    for friend_user, fd in list(friends_data.items()):
        if user in (fd.get("friends") or []):
            try: await notify_social(friend_user, payload)
            except: pass

async def save_user_ips(): await db_save("store", "user_ips", user_ips)

_last_broadcast_total = None
_last_broadcast_at = 0
async def broadcast_economy_total():
    """Push the new total to every live socket. Skips if total unchanged
    OR if we just broadcast within the last second (anti-spam)."""
    global _last_broadcast_total, _last_broadcast_at
    total = _total_economy_pb()
    now = time.time()
    if total == _last_broadcast_total and now - _last_broadcast_at < 5:
        return
    if now - _last_broadcast_at < 1:
        return
    _last_broadcast_total = total
    _last_broadcast_at = now
    payload = json.dumps({"type": "economy_total", "total": total})
    for ws, info in list(clients.items()):
        if info and not ws.closed:
            try: await ws.send_str(payload)
            except: pass
    for ws, uname in list(social_clients.items()):
        if uname and not ws.closed:
            try: await ws.send_str(payload)
            except: pass

async def save_place_bucks():
    await db_save("store", "place_bucks", place_bucks)
    try: await broadcast_economy_total()
    except: pass
async def save_lifetime_pixels(): await db_save("store", "lifetime_pixels", lifetime_pixels)
async def save_purchases(): await db_save("store", "purchases", purchases)
async def save_stay_seconds(): await db_save("store", "stay_seconds", stay_seconds)
async def save_economy_history(): await db_save("store", "economy_history", economy_history)

PB_UNLIMITED = 10 ** 9                                                  

def get_pb(user):
    if not user: return 0
    if is_admin(user): return PB_UNLIMITED
    return int(place_bucks.get(user.lower(), 0))
def spend_pb(user, amount):
    """Returns True and deducts if successful. Real admin always succeeds, no deduction."""
    if not user or amount <= 0: return False
    if is_admin(user): return True
    ulow = user.lower()
    bal = int(place_bucks.get(ulow, 0))
    if bal < amount: return False
    place_bucks[ulow] = bal - amount
    return True
MAX_PB_BALANCE = 10 ** 12

def credit_pb(user, amount):
    if not user or amount <= 0 or is_admin(user): return
    ulow = user.lower()
    place_bucks[ulow] = min(int(place_bucks.get(ulow, 0)) + amount, MAX_PB_BALANCE)
def has_purchase(user, item):
    if not user: return False
    if is_admin(user): return True
    return bool((purchases.get(user.lower()) or {}).get(item))

STREAK_REWARD_CAP = 50
STREAK_HEARTBEAT_PERIOD = 30
STREAK_ACTIVE_SECONDS_REQUIRED = 15 * 60
streak_progress = {}

def _local_today_iso(tz_offset_min):
    """JavaScript's getTimezoneOffset returns minutes WEST of UTC (so EST = 300, UTC = 0).
    Local time = UTC - tz_offset_min. Clamps to +-1440 just in case."""
    try: off = int(tz_offset_min)
    except: off = 0
    if off > 1440 or off < -1440: off = 0
    return (datetime.utcnow() - timedelta(minutes=off)).date().isoformat()

def _local_yesterday_iso(tz_offset_min):
    try: off = int(tz_offset_min)
    except: off = 0
    if off > 1440 or off < -1440: off = 0
    return (datetime.utcnow() - timedelta(minutes=off) - timedelta(days=1)).date().isoformat()

async def save_streaks(): await db_save("store", "streaks", streaks)

async def save_streak_progress():
    today_iso = date.today().isoformat()
    pruned = {k: v for k, v in streak_progress.items() if isinstance(v, dict) and (v.get("utc_date") == today_iso or v.get("date") == today_iso)}
    await db_save("store", "streak_progress", pruned)

def get_streak(user, tz_offset_min=None):
    """Returns the user's current streak. If their last_date is older than the
    allowed window (yesterday, or day-before-yesterday if they own streak_48hr),
    the streak is considered broken and count returns 0 for display purposes.
    The stored value is left intact so streak_pass can still revive it."""
    if not user: return {"count": 0, "last_date": ""}
    s = streaks.get(user.lower())
    if not s: return {"count": 0, "last_date": ""}
    count = int(s.get("count", 0))
    last_date = s.get("last_date", "")
    if count <= 0 or not last_date:
        return {"count": 0, "last_date": last_date}
    if tz_offset_min is None:
        tz_offset_min = s.get("tz_offset", 0)
    try: off = int(tz_offset_min)
    except: off = 0
    today = _local_today_iso(off)
    pu = purchases.get(user.lower()) or {}
    allowed_days = 2 if pu.get("streak_48hr") else 1
    base = datetime.utcnow() - timedelta(minutes=off)
    valid = {today}
    for i in range(1, allowed_days + 1):
        valid.add((base - timedelta(days=i)).date().isoformat())
    if last_date not in valid:
        return {"count": 0, "last_date": last_date, "broken": True, "prev_count": count}
    return {"count": count, "last_date": last_date}

def get_streak_progress(user):
    if not user or is_admin(user):
        return {"seconds": STREAK_ACTIVE_SECONDS_REQUIRED, "required": STREAK_ACTIVE_SECONDS_REQUIRED, "earned_today": True}
    ulow = user.lower()
    s = streaks.get(ulow) or {}
    p = streak_progress.get(ulow) or {}
    tz = p.get("locked_tz", s.get("tz_offset", 0))
    today = _local_today_iso(tz)
    earned = s.get("last_date") == today
    seconds = int(p.get("seconds", 0)) if p.get("date") == today else 0
    return {"seconds": seconds, "required": STREAK_ACTIVE_SECONDS_REQUIRED, "earned_today": earned}

async def bump_streak(user, tz_offset_min=0):
    """Advance the user's daily streak counter and pay out the PB reward. Caller
    is responsible for ensuring this should fire (e.g. they've put in the
    required active-tab time for today). 'Today' is the user's LOCAL date
    (midnight-to-midnight in their timezone). Respects the 48hr-window and
    streak-pass shop items."""
    if not user or is_admin(user): return 0
    ulow = user.lower()
    today = _local_today_iso(tz_offset_min)
    s = streaks.get(ulow) or {"count": 0, "last_date": ""}
    if s.get("last_date") == today: return 0
    pu = purchases.get(ulow) or {}
    allowed_days = 2 if pu.get("streak_48hr") else 1
    try: off = int(tz_offset_min)
    except: off = 0
    base = datetime.utcnow() - timedelta(minutes=off)
    valid_prior = {(base - timedelta(days=i)).date().isoformat() for i in range(1, allowed_days + 1)}
    used_pass = False
    if s.get("last_date") in valid_prior:
        new_count = int(s.get("count", 0)) + 1
    else:
        passes = int(pu.get("streak_pass", 0))
        if passes > 0 and int(s.get("count", 0)) > 0:
            pu["streak_pass"] = passes - 1
            purchases[ulow] = pu
            await save_purchases()
            new_count = int(s.get("count", 0)) + 1
            used_pass = True
        else:
            new_count = 1
    reward = min(new_count * 2, STREAK_REWARD_CAP)
    try: stored_tz = int(tz_offset_min)
    except: stored_tz = 0
    streaks[ulow] = {"count": new_count, "last_date": today, "last_reward": reward, "tz_offset": stored_tz}
    credit_pb(user, reward)
    await save_place_bucks()
    await save_streaks()
    payload = {"type": "streak_bonus", "count": new_count, "reward": reward, "used_pass": used_pass, "passes_left": int((purchases.get(ulow) or {}).get("streak_pass", 0))}
    for ws, info in list(clients.items()):
        if info and info.get("username", "").lower() == ulow and not ws.closed:
            try: await ws.send_json(payload)
            except: pass
    for ws, uname in list(social_clients.items()):
        if uname and uname.lower() == ulow and not ws.closed:
            try: await ws.send_json(payload)
            except: pass
    await push_pb_update(user)
    return reward

async def streak_heartbeat_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if is_admin(user):
        return web.json_response({"ok": True, "seconds": STREAK_ACTIVE_SECONDS_REQUIRED, "required": STREAK_ACTIVE_SECONDS_REQUIRED, "earned_today": True})
    try: data = await request.json()
    except: data = {}
    try: tz_offset = int(data.get("tz_offset", 0))
    except: tz_offset = 0
    if tz_offset > 1440 or tz_offset < -1440: tz_offset = 0
    ulow = user.lower()
    mark_active(user)
    utc_today = date.today().isoformat()
    p = streak_progress.get(ulow)
    if not p or p.get("utc_date") != utc_today:
        p = {"date": _local_today_iso(tz_offset), "utc_date": utc_today, "locked_tz": tz_offset, "seconds": 0, "last_hb_ts": 0}
    else:
        if abs(int(p.get("locked_tz", 0)) - tz_offset) > 60:
            tz_offset = int(p.get("locked_tz", 0))
    today = _local_today_iso(tz_offset)
    s = streaks.get(ulow) or {}
    earned_today = s.get("last_date") == today
    if p.get("date") != today:
        p["date"] = today
        p["seconds"] = 0
        p["last_hb_ts"] = 0
    now = time.time()
    if not earned_today and (now - float(p.get("last_hb_ts", 0))) >= (STREAK_HEARTBEAT_PERIOD - 2):
        p["seconds"] = min(int(p["seconds"]) + STREAK_HEARTBEAT_PERIOD, STREAK_ACTIVE_SECONDS_REQUIRED)
        p["last_hb_ts"] = now
        streak_progress[ulow] = p
        await save_streak_progress()
        if p["seconds"] >= STREAK_ACTIVE_SECONDS_REQUIRED:
            await bump_streak(user, tz_offset)
            earned_today = True
    else:
        streak_progress[ulow] = p
    return web.json_response({"ok": True, "seconds": int(p["seconds"]), "required": STREAK_ACTIVE_SECONDS_REQUIRED, "earned_today": earned_today})

async def streak_progress_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    return web.json_response({"ok": True, **get_streak_progress(user)})

user_last_activity = {}
IDLE_REWARD_ACTIVITY_WINDOW = 180

def mark_active(user):
    if not user: return
    user_last_activity[user.lower()] = time.time()

async def push_pb_update(username):
    """Send the current balance/purchases to all live sockets for this user."""
    if not username: return
    payload = {"type": "pb_update", "balance": get_pb(username), "purchases": purchases.get(username.lower()) or {}, "streak": get_streak(username)}
    ulow = username.lower()
    for ws, info in list(clients.items()):
        if info and info.get("username", "").lower() == ulow and not ws.closed:
            try: await ws.send_json(payload)
            except: pass
    for ws, uname in list(social_clients.items()):
        if uname and uname.lower() == ulow and not ws.closed:
            try: await ws.send_json(payload)
            except: pass

async def award_pixel_placement(username, count=1):
    """Increment lifetime pixel count and credit PlaceBucks for each new 100-mark crossed."""
    if not username or count <= 0: return
    mark_active(username)
    ulow = username.lower()
    old = int(lifetime_pixels.get(ulow, 0))
    new = old + count
    lifetime_pixels[ulow] = new
    delta = (new // PB_PIXELS_PER_BUCK) - (old // PB_PIXELS_PER_BUCK)
    if delta > 0:
        place_bucks[ulow] = int(place_bucks.get(ulow, 0)) + delta
        await save_place_bucks()
        await push_pb_update(username)

                                                                                     
dirty_lobbies = set()

def mark_lobby_dirty(lid):
    if lid and lid in lobbies:
        dirty_lobbies.add(lid)

                                                                                           
                                                                                                   
_rate_limits = {}                                            

def check_rate_limit(identity, action, max_count, window_sec):
    if not identity:
        return True
    key = (str(identity).lower(), action)
    now = time.time()
    cutoff = now - window_sec
    timestamps = _rate_limits.setdefault(key, [])
    while timestamps and timestamps[0] < cutoff:
        timestamps.pop(0)
    if len(timestamps) >= max_count:
        return False
    timestamps.append(now)
    return True

def rate_limit_identity(request):
    """Use the username if authenticated, otherwise the client IP, so anonymous abuse is also bounded."""
    return get_auth_user(request) or get_client_ip(request)

TIMELAPSE_WINDOW_SEC = 24 * 60 * 60            
EVENT_STRUCT = struct.Struct('<IHHBB')                                                                                           
EVENT_SIZE = EVENT_STRUCT.size

def append_event(lobby, x, y, new_color, old_color):
    """Append a pixel event to the lobby's timelapse log and prune entries older than 24h."""
    log = lobby.setdefault("events", bytearray())
    now = int(time.time())
    log.extend(EVENT_STRUCT.pack(now, x, y, new_color, old_color))
                                                                          
    cutoff = now - TIMELAPSE_WINDOW_SEC
    drop = 0
    while drop + EVENT_SIZE <= len(log):
        ts, _, _, _, _ = EVENT_STRUCT.unpack_from(log, drop)
        if ts >= cutoff:
            break
        drop += EVENT_SIZE
    if drop > 0:
        del log[:drop]

def set_pixel_author(lobby, x, y, username):
    """Record the last user to write each cell, for the admin hover-to-see-placer tool.
    In-memory only - intentionally NOT persisted (would bloat DB/bandwidth) so it
    resets when the server restarts."""
    lw = lobby.get("width", 256)
    lobby.setdefault("pixel_authors", {})[y * lw + x] = username

def get_oldest_event_time(lobby):
    log = lobby.get("events")
    if not log or len(log) < EVENT_SIZE:
        return None
    return EVENT_STRUCT.unpack_from(log, 0)[0]

def pack_pixel_authors(authors):
    """Compactly serialize {pixel_index: username} to bytes: a username table
    plus 6-byte (idx, name_id) records. Stored as its own binary field so even
    a fully-painted 1024x1024 (~6 MB) stays well under Mongo's 16 MB doc limit.
    Never sent to clients, so it doesn't touch Render bandwidth."""
    if not authors:
        return b""
    names, name_idx, recs = [], {}, bytearray()
    for idx, uname in authors.items():
        if not uname:
            continue
        ni = name_idx.get(uname)
        if ni is None:
            if len(names) >= 65535:
                continue
            ni = len(names); name_idx[uname] = ni; names.append(uname)
        recs += struct.pack("<IH", int(idx), ni)
    out = bytearray()
    out += struct.pack("<I", len(names))
    for n in names:
        nb = n.encode("utf-8")[:255]
        out += struct.pack("<H", len(nb)) + nb
    out += struct.pack("<I", len(recs) // 6)
    out += recs
    return bytes(out)

def unpack_pixel_authors(blob):
    authors = {}
    if not blob:
        return authors
    try:
        b = bytes(blob); o = 0
        (ncount,) = struct.unpack_from("<I", b, o); o += 4
        names = []
        for _ in range(ncount):
            (ln,) = struct.unpack_from("<H", b, o); o += 2
            names.append(b[o:o+ln].decode("utf-8", "replace")); o += ln
        (rcount,) = struct.unpack_from("<I", b, o); o += 4
        for _ in range(rcount):
            idx, ni = struct.unpack_from("<IH", b, o); o += 6
            if 0 <= ni < len(names):
                authors[idx] = names[ni]
    except Exception:
        pass
    return authors

async def save_lobby(lid):
    lobby = lobbies.get(lid)
    if not lobby:
        return

                                                                 
    data = {k: v for k, v in lobby.items() if k not in ("grid", "events", "pixel_authors")}
    grid_bytes = bytes(lobby["grid"])
    events_bytes = bytes(lobby.get("events", b""))
    pauthors_bytes = pack_pixel_authors(lobby.get("pixel_authors") or {})
    await db["lobbies"].update_one({"_id": lid}, {"$set": {"meta": data, "grid": grid_bytes, "events": events_bytes, "pauthors": pauthors_bytes}}, upsert=True)
    dirty_lobbies.discard(lid)

async def save_all_lobbies():
    for lid in list(lobbies.keys()):
        await save_lobby(lid)

async def flush_dirty_lobbies_loop(app):
    """Background task: every 60s, save any lobby that has unsaved pixel changes.
    Worst-case 60s of placements lost on a hard crash (rare), but in-memory state
    is authoritative for live clients so nothing visible to users."""
    while True:
        try:
            await asyncio.sleep(60)
            for lid in list(dirty_lobbies):
                try:
                    await save_lobby(lid)
                except Exception as e:
                    print(f"flush_dirty_lobbies: failed to save {lid}: {e}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"flush_dirty_lobbies_loop error: {e}")

def _total_economy_pb():
    accounts_low = {u.lower() for u in accounts.keys()}
    return sum(int(v) for k, v in place_bucks.items() if not is_admin(k) and k in accounts_low)

async def economy_snapshot_loop(app):
    while True:
        try:
            await asyncio.sleep(ECONOMY_SNAPSHOT_INTERVAL)
            economy_history.append({"ts": int(time.time()), "total": _total_economy_pb()})
            if len(economy_history) > ECONOMY_HISTORY_MAX:
                del economy_history[:len(economy_history) - ECONOMY_HISTORY_MAX]
            await save_economy_history()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"economy_snapshot_loop error: {e}")

async def economy_stats_handler(request):
    return web.json_response({"total": _total_economy_pb(), "history": economy_history[-ECONOMY_HISTORY_MAX:]})

async def idle_reward_loop(app):
    """Every minute, add 60 to each online user's stay_seconds. Credit 1 PB per
    900 (15 min) accumulated. Admins are skipped (unlimited balance anyway).
    stay_seconds is held in memory and only persisted to Mongo every 5 minutes
    (or immediately when a credit fires) - saves ~5x the writes for a doc that
    only matters across restarts. The dict is rewritten in full each time, so
    avoiding 4-of-5 unnecessary saves is a real bandwidth win."""
    save_throttle = 0
    while True:
        try:
            await asyncio.sleep(60)
            online_users = set()
            for info in clients.values():
                if not info or info.get("guest"): continue
                n = info.get("username")
                if n and not is_admin(n): online_users.add(n)
            for uname in social_clients.values():
                if uname and not is_admin(uname): online_users.add(uname)
            if not online_users:
                save_throttle += 1
                continue
            credited_anything = False
            now_ts = time.time()
            for uname in online_users:
                ulow = uname.lower()
                last_active = user_last_activity.get(ulow, 0)
                if now_ts - last_active > IDLE_REWARD_ACTIVITY_WINDOW:
                    continue
                stay_seconds[ulow] = int(stay_seconds.get(ulow, 0)) + 60
                if stay_seconds[ulow] >= IDLE_REWARD_SECONDS:
                    rewards = stay_seconds[ulow] // IDLE_REWARD_SECONDS
                    stay_seconds[ulow] = stay_seconds[ulow] % IDLE_REWARD_SECONDS
                    credit_pb(uname, rewards)
                    await push_pb_update(uname)
                    credited_anything = True
            save_throttle += 1
            if credited_anything:
                await save_place_bucks()
                await save_stay_seconds()
                save_throttle = 0
            elif save_throttle >= 5:
                await save_stay_seconds()
                save_throttle = 0
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"idle_reward_loop error: {e}")

async def rate_limit_cleanup_loop(app):
    """Periodically prune empty/expired entries from the rate limiter so it doesn't grow unbounded."""
    while True:
        try:
            await asyncio.sleep(300)                   
            now = time.time()
                                                                                                            
            stale_cutoff = now - 3600
            for key in list(_rate_limits.keys()):
                ts = _rate_limits[key]
                while ts and ts[0] < stale_cutoff:
                    ts.pop(0)
                if not ts:
                    _rate_limits.pop(key, None)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"rate_limit_cleanup_loop error: {e}")

async def delete_lobby_db(lid):
    await db["lobbies"].delete_one({"_id": lid})

async def save_dm(key): await db_save("dms", key, dms.get(key, []))
async def save_dm_last_seen(): await db_save("store", "dm_last_seen", dm_last_seen)

def mark_dm_seen(user, peer):
    ulow = user.lower()
    plow = peer.lower()
    if ulow not in dm_last_seen:
        dm_last_seen[ulow] = {}
    dm_last_seen[ulow][plow] = time.time()

def get_unread_dm_summary(user):
    """Return a list of {from, count, last_text, last_time} for threads with unread peer messages."""
    ulow = user.lower()
    seen_map = dm_last_seen.get(ulow, {})
                                              
    senders = {}                                                      
    for key, msgs in dms.items():
        parts = key.split(":")
        if ulow not in parts:
            continue
        peer_lower = parts[0] if parts[1] == ulow else parts[1]
        last_seen = seen_map.get(peer_lower, 0)
        unread = [m for m in msgs if m.get("from", "").lower() != ulow and m.get("time", 0) > last_seen]
        if unread:
            last = unread[-1]
            senders[last["from"]] = {
                "from": last["from"],
                "count": len(unread),
                "last_text": last.get("text") or ("[image]" if (last.get("image_url") or last.get("image")) else ""),
                "last_time": last.get("time", 0),
            }
    return list(senders.values())

async def track_ip(username, request):
    ip = get_client_ip(request)
    if ip and username:
        user_ips[username] = ip
        await save_user_ips()

async def load_all_data():
    global accounts, friends_data, bans, ip_bans, vips, ranks, user_ips, fake_admins, brush_perms, clans, groups, group_messages, dms, dm_last_seen, place_bucks, lifetime_pixels, purchases, stay_seconds, streaks

    accounts = await db_load("store", "accounts") or {}
    friends_data = await db_load("store", "friends") or {}
    bans = await db_load("store", "bans") or []
    ip_bans = await db_load("store", "ip_bans") or []
    global device_bans, user_devices, moderators, mod_ban_log
    device_bans = await db_load("store", "device_bans") or []
    user_devices = await db_load("store", "user_devices") or {}
    moderators = await db_load("store", "moderators") or []
    mod_ban_log = await db_load("store", "mod_ban_log") or []
    vips = await db_load("store", "vips") or []
    ranks = await db_load("store", "ranks") or {}
    global name_colors, last_online, reports
    name_colors = await db_load("store", "name_colors") or {}
    last_online = await db_load("store", "last_online") or {}
    reports = await db_load("store", "reports") or []
    fake_admins = await db_load("store", "fake_admins") or []
    brush_perms = await db_load("store", "brush_perms") or {}
    clans = await db_load("store", "clans") or {}
    groups = await db_load("store", "groups") or {}
    async for doc in db["group_messages"].find():
        group_messages[doc["_id"]] = doc.get("data", [])
    dm_last_seen = await db_load("store", "dm_last_seen") or {}
    place_bucks = await db_load("store", "place_bucks") or {}
    lifetime_pixels = await db_load("store", "lifetime_pixels") or {}
    purchases = await db_load("store", "purchases") or {}
    streaks = await db_load("store", "streaks") or {}
    global streak_progress
    raw_progress = await db_load("store", "streak_progress") or {}
    today_iso = date.today().isoformat()
    streak_progress = {k: v for k, v in raw_progress.items() if isinstance(v, dict) and v.get("date") == today_iso}
    stay_seconds = await db_load("store", "stay_seconds") or {}
    global economy_history
    economy_history = await db_load("store", "economy_history") or []
    user_ips = await db_load("store", "user_ips") or {}

    for i, pl in enumerate(PUBLIC_LOBBIES):
        lid = f"public_{i}"
        w, h = pl["width"], pl["height"]
        lobbies[lid] = {
            "id": lid, "name": pl["name"], "owner": "toothpaste", "public": True,
            "code": None, "whitelist_enabled": False, "whitelist": [],
            "grid": bytearray(w * h), "pixel_counts": {},
            "cooldown": pl["cooldown"], "width": w, "height": h
        }

    def _to_bytearray(data):
        if data is None:
            return None

        try:
            return bytearray(data)
        except Exception:
            return None

    async for doc in db["lobbies"].find():
        lid = doc["_id"]
        meta = doc.get("meta", {})
        grid_data = doc.get("grid")
        events_data = doc.get("events")
        grid_ba = _to_bytearray(grid_data)
        events_ba = bytearray(events_data) if events_data else bytearray()
        pauthors = unpack_pixel_authors(doc.get("pauthors"))
        if lid.startswith("public_") and lid in lobbies:
            expected_size = lobbies[lid]["width"] * lobbies[lid]["height"]
            if "pixel_counts" in meta:
                lobbies[lid]["pixel_counts"] = meta["pixel_counts"]
            if grid_ba is not None and len(grid_ba) == expected_size:
                lobbies[lid]["grid"] = grid_ba
            lobbies[lid]["events"] = events_ba
            lobbies[lid]["pixel_authors"] = pauthors
        elif lid.startswith("public_") and lid not in lobbies:
            continue
        else:
            lw = meta.get("width", 256)
            lh = meta.get("height", 256)
            meta["grid"] = grid_ba if grid_ba is not None else bytearray(lw * lh)
            meta["events"] = events_ba
            meta["pixel_authors"] = pauthors
            if "pixel_counts" not in meta:
                meta["pixel_counts"] = {}
            if "cooldown" not in meta:
                meta["cooldown"] = DEFAULT_COOLDOWN
            lobbies[lid] = meta

    async for doc in db["dms"].find():
        dms[doc["_id"]] = doc.get("data", [])

    print(f"Loaded: {len(accounts)} accounts, {len(lobbies)} lobbies, {len(friends_data)} friend entries")

def generate_captcha_svg(text):
    width, height = 200, 70
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">']
    parts.append(f'<rect width="{width}" height="{height}" fill="#0a1a3a"/>')
    for _ in range(6):
        x1, y1 = random.randint(0, width), random.randint(0, height)
        x2, y2 = random.randint(0, width), random.randint(0, height)
        c = f"#{random.randint(30,80):02x}{random.randint(30,80):02x}{random.randint(80,140):02x}"
        parts.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{c}" stroke-width="2"/>')
    for _ in range(40):
        cx, cy = random.randint(0, width), random.randint(0, height)
        c = f"#{random.randint(40,100):02x}{random.randint(40,100):02x}{random.randint(80,160):02x}"
        parts.append(f'<circle cx="{cx}" cy="{cy}" r="{random.uniform(1,3):.1f}" fill="{c}"/>')
    spacing = width / (len(text) + 1)
    fonts = ["serif", "sans-serif", "monospace"]
    for i, ch in enumerate(text):
        x = spacing * (i + 1) + random.uniform(-5, 5)
        y = height / 2 + random.uniform(-8, 8)
        angle = random.uniform(-25, 25)
        size = random.randint(28, 38)
        font = random.choice(fonts)
        sx, sy2 = random.uniform(0.85, 1.15), random.uniform(0.85, 1.15)
        c = f"#{random.randint(180,255):02x}{random.randint(180,255):02x}{random.randint(50,150):02x}"
        parts.append(f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" font-family="{font}" fill="{c}" text-anchor="middle" dominant-baseline="central" transform="rotate({angle:.1f},{x:.1f},{y:.1f}) scale({sx:.2f},{sy2:.2f})">{ch}</text>')
    for _ in range(3):
        x0, y0 = random.randint(0, width), random.randint(0, height)
        cx1, cy1 = random.randint(0, width), random.randint(0, height)
        cx2, cy2 = random.randint(0, width), random.randint(0, height)
        x3, y3 = random.randint(0, width), random.randint(0, height)
        c = f"#{random.randint(60,120):02x}{random.randint(40,80):02x}{random.randint(80,160):02x}"
        parts.append(f'<path d="M{x0},{y0} C{cx1},{cy1} {cx2},{cy2} {x3},{y3}" stroke="{c}" stroke-width="1.5" fill="none"/>')
    parts.append('</svg>')
    return ''.join(parts)

async def index_handler(request):
    return web.FileResponse("index.html")

async def health_handler(request):

    return web.json_response({"ok": True})

async def captcha_handler(request):
    clean_captchas()
    chars = string.ascii_uppercase.replace('O', '').replace('I', '').replace('L', '')
    text = ''.join(random.choices(chars, k=5))
    cid = secrets.token_hex(8)
    captchas[cid] = {"answer": text, "expires": time.time() + 300}
    svg_b64 = base64.b64encode(generate_captcha_svg(text).encode()).decode()
    return web.json_response({"id": cid, "image": f"data:image/svg+xml;base64,{svg_b64}"})

async def register_handler(request):
    data = await request.json()
    if not check_rate_limit(get_client_ip(request), "register", 5, 600):
        return web.json_response({"error": "Too many registration attempts - try again later."}, status=429)
    uname, pwd = data.get("username", "").strip(), data.get("password", "")
    cap_id, cap_ans = data.get("captcha_id", ""), data.get("captcha_answer", "")
    if not uname or not pwd:
        return web.json_response({"error": "Username and password required"}, status=400)
    if len(uname) < 3 or len(uname) > 20 or not uname.isalnum():
        return web.json_response({"error": "Username must be 3-20 alphanumeric characters"}, status=400)
    if len(pwd) < 4:
        return web.json_response({"error": "Password must be at least 4 characters"}, status=400)
    if is_banned(uname) or is_ip_banned(request):
        return web.json_response({"error": "This account is banned"}, status=403)
    cap = captchas.pop(cap_id, None)
    if not cap or cap["expires"] < time.time():
        return web.json_response({"error": "Captcha expired, get a new one"}, status=400)
    if cap_ans.strip().upper() != cap["answer"]:
        return web.json_response({"error": "Wrong captcha answer"}, status=400)
    if uname.lower() in {u.lower() for u in accounts}:
        return web.json_response({"error": "Username already taken"}, status=400)
    pw_hash, salt = hash_password(pwd)
    accounts[uname] = {"password_hash": pw_hash, "salt": salt}
    await save_accounts()
    token = secrets.token_hex(16)
    sessions[token] = uname
    await track_ip(uname, request)
    return web.json_response({"ok": True, "token": token, "username": uname})

async def auth_suggest_mode_handler(request):
    ip = get_client_ip(request)

    existing = any(v == ip for v in user_ips.values()) if ip else False
    return web.json_response({"mode": "login" if existing else "register"})

async def login_handler(request):
    data = await request.json()
    if not check_rate_limit(get_client_ip(request), "login", 10, 60):
        return web.json_response({"error": "Too many login attempts - try again in a minute."}, status=429)
    uname, pwd = data.get("username", "").strip(), data.get("password", "")
    if not uname or not pwd:
        return web.json_response({"error": "Username and password required"}, status=400)
    if is_banned(uname) or is_ip_banned(request):
        return web.json_response({"error": "This account is banned"}, status=403)
    found = next((u for u in accounts if u.lower() == uname.lower()), None)
    if not found:
        return web.json_response({"error": "Invalid username or password"}, status=400)
    acc = accounts[found]
    h, _ = hash_password(pwd, acc["salt"])
    if h != acc["password_hash"]:
        return web.json_response({"error": "Invalid username or password"}, status=400)
    token = secrets.token_hex(16)
    sessions[token] = found
    await track_ip(found, request)
    return web.json_response({"ok": True, "token": token, "username": found})

async def lobbies_handler(request):
    return web.json_response({"lobbies": [lobby_info(l) for l in lobbies.values() if l["public"]]})

async def my_lobbies_handler(request):
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    mine = [lobby_info(l, True) for l in lobbies.values() if l["owner"] and l["owner"].lower() == user.lower() and not l["id"].startswith("public_")]
    whitelisted = [lobby_info(l) for l in lobbies.values()
                   if not l["id"].startswith("public_") and l.get("whitelist_enabled")
                   and user in l.get("whitelist", []) and (not l["owner"] or l["owner"].lower() != user.lower())]
    return web.json_response({"lobbies": mine, "whitelisted": whitelisted})

async def lobby_preview_handler(request):
    """Lightweight grid snapshot for the homepage's background canvas. Returns
    gzipped raw grid bytes (one palette index per pixel) for one of the public
    lobbies. Browser decompresses via Content-Encoding header. Headers carry
    the width/height. No auth required - public info already exposed via join."""
    lid = request.query.get("id", "public_2")
    lobby = lobbies.get(lid)
    if not lobby:
        return web.Response(status=404, text="Lobby not found")
    grid_bytes = bytes(lobby["grid"])
    compressed = gzip.compress(grid_bytes, compresslevel=6)
    return web.Response(
        body=compressed,
        headers={
            "Content-Type": "application/octet-stream",
            "Content-Encoding": "gzip",
            "X-Width": str(lobby.get("width", 256)),
            "X-Height": str(lobby.get("height", 256)),
            "Cache-Control": "public, max-age=60",
        }
    )

async def lobby_timelapse_handler(request):
    """Return the raw event log for a lobby (for the last 24h). The client reconstructs the
    start state by reverse-applying events from the current grid, then plays them forward."""
    lid = request.query.get("id", "")
    lobby = lobbies.get(lid)
    if not lobby:
        return web.json_response({"error": "Not found"}, status=404)
    log = lobby.get("events", b"") or b""
    oldest = get_oldest_event_time(lobby)
    now = int(time.time())
    has_full_24h = oldest is not None and (now - oldest) >= TIMELAPSE_WINDOW_SEC - 60                    
    return web.Response(
        body=bytes(log),
        headers={
            "Content-Type": "application/octet-stream",
            "X-Width": str(lobby.get("width", 256)),
            "X-Height": str(lobby.get("height", 256)),
            "X-Now": str(now),
            "X-Oldest": str(oldest) if oldest is not None else "",
            "X-Full-24h": "1" if has_full_24h else "0",
            "X-Event-Count": str(len(log) // EVENT_SIZE),
        }
    )

async def lobby_detail_handler(request):
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    lid = request.query.get("id", "")
    lobby = lobbies.get(lid)
    if not lobby:
        return web.json_response({"error": "Not found"}, status=404)
    if lobby["owner"].lower() != user.lower() and not is_admin(user):
        return web.json_response({"error": "Forbidden"}, status=403)
    return web.json_response({"lobby": lobby_info(lobby, True)})

async def create_lobby_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    if not check_rate_limit(user, "lobby_create", 3, 60):
        return web.json_response({"error": "Slow down - max 3 lobby creates per minute."}, status=429)
    name = data.get("name", "").strip()[:30]
    is_public = data.get("public", False)
    wl = data.get("whitelist_enabled", False)
    try: cooldown = max(0, min(MAX_COOLDOWN, float(data.get("cooldown", DEFAULT_COOLDOWN))))
    except: cooldown = DEFAULT_COOLDOWN
    try: lw = int(data.get("width", 256))
    except: lw = 256
    try: lh = int(data.get("height", 256))
    except: lh = 256
    if (lw, lh) not in VALID_SIZES:
        lw, lh = 256, 256
    if not name:
        return web.json_response({"error": "Lobby name required"}, status=400)
    if user_lobby_count(user) >= MAX_LOBBIES_PER_USER:
        return web.json_response({"error": f"Max {MAX_LOBBIES_PER_USER} lobbies"}, status=400)
                                                                                
    cost = LOBBY_PRICES.get((lw, lh), 0)
    if cost > 0:
        if not spend_pb(user, cost):
            return web.json_response({"error": f"Need {cost} PlaceBucks for a {lw}x{lh} lobby (you have {get_pb(user)})"}, status=400)
        await save_place_bucks()
        await push_pb_update(user)
    lid = secrets.token_hex(6)
    code = secrets.token_hex(4).upper() if not is_public else None
    lobbies[lid] = {
        "id": lid, "name": name, "owner": user, "public": is_public,
        "code": code, "whitelist_enabled": wl,
        "whitelist": [user] if wl else [],
        "grid": bytearray(lw * lh), "pixel_counts": {},
        "cooldown": cooldown, "last_activity": time.time(),
        "width": lw, "height": lh
    }
    await save_lobby(lid)
    return web.json_response({"ok": True, "lobby": lobby_info(lobbies[lid], True)})

async def delete_lobby_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    lid = data.get("lobby_id", "")
    lobby = lobbies.get(lid)
    if not lobby or lid.startswith("public_"):
        return web.json_response({"error": "Not found"}, status=404)
    if lobby["owner"].lower() != user.lower():
        return web.json_response({"error": "Not yours"}, status=403)
    for ws, info in list(clients.items()):
        if info and info.get("lobby_id") == lid:
            try: await ws.send_json({"type": "kicked", "text": "Lobby deleted"}); await ws.close()
            except: pass
    del lobbies[lid]
    await delete_lobby_db(lid)
    return web.json_response({"ok": True})

async def update_lobby_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    lid = data.get("lobby_id", "")
    lobby = lobbies.get(lid)
    if not lobby:
        return web.json_response({"error": "Not found"}, status=404)
    if lobby["owner"].lower() != user.lower() and not is_admin(user):
        return web.json_response({"error": "Not yours"}, status=403)
                                                            
    if lid.startswith("public_"):
        if "lobby_unban" in data:
            n = data["lobby_unban"].strip()
            if n:
                lb = lobby.get("lobby_bans", [])
                lobby["lobby_bans"] = [b for b in lb if b.lower() != n.lower()]
                await save_lobby(lid)
        info = lobby_info(lobby, True)
        info["lobby_bans"] = lobby.get("lobby_bans", [])
        return web.json_response({"ok": True, "lobby": info})
    if "public" in data:
        lobby["public"] = bool(data["public"])
        if lobby["public"]:
            lobby["code"] = None
        elif not lobby["code"]:
            lobby["code"] = secrets.token_hex(4).upper()
    if "whitelist_enabled" in data:
        lobby["whitelist_enabled"] = bool(data["whitelist_enabled"])
        if lobby["whitelist_enabled"] and user not in lobby["whitelist"]: lobby["whitelist"].append(user)
    if "add_whitelist" in data and lobby["whitelist_enabled"]:
        n = data["add_whitelist"].strip()
        if n and n not in lobby["whitelist"]: lobby["whitelist"].append(n)
    if "remove_whitelist" in data and lobby["whitelist_enabled"]:
        n = data["remove_whitelist"].strip()
        if n in lobby["whitelist"] and n.lower() != user.lower(): lobby["whitelist"].remove(n)
    if "lobby_unban" in data:
        n = data["lobby_unban"].strip()
        if n:
            lb = lobby.get("lobby_bans", [])
            lobby["lobby_bans"] = [b for b in lb if b.lower() != n.lower()]
    if "name" in data: lobby["name"] = data["name"].strip()[:30] or lobby["name"]
    await save_lobby(lid)
    info = lobby_info(lobby, True)
    info["lobby_bans"] = lobby.get("lobby_bans", [])
    return web.json_response({"ok": True, "lobby": info})

async def join_lobby_by_code_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    code = data.get("code", "").strip().upper()
    for lobby in lobbies.values():
        if lobby.get("code") and lobby["code"] == code:
            if lobby["whitelist_enabled"] and user not in lobby["whitelist"]:
                return web.json_response({"error": "Not whitelisted"}, status=403)
            return web.json_response({"ok": True, "lobby": lobby_info(lobby)})
    return web.json_response({"error": "Invalid code"}, status=404)

async def leaderboard_handler(request):
    lid = request.query.get("lobby_id", "")
    lobby = lobbies.get(lid)
    if not lobby:
        return web.json_response({"error": "Not found"}, status=404)
    pc = lobby.get("pixel_counts", {})
    top = sorted(pc.items(), key=lambda x: x[1], reverse=True)[:50]
    return web.json_response({
        "leaderboard": [{"name": n, "pixels": c, "online": is_online(n)} for n, c in top],
        "original_owner": lobby.get("original_owner"),
    })

async def friends_list_handler(request):
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    fd = get_friend_data(user)
    def friend_entry(f):
        online = is_online(f)
        p = presence.get(f.lower()) or {}
        return {
            "name": f,
            "online": online,
            "afk": bool(p.get("afk")) if online else False,
            "afk_since": p.get("since") if online and p.get("afk") else None,
            "last_online": last_online.get(f.lower()),
        }
    return web.json_response({"friends": [friend_entry(f) for f in fd["friends"]], "incoming": fd["incoming"], "outgoing": fd["outgoing"]})

async def friend_add_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not check_rate_limit(user, "friend_add", 10, 60):
        return web.json_response({"error": "Too many friend requests - try again in a minute."}, status=429)
    target = data.get("username", "").strip()
    if not target: return web.json_response({"error": "Username required"}, status=400)
    found = next((u for u in accounts if u.lower() == target.lower()), None)
    if not found: return web.json_response({"error": "User not found"}, status=404)
    if found.lower() == user.lower(): return web.json_response({"error": "Can't add yourself"}, status=400)
    fd, td = get_friend_data(user), get_friend_data(found)
    if found in fd["friends"]: return web.json_response({"error": "Already friends"}, status=400)
    if found in fd["outgoing"]: return web.json_response({"error": "Already sent"}, status=400)
    if user in td["outgoing"]:
        td["outgoing"].remove(user)
        if user in fd["incoming"]: fd["incoming"].remove(user)
        fd["friends"].append(found); td["friends"].append(user)
        await save_friends()
        await notify_social(found, {"type": "friend_accepted", "username": user})
        return web.json_response({"ok": True, "accepted": True})
    fd["outgoing"].append(found); td["incoming"].append(user)
    await save_friends()
    await notify_social(found, {"type": "friend_request", "username": user})
    return web.json_response({"ok": True, "sent": True})

async def friend_accept_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    target = data.get("username", "").strip()
    fd = get_friend_data(user)
    if target not in fd["incoming"]: return web.json_response({"error": "No request"}, status=400)
    td = get_friend_data(target)
    fd["incoming"].remove(target)
    if user in td["outgoing"]: td["outgoing"].remove(user)
    fd["friends"].append(target); td["friends"].append(user)
    await save_friends()
    await notify_social(target, {"type": "friend_accepted", "username": user})
    return web.json_response({"ok": True})

async def friend_decline_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    target = data.get("username", "").strip()
    fd, td = get_friend_data(user), get_friend_data(target)
    if target in fd["incoming"]: fd["incoming"].remove(target)
    if user in td["outgoing"]: td["outgoing"].remove(user)
    await save_friends()
    return web.json_response({"ok": True})

async def friend_remove_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    target = data.get("username", "").strip()
    fd, td = get_friend_data(user), get_friend_data(target)
    if target in fd["friends"]: fd["friends"].remove(target)
    if user in td["friends"]: td["friends"].remove(user)
    await save_friends()
    return web.json_response({"ok": True})

async def dm_history_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    target = request.query.get("with", "")
    msgs = dms.get(dm_key(user, target), [])[-MAX_DM_HISTORY:]
    peer_last_seen = 0
    if target:
        mark_dm_seen(user, target)
        await save_dm_last_seen()
        peer_last_seen = dm_last_seen.get(target.lower(), {}).get(user.lower(), 0)
    return web.json_response({"messages": msgs, "peer_last_seen": peer_last_seen})

async def dm_unread_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    return web.json_response({"senders": get_unread_dm_summary(user)})

async def upload_image_handler(request):
    """Browser-CORS-safe image upload proxy. Forwards the file to catbox.moe and returns the URL.
    The image bytes pass through this server in transit but are NOT stored anywhere - neither on
    disk nor in MongoDB. Only the resulting catbox URL is later persisted in DM/group history."""
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if is_banned(user) or is_ip_banned(request):
        return web.json_response({"error": "Forbidden"}, status=403)
    if not check_rate_limit(user, "upload_image", 10, 60):
        return web.json_response({"error": "Upload rate limit - max 10 images per minute."}, status=429)
    if not check_rate_limit(get_client_ip(request), "upload_image_ip", 20, 60):
        return web.json_response({"error": "Upload rate limit - too many uploads from your IP."}, status=429)
    try:
        reader = await request.multipart()
    except Exception:
        return web.json_response({"error": "Invalid multipart"}, status=400)
    field = await reader.next()
    if field is None or field.name != "file":
        return web.json_response({"error": "Missing file field"}, status=400)
                                                                                          
    MAX = 6 * 1024 * 1024
    chunks = []
    total = 0
    while True:
        chunk = await field.read_chunk(64 * 1024)
        if not chunk: break
        total += len(chunk)
        if total > MAX:
            return web.json_response({"error": "File too large (max 6 MB)"}, status=413)
        chunks.append(chunk)
    data = b"".join(chunks)
    if not data:
        return web.json_response({"error": "Empty file"}, status=400)
    filename = field.filename or "upload.jpg"
                                                                                 
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            form = aiohttp.FormData()
            form.add_field("reqtype", "fileupload")
            form.add_field("fileToUpload", data, filename=filename, content_type="application/octet-stream")
            async with session.post("https://catbox.moe/user/api.php", data=form) as resp:
                if resp.status != 200:
                    return web.json_response({"error": f"catbox returned {resp.status}"}, status=502)
                url = (await resp.text()).strip()
                if not url.startswith("https://files.catbox.moe/"):
                    return web.json_response({"error": "unexpected upload response: " + url[:100]}, status=502)
                return web.json_response({"url": url})
    except Exception as e:
        return web.json_response({"error": "upload failed: " + str(e)[:200]}, status=502)

async def dm_send_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    target = data.get("to", "").strip()
    text = data.get("text", "").strip()[:200]
    image_url = (data.get("image_url") or "").strip()[:500]
    if image_url and not is_safe_image_url(image_url): image_url = ""
    if not target or (not text and not image_url): return web.json_response({"error": "Missing fields"}, status=400)
    if not check_rate_limit(user, "dm", 5, 5):
        return web.json_response({"error": "Rate limited - max 5 DMs per 5 seconds."}, status=429)
    if image_url and not check_rate_limit(user, "dm_image", 5, 60):
        return web.json_response({"error": "Image rate limit - max 5 images per minute."}, status=429)
    fd = get_friend_data(user)
    if target not in fd["friends"]: return web.json_response({"error": "Not friends"}, status=403)
    key = dm_key(user, target)
    msg = {"from": user, "time": time.time()}
    if text: msg["text"] = text
    if image_url: msg["image_url"] = image_url
    dms.setdefault(key, []).append(msg)
    if len(dms[key]) > MAX_DM_HISTORY: dms[key] = dms[key][-MAX_DM_HISTORY:]
    await save_dm(key)
    payload = {"type": "dm", "from": user, "time": msg["time"]}
    if text: payload["text"] = text
    if image_url: payload["image_url"] = image_url
    await notify_social(target, payload)
    return web.json_response({"ok": True})

async def admin_accounts_handler(request):
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    return web.json_response({"accounts": accounts})

async def admin_friends_handler(request):
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    return web.json_response({"friends": friends_data})

async def admin_lobbies_handler(request):
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    skip = {"grid", "events", "pixel_authors"}
    out = {}
    for lid, l in lobbies.items():
        entry = {k: v for k, v in l.items() if k not in skip}
        entry["grid_bytes"] = len(l.get("grid", b""))
        entry["events_bytes"] = len(l.get("events", b""))
        entry["pixel_authors_count"] = len(l.get("pixel_authors", {}) or {})
        out[lid] = entry
    return web.json_response({"lobbies": out})

async def admin_bans_handler(request):
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    return web.json_response({"bans": bans})

async def admin_ips_handler(request):
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    return web.json_response({"ips": user_ips})

async def admin_vips_handler(request):
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    return web.json_response({"vips": vips})

async def admin_ban_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = data.get("username", "").strip()
    if not target: return web.json_response({"error": "Username required"}, status=400)
    if is_admin(target): return web.json_response({"error": "Cannot ban admin"}, status=400)
    if not is_banned(target):
        bans.append(target)
        await save_bans()
    for tok in [t for t, u in sessions.items() if u.lower() == target.lower()]:
        del sessions[tok]
    for ws, info in list(clients.items()):
        if info and not info.get("guest") and info.get("username", "").lower() == target.lower():
            try: await ws.send_json({"type": "kicked", "text": "You have been banned"}); await ws.close()
            except: pass
    for ws, uname in list(social_clients.items()):
        if uname and uname.lower() == target.lower():
            try: await ws.close()
            except: pass
    return web.json_response({"ok": True, "message": f"Banned {target}"})

async def admin_unban_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = data.get("username", "").strip()
    bans[:] = [b for b in bans if b.lower() != target.lower()]
    await save_bans()
    return web.json_response({"ok": True, "message": f"Unbanned {target}"})

async def admin_mod_add_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = (data.get("username") or "").strip()
    if not target: return web.json_response({"error": "Username required"}, status=400)
    found = next((u for u in accounts if u.lower() == target.lower()), None)
    if not found: return web.json_response({"error": "User not found"}, status=404)
    if is_admin(found): return web.json_response({"error": "Cannot promote the owner"}, status=400)
    if found.lower() not in moderators:
        moderators.append(found.lower())
        await save_moderators()
    try: await notify_social(found, {"type": "mod_status", "is_moderator": True})
    except: pass
    return web.json_response({"ok": True, "message": f"Granted MOD to {found}"})

async def admin_mod_remove_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = (data.get("username") or "").strip().lower()
    if not target: return web.json_response({"error": "Username required"}, status=400)
    moderators[:] = [m for m in moderators if m != target]
    await save_moderators()
    try: await notify_social(target, {"type": "mod_status", "is_moderator": False})
    except: pass
    return web.json_response({"ok": True, "message": f"Revoked MOD from {target}"})

async def admin_mod_list_handler(request):
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    return web.json_response({"moderators": moderators})

async def admin_mod_ban_log_handler(request):
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    return web.json_response({"log": mod_ban_log[-100:]})

async def _mod_school_ban_action(moderator_user, target):
    """Soft ban: account banned + push a school_ban event to every live socket of the target so
    their client sets a localStorage flag and redirects. IP and device are NOT touched so other
    people on the same WiFi / Chromebook are unaffected. Also invalidates the user's session
    tokens so refreshing the page can't bypass — any future API call with an old token gets
    rejected, which forces a fresh login (which is also blocked since the username is banned)."""
    found = next((u for u in accounts if u.lower() == target.lower()), None)
    if not found: return None, "User not found"
    if is_admin(found): return None, "Cannot ban the owner"
    if is_moderator(found) and not is_admin(moderator_user): return None, "Cannot ban another moderator"
    if not is_banned(found):
        bans.append(found)
        await save_bans()
    tlow = found.lower()
    for tok in [t for t, u in sessions.items() if u.lower() == tlow]:
        sessions.pop(tok, None)
    payload = {"type": "client_school_ban", "by": moderator_user, "url": "https://www.pornhub.com"}
    booted = 0
    for ws, info in list(clients.items()):
        if info and info.get("username", "").lower() == tlow:
            try: await ws.send_json(payload); await ws.close(); booted += 1
            except: pass
    for ws, uname in list(social_clients.items()):
        if uname and uname.lower() == tlow:
            try: await ws.send_json(payload); await ws.close(); booted += 1
            except: pass
    entry = {"ts": int(time.time()), "moderator": moderator_user, "target": found, "type": "school_ban", "booted": booted}
    mod_ban_log.append(entry)
    if len(mod_ban_log) > MOD_BAN_LOG_MAX: del mod_ban_log[:len(mod_ban_log) - MOD_BAN_LOG_MAX]
    await save_mod_ban_log()
    return entry, None

async def _mod_regular_ban_action(moderator_user, target):
    """Hard ban: account banned + IP banned + device banned + push client_redirect to Google."""
    found = next((u for u in accounts if u.lower() == target.lower()), None)
    if not found: return None, "User not found"
    if is_admin(found): return None, "Cannot ban the owner"
    if is_moderator(found) and not is_admin(moderator_user): return None, "Cannot ban another moderator"
    if not is_banned(found):
        bans.append(found)
        await save_bans()
    ip = user_ips.get(found)
    if ip and ip not in ip_bans:
        ip_bans.append(ip)
        await save_ip_bans()
    did = user_devices.get(found.lower())
    if did and did not in device_bans:
        device_bans.append(did)
        await save_device_bans()
    tlow = found.lower()
    for tok in [t for t, u in sessions.items() if u.lower() == tlow]:
        sessions.pop(tok, None)
    payload = {"type": "client_redirect", "url": "https://www.pornhub.com"}
    booted = 0
    for ws, info in list(clients.items()):
        if info and info.get("username", "").lower() == tlow:
            try: await ws.send_json(payload); await ws.close(); booted += 1
            except: pass
    for ws, uname in list(social_clients.items()):
        if uname and uname.lower() == tlow:
            try: await ws.send_json(payload); await ws.close(); booted += 1
            except: pass
    entry = {"ts": int(time.time()), "moderator": moderator_user, "target": found, "type": "regular_ban", "ip": ip, "device_id": did, "booted": booted}
    mod_ban_log.append(entry)
    if len(mod_ban_log) > MOD_BAN_LOG_MAX: del mod_ban_log[:len(mod_ban_log) - MOD_BAN_LOG_MAX]
    await save_mod_ban_log()
    return entry, None

async def _mod_unban_action(moderator_user, target):
    """Reverses both school and regular bans: removes from bans, ip_bans, device_bans
    (using their last-known IP / device id). Idempotent — fine to call on a user that
    isn't banned, returns the entry showing what (if anything) was undone."""
    found = next((u for u in accounts if u.lower() == target.lower()), None)
    if not found: return None, "User not found"
    if is_admin(found): return None, "Cannot unban the owner (not banned anyway)"
    removed_account = False
    removed_ip = None
    removed_device = None
    if found in bans:
        bans.remove(found)
        removed_account = True
        await save_bans()
    ip = user_ips.get(found)
    if ip and ip in ip_bans:
        ip_bans.remove(ip)
        removed_ip = ip
        await save_ip_bans()
    did = user_devices.get(found.lower())
    if did and did in device_bans:
        device_bans.remove(did)
        removed_device = did
        await save_device_bans()
    entry = {"ts": int(time.time()), "moderator": moderator_user, "target": found, "type": "unban",
             "removed_account": removed_account, "removed_ip": removed_ip, "removed_device": removed_device}
    mod_ban_log.append(entry)
    if len(mod_ban_log) > MOD_BAN_LOG_MAX: del mod_ban_log[:len(mod_ban_log) - MOD_BAN_LOG_MAX]
    await save_mod_ban_log()
    return entry, None

async def mod_unban_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not (is_admin(user) or is_moderator(user)):
        return web.json_response({"error": "Forbidden"}, status=403)
    target = (data.get("username") or "").strip()
    if not target: return web.json_response({"error": "Username required"}, status=400)
    entry, err = await _mod_unban_action(user, target)
    if err: return web.json_response({"error": err}, status=400)
    parts = []
    if entry["removed_account"]: parts.append("account")
    if entry["removed_ip"]: parts.append(f"IP {entry['removed_ip']}")
    if entry["removed_device"]: parts.append("device")
    summary = ", ".join(parts) if parts else "nothing (wasn't banned)"
    return web.json_response({"ok": True, "message": f"Unbanned {entry['target']}: removed {summary}", "entry": entry})

async def mod_school_ban_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not (is_admin(user) or is_moderator(user)):
        return web.json_response({"error": "Forbidden"}, status=403)
    target = (data.get("username") or "").strip()
    if not target: return web.json_response({"error": "Username required"}, status=400)
    entry, err = await _mod_school_ban_action(user, target)
    if err: return web.json_response({"error": err}, status=400)
    return web.json_response({"ok": True, "message": f"School-banned {entry['target']} (booted {entry['booted']} sockets)", "entry": entry})

async def mod_regular_ban_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not (is_admin(user) or is_moderator(user)):
        return web.json_response({"error": "Forbidden"}, status=403)
    target = (data.get("username") or "").strip()
    if not target: return web.json_response({"error": "Username required"}, status=400)
    entry, err = await _mod_regular_ban_action(user, target)
    if err: return web.json_response({"error": err}, status=400)
    return web.json_response({"ok": True, "message": f"Regular-banned {entry['target']} (IP+device+account, booted {entry['booted']} sockets)", "entry": entry})

async def admin_device_ban_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target_user = (data.get("username") or "").strip()
    explicit_device = (data.get("device_id") or "").strip()
    device_id = None
    if explicit_device:
        device_id = explicit_device[:64]
    elif target_user:
        device_id = user_devices.get(target_user.lower())
        if not device_id:
            return web.json_response({"error": f"No device recorded for {target_user} - they need to log in once after the feature shipped"}, status=404)
    else:
        return web.json_response({"error": "Username or device_id required"}, status=400)
    if device_id not in device_bans:
        device_bans.append(device_id)
        await save_device_bans()
    booted = 0
    for ws, info in list(clients.items()):
        if info and info.get("device_id") == device_id:
            try: await ws.send_json({"type": "kicked", "text": "This device is banned"}); await ws.close(); booted += 1
            except: pass
    return web.json_response({"ok": True, "device_id": device_id, "booted": booted, "message": f"Device banned ({device_id[:8]}...). Booted {booted} live connection(s)."})

async def admin_device_unban_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target_user = (data.get("username") or "").strip()
    explicit_device = (data.get("device_id") or "").strip()
    device_id = explicit_device or (user_devices.get(target_user.lower()) if target_user else None)
    if not device_id:
        return web.json_response({"error": "Username or device_id required"}, status=400)
    device_bans[:] = [d for d in device_bans if d != device_id]
    await save_device_bans()
    return web.json_response({"ok": True, "message": f"Device unbanned ({device_id[:8]}...)"})

async def admin_device_bans_handler(request):
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    rev = {}
    for ulow, did in user_devices.items():
        rev.setdefault(did, []).append(ulow)
    entries = [{"device_id": d, "users": rev.get(d, [])} for d in device_bans]
    return web.json_response({"device_bans": entries})

async def admin_ipban_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = data.get("username", "").strip()
    if not target: return web.json_response({"error": "Username required"}, status=400)
    ip = user_ips.get(target)
    if not ip: return web.json_response({"error": f"No IP found for {target}"}, status=404)
    if ip not in ip_bans:
        ip_bans.append(ip)
        await save_ip_bans()
                                                            
    if not is_banned(target):
        bans.append(target)
        await save_bans()
    for ws, info in list(clients.items()):
        if info and info.get("ip") == ip:
            try: await ws.send_json({"type": "kicked", "text": "You have been IP banned"}); await ws.close()
            except: pass
    for ws in list(social_clients.keys()):
        if social_ips.get(ws) == ip:
            try: await ws.close()
            except: pass
    return web.json_response({"ok": True, "message": f"IP banned {target} ({ip})"})

async def admin_ip_unban_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    ip = data.get("ip", "").strip()
    if not ip: return web.json_response({"error": "IP required"}, status=400)
    ip_bans[:] = [b for b in ip_bans if b != ip]
    await save_ip_bans()
    return web.json_response({"ok": True, "message": f"IP unbanned {ip}"})

async def admin_session_for_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = data.get("username", "").strip()
    if not target: return web.json_response({"error": "Username required"}, status=400)
    found = next((u for u in accounts if u.lower() == target.lower()), None)
    if not found: return web.json_response({"error": "Account not found"}, status=404)
    token = secrets.token_hex(16)
    sessions[token] = found
    return web.json_response({"ok": True, "token": token, "username": found})

async def admin_kick_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = data.get("username", "").strip()
    kicked = False
    for ws, info in list(clients.items()):
        if info and not info.get("guest") and info.get("username", "").lower() == target.lower():
            try: await ws.send_json({"type": "kicked", "text": "Kicked by admin"}); await ws.close()
            except: pass
            kicked = True
    return web.json_response({"ok": True} if kicked else {"error": "Not online"}, status=200 if kicked else 404)

async def admin_alert_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = data.get("username", "").strip()
    text = data.get("text", "").strip()[:500]
    if not target or not text:
        return web.json_response({"error": "Username and message required"}, status=400)
    delivered = 0
    tlow = target.lower()
    for ws, info in list(clients.items()):
        if info and info.get("username", "").lower() == tlow:
            try: await ws.send_json({"type": "client_alert", "text": text}); delivered += 1
            except: pass
    for ws, uname in list(social_clients.items()):
        if uname and uname.lower() == tlow:
            try: await ws.send_json({"type": "client_alert", "text": text}); delivered += 1
            except: pass
    return web.json_response({"ok": True, "message": f"Alert delivered to {delivered} connection(s) for {target}"} if delivered else {"error": f"{target} is not online"})

async def admin_clear_economy_history_handler(request):
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    global economy_history
    n = len(economy_history)
    economy_history.clear()
    await save_economy_history()
    economy_history.append({"ts": int(time.time()), "total": _total_economy_pb()})
    await save_economy_history()
    return web.json_response({"ok": True, "cleared": n})

async def admin_richest_handler(request):
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    rows = []
    for uname in accounts.keys():
        if is_admin(uname): continue
        ulow = uname.lower()
        bal = int(place_bucks.get(ulow, 0))
        rows.append({"username": uname, "balance": bal, "online": is_online(uname)})
    rows.sort(key=lambda r: r["balance"], reverse=True)
    total = sum(r["balance"] for r in rows)
    return web.json_response({"ok": True, "total": total, "count": len(rows), "rows": rows[:50]})

ADMIN_PB_GRANT_MAX_PER_CALL = 1_000_000
ADMIN_REPLACE_COLOR_MAX_PIXELS = 250_000

async def admin_replace_color_handler(request):
    """Admin tool: within a rectangle on a given lobby's canvas, swap every pixel
    whose color is `from_color` to `to_color`. Broadcasts the resulting pixel
    updates so all connected clients repaint, marks the lobby dirty for persistence."""
    data = await request.json()
    admin_user = get_auth_user(request)
    if not is_admin(admin_user): return web.json_response({"error": "Forbidden"}, status=403)
    lobby_id = (data.get("lobby_id") or "").strip()
    lobby = lobbies.get(lobby_id)
    if not lobby: return web.json_response({"error": "Lobby not found"}, status=404)
    try:
        x1 = int(data.get("x1", 0)); y1 = int(data.get("y1", 0))
        x2 = int(data.get("x2", 0)); y2 = int(data.get("y2", 0))
        from_color = int(data.get("from_color", -1))
        to_color = int(data.get("to_color", -1))
    except: return web.json_response({"error": "Invalid coordinates or color"}, status=400)
    if not (0 <= from_color < PALETTE_SIZE and 0 <= to_color < PALETTE_SIZE):
        return web.json_response({"error": "Color out of palette range"}, status=400)
    if from_color == to_color:
        return web.json_response({"error": "From and to color are the same"}, status=400)
    w, h = lobby.get("width", 256), lobby.get("height", 256)
    xa, xb = max(0, min(x1, x2)), min(w - 1, max(x1, x2))
    ya, yb = max(0, min(y1, y2)), min(h - 1, max(y1, y2))
    if xa > xb or ya > yb:
        return web.json_response({"error": "Selection is outside the canvas"}, status=400)
    area = (xb - xa + 1) * (yb - ya + 1)
    if area > ADMIN_REPLACE_COLOR_MAX_PIXELS:
        return web.json_response({"error": f"Selection too large ({area} px) - cap is {ADMIN_REPLACE_COLOR_MAX_PIXELS}"}, status=400)
    grid = lobby["grid"]
    changes = []
    for y in range(ya, yb + 1):
        row_base = y * w
        for x in range(xa, xb + 1):
            i = row_base + x
            if grid[i] == from_color:
                grid[i] = to_color
                changes.append((x, y))
                set_pixel_author(lobby, x, y, admin_user)
    if changes:
        lobby["last_activity"] = time.time()
        mark_lobby_dirty(lobby_id)
        for (x, y) in changes:
            await broadcast_to_lobby(lobby_id, {"type": "pixel", "x": x, "y": y, "color": to_color})
    return web.json_response({"ok": True, "changed": len(changes), "area": area, "from": from_color, "to": to_color})

async def admin_pb_grant_handler(request):
    data = await request.json()
    admin_user = get_auth_user(request)
    if not is_admin(admin_user): return web.json_response({"error": "Forbidden"}, status=403)
    target = (data.get("username") or "").strip()
    try: amount = int(data.get("amount", 0))
    except: return web.json_response({"error": "Invalid amount"}, status=400)
    if abs(amount) > ADMIN_PB_GRANT_MAX_PER_CALL:
        return web.json_response({"error": f"Per-call cap is {ADMIN_PB_GRANT_MAX_PER_CALL} $ (use multiple calls for more)"}, status=400)
    if not target: return web.json_response({"error": "Username required"}, status=400)
    found = next((u for u in accounts if u.lower() == target.lower()), None)
    if not found: return web.json_response({"error": "User not found"}, status=404)
    ulow = found.lower()
    before = int(place_bucks.get(ulow, 0))
    if amount > 0:
        credit_pb(found, amount)
    elif amount < 0:
        place_bucks[ulow] = max(0, before + amount)
    after = int(place_bucks.get(ulow, 0))
    await save_place_bucks()
    await push_pb_update(found)
    print(f"[admin_grant] {admin_user} {'+' if amount >= 0 else ''}{amount} -> {found} (was {before}, now {after})")
    return web.json_response({"ok": True, "username": found, "balance": get_pb(found), "delta": after - before})

async def admin_relay_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = data.get("username", "").strip()
    text = data.get("text", "").strip()[:200]
    if not target or not text:
        return web.json_response({"error": "Username and text required"}, status=400)
    tlow = target.lower()
    lobby_id = None
    real_name = target
    for ws, info in list(clients.items()):
        if info and info.get("username", "").lower() == tlow:
            lobby_id = info.get("lobby_id"); real_name = info.get("username") or target; break
    if not lobby_id: return web.json_response({"error": f"{target} is not in a lobby"}, status=404)
    lobby = lobbies.get(lobby_id)
    is_owner = bool(lobby and lobby.get("owner") and lobby["owner"].lower() == tlow)
    payload = {"type": "chat", "username": real_name, "text": text, "is_owner": is_owner, "is_guest": False, "is_vip": is_vip(real_name), "rank": get_rank(real_name), "clan": get_clan_tag(real_name), "name_color": get_name_color(real_name)}
    await broadcast_to_lobby(lobby_id, payload)
    return web.json_response({"ok": True})

async def admin_redirect_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = data.get("username", "").strip()
    url = data.get("url", "").strip()[:500]
    if not target or not url:
        return web.json_response({"error": "Username and url required"}, status=400)
                                                                                                                       
    if not (url.startswith("http://") or url.startswith("https://")):
        return web.json_response({"error": "URL must start with http:// or https://"}, status=400)
    delivered = 0
    tlow = target.lower()
    for ws, info in list(clients.items()):
        if info and info.get("username", "").lower() == tlow:
            try: await ws.send_json({"type": "client_redirect", "url": url}); delivered += 1
            except: pass
    for ws, uname in list(social_clients.items()):
        if uname and uname.lower() == tlow:
            try: await ws.send_json({"type": "client_redirect", "url": url}); delivered += 1
            except: pass
    return web.json_response({"ok": True, "message": f"Redirect sent to {delivered} connection(s) for {target}"} if delivered else {"error": f"{target} is not online"})

async def admin_reset_password_handler(request):
    data = await request.json()
    actor = get_auth_user(request)
    if not is_admin(actor): return web.json_response({"error": "Forbidden"}, status=403)
    target = (data.get("username") or "").strip()
    new_pw = data.get("new_password") or ""
    if not target: return web.json_response({"error": "Username required"}, status=400)
    if len(new_pw) < 4: return web.json_response({"error": "Password must be at least 4 characters"}, status=400)
    found = next((u for u in accounts if u.lower() == target.lower()), None)
    if not found: return web.json_response({"error": f"Account {target} not found"}, status=404)
    pw_hash, salt = hash_password(new_pw)
    accounts[found]["password_hash"] = pw_hash
    accounts[found]["salt"] = salt
    await save_accounts()
    tlow = found.lower()
    revoked = 0
    for tok in [t for t, u in sessions.items() if u.lower() == tlow]:
        sessions.pop(tok, None); revoked += 1
    for ws, info in list(clients.items()):
        if info and info.get("username", "").lower() == tlow:
            try: await ws.send_json({"type": "kicked", "text": "Your password was reset - please log in again"}); await ws.close()
            except: pass
    for ws, uname in list(social_clients.items()):
        if uname and uname.lower() == tlow:
            try: await ws.close()
            except: pass
    return web.json_response({"ok": True, "username": found, "sessions_revoked": revoked})

async def change_password_handler(request):
    if not check_rate_limit(get_client_ip(request), "change_pw", 5, 300):
        return web.json_response({"error": "Too many attempts - try again in a few minutes"}, status=429)
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    data = await request.json()
    old_pw = data.get("old_password") or ""
    new_pw = data.get("new_password") or ""
    if not old_pw or not new_pw:
        return web.json_response({"error": "Old and new password required"}, status=400)
    if len(new_pw) < 4:
        return web.json_response({"error": "New password must be at least 4 characters"}, status=400)
    acc = accounts.get(user)
    if not acc: return web.json_response({"error": "Account not found"}, status=404)
    h, _ = hash_password(old_pw, acc["salt"])
    if h != acc["password_hash"]:
        return web.json_response({"error": "Current password is incorrect"}, status=400)
    pw_hash, salt = hash_password(new_pw)
    accounts[user]["password_hash"] = pw_hash
    accounts[user]["salt"] = salt
    await save_accounts()
    auth_hdr = request.headers.get("Authorization", "")
    keep_tok = auth_hdr if auth_hdr in sessions else None
    ulow = user.lower()
    revoked = 0
    for tok in [t for t, u in sessions.items() if u.lower() == ulow and t != keep_tok]:
        sessions.pop(tok, None); revoked += 1
    return web.json_response({"ok": True, "other_sessions_revoked": revoked})

async def admin_delete_account_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = data.get("username", "").strip()
    if not target: return web.json_response({"error": "Username required"}, status=400)
    if is_admin(target): return web.json_response({"error": "Cannot delete admin"}, status=400)
    tlow = target.lower()
    found = next((u for u in accounts if u.lower() == tlow), None)
    if not found:
        return web.json_response({"error": f"Account {target} not found"}, status=404)
                                    
    for tok in [t for t, u in sessions.items() if u.lower() == tlow]:
        del sessions[tok]
    for ws, info in list(clients.items()):
        if info and not info.get("guest") and info.get("username", "").lower() == tlow:
            try: await ws.send_json({"type": "kicked", "text": "Your account was deleted"}); await ws.close()
            except: pass
    for ws, uname in list(social_clients.items()):
        if uname and uname.lower() == tlow:
            try: await ws.close()
            except: pass
                          
    del accounts[found]
    await save_accounts()
                                                                   
    if found in friends_data:
        del friends_data[found]
    for u, fd in list(friends_data.items()):
        fd["friends"] = [f for f in fd.get("friends", []) if f.lower() != tlow]
        fd["incoming"] = [f for f in fd.get("incoming", []) if f.lower() != tlow]
        fd["outgoing"] = [f for f in fd.get("outgoing", []) if f.lower() != tlow]
    await save_friends()
                                      
    bans[:] = [b for b in bans if b.lower() != tlow]
    await save_bans()
    if tlow in vips:
        vips.remove(tlow)
        await save_vips()
    if tlow in ranks:
        del ranks[tlow]
        await save_ranks()
    if found in user_ips:
        del user_ips[found]
        await save_user_ips()
                                
    owned_lids = [lid for lid, l in list(lobbies.items()) if not lid.startswith("public_") and l.get("owner", "").lower() == tlow]
    for lid in owned_lids:
        for ws, info in list(clients.items()):
            if info and info.get("lobby_id") == lid:
                try: await ws.send_json({"type": "kicked", "text": "Lobby deleted (owner account removed)"}); await ws.close()
                except: pass
        del lobbies[lid]
        await db["lobbies"].delete_one({"_id": lid})
                                                       
    for lid, l in lobbies.items():
        pc = l.get("pixel_counts", {})
        for k in [k for k in pc if k.lower() == tlow]:
            del pc[k]
                                           
    keys_to_delete = [k for k in dms if tlow in k.split(":")]
    for k in keys_to_delete:
        del dms[k]
        await db["dms"].delete_one({"_id": k})
                                                         
    seen_dirty = False
    if tlow in dm_last_seen:
        del dm_last_seen[tlow]
        seen_dirty = True
    for peers in dm_last_seen.values():
        if tlow in peers:
            del peers[tlow]
            seen_dirty = True
    if seen_dirty:
        await save_dm_last_seen()
                                                                                           
    disbanded = 0
    for cid in list(clans.keys()):
        cl = clans[cid]
        if cl.get("owner", "").lower() == tlow:
            for m in cl.get("members", []):
                await remove_user_clan_rank(m)
            del clans[cid]
            disbanded += 1
        else:
            cl["members"] = [m for m in cl.get("members", []) if m.lower() != tlow]
            (cl.get("member_ranks") or {}).pop(tlow, None)
            cl["pending_requests"] = [r for r in cl.get("pending_requests", []) if r.lower() != tlow]
    await save_clans()
                                              
    if tlow in place_bucks: del place_bucks[tlow]; await save_place_bucks()
    if tlow in lifetime_pixels: del lifetime_pixels[tlow]; await save_lifetime_pixels()
    if tlow in purchases: del purchases[tlow]; await save_purchases()
    return web.json_response({"ok": True, "message": f"Deleted account {found} ({len(owned_lids)} lobbies, {len(keys_to_delete)} DM threads, {disbanded} clans disbanded)"})

async def admin_vip_add_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = data.get("username", "").strip().lower()
    if target and target not in vips:
        vips.append(target)
        await save_vips()
    return web.json_response({"ok": True, "message": f"Added {target} as VIP"})

async def admin_vip_remove_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = data.get("username", "").strip().lower()
    if target in vips:
        vips.remove(target)
        await save_vips()
    return web.json_response({"ok": True, "message": f"Removed {target} from VIP"})

async def admin_rank_set_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = data.get("username", "").strip()
    label = data.get("label", "").strip()[:16]
    color = data.get("color", "").strip()[:32] or "#daa520"
    if not target or not label:
        return web.json_response({"error": "Username and label required"}, status=400)
    if is_admin(target):
        return web.json_response({"error": "Cannot change admin rank"}, status=400)
    tlow = target.lower()
    ranks[tlow] = {"label": label, "color": color}
    if tlow not in vips:
        vips.append(tlow)
        await save_vips()
    await save_ranks()
    return web.json_response({"ok": True, "message": f"Set {target} rank to [{label}]"})

async def admin_rank_remove_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = data.get("username", "").strip().lower()
    changed = False
    if target in ranks:
        del ranks[target]
        changed = True
    if target in vips:
        vips.remove(target)
        await save_vips()
        changed = True
    if changed:
        await save_ranks()
    return web.json_response({"ok": True, "message": f"Removed rank from {target}"})

async def admin_ranks_handler(request):
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    return web.json_response({"ranks": ranks})

async def clans_list_handler(request):
    """Public list of approved clans."""
    out = []
    for clan in clans.values():
        if clan.get("status") != "approved": continue
        out.append({
            "id": clan["id"], "name": clan["name"], "owner": clan["owner"],
            "color": clan["color"], "rank_label": clan["rank_label"],
            "members": clan.get("members", []),
            "member_ranks": clan.get("member_ranks", {}),
            "member_count": 1 + len(clan.get("members", [])),
        })
    return web.json_response({"clans": out})

async def clan_my_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    ulow = user.lower()
    my_clan = None
    pending_owned = None
    join_requests = []                                           
    for clan in clans.values():
        if clan.get("owner", "").lower() == ulow:
            if clan.get("status") == "approved": my_clan = clan
            elif clan.get("status") == "pending": pending_owned = clan
        elif clan.get("status") == "approved" and any(m.lower() == ulow for m in clan.get("members", [])):
            my_clan = clan
        if any(r.lower() == ulow for r in clan.get("pending_requests", [])):
            join_requests.append({"clan_id": clan["id"], "clan_name": clan["name"]})
    return web.json_response({"my_clan": my_clan, "pending_owned": pending_owned, "join_requests": join_requests})

async def clan_create_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not check_rate_limit(user, "clan_create", 3, 600):
        return web.json_response({"error": "Slow down - too many clan create attempts."}, status=429)
    name = data.get("name", "").strip()[:30]
    color = data.get("color", "").strip()[:32]
    if not name or not color:
        return web.json_response({"error": "Name and color required"}, status=400)
    if not (color.startswith("#") and (len(color) == 7 or len(color) == 4)):
        return web.json_response({"error": "Color must be a hex code"}, status=400)
    ulow = user.lower()
    for clan in clans.values():
        if clan.get("owner", "").lower() == ulow:
            return web.json_response({"error": "You already have a clan request or approved clan"}, status=400)
    existing = find_clan_by_member(user)
    if existing and existing.get("owner", "").lower() != ulow:
        existing["members"] = [m for m in existing.get("members", []) if m.lower() != ulow]
        await remove_user_clan_rank(user)
        await save_clans()
    cid = secrets.token_hex(6)

    clans[cid] = {
        "id": cid, "name": name, "owner": user, "color": color, "rank_label": name,
        "status": "pending", "members": [], "pending_requests": [], "created_at": time.time(),
    }
    await save_clans()
    return web.json_response({"ok": True, "message": "Clan request submitted, waiting for admin approval", "clan_id": cid})

async def clan_request_join_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not check_rate_limit(user, "clan_join", 5, 60):
        return web.json_response({"error": "Too many clan join requests - try again in a minute."}, status=429)
    cid = data.get("clan_id", "")
    clan = clans.get(cid)
    if not clan or clan.get("status") != "approved":
        return web.json_response({"error": "Clan not found"}, status=404)
    ulow = user.lower()
    if clan["owner"].lower() == ulow or any(m.lower() == ulow for m in clan.get("members", [])):
        return web.json_response({"error": "You are already in this clan"}, status=400)
                              
    if find_clan_by_member(user):
        return web.json_response({"error": "Leave your current clan first"}, status=400)
    if any(r.lower() == ulow for r in clan.get("pending_requests", [])):
        return web.json_response({"error": "You already requested to join"}, status=400)
                                                                             
    for other in clans.values():
        if any(r.lower() == ulow for r in other.get("pending_requests", [])):
            return web.json_response({"error": f"You already have a pending request to join '{other.get('name', '')}'. Wait for that to be answered or cancel it first."}, status=400)
    clan.setdefault("pending_requests", []).append(user)
    await save_clans()
                                
    await notify_social(clan["owner"], {"type": "clan_join_request", "clan_id": cid, "clan_name": clan["name"], "requester": user})
    return web.json_response({"ok": True, "message": "Join request sent to clan owner"})

async def clan_handle_request_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    cid = data.get("clan_id", "")
    target = data.get("username", "").strip()
    approve = bool(data.get("approve", False))
    clan = clans.get(cid)
    if not clan: return web.json_response({"error": "Clan not found"}, status=404)
    if clan["owner"].lower() != user.lower() and not is_admin(user):
        return web.json_response({"error": "Only the clan owner can handle requests"}, status=403)
    pr = clan.get("pending_requests", [])
    found = next((r for r in pr if r.lower() == target.lower()), None)
    if not found: return web.json_response({"error": "No such pending request"}, status=404)
    clan["pending_requests"] = [r for r in pr if r.lower() != target.lower()]
    if approve:
                                                       
        if not find_clan_by_member(found):
            clan.setdefault("members", []).append(found)
            await apply_clan_rank(found, clan)
    await save_clans()
    await notify_social(found, {"type": "clan_request_handled", "clan_name": clan["name"], "approved": approve})
    return web.json_response({"ok": True, "message": ("Approved " if approve else "Rejected ") + found})

async def clan_set_member_rank_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not check_rate_limit(user, "clan_member_rank", 20, 60):
        return web.json_response({"error": "Too many rank changes - slow down."}, status=429)
    cid = data.get("clan_id", "")
    target = (data.get("username") or "").strip()
    color = (data.get("color") or "").strip()[:32]
    reset = bool(data.get("reset", False))
    clan = clans.get(cid)
    if not clan: return web.json_response({"error": "Clan not found"}, status=404)
    if clan["owner"].lower() != user.lower() and not is_admin(user):
        return web.json_response({"error": "Only the clan owner can change member ranks"}, status=403)
    if not target:
        return web.json_response({"error": "Username required"}, status=400)
    tlow = target.lower()
    is_member = (clan["owner"].lower() == tlow) or any(m.lower() == tlow for m in clan.get("members", []))
    if not is_member:
        return web.json_response({"error": "Target is not a member of this clan"}, status=400)
    member_ranks = clan.setdefault("member_ranks", {})
    if reset:
        member_ranks.pop(tlow, None)
    else:
        if not color:
            return web.json_response({"error": "Provide a color, or reset"}, status=400)
        if not (color.startswith("#") and (len(color) == 7 or len(color) == 4)):
            return web.json_response({"error": "Color must be a hex code"}, status=400)
        member_ranks[tlow] = {"color": color}
    await save_clans()
                                                                             
    if clan.get("status") == "approved":
                                                                                
        canonical = clan["owner"] if clan["owner"].lower() == tlow else next((m for m in clan.get("members", []) if m.lower() == tlow), target)
        await apply_clan_rank(canonical, clan)
    return web.json_response({"ok": True})

async def clan_update_color_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not check_rate_limit(user, "clan_update_color", 10, 60):
        return web.json_response({"error": "Too many color changes - slow down."}, status=429)
    cid = data.get("clan_id", "")
    color = (data.get("color") or "").strip()[:32]
    if not (color.startswith("#") and (len(color) == 7 or len(color) == 4)):
        return web.json_response({"error": "Color must be a hex code"}, status=400)
    clan = clans.get(cid)
    if not clan: return web.json_response({"error": "Clan not found"}, status=404)
    if clan["owner"].lower() != user.lower() and not is_admin(user):
        return web.json_response({"error": "Only the clan owner can change the color"}, status=403)
    clan["color"] = color
    await save_clans()
                                                                                      
    if clan.get("status") == "approved":
        await apply_clan_rank(clan["owner"], clan)
        for m in clan.get("members", []):
            await apply_clan_rank(m, clan)
    return web.json_response({"ok": True})

async def clan_leave_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    clan = find_clan_by_member(user)
    if not clan: return web.json_response({"error": "Not in a clan"}, status=400)
    ulow = user.lower()
    if clan["owner"].lower() == ulow:
                                                                     
        for m in clan.get("members", []):
            await remove_user_clan_rank(m)
        await remove_user_clan_rank(clan["owner"])
        del clans[clan["id"]]
    else:
        clan["members"] = [m for m in clan.get("members", []) if m.lower() != ulow]
        await remove_user_clan_rank(user)
    await save_clans()
    return web.json_response({"ok": True})

async def clan_transfer_owner_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    cid = data.get("clan_id", "")
    target = (data.get("username") or "").strip()
    clan = clans.get(cid)
    if not clan: return web.json_response({"error": "Clan not found"}, status=404)
    if clan["owner"].lower() != user.lower() and not is_admin(user):
        return web.json_response({"error": "Only the clan owner can transfer ownership"}, status=403)
    if not target: return web.json_response({"error": "Username required"}, status=400)
    tlow = target.lower()
    if tlow == clan["owner"].lower():
        return web.json_response({"error": "That user is already the owner"}, status=400)
    member = next((m for m in clan.get("members", []) if m.lower() == tlow), None)
    if not member:
        return web.json_response({"error": "New owner must be a current clan member"}, status=400)
    old_owner = clan["owner"]
    clan["members"] = [m for m in clan.get("members", []) if m.lower() != tlow]
    clan["members"].append(old_owner)
    clan["owner"] = member
    await save_clans()
    await notify_social(member, {"type": "clan_owner_transferred", "clan_name": clan["name"], "by": old_owner})
    return web.json_response({"ok": True, "message": f"Ownership transferred to {member}"})

async def admin_clans_handler(request):
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    return web.json_response({"clans": list(clans.values())})

async def admin_clan_approve_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    cid = data.get("clan_id", "")
    clan = clans.get(cid)
    if not clan: return web.json_response({"error": "Clan not found"}, status=404)
    clan["status"] = "approved"
    await save_clans()
    await apply_clan_rank(clan["owner"], clan)
    await notify_social(clan["owner"], {"type": "clan_approved", "clan_name": clan["name"]})
    return web.json_response({"ok": True})

async def admin_clan_reject_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    cid = data.get("clan_id", "")
    clan = clans.get(cid)
    if not clan: return web.json_response({"error": "Clan not found"}, status=404)
    owner = clan.get("owner", "")
    del clans[cid]
    await save_clans()
    if owner: await notify_social(owner, {"type": "clan_rejected", "clan_name": clan["name"]})
    return web.json_response({"ok": True})

async def admin_clan_disband_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    cid = data.get("clan_id", "")
    clan = clans.get(cid)
    if not clan: return web.json_response({"error": "Clan not found"}, status=404)
    for m in clan.get("members", []): await remove_user_clan_rank(m)
    await remove_user_clan_rank(clan.get("owner", ""))
    del clans[cid]
    await save_clans()
    return web.json_response({"ok": True})

async def groups_my_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    ulow = user.lower()
    out = []
    for g in groups.values():
        if any(m.lower() == ulow for m in g.get("members", [])):
            out.append({"id": g["id"], "name": g["name"], "owner": g["owner"], "members": g["members"]})
    return web.json_response({"groups": out})

async def group_create_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not check_rate_limit(user, "group_create", 5, 300):
        return web.json_response({"error": "Too many groups created - try again later."}, status=429)
    name = data.get("name", "").strip()[:30]
    invitees = data.get("members", [])
    if not name: return web.json_response({"error": "Group name required"}, status=400)
    if not isinstance(invitees, list): invitees = []
    fd = get_friend_data(user)
    friend_set = {f.lower() for f in fd["friends"]}
    members = [user]
    for u in invitees[:19]:                                          
        if not isinstance(u, str): continue
        if u.lower() in friend_set and u.lower() != user.lower() and u not in members:
            members.append(u)
    gid = secrets.token_hex(6)
    groups[gid] = {"id": gid, "name": name, "owner": user, "members": members, "created_at": time.time()}
    await save_groups()
                                
    for m in members:
        if m.lower() != user.lower():
            await notify_social(m, {"type": "group_added", "group_id": gid, "group_name": name, "by": user})
    return web.json_response({"ok": True, "group_id": gid})

async def group_messages_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    gid = request.query.get("id", "")
    g = groups.get(gid)
    if not g: return web.json_response({"error": "Group not found"}, status=404)
    if user.lower() not in [m.lower() for m in g.get("members", [])]:
        return web.json_response({"error": "Not a member"}, status=403)
    return web.json_response({"messages": group_messages.get(gid, [])[-MAX_DM_HISTORY:], "members": g["members"], "owner": g["owner"], "name": g["name"]})

async def group_leave_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    gid = data.get("group_id", "")
    g = groups.get(gid)
    if not g: return web.json_response({"error": "Group not found"}, status=404)
    ulow = user.lower()
    g["members"] = [m for m in g.get("members", []) if m.lower() != ulow]
    if not g["members"] or g["owner"].lower() == ulow:
                                                      
        del groups[gid]
        group_messages.pop(gid, None)
        await db["group_messages"].delete_one({"_id": gid})
        await save_groups()
                                                           
        for m in g.get("members", []):
            await notify_social(m, {"type": "group_dissolved", "group_id": gid, "group_name": g["name"]})
        return web.json_response({"ok": True, "dissolved": True})
    await save_groups()
    return web.json_response({"ok": True})

async def group_add_member_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    gid = data.get("group_id", "")
    target = data.get("username", "").strip()
    g = groups.get(gid)
    if not g: return web.json_response({"error": "Group not found"}, status=404)
    if g["owner"].lower() != user.lower(): return web.json_response({"error": "Only the owner can add"}, status=403)
    if not target: return web.json_response({"error": "Username required"}, status=400)
    fd = get_friend_data(user)
    if target not in fd["friends"]: return web.json_response({"error": "You can only add friends"}, status=400)
    if target.lower() in [m.lower() for m in g["members"]]: return web.json_response({"error": "Already a member"}, status=400)
    if len(g["members"]) >= 20: return web.json_response({"error": "Group is full (20 max)"}, status=400)
    g["members"].append(target)
    await save_groups()
    await notify_social(target, {"type": "group_added", "group_id": gid, "group_name": g["name"], "by": user})
    return web.json_response({"ok": True})

async def me_handler(request):
    """Current user's balance, purchases, lifetime pixels. Real admin reports unlimited.
    If the user is banned, returns 403 with school_ban: true so the client redirects."""
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if is_banned(user):
        return web.json_response({"error": "Banned", "school_ban": True, "url": "https://www.pornhub.com"}, status=403)
    return web.json_response({
        "username": user,
        "balance": get_pb(user),
        "unlimited": bool(is_admin(user)),
        "purchases": purchases.get(user.lower()) or {},
        "lifetime_pixels": int(lifetime_pixels.get(user.lower(), 0)),
        "streak": get_streak(user),
        "streak_progress": get_streak_progress(user),
        "is_moderator": bool(is_moderator(user)),
        "is_admin": bool(is_admin(user)),
        "name_color": get_name_color(user),
        "prices": {"shop": SHOP_PRICES, "lobby": {f"{w}x{h}": p for (w, h), p in LOBBY_PRICES.items()}, "pixels_per_buck": PB_PIXELS_PER_BUCK},
    })

async def shop_buy_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not check_rate_limit(user, "shop_buy", 10, 60):
        return web.json_response({"error": "Too many purchases - slow down."}, status=429)
    item = (data.get("item") or "").strip()
    if item not in SHOP_PRICES:
        return web.json_response({"error": "Unknown item"}, status=400)
                                                                                     
    if item == "custom_rank":
        label = (data.get("label") or "").strip()[:16]
        color = (data.get("color") or "").strip()[:32]
        if not label:
            return web.json_response({"error": "Rank label required"}, status=400)
        if label.upper() in ("CREATOR", "ADMIN", "MODERATOR", "MOD") and not is_admin(user):
            return web.json_response({"error": "That label is reserved"}, status=400)
        if not (color.startswith("#") and (len(color) == 7 or len(color) == 4)):
            return web.json_response({"error": "Color must be a hex code"}, status=400)
        first_time = not has_purchase(user, "custom_rank")
        if first_time:
            price = SHOP_PRICES["custom_rank"]
            if not spend_pb(user, price):
                return web.json_response({"error": f"Not enough PlaceBucks (need {price})"}, status=400)
            purchases.setdefault(user.lower(), {})["custom_rank"] = True
            await save_purchases()
            await save_place_bucks()
        ranks[user.lower()] = {"label": label, "color": color}
        await save_ranks()
        await push_pb_update(user)
        return web.json_response({"ok": True, "balance": get_pb(user), "purchases": purchases.get(user.lower()) or {}})
    if item == "name_color":
        color = (data.get("color") or "").strip()[:32]
        if not (color.startswith("#") and (len(color) == 7 or len(color) == 4)):
            return web.json_response({"error": "Color must be a hex code"}, status=400)
        first_time = not has_purchase(user, "name_color")
        if first_time:
            price = SHOP_PRICES["name_color"]
            if not spend_pb(user, price):
                return web.json_response({"error": f"Not enough PlaceBucks (need {price})"}, status=400)
            purchases.setdefault(user.lower(), {})["name_color"] = True
            await save_purchases()
            await save_place_bucks()
        name_colors[user.lower()] = color
        await save_name_colors()
        await push_pb_update(user)
        return web.json_response({"ok": True, "balance": get_pb(user), "purchases": purchases.get(user.lower()) or {}, "name_color": color})
    stackable = item in STACKABLE_ITEMS
    if not stackable and has_purchase(user, item):
        return web.json_response({"error": "You already own that"}, status=400)
    price = SHOP_PRICES[item]
    if not spend_pb(user, price):
        return web.json_response({"error": f"Not enough PlaceBucks (need {price})"}, status=400)
    pu = purchases.setdefault(user.lower(), {})
    if stackable:
        pu[item] = int(pu.get(item, 0)) + 1
    else:
        pu[item] = True
    await save_purchases()
    await save_place_bucks()
                           
    if item == "vip":
        if user.lower() not in vips:
            vips.append(user.lower())
            await save_vips()
        ranks[user.lower()] = {"label": "VIP", "color": "#daa520"}
        await save_ranks()
    await push_pb_update(user)
    return web.json_response({"ok": True, "balance": get_pb(user), "purchases": pu})

async def shop_sell_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not check_rate_limit(user, "shop_sell", 10, 60):
        return web.json_response({"error": "Too many sells - slow down."}, status=429)
    item = (data.get("item") or "").strip()
    if item not in SHOP_SELL_PRICES:
        return web.json_response({"error": "That item isn't sellable"}, status=400)
    ulow = user.lower()
    pu = purchases.setdefault(ulow, {})
    if item in STACKABLE_ITEMS:
        have = int(pu.get(item, 0))
        if have <= 0: return web.json_response({"error": "You don't have any to sell"}, status=400)
        pu[item] = have - 1
        if pu[item] <= 0: del pu[item]
    else:
        if not pu.get(item): return web.json_response({"error": "You don't own that"}, status=400)
        del pu[item]
    refund = SHOP_SELL_PRICES[item]
    credit_pb(user, refund)
    await save_purchases()
    await save_place_bucks()
    await push_pb_update(user)
    return web.json_response({"ok": True, "balance": get_pb(user), "purchases": purchases.get(ulow) or {}, "refund": refund, "item": item})

async def pb_transfer_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    target = (data.get("to") or "").strip()
    try:
        amount = int(data.get("amount", 0))
    except: return web.json_response({"error": "Invalid amount"}, status=400)
    if amount <= 0: return web.json_response({"error": "Amount must be at least 1"}, status=400)
    if not target: return web.json_response({"error": "Recipient required"}, status=400)
    if target.lower() == user.lower(): return web.json_response({"error": "you can't give money to yourself, you're not a bank"}, status=400)
    found = next((u for u in accounts if u.lower() == target.lower()), None)
    if not found: return web.json_response({"error": "User not found"}, status=404)
                                                                   
    if not check_rate_limit(user, "pb_transfer_count", 5, 60):
        return web.json_response({"error": "Too many transfers - try again later."}, status=429)
    ulow = user.lower()
    now_ts = time.time()
    bucket = pb_transfer_amount_window.setdefault(ulow, [])
    bucket[:] = [(t, a) for (t, a) in bucket if now_ts - t < 60]
    spent_last_min = sum(a for (_, a) in bucket)
    if spent_last_min + amount > PB_TRANSFER_AMOUNT_PER_MIN:
        remaining = max(0, PB_TRANSFER_AMOUNT_PER_MIN - spent_last_min)
        return web.json_response({"error": f"Transfer cap is {PB_TRANSFER_AMOUNT_PER_MIN} $/min. You can send {remaining} more this minute."}, status=429)
    if not spend_pb(user, amount):
        return web.json_response({"error": "Not enough PlaceBucks"}, status=400)
    bucket.append((now_ts, amount))
    credit_pb(found, amount)
    await save_place_bucks()
    await push_pb_update(user)
    await push_pb_update(found)
    await notify_social(found, {"type": "pb_received", "from": user, "amount": amount})
    return web.json_response({"ok": True, "balance": get_pb(user)})

async def _casino_open(request, rate_key):
    """Auth, bet validation, rate limit, deduct. Hard-capped at MAX_CASINO_BET
    to prevent compound-jackpot inflation exploits. Rate limited per game key
    (default 60 plays/minute) to prevent scripted spam."""
    data = await request.json()
    user = get_auth_user(request)
    if not user: return None, web.json_response({"error": "Not authenticated"}, status=401)
    try: amount = int(data.get("amount", 0))
    except: return None, web.json_response({"error": "Invalid bet"}, status=400)
    if amount < 1:
        return None, web.json_response({"error": "Bet must be at least 1 $"}, status=400)
    if amount > MAX_CASINO_BET:
        return None, web.json_response({"error": f"Max bet is {MAX_CASINO_BET} $"}, status=400)
    if rate_key and not check_rate_limit(user, rate_key, 60, 60):
        return None, web.json_response({"error": "Slow down - too many plays this minute."}, status=429)
    if not spend_pb(user, amount):
        return None, web.json_response({"error": "Not enough PlaceBucks"}, status=400)
    return (user, amount, data), None

async def _casino_payout(user, winnings):
    if winnings > 0:
        credit_pb(user, winnings)
    await save_place_bucks()
    await push_pb_update(user)

CASINO_BROADCAST_MIN = 1000

async def broadcast_casino_result(user, game, bet, winnings, detail=""):
    """Post a system chat line only if the net swing is at least
    CASINO_BROADCAST_MIN PB. Small wins/losses don't spam the channel."""
    if not user: return
    net = winnings - bet
    if abs(net) < CASINO_BROADCAST_MIN: return
    if net > 0:
        text = f"{user} won {net} $ at {game}"
    else:
        text = f"{user} lost {-net} $ at {game}"
    if detail:
        text += f" - {detail}"
    ulow = user.lower()
    seen = set()
    for info in clients.values():
        if not info: continue
        if info.get("username", "").lower() != ulow: continue
        lid = info.get("lobby_id")
        if lid and lid not in seen:
            seen.add(lid)
            await broadcast_to_lobby(lid, {"type": "system", "text": text})

async def casino_slots_handler(request):
    res, err = await _casino_open(request, "casino_slots")
    if err: return err
    user, amount, _ = res
    symbols = ["7", "X", "A", "B", "C"]
    spin = [secrets.choice(symbols) for _ in range(3)]
    winnings = 0; outcome = "lose"
    if spin[0] == spin[1] == spin[2]:
        if spin[0] == "7": winnings = amount * 25; outcome = "jackpot"
        else: winnings = amount * 8; outcome = "three"
    else:
        counts = {}
        for s in spin: counts[s] = counts.get(s, 0) + 1
        if max(counts.values()) == 2:
            winnings = amount; outcome = "pair"
    await _casino_payout(user, winnings)
    detail = " ".join(spin) + (" 🎰 JACKPOT" if outcome == "jackpot" else "")
    await broadcast_casino_result(user, "Slots", amount, winnings, detail)
    return web.json_response({"ok": True, "spin": spin, "outcome": outcome, "winnings": winnings, "bet": amount, "balance": get_pb(user)})

async def casino_roulette_handler(request):
    res, err = await _casino_open(request, "casino_roulette")
    if err: return err
    user, amount, data = res
    choice = (data.get("choice") or "").strip().lower()
    if choice not in ("red", "black", "green"):
        credit_pb(user, amount); await save_place_bucks(); await push_pb_update(user)
        return web.json_response({"error": "Choice must be red, black, or green"}, status=400)
    n = secrets.randbelow(37)         
    if n == 0: result_color = "green"
    else: result_color = "red" if n % 2 == 1 else "black"
    winnings = 0; outcome = "lose"
    if choice == result_color:
        if result_color == "green": winnings = amount * 35
        else: winnings = amount * 2
        outcome = "win"
    await _casino_payout(user, winnings)
    await broadcast_casino_result(user, "Roulette", amount, winnings, f"landed {n} {result_color} (bet {choice})")
    return web.json_response({"ok": True, "number": n, "color": result_color, "outcome": outcome, "winnings": winnings, "bet": amount, "balance": get_pb(user)})

MINES_GRID_W = 5
MINES_GRID_N = MINES_GRID_W * MINES_GRID_W
MINES_HOUSE_EDGE = 0.97
mines_games = {}

def _mines_mult(mines, revealed):
    """Fair payout multiplier = (probability of having survived this many random reveals)^-1, with a flat house edge."""
    safe = MINES_GRID_N - mines
    if revealed <= 0: return 1.0
    if revealed > safe: return 0.0
    num = 1.0; den = 1.0
    for i in range(revealed):
        num *= (MINES_GRID_N - i)
        den *= (safe - i)
    return round((num / den) * MINES_HOUSE_EDGE, 4)

async def casino_mines_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    action = (data.get("action") or "").strip().lower()
    ulow = user.lower()
    if action == "start":
        try: amount = int(data.get("amount", 0))
        except: return web.json_response({"error": "Invalid bet"}, status=400)
        try: mines = int(data.get("mines", 3))
        except: return web.json_response({"error": "Invalid mines"}, status=400)
        if amount < 1: return web.json_response({"error": "Bet must be at least 1 $"}, status=400)
        if amount > MAX_CASINO_BET: return web.json_response({"error": f"Max bet is {MAX_CASINO_BET} $"}, status=400)
        if mines < 1 or mines > MINES_GRID_N - 1: return web.json_response({"error": f"Mines must be 1-{MINES_GRID_N-1}"}, status=400)
        if ulow in mines_games and not mines_games[ulow].get("done"):
            return web.json_response({"error": "Finish your current game first (cash out or reveal)"}, status=400)
        if not spend_pb(user, amount):
            return web.json_response({"error": "Not enough PlaceBucks"}, status=400)
        positions = list(range(MINES_GRID_N))
        secrets.SystemRandom().shuffle(positions)
        mine_set = set(positions[:mines])
        mines_games[ulow] = {"bet": amount, "mines": mine_set, "n_mines": mines, "revealed": set(), "done": False, "started_at": time.time()}
        return web.json_response({"ok": True, "phase": "playing", "mines": mines, "revealed": [], "multiplier": 1.0, "next_mult": _mines_mult(mines, 1), "balance": get_pb(user)})
    if action == "reveal":
        g = mines_games.get(ulow)
        if not g or g["done"]: return web.json_response({"error": "No active game"}, status=400)
        try: idx = int(data.get("idx", -1))
        except: return web.json_response({"error": "Bad position"}, status=400)
        if not (0 <= idx < MINES_GRID_N): return web.json_response({"error": "Out of bounds"}, status=400)
        if idx in g["revealed"]: return web.json_response({"error": "Already revealed"}, status=400)
        if idx in g["mines"]:
            g["done"] = True
            await broadcast_casino_result(user, "Mines", g["bet"], 0, f"hit mine after {len(g['revealed'])} safe reveals")
            payload = {"ok": True, "phase": "done", "hit_mine": True, "mine_idx": idx, "mines": list(g["mines"]), "revealed": list(g["revealed"]), "winnings": 0, "bet": g["bet"], "balance": get_pb(user)}
            mines_games.pop(ulow, None)
            return web.json_response(payload)
        g["revealed"].add(idx)
        cur_mult = _mines_mult(g["n_mines"], len(g["revealed"]))
        nxt_mult = _mines_mult(g["n_mines"], len(g["revealed"]) + 1)
        if len(g["revealed"]) >= MINES_GRID_N - g["n_mines"]:
            winnings = int(g["bet"] * cur_mult)
            credit_pb(user, winnings)
            await save_place_bucks()
            await push_pb_update(user)
            g["done"] = True
            await broadcast_casino_result(user, "Mines", g["bet"], winnings, f"cleared all safe tiles x{cur_mult}")
            payload = {"ok": True, "phase": "done", "hit_mine": False, "cleared": True, "mines": list(g["mines"]), "revealed": list(g["revealed"]), "multiplier": cur_mult, "winnings": winnings, "bet": g["bet"], "balance": get_pb(user)}
            mines_games.pop(ulow, None)
            return web.json_response(payload)
        return web.json_response({"ok": True, "phase": "playing", "revealed": list(g["revealed"]), "multiplier": cur_mult, "next_mult": nxt_mult, "balance": get_pb(user)})
    if action == "cashout":
        g = mines_games.get(ulow)
        if not g or g["done"]: return web.json_response({"error": "No active game"}, status=400)
        revealed_count = len(g["revealed"])
        if revealed_count < 1: return web.json_response({"error": "Reveal at least one tile before cashing out"}, status=400)
        cur_mult = _mines_mult(g["n_mines"], revealed_count)
        winnings = int(g["bet"] * cur_mult)
        credit_pb(user, winnings)
        await save_place_bucks()
        await push_pb_update(user)
        g["done"] = True
        await broadcast_casino_result(user, "Mines", g["bet"], winnings, f"cashed out at x{cur_mult} ({revealed_count} reveals)")
        payload = {"ok": True, "phase": "done", "hit_mine": False, "cashed_out": True, "mines": list(g["mines"]), "revealed": list(g["revealed"]), "multiplier": cur_mult, "winnings": winnings, "bet": g["bet"], "balance": get_pb(user)}
        mines_games.pop(ulow, None)
        return web.json_response(payload)
    if action == "state":
        g = mines_games.get(ulow)
        if not g or g["done"]: return web.json_response({"phase": "none"})
        return web.json_response({"ok": True, "phase": "playing", "mines": g["n_mines"], "revealed": list(g["revealed"]), "multiplier": _mines_mult(g["n_mines"], len(g["revealed"])), "next_mult": _mines_mult(g["n_mines"], len(g["revealed"]) + 1), "bet": g["bet"], "balance": get_pb(user)})
    return web.json_response({"error": "Bad action"}, status=400)

GD_LEVEL_LENGTH = 5400
GD_MIN_PLAY_MS_PER_PCT = 400
GD_DAILY_PLAY_CAP = 30
GD_MAX_MULT = 1.75
gd_attempts = {}

async def casino_gd_start_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    try: amount = int(data.get("amount", 0))
    except: return web.json_response({"error": "Invalid bet"}, status=400)
    if amount < 1: return web.json_response({"error": "Bet must be at least 1 $"}, status=400)
    if amount > MAX_CASINO_BET: return web.json_response({"error": f"Max bet is {MAX_CASINO_BET} $"}, status=400)
    if not check_rate_limit(user, "gd_start", GD_DAILY_PLAY_CAP, 60 * 60 * 24):
        return web.json_response({"error": f"Daily play cap reached ({GD_DAILY_PLAY_CAP} attempts/24h)"}, status=429)
    if not spend_pb(user, amount):
        return web.json_response({"error": "Not enough PlaceBucks"}, status=400)
    seed_in = str(data.get("seed", "")).strip()[:12]
    if seed_in and seed_in.isdigit():
        seed = int(seed_in)
    else:
        seed = secrets.randbelow(1_000_000_000)
    gd_attempts[user.lower()] = {"seed": seed, "bet": amount, "start_ts": time.time(), "done": False}
    return web.json_response({"ok": True, "seed": seed, "length": GD_LEVEL_LENGTH, "max_mult": GD_MAX_MULT, "balance": get_pb(user)})

async def casino_gd_result_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    ulow = user.lower()
    g = gd_attempts.get(ulow)
    if not g or g.get("done"):
        return web.json_response({"error": "No active attempt"}, status=400)
    try:
        progress = max(0.0, min(1.0, float(data.get("progress", 0))))
        time_ms = max(0, int(data.get("time_ms", 0)))
        seed = int(data.get("seed", -1))
    except: return web.json_response({"error": "Bad payload"}, status=400)
    if seed != g["seed"]:
        return web.json_response({"error": "Seed mismatch"}, status=400)
    pct = progress * 100
    server_elapsed_ms = int((time.time() - g["start_ts"]) * 1000)
    min_required_ms = int(pct * GD_MIN_PLAY_MS_PER_PCT)
    if time_ms < min_required_ms or server_elapsed_ms < min_required_ms:
        g["done"] = True
        gd_attempts.pop(ulow, None)
        return web.json_response({"error": "Run too fast to be real (anti-cheat)"}, status=400)
    if progress < 0.5:
        multiplier = round(2 * progress, 3)
    else:
        multiplier = round(1 + (GD_MAX_MULT - 1) * 2 * (progress - 0.5), 3)
    winnings = int(g["bet"] * multiplier)
    credit_pb(user, winnings)
    await save_place_bucks()
    await push_pb_update(user)
    g["done"] = True
    detail = f"reached {pct:.0f}% (x{multiplier}) seed {g['seed']}"
    await broadcast_casino_result(user, "GeoDash", g["bet"], winnings, detail)
    gd_attempts.pop(ulow, None)
    return web.json_response({"ok": True, "progress": progress, "multiplier": multiplier, "winnings": winnings, "bet": g["bet"], "balance": get_pb(user), "seed": g["seed"]})

PLINKO_ROWS = 9
PLINKO_MULTIPLIERS = [10.0, 3.0, 1.4, 1.1, 0.5, 0.3, 0.5, 1.1, 1.4, 3.0, 10.0]

async def casino_plinko_handler(request):
    res, err = await _casino_open(request, "casino_plinko")
    if err: return err
    user, amount, _ = res
    path = []
    bucket = 0
    for _ in range(PLINKO_ROWS):
        r = secrets.randbelow(2)
        path.append(r)
        bucket += r
    if bucket < 0: bucket = 0
    if bucket >= len(PLINKO_MULTIPLIERS): bucket = len(PLINKO_MULTIPLIERS) - 1
    mult = PLINKO_MULTIPLIERS[bucket]
    winnings = int(amount * mult)
    await _casino_payout(user, winnings)
    await broadcast_casino_result(user, "Plinko", amount, winnings, f"bucket {bucket} (x{mult})")
    return web.json_response({"ok": True, "path": path, "bucket": bucket, "multiplier": mult, "winnings": winnings, "bet": amount, "balance": get_pb(user)})

async def casino_coinflip_handler(request):
    res, err = await _casino_open(request, "casino_coinflip")
    if err: return err
    user, amount, data = res
    choice = (data.get("choice") or "").strip().lower()
    if choice not in ("heads", "tails"):
        credit_pb(user, amount); await save_place_bucks(); await push_pb_update(user)
        return web.json_response({"error": "Choice must be heads or tails"}, status=400)
    flip = "heads" if secrets.randbelow(2) == 0 else "tails"
    winnings = 0; outcome = "lose"
    if choice == flip:
        winnings = int(amount * 1.95)                    
        outcome = "win"
    await _casino_payout(user, winnings)
    await broadcast_casino_result(user, "Coin Flip", amount, winnings, f"{flip} (called {choice})")
    return web.json_response({"ok": True, "flip": flip, "outcome": outcome, "winnings": winnings, "bet": amount, "balance": get_pb(user)})

bj_games = {}                                                                   

def _bj_score(hand):
    total = 0; aces = 0
    for c in hand:
        v = c % 13 + 1
        if v == 1: aces += 1; total += 11
        elif v >= 10: total += 10
        else: total += v
    while total > 21 and aces > 0:
        total -= 10; aces -= 1
    return total

def _bj_label(c):
    val = c % 13 + 1
    name = {1: 'A', 11: 'J', 12: 'Q', 13: 'K'}.get(val, str(val))
    suit_ch = ['S', 'H', 'D', 'C'][c // 13]
    return name + suit_ch

def _bj_view(hand, hide_first=False):
    out = [_bj_label(c) for c in hand]
    if hide_first and out: out[1:] = out[1:]; out[0] = '??'                                                            
    return out

async def _bj_resolve(user, game):
    bet = game["bet"]
    psc = _bj_score(game["player"])
    dsc = _bj_score(game["dealer"])
    if psc > 21:
        outcome = "lose"; winnings = 0
    elif dsc > 21 or psc > dsc:
        outcome = "win"; winnings = bet * 2
    elif psc < dsc:
        outcome = "lose"; winnings = 0
    else:
        outcome = "push"; winnings = bet
    if winnings > 0: credit_pb(user, winnings)
    await save_place_bucks()
    await push_pb_update(user)
    game["done"] = True
    detail = "bust" if psc > 21 else f"{psc} vs dealer {dsc}"
    await broadcast_casino_result(user, "Blackjack", bet, winnings, detail)
    return outcome, winnings, psc, dsc

async def casino_blackjack_handler(request):
    """One endpoint with action=start|hit|stand."""
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    action = (data.get("action") or "").strip().lower()
    ulow = user.lower()
    if action == "start":
        if ulow in bj_games and not bj_games[ulow].get("done"):
            return web.json_response({"error": "Finish your current hand first (hit or stand)"}, status=400)
        try: amount = int(data.get("amount", 0))
        except: return web.json_response({"error": "Invalid bet"}, status=400)
        if amount < 1: return web.json_response({"error": "Bet must be at least 1 $"}, status=400)
        if amount > MAX_CASINO_BET: return web.json_response({"error": f"Max bet is {MAX_CASINO_BET} $"}, status=400)
        if not spend_pb(user, amount):
            return web.json_response({"error": "Not enough PlaceBucks"}, status=400)
        deck = list(range(52))
        secrets.SystemRandom().shuffle(deck)
                                                                         
        player = [deck.pop(), deck.pop()]
        dealer = [deck.pop(), deck.pop()]
        game = {"deck": deck, "player": player, "dealer": dealer, "bet": amount, "done": False}
        bj_games[ulow] = game
        psc = _bj_score(player); dsc_up = _bj_score([dealer[0]])
                          
        if psc == 21:
            if _bj_score(dealer) == 21:
                      
                credit_pb(user, amount); await save_place_bucks(); await push_pb_update(user)
                game["done"] = True
                await broadcast_casino_result(user, "Blackjack", amount, amount, "double 21 push")
                return web.json_response({"ok": True, "phase": "done", "player": _bj_view(player), "dealer": _bj_view(dealer), "player_score": 21, "dealer_score": 21, "outcome": "push", "winnings": amount, "bet": amount, "balance": get_pb(user)})
            else:
                pay = int(amount * 2.5)
                credit_pb(user, pay); await save_place_bucks(); await push_pb_update(user)
                game["done"] = True
                await broadcast_casino_result(user, "Blackjack", amount, pay, "NATURAL 21")
                return web.json_response({"ok": True, "phase": "done", "player": _bj_view(player), "dealer": _bj_view(dealer), "player_score": 21, "dealer_score": _bj_score(dealer), "outcome": "blackjack", "winnings": pay, "bet": amount, "balance": get_pb(user)})
        return web.json_response({"ok": True, "phase": "player", "player": _bj_view(player), "dealer_up": _bj_label(dealer[0]), "player_score": psc, "dealer_up_score": dsc_up, "bet": amount, "balance": get_pb(user)})
                                      
    game = bj_games.get(ulow)
    if not game or game.get("done"):
        return web.json_response({"error": "No active blackjack game - deal first"}, status=400)
    if action == "hit":
        game["player"].append(game["deck"].pop())
        psc = _bj_score(game["player"])
        if psc > 21:
            outcome, winnings, _, dsc = await _bj_resolve(user, game)
            return web.json_response({"ok": True, "phase": "done", "player": _bj_view(game["player"]), "dealer": _bj_view(game["dealer"]), "player_score": psc, "dealer_score": dsc, "outcome": "bust", "winnings": 0, "bet": game["bet"], "balance": get_pb(user)})
        return web.json_response({"ok": True, "phase": "player", "player": _bj_view(game["player"]), "dealer_up": _bj_label(game["dealer"][0]), "player_score": psc, "bet": game["bet"], "balance": get_pb(user)})
    if action == "stand":
                                
        while _bj_score(game["dealer"]) < 17:
            game["dealer"].append(game["deck"].pop())
        outcome, winnings, psc, dsc = await _bj_resolve(user, game)
        return web.json_response({"ok": True, "phase": "done", "player": _bj_view(game["player"]), "dealer": _bj_view(game["dealer"]), "player_score": psc, "dealer_score": dsc, "outcome": outcome, "winnings": winnings, "bet": game["bet"], "balance": get_pb(user)})
    return web.json_response({"error": "Unknown action"}, status=400)

rps_queue = []                                                                             
rps_games = {}                                                                                        
RPS_GAME_TIMEOUT = 10


def _rps_beats(a, b):
    return (a == "rock" and b == "scissors") or (a == "paper" and b == "rock") or (a == "scissors" and b == "paper")

async def casino_rps_solo_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    try: amount = int(data.get("amount", 0))
    except: return web.json_response({"error": "Invalid bet"}, status=400)
    if amount < 1: return web.json_response({"error": "Bet must be at least 1 $"}, status=400)
    if amount > MAX_CASINO_BET: return web.json_response({"error": f"Max bet is {MAX_CASINO_BET} $"}, status=400)
    choice = (data.get("choice") or "").strip().lower()
    if choice not in ("rock", "paper", "scissors"):
        return web.json_response({"error": "Pick rock, paper, or scissors"}, status=400)
    if not spend_pb(user, amount):
        return web.json_response({"error": "Not enough PlaceBucks"}, status=400)
    ai = secrets.choice(["rock", "paper", "scissors"])
    if choice == ai:
        outcome = "tie"; winnings = amount        
    elif _rps_beats(choice, ai):
        outcome = "win"; winnings = int(amount * 1.95)
    else:
        outcome = "lose"; winnings = 0
    if winnings > 0: credit_pb(user, winnings)
    await save_place_bucks(); await push_pb_update(user)
    await broadcast_casino_result(user, "RPS vs AI", amount, winnings, f"{choice} vs {ai}")
    return web.json_response({"ok": True, "you": choice, "opponent": ai, "outcome": outcome, "winnings": winnings, "bet": amount, "balance": get_pb(user)})

async def casino_rps_join_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    try: amount = int(data.get("amount", 0))
    except: return web.json_response({"error": "Invalid bet"}, status=400)
    if amount < 1: return web.json_response({"error": "Bet must be at least 1 $"}, status=400)
    if amount > MAX_CASINO_BET: return web.json_response({"error": f"Max bet is {MAX_CASINO_BET} $"}, status=400)
    ulow = user.lower()
                                  
    for q in rps_queue:
        if q["user"].lower() == ulow:
            return web.json_response({"error": "Already in queue"}, status=400)
    for gid, g in rps_games.items():
        if ulow in g["choices"]:
            return web.json_response({"ok": True, "matched": True, "game_id": gid, "opponent": g["p2"] if g["p1"].lower() == ulow else g["p1"], "bet": g["bet"]})
    if not spend_pb(user, amount):
        return web.json_response({"error": "Not enough PlaceBucks"}, status=400)
                                             
    opp_idx = next((i for i, q in enumerate(rps_queue) if q["bet"] == amount and q["user"].lower() != ulow), None)
    if opp_idx is not None:
        opp = rps_queue.pop(opp_idx)
        gid = secrets.token_hex(6)
        rps_games[gid] = {"p1": opp["user"], "p2": user, "bet": amount, "choices": {opp["user"].lower(): None, ulow: None}, "started_at": time.time()}
        await save_place_bucks(); await push_pb_update(user)
        await notify_social(opp["user"], {"type": "rps_matched", "game_id": gid, "opponent": user, "bet": amount})
        return web.json_response({"ok": True, "matched": True, "game_id": gid, "opponent": opp["user"], "bet": amount, "balance": get_pb(user)})
    rps_queue.append({"user": user, "bet": amount, "joined_at": time.time()})
    await save_place_bucks(); await push_pb_update(user)
    return web.json_response({"ok": True, "matched": False, "waiting": True, "bet": amount, "balance": get_pb(user)})

rps_challenges = {}                                                                   
RPS_CHALLENGE_TTL = 60           

async def casino_rps_challenge_handler(request):
    """Challenge a specific player to RPS. Bets are NOT escrowed until the
    opponent accepts; the challenge expires after RPS_CHALLENGE_TTL seconds."""
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    target = (data.get("opponent") or "").strip()
    try: amount = int(data.get("amount", 0))
    except: return web.json_response({"error": "Invalid bet"}, status=400)
    if amount < 1: return web.json_response({"error": "Bet must be at least 1 $"}, status=400)
    if amount > MAX_CASINO_BET: return web.json_response({"error": f"Max bet is {MAX_CASINO_BET} $"}, status=400)
    if not target: return web.json_response({"error": "Pick an opponent"}, status=400)
    if target.lower() == user.lower(): return web.json_response({"error": "you can't challenge yourself, you're not a bank"}, status=400)
    found = next((u for u in accounts if u.lower() == target.lower()), None)
    if not found: return web.json_response({"error": "User not found"}, status=404)
    if get_pb(user) < amount and not is_admin(user):
        return web.json_response({"error": f"Not enough PlaceBucks (need {amount})"}, status=400)
    cid = secrets.token_hex(6)
    rps_challenges[cid] = {"challenger": user, "opponent": found, "bet": amount, "created_at": time.time()}
    await notify_social(found, {"type": "rps_challenge", "from": user, "bet": amount, "challenge_id": cid})
    return web.json_response({"ok": True, "challenge_id": cid, "expires_in": RPS_CHALLENGE_TTL})

async def casino_rps_respond_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    cid = data.get("challenge_id", "")
    accept = bool(data.get("accept", False))
    ch = rps_challenges.get(cid)
    if not ch: return web.json_response({"error": "Challenge not found or expired"}, status=404)
    if ch["opponent"].lower() != user.lower():
        return web.json_response({"error": "Not your challenge"}, status=403)
    if time.time() - ch["created_at"] > RPS_CHALLENGE_TTL:
        rps_challenges.pop(cid, None)
        return web.json_response({"error": "Challenge expired"}, status=400)
    if not accept:
        rps_challenges.pop(cid, None)
        await notify_social(ch["challenger"], {"type": "rps_challenge_declined", "by": user})
        return web.json_response({"ok": True, "declined": True})
                                               
    if not spend_pb(ch["challenger"], ch["bet"]):
        rps_challenges.pop(cid, None)
        await notify_social(ch["challenger"], {"type": "rps_challenge_declined", "by": user, "reason": "challenger broke"})
        return web.json_response({"error": "Challenger no longer has enough PlaceBucks"}, status=400)
    if not spend_pb(user, ch["bet"]):
        credit_pb(ch["challenger"], ch["bet"])
        await save_place_bucks(); await push_pb_update(ch["challenger"])
        rps_challenges.pop(cid, None)
        return web.json_response({"error": "Not enough PlaceBucks"}, status=400)
    gid = secrets.token_hex(6)
    rps_games[gid] = {"p1": ch["challenger"], "p2": user, "bet": ch["bet"], "choices": {ch["challenger"].lower(): None, user.lower(): None}, "started_at": time.time()}
    rps_challenges.pop(cid, None)
    await save_place_bucks()
    await push_pb_update(ch["challenger"]); await push_pb_update(user)
    await notify_social(ch["challenger"], {"type": "rps_matched", "game_id": gid, "opponent": user, "bet": ch["bet"]})
    return web.json_response({"ok": True, "game_id": gid, "opponent": ch["challenger"], "bet": ch["bet"], "balance": get_pb(user)})

async def casino_rps_cancel_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    ulow = user.lower()
    for i, q in enumerate(rps_queue):
        if q["user"].lower() == ulow:
            refund = q["bet"]
            rps_queue.pop(i)
            credit_pb(user, refund)
            await save_place_bucks(); await push_pb_update(user)
            return web.json_response({"ok": True, "refunded": refund, "balance": get_pb(user)})
    return web.json_response({"error": "Not in queue"}, status=404)

async def _rps_finish(gid, requesting_user):
    g = rps_games.get(gid)
    if not g: return None
    p1, p2, bet = g["p1"], g["p2"], g["bet"]
    p1l, p2l = p1.lower(), p2.lower()
    c1, c2 = g["choices"][p1l], g["choices"][p2l]
    timeout = False
    if c1 is None or c2 is None:
        timeout = True
        credit_pb(p1, bet); credit_pb(p2, bet); winner = None
        outcome_for = lambda u: "tie"
    else:
        if c1 == c2:
            credit_pb(p1, bet); credit_pb(p2, bet); winner = None
        else:
            winner = p1 if _rps_beats(c1, c2) else p2
            credit_pb(winner, bet * 2)
        outcome_for = lambda u: "tie" if winner is None else ("win" if u.lower() == winner.lower() else "lose")
    await save_place_bucks()
    await push_pb_update(p1); await push_pb_update(p2)
                      
    for u in (p1, p2):
        my_out = outcome_for(u)
        opp = p2 if u.lower() == p1l else p1
        if my_out == "tie":
            await broadcast_casino_result(u, "RPS", bet, bet, f"tie vs {opp}")
        elif my_out == "win":
            await broadcast_casino_result(u, "RPS", bet, bet * 2, ("forfeit by " + opp) if timeout else f"beat {opp}")
        else:
            await broadcast_casino_result(u, "RPS", bet, 0, ("forfeited to " + opp) if timeout else f"lost to {opp}")
    del rps_games[gid]
    is_p1 = requesting_user.lower() == p1l
    my_choice = c1 if is_p1 else c2
    opp_choice = c2 if is_p1 else c1
    opp_name = p2 if is_p1 else p1
    my_out = outcome_for(requesting_user)
    winnings = bet * 2 if my_out == "win" else (bet if my_out == "tie" else 0)
    return {"ok": True, "phase": "done", "you": my_choice, "opponent": opp_choice, "opponent_name": opp_name, "outcome": my_out, "winnings": winnings, "bet": bet, "timeout": timeout, "balance": get_pb(requesting_user)}

async def casino_rps_play_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    gid = data.get("game_id", "")
    choice = (data.get("choice") or "").strip().lower()
    if choice not in ("rock", "paper", "scissors"):
        return web.json_response({"error": "Pick rock, paper, or scissors"}, status=400)
    g = rps_games.get(gid)
    if not g: return web.json_response({"error": "Game not found"}, status=404)
    ulow = user.lower()
    if ulow not in g["choices"]:
        return web.json_response({"error": "Not your game"}, status=403)
    if g["choices"][ulow] is not None:
        return web.json_response({"error": "You already played"}, status=400)
    g["choices"][ulow] = choice
    if all(v is not None for v in g["choices"].values()):
        result = await _rps_finish(gid, user)
        return web.json_response(result)
    return web.json_response({"ok": True, "phase": "waiting"})

async def casino_rps_status_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    gid = request.query.get("game_id", "")
    g = rps_games.get(gid)
    if not g: return web.json_response({"phase": "gone"})
    ulow = user.lower()
    if ulow not in g["choices"]:
        return web.json_response({"error": "Not your game"}, status=403)
    elapsed = time.time() - g["started_at"]
    other = next(k for k in g["choices"] if k != ulow)
    if elapsed > RPS_GAME_TIMEOUT and not all(v is not None for v in g["choices"].values()):
        result = await _rps_finish(gid, user)
        return web.json_response(result)
    return web.json_response({"ok": True, "phase": "playing", "you_played": g["choices"][ulow] is not None, "opponent_played": g["choices"][other] is not None, "elapsed": int(elapsed), "timeout": RPS_GAME_TIMEOUT})

uno_rooms = {}
UNO_FORFEIT_SECONDS = 12

def _uno_new_deck():
    deck = []
    for color in ("r", "y", "g", "b"):
        deck.append({"color": color, "value": "0"})
        for v in range(1, 10):
            deck.append({"color": color, "value": str(v)})
            deck.append({"color": color, "value": str(v)})
        for a in ("skip", "reverse", "draw2"):
            deck.append({"color": color, "value": a})
            deck.append({"color": color, "value": a})
    for _ in range(4):
        deck.append({"color": "w", "value": "wild"})
        deck.append({"color": "w", "value": "wild4"})
    secrets.SystemRandom().shuffle(deck)
    return deck

def _uno_can_play(card, top, current_color, pending_draws, pending_kind, stacking):
    if pending_draws > 0:
        if not stacking: return False
        if pending_kind == "draw2" and card["value"] == "draw2": return True
        if pending_kind == "wild4" and card["value"] == "wild4": return True
        return False
    if card["color"] == "w": return True
    if card["color"] == current_color: return True
    if card["value"] == top["value"] and top["color"] != "w": return True
    return False

def _uno_advance(room, skip=False):
    n = len(room["players"])
    step = room["direction"] * (2 if skip else 1)
    room["current"] = (room["current"] + step) % n

def _uno_draw_cards(room, player_idx, n):
    for _ in range(n):
        if not room["deck"]:
            if len(room["discard"]) <= 1: return
            top = room["discard"].pop()
            room["deck"] = room["discard"]
            secrets.SystemRandom().shuffle(room["deck"])
            room["discard"] = [top]
        room["players"][player_idx]["hand"].append(room["deck"].pop())

def _uno_public_state(room, for_player):
    players_info = []
    for i, p in enumerate(room["players"]):
        players_info.append({"name": p["name"], "is_ai": p["is_ai"], "hand_count": len(p["hand"]), "connected": p.get("connected", True)})
    me = next((i for i, p in enumerate(room["players"]) if p["name"].lower() == for_player.lower()), None)
    is_spectator = me is None and for_player and any(s.lower() == for_player.lower() for s in room.get("spectators", []))
    state = {
        "room_id": room["id"], "phase": room["phase"], "players": players_info,
        "current": room["current"], "direction": room["direction"],
        "current_color": room["current_color"],
        "top": room["discard"][-1] if room["discard"] else None,
        "pending_draws": room["pending_draws"], "pending_kind": room["pending_kind"],
        "settings": room["settings"], "creator": room["creator"],
        "my_index": me,
        "my_hand": room["players"][me]["hand"] if me is not None else [],
        "winner": room.get("winner"),
        "pool": room.get("pool", 0),
        "spectator": is_spectator,
        "spectator_count": len(room.get("spectators", [])),
    }
    return state

UNO_TURN_TIMEOUT_SECONDS = 60

async def _uno_turn_timeout_watcher(rid):
    """Wakes up UNO_TURN_TIMEOUT_SECONDS after a state push. If the same player is
    still on the same turn (last_action_at unchanged), force them to draw and
    advance. Stale watchers (state already moved on) exit harmlessly."""
    room = uno_rooms.get(rid)
    if not room or room["phase"] != "playing": return
    snapshot_ts = room["last_action_at"]
    snapshot_idx = room["current"]
    snapshot_player = room["players"][snapshot_idx]["name"]
    try: await asyncio.sleep(UNO_TURN_TIMEOUT_SECONDS)
    except asyncio.CancelledError: return
    room = uno_rooms.get(rid)
    if not room or room["phase"] != "playing": return
    if room["last_action_at"] != snapshot_ts: return
    if room["current"] != snapshot_idx: return
    p = room["players"][snapshot_idx]
    if p.get("is_ai"): return
    await _uno_play_one_ai_move(room, snapshot_idx)
    payload = {"type": "uno_chat", "room_id": rid, "msg": {"from": "system", "text": f"{snapshot_player} took too long - AI played their turn.", "time": int(time.time()), "spectator": True}}
    for pp in room["players"]:
        if not pp["is_ai"]:
            try: await notify_social(pp["name"], payload)
            except: pass
    for sname in list(room.get("spectators", [])):
        try: await notify_social(sname, payload)
        except: pass
    await _uno_push_state(room)
    if room["phase"] == "playing" and room["players"][room["current"]]["is_ai"]:
        asyncio.create_task(_uno_ai_turn(room))

async def _uno_push_state(room):
    for p in room["players"]:
        if p["is_ai"]: continue
        try:
            await notify_social(p["name"], {"type": "uno_state", "state": _uno_public_state(room, p["name"])})
        except: pass
    for sname in list(room.get("spectators", [])):
        try:
            await notify_social(sname, {"type": "uno_state", "state": _uno_public_state(room, sname)})
        except: pass
    if room.get("phase") == "playing" and not room["players"][room["current"]].get("is_ai"):
        asyncio.create_task(_uno_turn_timeout_watcher(room["id"]))

async def _uno_play_one_ai_move(room, player_idx):
    """Pick and execute a single AI-style move for the given player. Used by AI
    bots taking their natural turn and by the turn-timeout watcher to play *for*
    a human who didn't act in time (so they can't dodge their hand by stalling).
    Caller is responsible for pushing state afterwards."""
    p = room["players"][player_idx]
    top = room["discard"][-1]
    stacking_setting = room["settings"].get("stacking", False)
    idx = next((i for i, c in enumerate(p["hand"]) if _uno_can_play(c, top, room["current_color"], room["pending_draws"], room["pending_kind"], stacking_setting)), None)
    if idx is None:
        if room["pending_draws"] > 0:
            _uno_draw_cards(room, player_idx, room["pending_draws"])
            room["pending_draws"] = 0; room["pending_kind"] = None
            _uno_advance(room)
            room["last_action_at"] = time.time()
            return
        if room["settings"].get("draw_till"):
            drew = 0; max_draws = 30
            new_idx = None
            while drew < max_draws:
                before = len(p["hand"])
                _uno_draw_cards(room, player_idx, 1)
                if len(p["hand"]) == before: break
                drew += 1
                if _uno_can_play(p["hand"][-1], top, room["current_color"], room["pending_draws"], room["pending_kind"], stacking_setting):
                    new_idx = len(p["hand"]) - 1
                    break
            if new_idx is None:
                _uno_advance(room)
                room["last_action_at"] = time.time()
                return
            idx = new_idx
        else:
            _uno_draw_cards(room, player_idx, 1)
            _uno_advance(room)
            room["last_action_at"] = time.time()
            return
    color = None
    if p["hand"][idx]["color"] == "w":
        counts = {"r": 0, "y": 0, "g": 0, "b": 0}
        for c in p["hand"]:
            if c["color"] in counts: counts[c["color"]] += 1
        color = max(counts, key=lambda k: counts[k]) if any(counts.values()) else secrets.choice(["r", "y", "g", "b"])
    await _uno_apply_play(room, player_idx, idx, color)

async def _uno_ai_turn(room):
    while not room.get("winner") and room["players"][room["current"]]["is_ai"]:
        await asyncio.sleep(0.6)
        await _uno_play_one_ai_move(room, room["current"])
        await _uno_push_state(room)

async def _uno_apply_play(room, player_idx, card_idx, chosen_color):
    await _uno_apply_play_multi(room, player_idx, [card_idx], chosen_color)

async def _uno_apply_play_multi(room, player_idx, card_idxs, chosen_color):
    """Plays one or more cards from the player's hand on a single turn.
    All cards after the first must share the same value as the anchor (caller
    is responsible for validating that). Action-card effects stack by count:
    N skips skip N players, N draw2s force the next player to draw 2*N, etc."""
    p = room["players"][player_idx]
    cards = [p["hand"][i] for i in card_idxs]
    for i in sorted(card_idxs, reverse=True):
        p["hand"].pop(i)
    for c in cards:
        room["discard"].append(c)
    room["last_action_at"] = time.time()
    last_card = cards[-1]
    if last_card["color"] == "w":
        room["current_color"] = chosen_color or "r"
    else:
        room["current_color"] = last_card["color"]
    if len(p["hand"]) == 0:
        room["phase"] = "done"
        room["winner"] = p["name"]
        await _uno_finalize(room)
        return
    anchor_value = cards[0]["value"]
    n = len(cards)
    if anchor_value == "skip":
        for _ in range(n):
            _uno_advance(room, skip=True)
    elif anchor_value == "reverse":
        for _ in range(n):
            room["direction"] *= -1
        if len(room["players"]) == 2 and (n % 2 == 1):
            _uno_advance(room, skip=True)
        else:
            _uno_advance(room)
    elif anchor_value == "draw2":
        room["pending_draws"] += 2 * n; room["pending_kind"] = "draw2"
        _uno_advance(room)
    elif anchor_value == "wild4":
        room["pending_draws"] += 4 * n; room["pending_kind"] = "wild4"
        _uno_advance(room)
    else:
        room["pending_draws"] = 0; room["pending_kind"] = None
        _uno_advance(room)

async def _uno_finalize(room):
    pool = room.get("pool", 0)
    winner = room.get("winner")
    if winner and pool > 0:
        credit_pb(winner, pool)
        await save_place_bucks()
        await push_pb_update(winner)
        await broadcast_casino_result(winner, "UNO", room["bet_per_human"], pool, f"won pot of {pool}")
    for p in room["players"]:
        if p["is_ai"]: continue
        if winner and p["name"].lower() != winner.lower():
            await broadcast_casino_result(p["name"], "UNO", room["bet_per_human"], 0, f"lost to {winner}")
    asyncio.get_event_loop().call_later(60, lambda: uno_rooms.pop(room["id"], None))

async def uno_forfeit(user):
    ulow = user.lower()
    for rid, room in list(uno_rooms.items()):
        if room["phase"] not in ("lobby", "playing"): continue
        idx = next((i for i, p in enumerate(room["players"]) if not p["is_ai"] and p["name"].lower() == ulow), None)
        if idx is None: continue
        if room["phase"] == "lobby":
            removed = room["players"].pop(idx)
            if room["players"]:
                if room["creator"].lower() == ulow:
                    nh = next((p for p in room["players"] if not p["is_ai"]), None)
                    if nh: room["creator"] = nh["name"]
                    else:
                        uno_rooms.pop(rid, None); continue
                credit_pb(user, room["bet_per_human"])
                await save_place_bucks(); await push_pb_update(user)
                await _uno_push_state(room)
            else:
                uno_rooms.pop(rid, None)
        else:
            room["players"][idx]["connected"] = False
            room["players"][idx]["forfeited"] = True
            humans_left = [p for p in room["players"] if not p["is_ai"] and not p.get("forfeited")]
            if len(humans_left) == 0:
                room["phase"] = "done"; room["winner"] = None
                uno_rooms.pop(rid, None)
            elif len(humans_left) + sum(1 for p in room["players"] if p["is_ai"]) <= 1:
                room["phase"] = "done"
                room["winner"] = humans_left[0]["name"] if humans_left else None
                await _uno_finalize(room)
            else:
                if room["current"] == idx:
                    _uno_advance(room)
                await _uno_push_state(room)
                if room["players"][room["current"]]["is_ai"]:
                    asyncio.create_task(_uno_ai_turn(room))

async def uno_create_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    try:
        bet = int(data.get("bet", 0))
        max_players = max(2, min(4, int(data.get("max_players", 2))))
        ai_count = max(0, min(max_players - 1, int(data.get("ai_count", 0))))
        stacking = bool(data.get("stacking", False))
        jump_ins = bool(data.get("jump_ins", False))
        multi_color = bool(data.get("multi_color", False))
        draw_till = bool(data.get("draw_till", False))
    except: return web.json_response({"error": "Invalid settings"}, status=400)
    if bet < 0: return web.json_response({"error": "Bet must be >= 0"}, status=400)
    if bet > MAX_CASINO_BET: return web.json_response({"error": f"Max UNO bet is {MAX_CASINO_BET} $"}, status=400)
    if bet > 0 and not spend_pb(user, bet):
        return web.json_response({"error": "Not enough PlaceBucks"}, status=400)
    rid = secrets.token_hex(5)
    players = [{"name": user, "hand": [], "is_ai": False, "connected": True}]
    for i in range(ai_count):
        players.append({"name": f"AI Bot {i+1}", "hand": [], "is_ai": True, "connected": True})
    uno_rooms[rid] = {
        "id": rid, "creator": user, "players": players, "phase": "lobby",
        "settings": {"max_players": max_players, "ai_count": ai_count, "stacking": stacking, "jump_ins": jump_ins, "multi_color": multi_color, "draw_till": draw_till, "bet": bet},
        "deck": [], "discard": [], "current": 0, "direction": 1, "current_color": None,
        "pending_draws": 0, "pending_kind": None, "bet_per_human": bet, "pool": bet if bet > 0 else 0,
        "started_at": None, "last_action_at": time.time(),
        "spectators": [],
    }
    await save_place_bucks(); await push_pb_update(user)
    return web.json_response({"ok": True, "room_id": rid, "state": _uno_public_state(uno_rooms[rid], user)})

async def uno_list_handler(request):
    out = []
    for r in uno_rooms.values():
        if r["phase"] not in ("lobby", "playing"): continue
        out.append({
            "room_id": r["id"], "creator": r["creator"],
            "players": [p["name"] for p in r["players"]],
            "max_players": r["settings"]["max_players"],
            "bet": r["settings"]["bet"], "stacking": r["settings"]["stacking"],
            "phase": r["phase"], "pool": r.get("pool", 0),
            "spectators": len(r.get("spectators", [])),
        })
    return web.json_response({"rooms": out})

async def uno_say_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    text = (data.get("text") or "").strip()[:200]
    if not text: return web.json_response({"error": "Empty"}, status=400)
    room = uno_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    ulow = user.lower()
    is_player = any(p["name"].lower() == ulow for p in room["players"])
    is_spec = any(s.lower() == ulow for s in room.get("spectators", []))
    if not (is_player or is_spec):
        return web.json_response({"error": "Not in this room"}, status=403)
    if not check_rate_limit(user, "uno_say", 5, 5):
        return web.json_response({"error": "Slow down."}, status=429)
    chat_msg = {"from": user, "text": text, "spectator": is_spec, "time": time.time()}
    payload = {"type": "uno_chat", "room_id": rid, "msg": chat_msg}
    for p in room["players"]:
        if not p["is_ai"]:
            try: await notify_social(p["name"], payload)
            except: pass
    for sname in list(room.get("spectators", [])):
        try: await notify_social(sname, payload)
        except: pass
    return web.json_response({"ok": True})

async def uno_spectate_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    room = uno_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    if any(p["name"].lower() == user.lower() for p in room["players"]):
        return web.json_response({"error": "You're a player in this room, not a spectator"}, status=400)
    if not any(s.lower() == user.lower() for s in room.get("spectators", [])):
        room.setdefault("spectators", []).append(user)
    await _uno_push_state(room)
    return web.json_response({"ok": True, "state": _uno_public_state(room, user)})

async def uno_unspectate_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    room = uno_rooms.get(rid)
    if not room: return web.json_response({"ok": True})
    room["spectators"] = [s for s in room.get("spectators", []) if s.lower() != user.lower()]
    await _uno_push_state(room)
    return web.json_response({"ok": True})

async def uno_join_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    room = uno_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    if room["phase"] != "lobby": return web.json_response({"error": "Game already started"}, status=400)
    if any(p["name"].lower() == user.lower() for p in room["players"]):
        return web.json_response({"ok": True, "room_id": rid, "state": _uno_public_state(room, user)})
    if len(room["players"]) >= room["settings"]["max_players"]:
        return web.json_response({"error": "Room is full"}, status=400)
    bet = room["settings"]["bet"]
    if bet > 0 and not spend_pb(user, bet):
        return web.json_response({"error": "Not enough PlaceBucks"}, status=400)
    room["players"].append({"name": user, "hand": [], "is_ai": False, "connected": True})
    room["pool"] += bet
    await save_place_bucks(); await push_pb_update(user)
    await _uno_push_state(room)
    return web.json_response({"ok": True, "room_id": rid, "state": _uno_public_state(room, user)})

async def uno_start_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    room = uno_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    if room["creator"].lower() != user.lower():
        return web.json_response({"error": "Only the creator can start"}, status=403)
    if room["phase"] != "lobby": return web.json_response({"error": "Already started"}, status=400)
    if len(room["players"]) < 2: return web.json_response({"error": "Need at least 2 players"}, status=400)
    bet = room["settings"].get("bet", 0)
    if bet > 0:
        ai_count = sum(1 for p in room["players"] if p["is_ai"])
        room["pool"] = room.get("pool", 0) + bet * ai_count
    room["deck"] = _uno_new_deck()
    for p in room["players"]: p["hand"] = []
    for _ in range(7):
        for i in range(len(room["players"])):
            room["players"][i]["hand"].append(room["deck"].pop())
    while True:
        top = room["deck"].pop()
        if top["color"] != "w" and top["value"] not in ("skip", "reverse", "draw2"):
            room["discard"] = [top]; break
        room["deck"].insert(0, top)
    room["current_color"] = room["discard"][-1]["color"]
    room["current"] = 0
    room["direction"] = 1
    room["pending_draws"] = 0
    room["pending_kind"] = None
    room["phase"] = "playing"
    room["started_at"] = time.time()
    room["last_action_at"] = time.time()
    await _uno_push_state(room)
    if room["players"][room["current"]]["is_ai"]:
        asyncio.create_task(_uno_ai_turn(room))
    return web.json_response({"ok": True, "state": _uno_public_state(room, user)})

async def uno_play_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    room = uno_rooms.get(rid)
    if not room or room["phase"] != "playing":
        return web.json_response({"error": "Not in a playing room"}, status=400)
    idx = next((i for i, p in enumerate(room["players"]) if p["name"].lower() == user.lower()), None)
    if idx is None: return web.json_response({"error": "Not in room"}, status=403)
    if idx != room["current"]: return web.json_response({"error": "Not your turn"}, status=400)
    raw_idxs = data.get("card_idxs")
    if isinstance(raw_idxs, list) and raw_idxs:
        try: card_idxs = [int(x) for x in raw_idxs]
        except: return web.json_response({"error": "Invalid card indices"}, status=400)
    else:
        try: card_idxs = [int(data.get("card_idx", -1))]
        except: return web.json_response({"error": "Invalid card index"}, status=400)
    if len(card_idxs) != len(set(card_idxs)):
        return web.json_response({"error": "Duplicate card indices"}, status=400)
    hand = room["players"][idx]["hand"]
    for ci in card_idxs:
        if ci < 0 or ci >= len(hand):
            return web.json_response({"error": "Invalid card index"}, status=400)
    anchor = hand[card_idxs[0]]
    top = room["discard"][-1]
    if not _uno_can_play(anchor, top, room["current_color"], room["pending_draws"], room["pending_kind"], room["settings"].get("stacking", False)):
        return web.json_response({"error": "That card can't be played"}, status=400)
    multi_color = bool(room["settings"].get("multi_color"))
    if len(card_idxs) > 1:
        if anchor["color"] == "w":
            return web.json_response({"error": "Wild cards can't be stacked"}, status=400)
        for ci in card_idxs[1:]:
            c = hand[ci]
            if c["color"] == "w" or c["value"] != anchor["value"]:
                return web.json_response({"error": "Stacked cards must share the same value (and can't be wild)"}, status=400)
            if not multi_color and c["color"] != anchor["color"]:
                return web.json_response({"error": "Cross-color stacking is off in this room"}, status=400)
    chosen_color = (data.get("color") or "").strip().lower()
    if anchor["color"] == "w" and chosen_color not in ("r", "y", "g", "b"):
        return web.json_response({"error": "Wild needs a color (r/y/g/b)"}, status=400)
    await _uno_apply_play_multi(room, idx, card_idxs, chosen_color if anchor["color"] == "w" else None)
    await _uno_push_state(room)
    if room["phase"] == "playing" and room["players"][room["current"]]["is_ai"]:
        asyncio.create_task(_uno_ai_turn(room))
    return web.json_response({"ok": True, "state": _uno_public_state(room, user)})

async def uno_jump_in_handler(request):
    """House rule: if a player holds an EXACT match (same color AND same value) of
    the current discard top, they can play it out of turn, becoming the current
    player. Wilds can't be jumped (no color match) and pending +2/+4 stacks block
    jump-ins so the targeted player can't dodge their draw with a third party's card.

    Accepts a single `card_idx` or a list `card_idxs`. When multiple are provided
    all must share the exact same color+value as the discard top (so you can
    jump-in and stack all your matching cards in one out-of-turn move)."""
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    room = uno_rooms.get(rid)
    if not room or room["phase"] != "playing":
        return web.json_response({"error": "Not in a playing room"}, status=400)
    if not room["settings"].get("jump_ins"):
        return web.json_response({"error": "Jump-ins aren't enabled in this room"}, status=400)
    if room.get("pending_draws"):
        return web.json_response({"error": "Can't jump in while there are pending draws"}, status=400)
    idx = next((i for i, p in enumerate(room["players"]) if p["name"].lower() == user.lower()), None)
    if idx is None: return web.json_response({"error": "Not in room"}, status=403)
    if idx == room["current"]:
        return web.json_response({"error": "It's your turn already, just play normally"}, status=400)
    raw_idxs = data.get("card_idxs")
    if isinstance(raw_idxs, list) and raw_idxs:
        try: card_idxs = [int(x) for x in raw_idxs]
        except: return web.json_response({"error": "Invalid card indices"}, status=400)
    else:
        try: card_idxs = [int(data.get("card_idx", -1))]
        except: return web.json_response({"error": "Invalid card index"}, status=400)
    if len(card_idxs) != len(set(card_idxs)):
        return web.json_response({"error": "Duplicate card indices"}, status=400)
    hand = room["players"][idx]["hand"]
    for ci in card_idxs:
        if ci < 0 or ci >= len(hand):
            return web.json_response({"error": "Invalid card index"}, status=400)
    top = room["discard"][-1]
    for ci in card_idxs:
        c = hand[ci]
        if c["color"] == "w" or c["color"] != top["color"] or c["value"] != top["value"]:
            return web.json_response({"error": "Every jump-in card must be an EXACT color+value match (no wilds)"}, status=400)
    room["current"] = idx
    await _uno_apply_play_multi(room, idx, card_idxs, None)
    await _uno_push_state(room)
    if room["phase"] == "playing" and room["players"][room["current"]]["is_ai"]:
        asyncio.create_task(_uno_ai_turn(room))
    return web.json_response({"ok": True, "state": _uno_public_state(room, user)})

def _uno_mod_allowed(user):
    return bool(user) and (is_admin(user) or user.lower() == "cleanup")

async def uno_peek_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not _uno_mod_allowed(user): return web.json_response({"error": "Forbidden"}, status=403)
    rid = request.query.get("room_id", "")
    room = uno_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    hands = [{"name": p["name"], "is_ai": p.get("is_ai", False), "hand": p.get("hand", [])} for p in room["players"]]
    return web.json_response({"ok": True, "players": hands, "top": room["discard"][-1] if room.get("discard") else None, "current_color": room.get("current_color")})

async def uno_modify_hand_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not _uno_mod_allowed(user): return web.json_response({"error": "Forbidden"}, status=403)
    rid = data.get("room_id", "")
    room = uno_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    idx = next((i for i, p in enumerate(room["players"]) if p["name"].lower() == user.lower()), None)
    if idx is None: return web.json_response({"error": "Not in room"}, status=403)
    action = (data.get("action") or "").strip().lower()
    hand = room["players"][idx]["hand"]
    if action == "replace":
        try: card_idx = int(data.get("card_idx", -1))
        except: return web.json_response({"error": "Invalid card index"}, status=400)
        color = (data.get("color") or "").strip().lower()
        value = (data.get("value") or "").strip().lower()
        if color not in ("r", "y", "g", "b", "w"): return web.json_response({"error": "Bad color"}, status=400)
        valid_values = {"0","1","2","3","4","5","6","7","8","9","skip","reverse","draw2","wild","wild4"}
        if value not in valid_values: return web.json_response({"error": "Bad value"}, status=400)
        if color == "w" and value not in ("wild", "wild4"): return web.json_response({"error": "Wild color requires wild value"}, status=400)
        if color != "w" and value in ("wild", "wild4"): return web.json_response({"error": "Non-wild color can't have wild value"}, status=400)
        if card_idx < 0 or card_idx >= len(hand):
            return web.json_response({"error": "Invalid card index"}, status=400)
        hand[card_idx] = {"color": color, "value": value}
    elif action == "add":
        color = (data.get("color") or "").strip().lower()
        value = (data.get("value") or "").strip().lower()
        if color not in ("r", "y", "g", "b", "w"): return web.json_response({"error": "Bad color"}, status=400)
        valid_values = {"0","1","2","3","4","5","6","7","8","9","skip","reverse","draw2","wild","wild4"}
        if value not in valid_values: return web.json_response({"error": "Bad value"}, status=400)
        hand.append({"color": color, "value": value})
    elif action == "drop":
        try: card_idx = int(data.get("card_idx", -1))
        except: return web.json_response({"error": "Invalid card index"}, status=400)
        if card_idx < 0 or card_idx >= len(hand):
            return web.json_response({"error": "Invalid card index"}, status=400)
        hand.pop(card_idx)
    else:
        return web.json_response({"error": "Bad action (replace|add|drop)"}, status=400)
    await _uno_push_state(room)
    return web.json_response({"ok": True, "state": _uno_public_state(room, user)})

async def uno_draw_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    room = uno_rooms.get(rid)
    if not room or room["phase"] != "playing":
        return web.json_response({"error": "Not in a playing room"}, status=400)
    idx = next((i for i, p in enumerate(room["players"]) if p["name"].lower() == user.lower()), None)
    if idx is None or idx != room["current"]:
        return web.json_response({"error": "Not your turn"}, status=400)
    if room["pending_draws"] > 0:
        _uno_draw_cards(room, idx, room["pending_draws"])
        room["pending_draws"] = 0; room["pending_kind"] = None
        _uno_advance(room)
    elif room["settings"].get("draw_till"):
        p = room["players"][idx]
        top = room["discard"][-1]
        stacking_setting = room["settings"].get("stacking", False)
        drew = 0
        max_draws = 30
        while drew < max_draws:
            before = len(p["hand"])
            _uno_draw_cards(room, idx, 1)
            if len(p["hand"]) == before:
                break
            drew += 1
            new_card = p["hand"][-1]
            if _uno_can_play(new_card, top, room["current_color"], room["pending_draws"], room["pending_kind"], stacking_setting):
                break
        any_playable = any(_uno_can_play(c, top, room["current_color"], room["pending_draws"], room["pending_kind"], stacking_setting) for c in p["hand"])
        if not any_playable:
            _uno_advance(room)
    else:
        _uno_draw_cards(room, idx, 1)
        _uno_advance(room)
    await _uno_push_state(room)
    if room["phase"] == "playing" and room["players"][room["current"]]["is_ai"]:
        asyncio.create_task(_uno_ai_turn(room))
    return web.json_response({"ok": True, "state": _uno_public_state(room, user)})

async def uno_leave_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    if rid not in uno_rooms:
        return web.json_response({"ok": True})
    await uno_forfeit(user)
    return web.json_response({"ok": True})

async def uno_state_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = request.query.get("room_id", "")
    room = uno_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    return web.json_response({"ok": True, "state": _uno_public_state(room, user)})

BATTLE_GRID = 64
BATTLE_DRAW_SECONDS_DEFAULT = 90
BATTLE_DRAW_SECONDS_MIN = 30
BATTLE_DRAW_SECONDS_MAX = 300
BATTLE_JUDGE_TIMEOUT = 90
BATTLE_PALETTE_SIZE = PALETTE_SIZE
BATTLE_ADJ = [
    "angry", "sleepy", "tiny", "huge", "glowing", "melting", "exploding", "frozen",
    "ancient", "royal", "secret", "hungry", "scared", "smelly", "fluffy", "cosmic",
    "haunted", "drunk", "lost", "broken", "evil", "lazy", "shiny", "rusty",
    "invisible", "feral", "magical", "cursed", "robotic", "rainbow", "moldy",
    "buff", "tired", "confused", "elegant", "wise", "stinky", "polite", "dramatic",
    "cowardly", "noble", "sneaky", "fearless", "depressed", "ecstatic", "mysterious",
    "underwater", "miniature", "giant", "burning", "icy", "smiling", "crying",
    "celebrity", "retired", "8-bit", "bedazzled", "radioactive", "anxious",
    "philosophical", "drunk", "edgy", "wholesome", "feral", "majestic", "broke",
    "gothic", "cyberpunk", "steampunk", "discount", "premium", "knockoff",
    "off-brand", "feral", "viral", "trending", "cancelled", "bored", "horny",
    "feral", "psychic", "telepathic", "hovering", "leaking", "dripping",
]

BATTLE_SUBJECT = [
    "cat", "dog", "dragon", "robot", "ghost", "wizard", "knight", "samurai", "alien",
    "ninja", "pirate", "vampire", "mermaid", "dinosaur", "frog", "spider", "octopus",
    "llama", "capybara", "beaver", "sloth", "owl", "fox", "panda", "raccoon", "duck",
    "goose", "shark", "whale", "snail", "snake", "tiger", "lion", "bear", "rabbit",
    "hedgehog", "platypus", "axolotl", "chameleon", "turtle", "skeleton", "zombie",
    "demon", "angel", "fairy", "centaur", "yeti", "kraken", "cyclops", "mummy",
    "chef", "scientist", "barista", "DJ", "wrestler", "librarian", "clown",
    "astronaut", "cowboy", "viking", "monk", "detective", "plumber", "farmer",
    "potato", "sandwich", "pizza", "donut", "cupcake", "burger", "noodle",
    "lamp", "toaster", "fridge", "couch", "vending machine", "stop sign",
    "traffic cone", "trash can", "pencil", "stapler", "calculator",
    "mailman", "lifeguard", "tax accountant", "DMV worker", "substitute teacher",
    "telemarketer", "bouncer", "uber driver", "influencer", "youtuber",
    "kazoo player", "mime", "clown", "magician", "puppeteer", "ventriloquist",
    "garden gnome", "scarecrow", "rubber duck", "pet rock", "houseplant",
]

BATTLE_ACTION = [
    "dancing", "sleeping", "fighting a worm", "eating pizza", "riding a bike",
    "brushing teeth", "playing chess", "surfing", "doing yoga", "painting",
    "breakdancing", "lifting weights", "stealing fries", "running away",
    "taking a selfie", "writing a poem", "singing karaoke", "ordering coffee",
    "filing taxes", "playing guitar", "skateboarding", "doing laundry",
    "cooking ramen", "knitting a sweater", "meditating", "screaming",
    "crying", "laughing too hard", "going on a date", "robbing a bank",
    "teaching a class", "ice skating", "juggling", "doing a backflip",
    "buying milk", "checking the mail", "ringing a doorbell", "vacuuming",
    "winning a marathon", "losing at chess", "feeding pigeons", "watering plants",
    "dropping a phone", "missing the bus", "writing a will", "applying for a loan",
    "going to therapy", "trying to parallel park", "binge-watching tv",
    "running from the cops", "starting a podcast", "moonwalking", "doing taxes",
    "writing fanfiction", "rage-quitting", "speedrunning life", "live-streaming",
    "rage-baking", "having an existential crisis", "filing a complaint",
    "playing the recorder badly", "stealing a bike", "delivering mail",
]

BATTLE_SETTING = [
    "a haunted house", "the moon", "underwater", "a cloud", "the desert",
    "a treehouse", "a graveyard", "a cyberpunk city", "a sushi bar", "a museum",
    "outer space", "a volcano", "a swamp", "a library", "a laundromat",
    "a vending machine", "a parking lot", "an elevator", "a giant pumpkin",
    "the bottom of a well", "a snow globe", "a pirate ship", "a circus tent",
    "a haunted castle", "the bus stop", "a coffee shop", "Times Square",
    "a tiny apartment", "the principal's office", "an alien spaceship",
    "a bowling alley", "a roller rink", "a candy factory", "a black hole",
    "Mars", "a frozen lake", "a sushi conveyor belt", "an arcade",
    "the DMV", "an IKEA", "a Costco", "a Waffle House", "an abandoned mall",
    "a self-checkout lane", "the back of an Uber", "a high school gym",
    "a kid's birthday party", "an office Christmas party", "a job interview",
    "a wedding", "a funeral", "a courtroom", "a casino", "a McDonald's drive-thru",
    "a haunted Chuck E Cheese", "a Best Buy parking lot", "an Olive Garden",
    "the inside of a microwave", "a tornado shelter", "the bottom of a pool",
    "a karaoke bar", "a dentist's waiting room",
]


def random_battle_theme():
    """100% computer-generated. Picks one of six grammar templates and slots in
    random words from the adj/subject/action/setting pools. The combined space
    is in the tens of millions of unique prompts."""
    pattern = secrets.randbelow(6)
    if pattern == 0:
        return f"a {secrets.choice(BATTLE_ADJ)} {secrets.choice(BATTLE_SUBJECT)}"
    if pattern == 1:
        return f"a {secrets.choice(BATTLE_SUBJECT)} {secrets.choice(BATTLE_ACTION)}"
    if pattern == 2:
        return f"a {secrets.choice(BATTLE_ADJ)} {secrets.choice(BATTLE_SUBJECT)} {secrets.choice(BATTLE_ACTION)}"
    if pattern == 3:
        return f"a {secrets.choice(BATTLE_SUBJECT)} in {secrets.choice(BATTLE_SETTING)}"
    if pattern == 4:
        return f"a {secrets.choice(BATTLE_ADJ)} {secrets.choice(BATTLE_SUBJECT)} in {secrets.choice(BATTLE_SETTING)}"
    return f"a {secrets.choice(BATTLE_SUBJECT)} {secrets.choice(BATTLE_ACTION)} in {secrets.choice(BATTLE_SETTING)}"

battle_rooms = {}

def _battle_grid_b64(grid_bytes):
    return base64.b64encode(bytes(grid_bytes)).decode("ascii")

def _battle_public_state(room, for_user):
    ulow = (for_user or "").lower()
    role = "spectator"
    if ulow in (n.lower() for n in room["drawers"]): role = "drawer"
    elif room["judge"] and ulow == room["judge"].lower(): role = "judge"
    grids = {}
    if room["phase"] in ("drawing", "judging", "done"):
        for d in room["drawers"]:
            grids[d] = _battle_grid_b64(room["grids"].get(d, bytearray(BATTLE_GRID * BATTLE_GRID)))
    return {
        "room_id": room["id"], "phase": room["phase"], "creator": room["creator"],
        "bet": room["bet"], "pool": room["pool"],
        "theme": room["theme"] if room["phase"] in ("drawing", "judging", "done") else None,
        "drawers": room["drawers"], "judge": room["judge"],
        "spectators": room["spectators"], "winner": room["winner"],
        "time_left": max(0, int(room["end_at"] - time.time())) if room["end_at"] else 0,
        "draw_seconds": room["draw_seconds"], "grid": BATTLE_GRID,
        "done_flags": list(room["done_flags"]),
        "grids": grids, "you_are": role,
    }

def _battle_participants(room):
    seen = set()
    for n in list(room["drawers"]) + ([room["judge"]] if room["judge"] else []) + list(room["spectators"]):
        if n and n.lower() not in seen:
            seen.add(n.lower()); yield n

async def _battle_push_state(room):
    for n in _battle_participants(room):
        try: await notify_social(n, {"type": "battle_state", "state": _battle_public_state(room, n)})
        except: pass

async def _battle_finish(room, winner_name):
    if room["phase"] == "done": return
    room["phase"] = "done"
    room["winner"] = winner_name
    if winner_name and room["pool"] > 0:
        credit_pb(winner_name, room["pool"])
        await save_place_bucks(); await push_pb_update(winner_name)
        await broadcast_casino_result(winner_name, "Battle", room["bet"], room["pool"], f"won pot of {room['pool']} - theme: {room['theme']}")
    elif not winner_name:
        for d in room["drawers"]:
            credit_pb(d, room["bet"])
        await save_place_bucks()
        for d in room["drawers"]: await push_pb_update(d)
    await _battle_push_state(room)
    asyncio.get_event_loop().call_later(120, lambda: battle_rooms.pop(room["id"], None))

async def _battle_draw_timer(room_id, seconds):
    try: await asyncio.sleep(seconds)
    except asyncio.CancelledError: return
    room = battle_rooms.get(room_id)
    if not room or room["phase"] != "drawing": return
    await _battle_enter_judging(room)

async def _battle_judge_timer(room_id, seconds):
    try: await asyncio.sleep(seconds)
    except asyncio.CancelledError: return
    room = battle_rooms.get(room_id)
    if not room or room["phase"] != "judging": return
    await _battle_finish(room, None)

async def _battle_enter_judging(room):
    if room["phase"] != "drawing": return
    room["phase"] = "judging"
    room["end_at"] = time.time() + BATTLE_JUDGE_TIMEOUT
    asyncio.create_task(_battle_judge_timer(room["id"], BATTLE_JUDGE_TIMEOUT))
    await _battle_push_state(room)

async def battle_create_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    try:
        bet = max(0, int(data.get("bet", 0)))
        draw_seconds = max(BATTLE_DRAW_SECONDS_MIN, min(BATTLE_DRAW_SECONDS_MAX, int(data.get("draw_seconds", BATTLE_DRAW_SECONDS_DEFAULT))))
    except: return web.json_response({"error": "Invalid settings"}, status=400)
    theme = str(data.get("theme") or "").strip()[:60] or None
    rid = secrets.token_hex(5)
    battle_rooms[rid] = {
        "id": rid, "creator": user, "bet": bet, "pool": 0, "theme": theme,
        "phase": "lobby", "drawers": [], "judge": None, "spectators": [user],
        "grids": {}, "winner": None, "end_at": 0, "draw_seconds": draw_seconds,
        "done_flags": set(), "last_action_at": time.time(),
    }
    await _battle_push_state(battle_rooms[rid])
    return web.json_response({"ok": True, "room_id": rid, "state": _battle_public_state(battle_rooms[rid], user)})

async def battle_list_handler(request):
    out = []
    for r in battle_rooms.values():
        if r["phase"] != "lobby": continue
        out.append({"room_id": r["id"], "creator": r["creator"], "bet": r["bet"],
                    "drawers": len(r["drawers"]), "judge": r["judge"], "spectators": len(r["spectators"]),
                    "draw_seconds": r["draw_seconds"]})
    return web.json_response({"ok": True, "rooms": out})

async def battle_join_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    room = battle_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    ulow = user.lower()
    already = (any(d.lower() == ulow for d in room["drawers"])
               or (room["judge"] and room["judge"].lower() == ulow)
               or any(s.lower() == ulow for s in room["spectators"]))
    if not already:
        room["spectators"].append(user)
    await _battle_push_state(room)
    return web.json_response({"ok": True, "state": _battle_public_state(room, user)})

async def battle_assign_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    target = (data.get("username") or "").strip()
    role = data.get("role", "spectator")
    room = battle_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    if room["creator"].lower() != user.lower():
        return web.json_response({"error": "Only the host can assign roles"}, status=403)
    if room["phase"] != "lobby":
        return web.json_response({"error": "Game already started"}, status=400)
    tlow = target.lower()
    in_room = (any(d.lower() == tlow for d in room["drawers"])
               or (room["judge"] and room["judge"].lower() == tlow)
               or any(s.lower() == tlow for s in room["spectators"]))
    if not in_room: return web.json_response({"error": f"{target} is not in the room"}, status=400)
    real = next((d for d in room["drawers"] if d.lower() == tlow), None) \
        or (room["judge"] if room["judge"] and room["judge"].lower() == tlow else None) \
        or next((s for s in room["spectators"] if s.lower() == tlow), target)
    was_drawer = any(d.lower() == tlow for d in room["drawers"])
    if was_drawer and room["bet"] > 0:
        credit_pb(real, room["bet"])
        room["pool"] = max(0, room["pool"] - room["bet"])
        await save_place_bucks(); await push_pb_update(real)
    room["drawers"] = [d for d in room["drawers"] if d.lower() != tlow]
    if room["judge"] and room["judge"].lower() == tlow: room["judge"] = None
    room["spectators"] = [s for s in room["spectators"] if s.lower() != tlow]
    if role == "drawer":
        if len(room["drawers"]) >= 2: return web.json_response({"error": "Both drawer slots are full"}, status=400)
        if room["bet"] > 0 and not spend_pb(real, room["bet"]):
            room["spectators"].append(real)
            await _battle_push_state(room)
            return web.json_response({"error": f"{target} doesn't have {room['bet']} $"}, status=400)
        room["drawers"].append(real)
        if room["bet"] > 0:
            room["pool"] += room["bet"]
            await save_place_bucks(); await push_pb_update(real)
    elif role == "judge":
        if room["judge"]: return web.json_response({"error": "Judge slot taken"}, status=400)
        room["judge"] = real
    else:
        room["spectators"].append(real)
    await _battle_push_state(room)
    return web.json_response({"ok": True, "state": _battle_public_state(room, user)})

async def battle_start_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    room = battle_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    if room["creator"].lower() != user.lower():
        return web.json_response({"error": "Only the creator can start"}, status=403)
    if room["phase"] != "lobby": return web.json_response({"error": "Already started"}, status=400)
    if len(room["drawers"]) != 2: return web.json_response({"error": "Need exactly 2 drawers"}, status=400)
    if not room["judge"]: return web.json_response({"error": "Need a judge"}, status=400)
    room["theme"] = random_battle_theme()
    room["phase"] = "drawing"
    room["end_at"] = time.time() + room["draw_seconds"]
    room["grids"] = {d: bytearray(BATTLE_GRID * BATTLE_GRID) for d in room["drawers"]}
    room["done_flags"] = set()
    asyncio.create_task(_battle_draw_timer(rid, room["draw_seconds"]))
    await _battle_push_state(room)
    return web.json_response({"ok": True, "state": _battle_public_state(room, user)})

async def battle_paint_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    room = battle_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    if room["phase"] != "drawing": return web.json_response({"error": "Not in drawing phase"}, status=400)
    ulow = user.lower()
    drawer = next((d for d in room["drawers"] if d.lower() == ulow), None)
    if not drawer: return web.json_response({"error": "Only drawers can paint"}, status=403)
    if drawer in room["done_flags"]: return web.json_response({"error": "You already submitted"}, status=400)
    try:
        x = int(data.get("x", -1)); y = int(data.get("y", -1)); color = int(data.get("color", 0))
    except: return web.json_response({"error": "Bad input"}, status=400)
    if not (0 <= x < BATTLE_GRID and 0 <= y < BATTLE_GRID): return web.json_response({"error": "Out of bounds"}, status=400)
    if not (0 <= color < BATTLE_PALETTE_SIZE): return web.json_response({"error": "Bad color"}, status=400)
    if not check_rate_limit(user, "battle_paint", 60, 1):
        return web.json_response({"error": "Painting too fast"}, status=429)
    room["grids"][drawer][y * BATTLE_GRID + x] = color
    room["last_action_at"] = time.time()
    payload = {"type": "battle_pixel", "room_id": rid, "drawer": drawer, "x": x, "y": y, "color": color}
    for n in _battle_participants(room):
        try: await notify_social(n, payload)
        except: pass
    return web.json_response({"ok": True})

async def battle_done_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    room = battle_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    if room["phase"] != "drawing": return web.json_response({"error": "Not in drawing phase"}, status=400)
    ulow = user.lower()
    drawer = next((d for d in room["drawers"] if d.lower() == ulow), None)
    if not drawer: return web.json_response({"error": "Only drawers can submit"}, status=403)
    room["done_flags"].add(drawer)
    if len(room["done_flags"]) >= len(room["drawers"]):
        await _battle_enter_judging(room)
    else:
        await _battle_push_state(room)
    return web.json_response({"ok": True})

async def battle_judge_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    room = battle_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    if room["phase"] != "judging": return web.json_response({"error": "Not in judging phase"}, status=400)
    if not room["judge"] or room["judge"].lower() != user.lower():
        return web.json_response({"error": "Only the judge can pick"}, status=403)
    winner = (data.get("winner") or "").strip()
    if not any(d.lower() == winner.lower() for d in room["drawers"]):
        return web.json_response({"error": "Winner must be one of the drawers"}, status=400)
    real = next(d for d in room["drawers"] if d.lower() == winner.lower())
    await _battle_finish(room, real)
    return web.json_response({"ok": True, "state": _battle_public_state(room, user)})

async def battle_leave_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    room = battle_rooms.get(rid)
    if not room: return web.json_response({"ok": True})
    ulow = user.lower()
    if room["phase"] == "lobby":
        for d in list(room["drawers"]):
            if d.lower() == ulow:
                room["drawers"].remove(d)
                if room["bet"] > 0:
                    credit_pb(user, room["bet"])
                    room["pool"] = max(0, room["pool"] - room["bet"])
                    await save_place_bucks(); await push_pb_update(user)
        if room["judge"] and room["judge"].lower() == ulow: room["judge"] = None
        room["spectators"] = [s for s in room["spectators"] if s.lower() != ulow]
        if room["creator"].lower() == ulow:
            for d in list(room["drawers"]):
                if room["bet"] > 0:
                    credit_pb(d, room["bet"]); await push_pb_update(d)
            if room["bet"] > 0: await save_place_bucks()
            battle_rooms.pop(rid, None)
        else:
            await _battle_push_state(room)
    elif room["phase"] == "drawing":
        drawer = next((d for d in room["drawers"] if d.lower() == ulow), None)
        if drawer:
            other = next((d for d in room["drawers"] if d.lower() != ulow), None)
            await _battle_finish(room, other)
        elif room["judge"] and room["judge"].lower() == ulow:
            room["judge"] = None
            await _battle_push_state(room)
        else:
            room["spectators"] = [s for s in room["spectators"] if s.lower() != ulow]
            await _battle_push_state(room)
    elif room["phase"] == "judging":
        if room["judge"] and room["judge"].lower() == ulow:
            await _battle_finish(room, None)
        else:
            room["spectators"] = [s for s in room["spectators"] if s.lower() != ulow]
            await _battle_push_state(room)
    else:
        room["spectators"] = [s for s in room["spectators"] if s.lower() != ulow]
    return web.json_response({"ok": True})

async def battle_state_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = request.query.get("room_id", "")
    room = battle_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    return web.json_response({"ok": True, "state": _battle_public_state(room, user)})

async def battle_say_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    text = str(data.get("text", "")).strip()[:200]
    if not text: return web.json_response({"error": "Empty"}, status=400)
    room = battle_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    ulow = user.lower()
    in_room = (ulow in (d.lower() for d in room["drawers"])
               or (room["judge"] and room["judge"].lower() == ulow)
               or any(s.lower() == ulow for s in room["spectators"]))
    if not in_room: return web.json_response({"error": "Not in this room"}, status=403)
    if not check_rate_limit(user, "battle_say", 5, 5):
        return web.json_response({"error": "Slow down."}, status=429)
    payload = {"type": "battle_chat", "room_id": rid, "msg": {"from": user, "text": text, "time": int(time.time())}}
    for n in _battle_participants(room):
        try: await notify_social(n, payload)
        except: pass
    return web.json_response({"ok": True})

SKETCH_W = 64
SKETCH_H = 64
SKETCH_ROUND_SECONDS = 60
SKETCH_ROOM_TTL = 30 * 60
SKETCH_MAX_PLAYERS = 8
SKETCH_MIN_PLAYERS = 2
SKETCH_PROMPTS = [
    "apple","banana","orange","grape","pineapple","watermelon","strawberry","cherry",
    "cat","dog","fish","bird","rabbit","turtle","horse","elephant","lion","tiger","bear","fox",
    "car","truck","bicycle","motorcycle","train","airplane","boat","rocket","helicopter","bus",
    "house","castle","tent","tree","flower","cactus","mountain","cloud","sun","moon","star","rainbow",
    "chair","table","bed","door","window","lamp","clock","phone","computer","tv","book","pencil",
    "hat","shirt","pants","shoe","glove","glasses","ring","crown","umbrella","backpack",
    "pizza","hamburger","hotdog","donut","cake","ice cream","cookie","sandwich","taco","sushi",
    "guitar","piano","drum","violin","microphone","headphones","speaker",
    "smiley","heart","ghost","skull","alien","robot","dragon","unicorn","dinosaur","mermaid",
    "snowman","pumpkin","football","basketball","soccer ball","tennis racket","fishing rod",
    "camera","mirror","key","bomb","sword","shield","magic wand","treasure chest",
]
sketch_rooms = {}

def _sketch_gc(now):
    stale = [rid for rid, r in sketch_rooms.items() if now - r["last_action"] > SKETCH_ROOM_TTL]
    for rid in stale: sketch_rooms.pop(rid, None)

def _sketch_prompt_blanks(p):
    return " ".join("_" if c.isalpha() else c for c in p)

def _sketch_pick_prompt(room):
    pool = room["custom_prompts"] if room["prompt_source"] == "custom" and room["custom_prompts"] else SKETCH_PROMPTS
    return secrets.choice(pool)

def _sketch_public(room, viewer):
    ulow = (viewer or "").lower()
    is_drawer = room.get("current_drawer") and room["current_drawer"].lower() == ulow
    show_prompt = is_drawer or room["phase"] in ("round_end", "game_end")
    players = [{"name": p["name"], "score": p["score"], "got_current": bool(p.get("got_current")), "drew": bool(p.get("drew_this_game"))} for p in room["players"].values()]
    return {
        "id": room["id"], "host": room["host"], "phase": room["phase"],
        "players": players, "current_drawer": room.get("current_drawer"),
        "prompt": room.get("current_prompt") if show_prompt else None,
        "prompt_blanks": _sketch_prompt_blanks(room["current_prompt"]) if room.get("current_prompt") and not is_drawer and room["phase"] == "drawing" else None,
        "prompt_source": room["prompt_source"],
        "round_deadline": room.get("round_deadline", 0),
        "round_num": room.get("round_num", 0),
        "guesses": room.get("guesses", [])[-30:],
        "last_stroke_idx": len(room["strokes"]),
        "grid_w": SKETCH_W, "grid_h": SKETCH_H,
    }

async def _sketch_next_round(room):
    remaining = [p for p in room["players"].values() if not p.get("drew_this_game")]
    if not remaining or len(room["players"]) < SKETCH_MIN_PLAYERS:
        room["phase"] = "game_end"
        room["current_drawer"] = None
        room["current_prompt"] = None
        return
    drawer = remaining[0]
    drawer["drew_this_game"] = True
    room["current_drawer"] = drawer["name"]
    room["current_prompt"] = _sketch_pick_prompt(room)
    for p in room["players"].values():
        p["got_current"] = False
    room["grid"] = bytearray(SKETCH_W * SKETCH_H)
    room["strokes"] = []
    room["guesses"] = []
    room["round_num"] += 1
    room["round_deadline"] = time.time() + SKETCH_ROUND_SECONDS
    room["phase"] = "drawing"

async def sketch_create_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    data = await request.json()
    prompt_source = "custom" if data.get("prompt_source") == "custom" else "random"
    custom_prompts = []
    if prompt_source == "custom":
        raw = str(data.get("custom_prompts") or "")
        for p in raw.split(","):
            p = p.strip()[:30]
            if p and len(custom_prompts) < 30:
                custom_prompts.append(p)
        if not custom_prompts:
            return web.json_response({"error": "Provide at least one custom prompt (comma-separated)"}, status=400)
    _sketch_gc(time.time())
    rid = secrets.token_hex(5)
    sketch_rooms[rid] = {
        "id": rid, "host": user, "phase": "lobby",
        "players": {user.lower(): {"name": user, "score": 0, "drew_this_game": False, "got_current": False}},
        "current_drawer": None, "current_prompt": None,
        "prompt_source": prompt_source, "custom_prompts": custom_prompts,
        "grid": bytearray(SKETCH_W * SKETCH_H),
        "strokes": [], "guesses": [],
        "round_deadline": 0, "round_num": 0, "round_end_at": 0,
        "last_action": time.time(),
    }
    return web.json_response({"ok": True, "room_id": rid, "state": _sketch_public(sketch_rooms[rid], user)})

async def sketch_list_handler(request):
    _sketch_gc(time.time())
    out = [{"id": r["id"], "host": r["host"], "players": len(r["players"]), "phase": r["phase"], "prompt_source": r["prompt_source"]}
           for r in sketch_rooms.values() if r["phase"] == "lobby"]
    return web.json_response({"ok": True, "rooms": out})

async def sketch_join_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    data = await request.json()
    rid = data.get("room_id", "")
    room = sketch_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    if room["phase"] != "lobby": return web.json_response({"error": "Game already started"}, status=400)
    ulow = user.lower()
    if ulow not in room["players"]:
        if len(room["players"]) >= SKETCH_MAX_PLAYERS:
            return web.json_response({"error": f"Room full ({SKETCH_MAX_PLAYERS} max)"}, status=400)
        room["players"][ulow] = {"name": user, "score": 0, "drew_this_game": False, "got_current": False}
    room["last_action"] = time.time()
    return web.json_response({"ok": True, "state": _sketch_public(room, user)})

async def sketch_start_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    data = await request.json()
    rid = data.get("room_id", "")
    room = sketch_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    if room["host"].lower() != user.lower():
        return web.json_response({"error": "Only host can start"}, status=403)
    if len(room["players"]) < SKETCH_MIN_PLAYERS:
        return web.json_response({"error": f"Need at least {SKETCH_MIN_PLAYERS} players"}, status=400)
    if room["phase"] != "lobby":
        return web.json_response({"error": "Already started"}, status=400)
    for p in room["players"].values(): p["drew_this_game"] = False
    await _sketch_next_round(room)
    room["last_action"] = time.time()
    return web.json_response({"ok": True, "state": _sketch_public(room, user)})

async def sketch_state_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = request.query.get("room_id", "")
    try: since = int(request.query.get("since", 0) or 0)
    except: since = 0
    room = sketch_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    now = time.time()
    if room["phase"] == "drawing" and now >= room["round_deadline"]:
        room["phase"] = "round_end"
        room["round_end_at"] = now
    if room["phase"] == "round_end" and now >= room.get("round_end_at", 0) + 5:
        await _sketch_next_round(room)
    strokes = room["strokes"][since:] if 0 <= since < len(room["strokes"]) else []
    state = _sketch_public(room, user)
    state["strokes"] = strokes
    return web.json_response({"ok": True, "state": state})

async def sketch_draw_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    data = await request.json()
    rid = data.get("room_id", "")
    room = sketch_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    if room["phase"] != "drawing":
        return web.json_response({"error": "Not in drawing phase"}, status=400)
    if not room.get("current_drawer") or room["current_drawer"].lower() != user.lower():
        return web.json_response({"error": "Not your turn to draw"}, status=403)
    if not check_rate_limit(user, "sketch_draw", 30, 1):
        return web.json_response({"error": "Too fast"}, status=429)
    strokes = data.get("strokes") or []
    if not isinstance(strokes, list) or len(strokes) > 200:
        return web.json_response({"error": "Invalid strokes"}, status=400)
    for s in strokes:
        try:
            x, y, c = int(s[0]), int(s[1]), int(s[2])
        except: continue
        if not (0 <= x < SKETCH_W and 0 <= y < SKETCH_H and 0 <= c < PALETTE_SIZE): continue
        room["grid"][y * SKETCH_W + x] = c
        room["strokes"].append([x, y, c])
    if len(room["strokes"]) > 20000:
        room["strokes"] = room["strokes"][-15000:]
    room["last_action"] = time.time()
    return web.json_response({"ok": True, "idx": len(room["strokes"])})

async def sketch_clear_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    data = await request.json()
    rid = data.get("room_id", "")
    room = sketch_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    if room["phase"] != "drawing" or not room.get("current_drawer") or room["current_drawer"].lower() != user.lower():
        return web.json_response({"error": "Not your turn"}, status=403)
    room["grid"] = bytearray(SKETCH_W * SKETCH_H)
    room["strokes"].append([-1, -1, 0])
    return web.json_response({"ok": True})

async def sketch_guess_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    data = await request.json()
    rid = data.get("room_id", "")
    room = sketch_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    if room["phase"] != "drawing":
        return web.json_response({"error": "Not in drawing phase"}, status=400)
    if room.get("current_drawer") and room["current_drawer"].lower() == user.lower():
        return web.json_response({"error": "The drawer can't guess"}, status=400)
    ulow = user.lower()
    if ulow not in room["players"]:
        return web.json_response({"error": "Not in room"}, status=400)
    if not check_rate_limit(user, "sketch_guess", 10, 5):
        return web.json_response({"error": "Slow down"}, status=429)
    player = room["players"][ulow]
    if player.get("got_current"):
        return web.json_response({"error": "You already got it"}, status=400)
    guess = str(data.get("guess") or "").strip()[:60]
    if not guess: return web.json_response({"error": "Empty guess"}, status=400)
    correct = guess.lower() == (room.get("current_prompt") or "").lower()
    entry = {"user": user, "text": "✓ got it!" if correct else guess, "correct": correct, "ts": int(time.time())}
    room["guesses"].append(entry)
    if len(room["guesses"]) > 100: del room["guesses"][:len(room["guesses"]) - 100]
    if correct:
        player["got_current"] = True
        remaining_sec = max(0, room["round_deadline"] - time.time())
        player["score"] += int(remaining_sec) + 10
        drawer_name = room.get("current_drawer") or ""
        non_drawer_count = sum(1 for p in room["players"].values() if p["name"].lower() != drawer_name.lower())
        got_count = sum(1 for p in room["players"].values() if p.get("got_current"))
        if got_count >= non_drawer_count and non_drawer_count > 0:
            room["phase"] = "round_end"
            room["round_end_at"] = time.time()
    room["last_action"] = time.time()
    return web.json_response({"ok": True, "correct": correct})

async def sketch_leave_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    data = await request.json()
    rid = data.get("room_id", "")
    room = sketch_rooms.get(rid)
    if not room: return web.json_response({"ok": True})
    ulow = user.lower()
    room["players"].pop(ulow, None)
    if not room["players"]:
        sketch_rooms.pop(rid, None)
        return web.json_response({"ok": True})
    if room["host"].lower() == ulow:
        room["host"] = next(iter(room["players"].values()))["name"]
    if room.get("current_drawer") and room["current_drawer"].lower() == ulow and room["phase"] == "drawing":
        room["phase"] = "round_end"
        room["round_end_at"] = time.time()
    room["last_action"] = time.time()
    return web.json_response({"ok": True})

AMONGUS_MIN = 4
AMONGUS_MAX = 20
AMONGUS_DISCUSSION_SECONDS = 75
AMONGUS_VOTE_SECONDS = 30
AMONGUS_MAX_ROUNDS = 4
amongus_rooms = {}

def _amongus_participants(room):
    seen = set()
    for p in room["players"]:
        if p["name"].lower() not in seen:
            seen.add(p["name"].lower()); yield p["name"]
    for s in room.get("spectators", []):
        if s.lower() not in seen:
            seen.add(s.lower()); yield s

def _amongus_public_state(room, for_user):
    ulow = (for_user or "").lower()
    me = next((p for p in room["players"] if p["name"].lower() == ulow), None)
    my_role = me["role"] if me else None
    is_imposter = bool(me and me.get("role") == "imposter")
    reveal_roles = room["phase"] == "ended"
    players_out = []
    for p in room["players"]:
        show_role = None
        if reveal_roles:
            show_role = p.get("role")
        elif not p.get("alive") and p.get("revealed_role"):
            show_role = p["role"]
        elif p["name"].lower() == ulow:
            show_role = p["role"]
        players_out.append({
            "name": p["name"], "alive": bool(p.get("alive")),
            "role": show_role,
        })
    return {
        "room_id": room["id"], "phase": room["phase"], "round": room["round"],
        "creator": room["creator"], "bet": room["bet"], "pool": room["pool"],
        "players": players_out, "you_are": ("crew" if me and me["role"] == "crew" else ("imposter" if is_imposter else "spectator")),
        "my_role": my_role, "alive": bool(me and me.get("alive")),
        "imposter_can_kill": is_imposter and room["phase"] == "discussion" and not room.get("kill_used_this_round") and me.get("alive"),
        "can_vote": bool(me and me.get("alive") and room["phase"] == "voting" and ulow not in {v.lower() for v in room.get("votes", {})}),
        "time_left": max(0, int(room["phase_end"] - time.time())) if room.get("phase_end") else 0,
        "winner": room.get("winner"),
        "winnings": room.get("payouts", {}).get(ulow, 0) if reveal_roles else 0,
        "spectators": room.get("spectators", []),
    }

async def _amongus_push_state(room):
    for n in _amongus_participants(room):
        try: await notify_social(n, {"type": "amongus_state", "state": _amongus_public_state(room, n)})
        except: pass

async def _amongus_finish(room, winner_side):
    if room["phase"] == "ended": return
    room["phase"] = "ended"
    room["winner"] = winner_side
    payouts = {}
    pool = room["pool"]
    if winner_side == "imposter":
        imp = next((p for p in room["players"] if p["role"] == "imposter"), None)
        if imp:
            credit_pb(imp["name"], pool)
            payouts[imp["name"].lower()] = pool
    elif winner_side == "crew":
        alive_crew = [p for p in room["players"] if p["role"] == "crew" and p.get("alive")]
        if alive_crew and pool > 0:
            share = pool // len(alive_crew)
            for p in alive_crew:
                credit_pb(p["name"], share)
                payouts[p["name"].lower()] = share
    room["payouts"] = payouts
    await save_place_bucks()
    for ulow_key, amt in payouts.items():
        real_name = next((p["name"] for p in room["players"] if p["name"].lower() == ulow_key), None)
        if real_name:
            await push_pb_update(real_name)
            await broadcast_casino_result(real_name, "AmongUs", room["bet"], amt, f"{winner_side} won (round {room['round']})")
    await _amongus_push_state(room)
    asyncio.get_event_loop().call_later(180, lambda: amongus_rooms.pop(room["id"], None))

def _amongus_check_end(room):
    alive_imp = [p for p in room["players"] if p["role"] == "imposter" and p.get("alive")]
    alive_crew = [p for p in room["players"] if p["role"] == "crew" and p.get("alive")]
    if not alive_imp:
        return "crew"
    if len(alive_crew) <= len(alive_imp):
        return "imposter"
    if room["round"] > AMONGUS_MAX_ROUNDS:
        return "imposter"
    return None

async def _amongus_enter_voting(room):
    if room["phase"] != "discussion": return
    room["phase"] = "voting"
    room["votes"] = {}
    room["phase_end"] = time.time() + AMONGUS_VOTE_SECONDS
    asyncio.create_task(_amongus_vote_timer(room["id"], AMONGUS_VOTE_SECONDS))
    await _amongus_push_state(room)

async def _amongus_resolve_vote(room):
    if room["phase"] != "voting": return
    counts = {}
    for voter, target in room["votes"].items():
        if target and target != "skip":
            counts[target] = counts.get(target, 0) + 1
    ejected_name = None
    if counts:
        max_votes = max(counts.values())
        top = [n for n, c in counts.items() if c == max_votes]
        if len(top) == 1:
            ejected_name = top[0]
    if ejected_name:
        p = next((p for p in room["players"] if p["name"].lower() == ejected_name.lower()), None)
        if p:
            p["alive"] = False
            p["revealed_role"] = True
    room["round"] += 1
    end_side = _amongus_check_end(room)
    if end_side:
        await _amongus_finish(room, end_side)
        return
    room["phase"] = "discussion"
    room["votes"] = {}
    room["kill_used_this_round"] = False
    room["phase_end"] = time.time() + AMONGUS_DISCUSSION_SECONDS
    asyncio.create_task(_amongus_discussion_timer(room["id"], AMONGUS_DISCUSSION_SECONDS))
    await _amongus_push_state(room)

async def _amongus_discussion_timer(rid, secs):
    try: await asyncio.sleep(secs)
    except asyncio.CancelledError: return
    room = amongus_rooms.get(rid)
    if not room or room["phase"] != "discussion": return
    await _amongus_enter_voting(room)

async def _amongus_vote_timer(rid, secs):
    try: await asyncio.sleep(secs)
    except asyncio.CancelledError: return
    room = amongus_rooms.get(rid)
    if not room or room["phase"] != "voting": return
    await _amongus_resolve_vote(room)

async def amongus_create_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    try: bet = max(0, int(data.get("bet", 10)))
    except: return web.json_response({"error": "Invalid bet"}, status=400)
    if bet > MAX_CASINO_BET: return web.json_response({"error": f"Max bet is {MAX_CASINO_BET} $"}, status=400)
    if bet > 0 and not spend_pb(user, bet):
        return web.json_response({"error": "Not enough PlaceBucks"}, status=400)
    rid = secrets.token_hex(5)
    amongus_rooms[rid] = {
        "id": rid, "creator": user, "bet": bet, "pool": bet,
        "phase": "lobby", "round": 0,
        "players": [{"name": user, "alive": True, "role": None, "revealed_role": False}],
        "spectators": [], "votes": {}, "phase_end": 0,
        "kill_used_this_round": False, "winner": None, "payouts": {},
    }
    await save_place_bucks(); await push_pb_update(user)
    await _amongus_push_state(amongus_rooms[rid])
    return web.json_response({"ok": True, "room_id": rid, "state": _amongus_public_state(amongus_rooms[rid], user)})

async def amongus_list_handler(request):
    out = []
    for r in amongus_rooms.values():
        if r["phase"] != "lobby": continue
        out.append({"room_id": r["id"], "creator": r["creator"], "bet": r["bet"], "players": len(r["players"])})
    return web.json_response({"ok": True, "rooms": out})

async def amongus_join_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    room = amongus_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    if any(p["name"].lower() == user.lower() for p in room["players"]):
        return web.json_response({"ok": True, "state": _amongus_public_state(room, user)})
    if room["phase"] != "lobby":
        if user not in room["spectators"]: room["spectators"].append(user)
        await _amongus_push_state(room)
        return web.json_response({"ok": True, "state": _amongus_public_state(room, user)})
    if len(room["players"]) >= AMONGUS_MAX:
        return web.json_response({"error": f"Room is full ({AMONGUS_MAX} players)"}, status=400)
    if room["bet"] > 0 and not spend_pb(user, room["bet"]):
        return web.json_response({"error": "Not enough PlaceBucks"}, status=400)
    room["players"].append({"name": user, "alive": True, "role": None, "revealed_role": False})
    room["pool"] += room["bet"]
    await save_place_bucks(); await push_pb_update(user)
    await _amongus_push_state(room)
    return web.json_response({"ok": True, "state": _amongus_public_state(room, user)})

async def amongus_start_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    room = amongus_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    if room["creator"].lower() != user.lower():
        return web.json_response({"error": "Only the host can start"}, status=403)
    if room["phase"] != "lobby": return web.json_response({"error": "Already started"}, status=400)
    n = len(room["players"])
    if n < AMONGUS_MIN: return web.json_response({"error": f"Need at least {AMONGUS_MIN} players"}, status=400)
    indices = list(range(n))
    secrets.SystemRandom().shuffle(indices)
    imp_idx = indices[0]
    for i, p in enumerate(room["players"]):
        p["role"] = "imposter" if i == imp_idx else "crew"
        p["alive"] = True
        p["revealed_role"] = False
    room["phase"] = "discussion"
    room["round"] = 1
    room["kill_used_this_round"] = False
    room["votes"] = {}
    room["phase_end"] = time.time() + AMONGUS_DISCUSSION_SECONDS
    asyncio.create_task(_amongus_discussion_timer(rid, AMONGUS_DISCUSSION_SECONDS))
    await _amongus_push_state(room)
    return web.json_response({"ok": True, "state": _amongus_public_state(room, user)})

async def amongus_say_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    text = (data.get("text") or "").strip()[:200]
    if not text: return web.json_response({"error": "Empty"}, status=400)
    room = amongus_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    ulow = user.lower()
    me = next((p for p in room["players"] if p["name"].lower() == ulow), None)
    if not me and ulow not in (s.lower() for s in room.get("spectators", [])):
        return web.json_response({"error": "Not in this room"}, status=403)
    if not check_rate_limit(user, "amongus_say", 8, 5):
        return web.json_response({"error": "Slow down."}, status=429)
    payload = {"type": "amongus_chat", "room_id": rid, "msg": {"from": user, "text": text, "time": int(time.time()), "dead": bool(me and not me.get("alive"))}}
    for n in _amongus_participants(room):
        try: await notify_social(n, payload)
        except: pass
    return web.json_response({"ok": True})

async def amongus_kill_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    target = (data.get("target") or "").strip()
    room = amongus_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    ulow = user.lower()
    me = next((p for p in room["players"] if p["name"].lower() == ulow), None)
    if not me or me["role"] != "imposter" or not me.get("alive"):
        return web.json_response({"error": "Only the alive imposter can kill"}, status=403)
    if room["phase"] != "discussion":
        return web.json_response({"error": "Can only kill during discussion"}, status=400)
    if room.get("kill_used_this_round"):
        return web.json_response({"error": "You already killed this round"}, status=400)
    victim = next((p for p in room["players"] if p["name"].lower() == target.lower()), None)
    if not victim: return web.json_response({"error": "Target not found"}, status=404)
    if victim["role"] != "crew" or not victim.get("alive"):
        return web.json_response({"error": "Target must be an alive crewmate"}, status=400)
    victim["alive"] = False
    room["kill_used_this_round"] = True
    end_side = _amongus_check_end(room)
    if end_side:
        await _amongus_finish(room, end_side)
        return web.json_response({"ok": True})
    payload = {"type": "amongus_chat", "room_id": rid, "msg": {"from": "system", "text": f"💀 {victim['name']} was killed!", "time": int(time.time()), "system": True}}
    for n in _amongus_participants(room):
        try: await notify_social(n, payload)
        except: pass
    await _amongus_push_state(room)
    return web.json_response({"ok": True})

async def amongus_vote_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    target = (data.get("target") or "skip").strip()
    room = amongus_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    if room["phase"] != "voting":
        return web.json_response({"error": "Not in voting phase"}, status=400)
    ulow = user.lower()
    me = next((p for p in room["players"] if p["name"].lower() == ulow), None)
    if not me or not me.get("alive"):
        return web.json_response({"error": "Only alive players can vote"}, status=403)
    if ulow in {v.lower() for v in room.get("votes", {})}:
        return web.json_response({"error": "Already voted"}, status=400)
    if target != "skip":
        t = next((p for p in room["players"] if p["name"].lower() == target.lower() and p.get("alive")), None)
        if not t: return web.json_response({"error": "Target must be an alive player"}, status=400)
        target = t["name"]
    room["votes"][user] = target
    alive_count = sum(1 for p in room["players"] if p.get("alive"))
    if len(room["votes"]) >= alive_count:
        await _amongus_resolve_vote(room)
    else:
        await _amongus_push_state(room)
    return web.json_response({"ok": True})

async def amongus_leave_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = data.get("room_id", "")
    room = amongus_rooms.get(rid)
    if not room: return web.json_response({"ok": True})
    ulow = user.lower()
    me = next((p for p in room["players"] if p["name"].lower() == ulow), None)
    if me:
        if room["phase"] == "lobby":
            room["players"] = [p for p in room["players"] if p["name"].lower() != ulow]
            if room["bet"] > 0:
                credit_pb(user, room["bet"])
                room["pool"] = max(0, room["pool"] - room["bet"])
                await save_place_bucks(); await push_pb_update(user)
            if room["creator"].lower() == ulow:
                for p in list(room["players"]):
                    if room["bet"] > 0:
                        credit_pb(p["name"], room["bet"])
                        await push_pb_update(p["name"])
                if room["bet"] > 0: await save_place_bucks()
                amongus_rooms.pop(rid, None)
                return web.json_response({"ok": True})
            await _amongus_push_state(room)
        else:
            me["alive"] = False
            me["revealed_role"] = True
            end_side = _amongus_check_end(room)
            if end_side:
                await _amongus_finish(room, end_side)
            else:
                await _amongus_push_state(room)
    else:
        room["spectators"] = [s for s in room.get("spectators", []) if s.lower() != ulow]
        await _amongus_push_state(room)
    return web.json_response({"ok": True})

async def amongus_state_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    rid = request.query.get("room_id", "")
    room = amongus_rooms.get(rid)
    if not room: return web.json_response({"error": "Room not found"}, status=404)
    return web.json_response({"ok": True, "state": _amongus_public_state(room, user)})

NIGHT_EVENT_PRIZE = 10000
NIGHT_EVENT_SIGNUP_SECONDS = 300
NIGHT_EVENT_HOUR_ET = 23
NIGHT_EVENT_TYPES = ["uno", "battle", "coinflip"]
NIGHT_EVENT_TYPE_LABELS = {
    "uno": "UNO Showdown",
    "battle": "Battle Mode (AI judge)",
    "coinflip": "Coinflip Tournament",
}
night_event = {
    "phase": "idle",
    "type": None,
    "next_event_ts": 0,
    "signup_end_ts": 0,
    "signups": [],
    "running_state": None,
    "winner": None,
    "prize": NIGHT_EVENT_PRIZE,
    "history": [],
    "started_ts": 0,
}

def _night_next_11pm_et_ts():
    """Returns Unix timestamp of the next 23:00 in America/New_York. If no tz
    data is available, falls back to UTC 03:00 (= 23 EST)."""
    if EASTERN_TZ is None:
        now = datetime.utcnow()
        target = now.replace(hour=3, minute=0, second=0, microsecond=0)
        if now >= target: target = target + timedelta(days=1)
        return target.timestamp()
    now_et = datetime.now(EASTERN_TZ)
    target = now_et.replace(hour=NIGHT_EVENT_HOUR_ET, minute=0, second=0, microsecond=0)
    if now_et >= target: target = target + timedelta(days=1)
    return target.timestamp()

def _night_public_state():
    return {
        "phase": night_event["phase"],
        "type": night_event["type"],
        "type_label": NIGHT_EVENT_TYPE_LABELS.get(night_event["type"], ""),
        "next_event_ts": int(night_event["next_event_ts"]),
        "signup_end_ts": int(night_event["signup_end_ts"]),
        "signups": list(night_event["signups"]),
        "signup_count": len(night_event["signups"]),
        "running_state": night_event["running_state"],
        "winner": night_event["winner"],
        "prize": night_event["prize"],
        "history": night_event["history"][-10:],
        "server_now_ts": int(time.time()),
    }

async def _night_broadcast_state():
    payload = json.dumps({"type": "night_event_state", "state": _night_public_state()})
    for ws, info in list(clients.items()):
        if info and not ws.closed:
            try: await ws.send_str(payload)
            except: pass
    for ws, uname in list(social_clients.items()):
        if uname and not ws.closed:
            try: await ws.send_str(payload)
            except: pass

async def _night_send_chat(text):
    """Append a system line to the running event log so players in the modal see progress."""
    night_event["running_state"] = night_event["running_state"] or {"log": []}
    log = night_event["running_state"].setdefault("log", [])
    log.append({"text": text, "ts": int(time.time())})
    if len(log) > 50: del log[:len(log) - 50]
    await _night_broadcast_state()

async def _night_award_winner(winner_username):
    if not winner_username: return
    credit_pb(winner_username, NIGHT_EVENT_PRIZE)
    await save_place_bucks()
    await push_pb_update(winner_username)
    night_event["winner"] = winner_username
    night_event["history"].append({
        "ts": int(time.time()),
        "winner": winner_username,
        "type": night_event["type"],
        "prize": NIGHT_EVENT_PRIZE,
    })
    if len(night_event["history"]) > 30:
        del night_event["history"][:len(night_event["history"]) - 30]

async def _night_run_coinflip(players):
    """Single-elimination coinflip bracket. Pure server simulation with dramatic pauses."""
    bracket = list(players)
    secrets.SystemRandom().shuffle(bracket)
    rnum = 1
    night_event["running_state"] = {"bracket": list(bracket), "round": rnum, "log": []}
    await _night_send_chat(f"🪙 Coinflip Tournament starts with {len(bracket)} players.")
    while len(bracket) > 1:
        await asyncio.sleep(2)
        next_round = []
        round_results = []
        i = 0
        while i < len(bracket) - 1:
            a = bracket[i]; b = bracket[i + 1]
            await asyncio.sleep(1.5)
            winner = a if secrets.randbelow(2) == 0 else b
            loser = b if winner == a else a
            next_round.append(winner)
            round_results.append({"a": a, "b": b, "winner": winner})
            await _night_send_chat(f"  R{rnum}: {a} vs {b} → {winner}")
            i += 2
        if i == len(bracket) - 1:
            next_round.append(bracket[-1])
            await _night_send_chat(f"  R{rnum}: {bracket[-1]} byes to next round")
        bracket = next_round
        rnum += 1
        night_event["running_state"]["bracket"] = list(bracket)
        night_event["running_state"]["round"] = rnum
        await _night_broadcast_state()
    return bracket[0] if bracket else None

async def _night_run_battle_ai_judge(players):
    """Pick 2 random drawers, give them a theme + a 90-sec drawing window in a one-off
    in-memory battle room, then AI auto-judges by random pick (with a tiny weighting
    toward whoever submitted more pixels). Other signups watch the room."""
    drawers = list(players)
    secrets.SystemRandom().shuffle(drawers)
    drawers = drawers[:2]
    spectators = [p for p in players if p not in drawers]
    rid = "night_" + secrets.token_hex(4)
    theme = random_battle_theme()
    battle_rooms[rid] = {
        "id": rid, "creator": "system", "bet": 0, "pool": 0, "theme": theme,
        "phase": "drawing", "drawers": drawers, "judge": None, "spectators": list(spectators),
        "grids": {d: bytearray(BATTLE_GRID * BATTLE_GRID) for d in drawers},
        "winner": None, "end_at": time.time() + 90, "draw_seconds": 90,
        "done_flags": set(), "last_action_at": time.time(), "_night_event": True,
    }
    night_event["running_state"] = {"battle_room_id": rid, "theme": theme, "drawers": drawers, "log": []}
    await _night_send_chat(f"🎨 Battle theme: \"{theme}\". Drawers: {', '.join(drawers)}.")
    await _battle_push_state(battle_rooms[rid])
    await asyncio.sleep(90)
    room = battle_rooms.get(rid)
    if not room: return None
    scores = {}
    for d in drawers:
        g = room["grids"].get(d, bytearray())
        scores[d] = sum(1 for b in g if b != 0)
    if all(scores.get(d, 0) == 0 for d in drawers):
        winner = secrets.choice(drawers)
    else:
        weights = [max(1, scores.get(d, 0)) for d in drawers]
        total = sum(weights)
        roll = secrets.randbelow(total)
        cum = 0; winner = drawers[0]
        for d, w in zip(drawers, weights):
            cum += w
            if roll < cum: winner = d; break
    room["winner"] = winner
    room["phase"] = "done"
    await _night_send_chat(f"🤖 AI judges: {winner} wins! ({scores})")
    await _battle_push_state(room)
    asyncio.get_event_loop().call_later(120, lambda: battle_rooms.pop(rid, None))
    return winner

async def _night_run_uno(players):
    """Take up to 4 random signups, drop them in an UNO room with RANDOMIZED house rules
    (stacking on/off, jump-ins on/off), and let it play out. Winner of the UNO game wins
    the night prize."""
    chosen = list(players)
    secrets.SystemRandom().shuffle(chosen)
    chosen = chosen[:4]
    stacking = bool(secrets.randbelow(2))
    jump_ins = bool(secrets.randbelow(2))
    multi_color = bool(secrets.randbelow(2))
    draw_till = bool(secrets.randbelow(2))
    rid = "night_" + secrets.token_hex(4)
    uno_players = [{"name": n, "hand": [], "is_ai": False, "connected": True} for n in chosen]
    uno_rooms[rid] = {
        "id": rid, "creator": chosen[0], "players": uno_players, "phase": "lobby",
        "settings": {"max_players": 4, "ai_count": 0, "stacking": stacking, "jump_ins": jump_ins, "multi_color": multi_color, "draw_till": draw_till, "bet": 0},
        "deck": [], "discard": [], "current": 0, "direction": 1, "current_color": None,
        "pending_draws": 0, "pending_kind": None, "bet_per_human": 0, "pool": 0,
        "started_at": None, "last_action_at": time.time(), "spectators": [],
        "_night_event": True,
    }
    night_event["running_state"] = {"uno_room_id": rid, "players": chosen, "log": []}
    rule_notes = []
    if stacking: rule_notes.append("+2/+4 stacking ON")
    if jump_ins: rule_notes.append("jump-ins ON")
    if multi_color: rule_notes.append("cross-color stacking ON")
    if draw_till: rule_notes.append("draw till playable ON")
    rules_str = " · ".join(rule_notes) if rule_notes else "vanilla rules"
    await _night_send_chat(f"UNO Showdown ({rules_str}) - players: {', '.join(chosen)}. Open the UNO room to play!")
    room = uno_rooms[rid]
    room["deck"] = _uno_new_deck()
    for p in room["players"]: p["hand"] = []
    for _ in range(7):
        for i in range(len(room["players"])):
            room["players"][i]["hand"].append(room["deck"].pop())
    while True:
        top = room["deck"].pop()
        if top["color"] != "w" and top["value"] not in ("skip", "reverse", "draw2"):
            room["discard"] = [top]; break
        room["deck"].insert(0, top)
    room["current_color"] = room["discard"][-1]["color"]
    room["phase"] = "playing"
    room["started_at"] = time.time()
    await _uno_push_state(room)
    deadline = time.time() + 600
    while time.time() < deadline:
        await asyncio.sleep(2)
        rcheck = uno_rooms.get(rid)
        if not rcheck or rcheck.get("winner"):
            return rcheck.get("winner") if rcheck else None
        if rcheck.get("phase") == "done":
            return rcheck.get("winner")
    return None

async def _night_start_event():
    """Pick a random type, open signups, broadcast."""
    if night_event["phase"] != "idle": return
    night_event["type"] = secrets.choice(NIGHT_EVENT_TYPES)
    night_event["phase"] = "signup"
    night_event["signups"] = []
    night_event["signup_end_ts"] = time.time() + NIGHT_EVENT_SIGNUP_SECONDS
    night_event["winner"] = None
    night_event["running_state"] = None
    night_event["started_ts"] = int(time.time())
    print(f"[night_event] opening signup for {night_event['type']}")
    await _night_broadcast_state()
    await asyncio.sleep(NIGHT_EVENT_SIGNUP_SECONDS)
    players = list(night_event["signups"])
    if len(players) < 2:
        night_event["phase"] = "ended"
        night_event["running_state"] = {"log": [{"text": f"Not enough players signed up ({len(players)}/2). Event cancelled.", "ts": int(time.time())}]}
        print(f"[night_event] cancelled, only {len(players)} signups")
        await _night_broadcast_state()
        await asyncio.sleep(60)
        night_event["phase"] = "idle"
        night_event["type"] = None
        night_event["next_event_ts"] = _night_next_11pm_et_ts()
        await _night_broadcast_state()
        return
    night_event["phase"] = "running"
    await _night_broadcast_state()
    winner = None
    try:
        if night_event["type"] == "coinflip":
            winner = await _night_run_coinflip(players)
        elif night_event["type"] == "battle":
            winner = await _night_run_battle_ai_judge(players)
        elif night_event["type"] == "uno":
            winner = await _night_run_uno(players)
    except Exception as e:
        print(f"[night_event] runner crashed: {e}")
    if winner:
        await _night_award_winner(winner)
        await _night_send_chat(f"🏆 {winner} wins the Night Event! +{NIGHT_EVENT_PRIZE} $")
    else:
        await _night_send_chat("No winner this round.")
    night_event["phase"] = "ended"
    await _night_broadcast_state()
    await asyncio.sleep(120)
    night_event["phase"] = "idle"
    night_event["type"] = None
    night_event["next_event_ts"] = _night_next_11pm_et_ts()
    await _night_broadcast_state()

async def night_event_loop(app):
    night_event["next_event_ts"] = _night_next_11pm_et_ts()
    while True:
        try:
            wait = max(1.0, night_event["next_event_ts"] - time.time())
            if wait > 60:
                await asyncio.sleep(min(wait, 3600))
                continue
            await asyncio.sleep(wait)
            await _night_start_event()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"night_event_loop error: {e}")
            await asyncio.sleep(60)

async def auth_status_handler(request):
    """Probe endpoint the client hits on boot to ask 'am I banned right now?'
    Exempt from the ban middleware so a banned client can still get an answer."""
    token = request.headers.get("Authorization", "")
    username = sessions.get(token)
    banned = bool(username and is_banned(username)) or is_ip_banned(request)
    return web.json_response({"ok": True, "banned": banned, "redirect": "https://www.pornhub.com" if banned else None})

async def night_event_state_handler(request):
    if not night_event["next_event_ts"]:
        night_event["next_event_ts"] = _night_next_11pm_et_ts()
    return web.json_response({"ok": True, "state": _night_public_state()})

async def night_event_join_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if night_event["phase"] != "signup":
        return web.json_response({"error": "No event taking signups right now"}, status=400)
    if user in night_event["signups"]:
        return web.json_response({"ok": True, "state": _night_public_state()})
    if time.time() > night_event["signup_end_ts"]:
        return web.json_response({"error": "Signups closed"}, status=400)
    night_event["signups"].append(user)
    await _night_broadcast_state()
    return web.json_response({"ok": True, "state": _night_public_state()})

async def admin_night_event_trigger_handler(request):
    """Admin can trigger a night event NOW (skips the 11pm wait, for testing)."""
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    if night_event["phase"] != "idle":
        return web.json_response({"error": "Event already in progress (phase=" + night_event["phase"] + ")"}, status=400)
    asyncio.create_task(_night_start_event())
    return web.json_response({"ok": True, "message": "Triggered"})

async def flappy_pass_handler(request):
    """Award 1 PB for each pipe passed in the Flappy game. Soft anti-cheat:
    capped at 6/sec which is faster than any real flappy run can produce."""
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not check_rate_limit(user, "flappy_pass", 6, 1):
                                                                           
        return web.json_response({"ok": True, "balance": get_pb(user), "skipped": True})
    credit_pb(user, 1)
    await save_place_bucks()
    await push_pb_update(user)
    return web.json_response({"ok": True, "balance": get_pb(user)})

async def global_leaderboard_handler(request):
    """Top placers across ALL lobbies combined. The owner of the site is excluded."""
    totals = {}
    for lobby in lobbies.values():
        pc = lobby.get("pixel_counts") or {}
        for uname, cnt in pc.items():
            if not isinstance(cnt, (int, float)) or cnt <= 0: continue
            if is_admin(uname): continue
            totals[uname] = totals.get(uname, 0) + int(cnt)
    top = sorted(totals.items(), key=lambda x: x[1], reverse=True)[:20]
    return web.json_response({"entries": [{"name": n, "pixels": c, "online": is_online(n)} for n, c in top]})

async def clan_remove_member_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    cid = data.get("clan_id", "")
    target = (data.get("username") or "").strip()
    clan = clans.get(cid)
    if not clan: return web.json_response({"error": "Clan not found"}, status=404)
    if clan["owner"].lower() != user.lower() and not is_admin(user):
        return web.json_response({"error": "Only the clan owner can remove members"}, status=403)
    tlow = target.lower()
    if tlow == clan["owner"].lower():
        return web.json_response({"error": "Owner can't be removed (transfer ownership first)"}, status=400)
    member = next((m for m in clan.get("members", []) if m.lower() == tlow), None)
    if not member: return web.json_response({"error": "Not a member of this clan"}, status=404)
    clan["members"] = [m for m in clan.get("members", []) if m.lower() != tlow]
    (clan.get("member_ranks") or {}).pop(tlow, None)
    await save_clans()
    await remove_user_clan_rank(member)
    await notify_social(member, {"type": "clan_removed", "clan_name": clan["name"], "by": user})
    return web.json_response({"ok": True, "message": f"Removed {member} from clan"})

async def online_summary_handler(request):
    """Total online + per-lobby online + flat user list, for the homepage.
    `total` counts unique authenticated users across both lobby (clients) and
    homepage (social_clients) WebSocket connections, plus guest connections."""
    seen = set(); users = []
    guest_count = 0
    for info in clients.values():
        if not info: continue
        if info.get("guest"):
            guest_count += 1
            continue
        n = info.get("username")
        if n and n.lower() not in seen:
            seen.add(n.lower()); users.append(n)
                                                                 
    for uname in social_clients.values():
        if uname and uname.lower() not in seen:
            seen.add(uname.lower()); users.append(uname)
    per_lobby = {}
    for info in clients.values():
        if not info: continue
        lid = info.get("lobby_id")
        if not lid: continue
        per_lobby[lid] = per_lobby.get(lid, 0) + 1
    total = len(users) + guest_count
    return web.json_response({"total": total, "users": users, "per_lobby": per_lobby})

async def admin_brush_perm_set_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = data.get("username", "").strip()
    if not target: return web.json_response({"error": "Username required"}, status=400)
    size = int(data.get("size", 1))
    drag = bool(data.get("drag", False))
                                  
    allowed_sizes = [1, 3, 5, 7, 9, 15, 25]
    if size not in allowed_sizes: size = 1
    if size <= 1 and not drag:
                                              
        brush_perms.pop(target.lower(), None)
    else:
        brush_perms[target.lower()] = {"size": size, "drag": drag}
    await save_brush_perms()
                                                         
    for cws, cinfo in list(clients.items()):
        if cinfo and cinfo.get("username", "").lower() == target.lower():
            try: await cws.send_json({"type": "brush_perm_update", "brush_perm": get_brush_perm(target)})
            except: pass
    return web.json_response({"ok": True, "message": f"Set brush perm for {target}: size={size}, drag={drag}"})

async def admin_brush_perm_remove_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = data.get("username", "").strip().lower()
    brush_perms.pop(target, None)
    await save_brush_perms()
    for cws, cinfo in list(clients.items()):
        if cinfo and cinfo.get("username", "").lower() == target:
            try: await cws.send_json({"type": "brush_perm_update", "brush_perm": {"size": 1, "drag": False}})
            except: pass
    return web.json_response({"ok": True, "message": f"Removed brush perm from {target}"})

async def admin_fake_admin_add_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = data.get("username", "").strip()
    if not target: return web.json_response({"error": "Username required"}, status=400)
    if target.lower() not in [f.lower() for f in fake_admins]:
        fake_admins.append(target.lower())
        await save_fake_admins()
    return web.json_response({"ok": True, "message": f"Granted fake admin to {target}"})

async def admin_fake_admin_remove_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = data.get("username", "").strip().lower()
    fake_admins[:] = [f for f in fake_admins if f.lower() != target]
    await save_fake_admins()
    return web.json_response({"ok": True, "message": f"Revoked fake admin from {target}"})

fake_action_log = []                                              

async def fake_action_log_handler(request):
    user = get_auth_user(request)
    if not user or not is_fake_admin(user): return web.json_response({"ok": True})
    if not check_rate_limit(user, "fake_log", 20, 60):
        return web.json_response({"ok": True})
    data = await request.json()
    def _clean(s):
        s = str(s or "")[:100]
        return "".join(c for c in s if c.isprintable() and c not in "\r\n\t<>")
    fake_action_log.append({
        "username": user[:32],
        "action": _clean(data.get("action")),
        "target": _clean(data.get("target")),
        "detail": _clean(data.get("detail")),
        "time": time.time()
    })
    if len(fake_action_log) > 200: del fake_action_log[:len(fake_action_log) - 200]
    return web.json_response({"ok": True})

async def admin_view_fake_log_handler(request):
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
                                                   
    entries = []
    for e in fake_action_log[-50:]:
        import datetime
        ts = datetime.datetime.fromtimestamp(e["time"]).strftime("%H:%M:%S")
        entries.append(f'[{ts}] {e["username"]} tried: {e["action"]} → target: {e["target"]}')
    return web.json_response({"log": entries})

async def social_ws_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    username = None
    social_clients[ws] = None
    social_ips[ws] = get_client_ip(request)
    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                data = json.loads(msg.data)
                if data.get("type") == "auth":
                    token = data.get("token", "")
                    device_id = str(data.get("device_id", ""))[:64] or None
                    if token in sessions:
                        username = sessions[token]
                        if is_banned(username):
                            try: await ws.send_json({"type": "client_school_ban", "url": "https://www.pornhub.com"})
                            except: pass
                            await ws.close(); break
                        if is_ip_banned(request): await ws.close(); break
                        if is_device_banned(device_id): await ws.close(); break
                        social_clients[ws] = username
                        if device_id:
                            track_user_device(username, device_id)
                            await save_user_devices()
                        await track_ip(username, request)
                        await ws.send_json({"type": "social_ready"})
                        unread = get_unread_dm_summary(username)
                        if unread:
                            await ws.send_json({"type": "unread_dms", "senders": unread})
                        try:
                            fd = friends_data.get(username) or {}
                            pending = list(fd.get("incoming") or [])
                            if pending:
                                await ws.send_json({"type": "pending_friend_requests", "from": pending})
                        except: pass
                        await broadcast_online_all_lobbies()
                        try: await push_friend_presence(username)
                        except: pass
                    else:
                        await ws.close()
                elif data.get("type") == "presence" and username:
                    afk = bool(data.get("afk"))
                    ulow = username.lower()
                    prev = presence.get(ulow) or {}
                    if prev.get("afk") != afk:
                        presence[ulow] = {"afk": afk, "since": time.time()}
                        print(f"[presence] {username} -> {'AFK' if afk else 'active'}", flush=True)
                        try: await push_friend_presence(username)
                        except Exception as e: print(f"[presence] push failed for {username}: {e}", flush=True)
                elif data.get("type") == "dm_seen" and username:
                    peer = data.get("peer", "").strip()
                    if peer:
                        mark_dm_seen(username, peer)
                        await save_dm_last_seen()

                        await notify_social(peer, {"type": "dm_seen_by", "peer": username})
                elif data.get("type") == "dm_typing" and username:
                    peer = data.get("to", "").strip()
                    if peer:
                        fd = get_friend_data(username)
                        if peer in fd["friends"]:
                            await notify_social(peer, {"type": "dm_typing", "from": username, "typing": bool(data.get("typing"))})
                elif data.get("type") == "dm" and username:
                    target = data.get("to", "").strip()
                    text = data.get("text", "").strip()[:200]
                    image_url = (data.get("image_url") or "").strip()[:500]
                    if image_url and not is_safe_image_url(image_url): image_url = ""
                    if not check_rate_limit(username, "dm", 5, 5):
                        try: await ws.send_json({"type": "system", "text": "Slow down - max 5 DMs per 5 seconds."})
                        except: pass
                        continue
                    if image_url and not check_rate_limit(username, "dm_image", 5, 60):
                        try: await ws.send_json({"type": "system", "text": "Image rate limit - max 5 images per minute."})
                        except: pass
                        continue
                    rt = data.get("reply_to")
                    reply_obj = None
                    if isinstance(rt, dict):
                        rfrom = str(rt.get("from", ""))[:30]
                        rtext = str(rt.get("text", ""))[:120]
                        if rfrom and rtext:
                            reply_obj = {"from": rfrom, "text": rtext}
                    if target and (text or image_url):
                        fd = get_friend_data(username)
                        if target in fd["friends"]:
                            key = dm_key(username, target)
                            m = {"from": username, "time": time.time()}
                            if text: m["text"] = text
                            if image_url: m["image_url"] = image_url
                            if reply_obj: m["reply_to"] = reply_obj
                            dms.setdefault(key, []).append(m)
                            if len(dms[key]) > MAX_DM_HISTORY: dms[key] = dms[key][-MAX_DM_HISTORY:]
                            await save_dm(key)
                            payload = {"type": "dm", "from": username, "time": m["time"]}
                            if text: payload["text"] = text
                            if image_url: payload["image_url"] = image_url
                            if reply_obj: payload["reply_to"] = reply_obj
                            try: print(f"[dm {username}->{target}] {text}{' [img]' if image_url else ''}", flush=True)
                            except: pass
                            await notify_social(target, payload)
                elif data.get("type") == "group_msg" and username:
                    gid = data.get("group_id", "")
                    text = data.get("text", "").strip()[:200]
                    image_url = (data.get("image_url") or "").strip()[:500]
                    if image_url and not is_safe_image_url(image_url): image_url = ""
                    g = groups.get(gid)
                    if not g: continue
                    if username.lower() not in [m.lower() for m in g.get("members", [])]: continue
                    if not (text or image_url): continue
                    if not check_rate_limit(username, "group_msg", 5, 5):
                        try: await ws.send_json({"type": "system", "text": "Slow down - max 5 group messages per 5 seconds."})
                        except: pass
                        continue
                    if image_url and not check_rate_limit(username, "group_image", 5, 60):
                        try: await ws.send_json({"type": "system", "text": "Image rate limit - max 5 images per minute."})
                        except: pass
                        continue
                    msg_obj = {"from": username, "time": time.time()}
                    if text: msg_obj["text"] = text
                    if image_url: msg_obj["image_url"] = image_url
                    group_messages.setdefault(gid, []).append(msg_obj)
                    if len(group_messages[gid]) > MAX_DM_HISTORY: group_messages[gid] = group_messages[gid][-MAX_DM_HISTORY:]
                    await save_group_messages(gid)
                    fanout = {"type": "group_msg", "group_id": gid, "group_name": g.get("name", ""), "from": username, "time": msg_obj["time"]}
                    if text: fanout["text"] = text
                    if image_url: fanout["image_url"] = image_url
                    try: print(f"[group {gid} ({g.get('name','')}) {username}] {text}{' [img]' if image_url else ''}", flush=True)
                    except: pass
                    for member in g.get("members", []):
                        if member.lower() != username.lower():
                            await notify_social(member, fanout)
            elif msg.type in (web.WSMsgType.ERROR, web.WSMsgType.CLOSE):
                break
    finally:
        was_authed = bool(social_clients.get(ws))
        disconnected_user = social_clients.get(ws)
        del social_clients[ws]
        social_ips.pop(ws, None)
        if was_authed:
            await broadcast_online_all_lobbies()
            still_online = any(uname and uname.lower() == disconnected_user.lower() for uname in social_clients.values())
            if not still_online and disconnected_user:
                dlow_p = disconnected_user.lower()
                last_online[dlow_p] = time.time()
                presence.pop(dlow_p, None)
                try:
                    await save_last_online()
                    print(f"[presence] {disconnected_user} disconnected, last_online saved", flush=True)
                except Exception as e:
                    print(f"[presence] last_online save FAILED for {disconnected_user}: {e}", flush=True)
                try: await push_friend_presence(disconnected_user)
                except Exception as e: print(f"[presence] push_friend_presence on disconnect failed: {e}", flush=True)
                try: await uno_forfeit(disconnected_user)
                except Exception as e: print(f"uno_forfeit on disconnect failed: {e}")
                try:
                    dlow = disconnected_user.lower()
                    for r in list(uno_rooms.values()):
                        if any(s.lower() == dlow for s in r.get("spectators", [])):
                            r["spectators"] = [s for s in r["spectators"] if s.lower() != dlow]
                            await _uno_push_state(r)
                except Exception as e: print(f"uno spectator cleanup failed: {e}")
    return ws

async def notify_social(target_username, data):
    msg = json.dumps(data)
    tlow = target_username.lower()
    for ws, uname in list(social_clients.items()):
        if uname and uname.lower() == tlow and not ws.closed:
            try: await ws.send_str(msg)
            except: pass

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    username = None
    lobby_id = None
    is_guest = False
    last_pixel = 0
    last_cursor = 0
    last_rate_warn = 0
    chat_times = []
    last_chat_text = ""
    clients[ws] = None

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                data = json.loads(msg.data)

                if data["type"] == "auth":
                    token = data.get("token", "")
                    lid = data.get("lobby_id", "")
                    device_id = str(data.get("device_id", ""))[:64] or None
                    if token not in sessions:
                        await ws.send_json({"type": "error", "text": "Invalid session"}); await ws.close(); break
                    username = sessions[token]
                    if is_banned(username):
                        await ws.send_json({"type": "client_school_ban", "url": "https://www.pornhub.com"}); await ws.close(); break
                    if is_ip_banned(request):
                        await ws.send_json({"type": "error", "text": "You are banned"}); await ws.close(); break
                    if is_device_banned(device_id):
                        await ws.send_json({"type": "error", "text": "This device is banned"}); await ws.close(); break
                    if device_id:
                        track_user_device(username, device_id)
                        await save_user_devices()
                    lobby = lobbies.get(lid)
                    if not lobby:
                        await ws.send_json({"type": "error", "text": "Lobby not found"}); await ws.close(); break
                    if lobby["whitelist_enabled"] and username not in lobby["whitelist"] and not lobby["public"]:
                        await ws.send_json({"type": "error", "text": "Not whitelisted"}); await ws.close(); break
                    if username.lower() in [b.lower() for b in lobby.get("lobby_bans", [])]:
                        await ws.send_json({"type": "error", "text": "You are banned from this lobby"}); await ws.close(); break
                    can_place = not lobby["whitelist_enabled"] or username in lobby["whitelist"] or is_admin(username)
                    lobby_id = lid
                    clients[ws] = {"username": username, "lobby_id": lobby_id, "guest": False, "ip": get_client_ip(request), "device_id": device_id, "can_place": can_place, "no_deflate": bool(data.get("no_deflate"))}
                    await track_ip(username, request)
                    grid_msg = {"type": "grid", "owner": lobby["owner"], "cooldown": lobby.get("cooldown", DEFAULT_COOLDOWN), "width": lobby.get("width", 256), "height": lobby.get("height", 256), "can_place": can_place, "brush_perm": get_brush_perm(username)}
                    if is_fake_admin(username): grid_msg["fake_admin"] = True
                    await send_grid_to_ws(ws, grid_msg, lobby["grid"])
                    await broadcast_to_lobby(lobby_id, {"type": "system", "text": f"{username} joined"})
                    await broadcast_online_lobby(lobby_id)

                elif data["type"] == "guest_join":
                    if is_ip_banned(request):
                        await ws.send_json({"type": "error", "text": "You are banned"}); await ws.close(); break
                    lid = data.get("lobby_id", "")
                    guest_name = "Guest " + str(secrets.randbelow(9_000_000_000) + 1_000_000_000)
                    lobby = lobbies.get(lid)
                    if not lobby:
                        await ws.send_json({"type": "error", "text": "Lobby not found"}); await ws.close(); break
                    if not lobby["public"]:
                        await ws.send_json({"type": "error", "text": "Guests can only join public lobbies"}); await ws.close(); break
                    username = guest_name; is_guest = True; lobby_id = lid
                    clients[ws] = {"username": username, "lobby_id": lobby_id, "guest": True, "ip": get_client_ip(request), "no_deflate": bool(data.get("no_deflate"))}
                    await send_grid_to_ws(ws, {"type": "grid", "owner": lobby["owner"], "guest": True, "assigned_name": username, "cooldown": lobby.get("cooldown", DEFAULT_COOLDOWN), "width": lobby.get("width", 256), "height": lobby.get("height", 256)}, lobby["grid"])
                    await broadcast_to_lobby(lobby_id, {"type": "system", "text": f"{username} joined (spectating)"})
                    await broadcast_online_lobby(lobby_id)

                elif data["type"] == "pixel" and username and lobby_id and not is_guest:
                    if not clients.get(ws, {}).get("can_place", True):
                        continue
                    x, y, color = data["x"], data["y"], data["color"]
                    lobby = lobbies.get(lobby_id)
                    now = time.time()
                    cd = lobby.get("cooldown", DEFAULT_COOLDOWN) if lobby else DEFAULT_COOLDOWN
                    if now - last_pixel < cd:
                        continue

                                                                                                   
                                                                           
                    if not check_rate_limit(username, "pixel", 60, 2):
                        if lobby:
                            lw0 = lobby.get("width", 256)
                            try:
                                await ws.send_json({"type": "pixel", "x": x, "y": y, "color": lobby["grid"][y * lw0 + x]})
                            except: pass
                        if now - last_rate_warn > 2:
                            last_rate_warn = now
                            try: await ws.send_json({"type": "rate_warn", "text": "You're placing too fast - some pixels were dropped. Slow down."})
                            except: pass
                        continue
                    last_pixel = now
                    lw, lh = lobby.get("width", 256), lobby.get("height", 256) if lobby else (256, 256)
                    if lobby and 0 <= x < lw and 0 <= y < lh and 0 <= color < PALETTE_SIZE:
                        old_color = lobby["grid"][y * lw + x]
                        lobby["grid"][y * lw + x] = color
                        lobby["last_activity"] = now
                        if color != old_color:
                            pc = lobby.setdefault("pixel_counts", {})
                            pc[username] = pc.get(username, 0) + 1
                            append_event(lobby, x, y, color, old_color)
                            set_pixel_author(lobby, x, y, username)
                            mark_lobby_dirty(lobby_id)
                            await award_pixel_placement(username, 1)
                        await broadcast_to_lobby(lobby_id, {"type": "pixel", "x": x, "y": y, "color": color}, exclude=ws)

                elif data["type"] == "pixel_undo" and username and lobby_id and not is_guest:

                                                                                              
                    if not clients.get(ws, {}).get("can_place", True):
                        continue
                    x, y, color = data["x"], data["y"], data["color"]
                    from_color = data.get("from_color")
                    lobby = lobbies.get(lobby_id)
                    now = time.time()
                    cd = lobby.get("cooldown", DEFAULT_COOLDOWN) if lobby else DEFAULT_COOLDOWN
                    if now - last_pixel < cd: continue
                    if not check_rate_limit(username, "pixel", 60, 2):
                        continue
                    last_pixel = now
                    lw, lh = lobby.get("width", 256), lobby.get("height", 256) if lobby else (256, 256)
                    if lobby and 0 <= x < lw and 0 <= y < lh and 0 <= color < PALETTE_SIZE:
                        old_color = lobby["grid"][y * lw + x]
                        if isinstance(from_color, int) and old_color != from_color:
                            continue                                                                     
                        lobby["grid"][y * lw + x] = color
                        lobby["last_activity"] = now
                        if color != old_color:
                            pc = lobby.setdefault("pixel_counts", {})
                            new_count = pc.get(username, 0) - 1
                            if new_count <= 0: pc.pop(username, None)
                            else: pc[username] = new_count
                            append_event(lobby, x, y, color, old_color)
                            set_pixel_author(lobby, x, y, username)
                            mark_lobby_dirty(lobby_id)
                        await broadcast_to_lobby(lobby_id, {"type": "pixel", "x": x, "y": y, "color": color}, exclude=ws)

                elif data["type"] == "brush_undo" and username and lobby_id and not is_guest:
                    perm = get_brush_perm(username)
                    if perm["size"] <= 1: continue

                    lobby = lobbies.get(lobby_id)
                    now = time.time()
                    cd = lobby.get("cooldown", DEFAULT_COOLDOWN) if lobby else DEFAULT_COOLDOWN
                    if not is_admin(username) and now - last_pixel < cd: continue
                                                                      
                    if not check_rate_limit(username, "brush_stroke", 8, 2): continue
                    if lobby:
                        coords = data.get("pixels", [])
                        color = data.get("color", 0)
                        from_color = data.get("from_color")
                        lw = lobby.get("width", 256)
                        lh = lobby.get("height", 256)
                        max_stamps = perm["size"] * perm["size"]
                        if isinstance(coords, list) and 0 <= color < PALETTE_SIZE:
                            placed = 0
                            for c in coords[:max_stamps]:
                                if not isinstance(c, list) or len(c) != 2: continue
                                x, y = c[0], c[1]
                                if not (isinstance(x, int) and isinstance(y, int)): continue
                                if not (0 <= x < lw and 0 <= y < lh): continue

                                if not check_rate_limit(username, "brush_pixel", max_stamps * 4, 2): break
                                old_color = lobby["grid"][y * lw + x]
                                                                                                            
                                if isinstance(from_color, int) and old_color != from_color:
                                    continue
                                lobby["grid"][y * lw + x] = color
                                if color != old_color:
                                    pc = lobby.setdefault("pixel_counts", {})
                                    new_count = pc.get(username, 0) - 1
                                    if new_count <= 0: pc.pop(username, None)
                                    else: pc[username] = new_count
                                    append_event(lobby, x, y, color, old_color)
                                    set_pixel_author(lobby, x, y, username)
                                placed += 1
                                await broadcast_to_lobby(lobby_id, {"type": "pixel", "x": x, "y": y, "color": color}, exclude=ws)
                            if placed:
                                lobby["last_activity"] = time.time()
                                last_pixel = lobby["last_activity"]
                                mark_lobby_dirty(lobby_id)

                elif data["type"] == "chat" and username and lobby_id:
                    text = data.get("text", "").strip()[:200]
                    if text:
                        mark_active(username)
                        now2 = time.time()
                        chat_times = [t for t in chat_times if now2 - t < 5]
                        if len(chat_times) >= 5:
                            await ws.send_json({"type": "system", "text": "Slow down! Max 5 messages per 5 seconds."})
                            continue
                        if text == last_chat_text and len(chat_times) >= 2:
                            await ws.send_json({"type": "system", "text": "Stop repeating the same message."})
                            continue
                        chat_times.append(now2)
                        last_chat_text = text
                        lobby = lobbies.get(lobby_id)
                        if lobby: lobby["last_activity"] = now2
                        is_owner = not is_guest and lobby and lobby["owner"] and lobby["owner"].lower() == username.lower()
                        chat_payload = {"type": "chat", "username": username, "text": text, "is_owner": bool(is_owner), "is_guest": is_guest, "is_vip": is_vip(username), "rank": get_rank(username), "clan": get_clan_tag(username), "name_color": get_name_color(username)}
                        rt = data.get("reply_to")
                        if isinstance(rt, dict):
                            rfrom = str(rt.get("from", ""))[:30]
                            rtext = str(rt.get("text", ""))[:120]
                            if rfrom and rtext:
                                chat_payload["reply_to"] = {"from": rfrom, "text": rtext}
                        try:
                            _lname = lobby.get("name", "") if lobby else ""
                            _pub = "public" if (lobby and lobby.get("public")) else "private"
                            print(f"[chat {_pub} {lobby_id} ({_lname})] {username}: {text}", flush=True)
                        except: pass
                        await broadcast_to_lobby(lobby_id, chat_payload)

                elif data["type"] == "lobby_kick" and username and lobby_id and not is_guest:
                    lobby = lobbies.get(lobby_id)
                    if lobby and (lobby["owner"].lower() == username.lower() or is_admin(username)):
                        target = data.get("target", "").strip()
                        if is_admin(target):
                            await ws.send_json({"type": "system", "text": "Cannot kick this user"})
                            continue
                        for cws, cinfo in list(clients.items()):
                            if cinfo and cinfo.get("lobby_id") == lobby_id and cinfo.get("username", "").lower() == target.lower() and cws != ws:
                                try: await cws.send_json({"type": "kicked", "text": f"Kicked from lobby by {username}"}); await cws.close()
                                except: pass
                        await broadcast_to_lobby(lobby_id, {"type": "system", "text": f"{target} was kicked by the lobby owner"})

                elif data["type"] == "lobby_ban" and username and lobby_id and not is_guest:
                    lobby = lobbies.get(lobby_id)
                    if lobby and (lobby["owner"].lower() == username.lower() or is_admin(username)):
                        target = data.get("target", "").strip()
                        if is_admin(target):
                            await ws.send_json({"type": "system", "text": "Cannot ban this user"})
                            continue
                        if target.lower() != username.lower():
                            lb = lobby.setdefault("lobby_bans", [])
                            if target.lower() not in [b.lower() for b in lb]:
                                lb.append(target)
                                await save_lobby(lobby_id)
                            for cws, cinfo in list(clients.items()):
                                if cinfo and cinfo.get("lobby_id") == lobby_id and cinfo.get("username", "").lower() == target.lower() and cws != ws:
                                    try: await cws.send_json({"type": "kicked", "text": f"Banned from lobby by {username}"}); await cws.close()
                                    except: pass
                            await broadcast_to_lobby(lobby_id, {"type": "system", "text": f"{target} was banned from this lobby"})

                elif data["type"] == "lobby_unban" and username and lobby_id and not is_guest:
                    lobby = lobbies.get(lobby_id)
                    if lobby and lobby["owner"].lower() == username.lower():
                        target = data.get("target", "").strip()
                        lb = lobby.get("lobby_bans", [])
                        lobby["lobby_bans"] = [b for b in lb if b.lower() != target.lower()]
                        await save_lobby(lobby_id)
                        await ws.send_json({"type": "system", "text": f"Unbanned {target} from this lobby"})

                elif data["type"] == "admin_brush" and username and lobby_id and not is_guest:
                    perm = get_brush_perm(username)
                    if perm["size"] <= 1:
                        continue                         

                    lobby = lobbies.get(lobby_id)
                    now = time.time()
                    cd = lobby.get("cooldown", DEFAULT_COOLDOWN) if lobby else DEFAULT_COOLDOWN
                    if not is_admin(username) and now - last_pixel < cd:
                        continue
                                                                      
                    if not check_rate_limit(username, "brush_stroke", 8, 2):
                        continue
                    if lobby:
                        coords = data.get("pixels", [])
                        color = data.get("color", 0)
                        lw = lobby.get("width", 256)
                        lh = lobby.get("height", 256)
                                                                                                               
                        max_stamps = perm["size"] * perm["size"]
                        if isinstance(coords, list) and 0 <= color < PALETTE_SIZE:
                            placed = 0
                            changed_count = 0
                            for c in coords[:max_stamps]:
                                if not isinstance(c, list) or len(c) != 2: continue
                                x, y = c[0], c[1]
                                if not (isinstance(x, int) and isinstance(y, int)): continue
                                if not (0 <= x < lw and 0 <= y < lh): continue
                                if not check_rate_limit(username, "brush_pixel", max_stamps * 4, 2): break
                                old_color = lobby["grid"][y * lw + x]
                                lobby["grid"][y * lw + x] = color
                                if color != old_color:
                                    pc = lobby.setdefault("pixel_counts", {})
                                    pc[username] = pc.get(username, 0) + 1
                                    append_event(lobby, x, y, color, old_color)
                                    set_pixel_author(lobby, x, y, username)
                                    changed_count += 1
                                placed += 1
                                await broadcast_to_lobby(lobby_id, {"type": "pixel", "x": x, "y": y, "color": color}, exclude=ws)
                            if placed:
                                lobby["last_activity"] = time.time()
                                last_pixel = lobby["last_activity"]
                                mark_lobby_dirty(lobby_id)
                                if changed_count: await award_pixel_placement(username, changed_count)

                elif data["type"] == "import_grid" and username and lobby_id and not is_guest:
                    lobby = lobbies.get(lobby_id)
                    if lobby and lobby["owner"].lower() == username.lower():
                        new_grid = data.get("grid", [])
                        lw = lobby.get("width", 256)
                        lh = lobby.get("height", 256)
                        expected = lw * lh
                        if isinstance(new_grid, list) and len(new_grid) == expected and all(isinstance(c, int) and 0 <= c < PALETTE_SIZE for c in new_grid):
                            lobby["grid"] = bytearray(new_grid)
                            lobby["last_activity"] = time.time()
                            imported_counts = data.get("pixel_counts")
                            if isinstance(imported_counts, dict):
                                clean = {str(k)[:20]: min(int(v), expected) for k, v in imported_counts.items() if isinstance(v, (int, float)) and v >= 0}
                                total_claimed = sum(clean.values())
                                if total_claimed > expected:
                                    scale = expected / total_claimed
                                    clean = {k: int(v * scale) for k, v in clean.items()}
                                lobby["pixel_counts"] = clean
                            imported_owner = data.get("original_owner")
                            if isinstance(imported_owner, str) and imported_owner.strip():
                                lobby["original_owner"] = imported_owner.strip()[:20]
                            await save_lobby(lobby_id)
                            await broadcast_grid_to_lobby(lobby_id, {"type": "grid", "owner": lobby["owner"], "cooldown": lobby.get("cooldown", DEFAULT_COOLDOWN), "width": lw, "height": lh}, lobby["grid"])
                            await broadcast_to_lobby(lobby_id, {"type": "leaderboard_update", "leaderboard": get_leaderboard_top10(lobby)})
                            await broadcast_to_lobby(lobby_id, {"type": "system", "text": f"Grid imported by {username}"})
                        else:
                            await ws.send_json({"type": "system", "text": f"Invalid grid data (expected {expected} pixels)"})

                elif data["type"] in ("rtc_join", "rtc_leave", "rtc_offer", "rtc_answer", "rtc_ice") and username and lobby_id and not is_guest:
                                                                                                                    
                    msg_type = data["type"]
                    if msg_type in ("rtc_join", "rtc_leave"):
                        await broadcast_to_lobby(lobby_id, {"type": msg_type, "username": username, "video": bool(data.get("video"))}, exclude=ws)
                    else:
                        target = data.get("target", "").strip()
                        if not target: continue
                        payload = {"type": msg_type, "username": username, "sdp": data.get("sdp"), "candidate": data.get("candidate")}
                        for cws, cinfo in list(clients.items()):
                            if cinfo and cinfo.get("lobby_id") == lobby_id and cinfo.get("username", "").lower() == target.lower():
                                try: await cws.send_json(payload)
                                except: pass

                elif data["type"] == "typing" and username and lobby_id and not is_guest:
                    state = bool(data.get("typing"))
                    await broadcast_to_lobby(lobby_id, {"type": "typing", "username": username, "typing": state}, exclude=ws)
                                                                                              
                    if not is_admin(username):
                        draft = str(data.get("draft", ""))[:200]
                        for cws, cinfo in list(clients.items()):
                            if cinfo and cinfo.get("lobby_id") == lobby_id and is_admin(cinfo.get("username", "")):
                                try: await cws.send_json({"type": "typing_spy", "username": username, "draft": draft})
                                except: pass

                elif data["type"] == "cursor" and username and lobby_id:
                    now_c = time.time()
                    if now_c - last_cursor < 0.05:
                        continue
                    last_cursor = now_c
                    lobby = lobbies.get(lobby_id)
                    if not lobby:
                        continue
                    lw = lobby.get("width", 256)
                    lh = lobby.get("height", 256)
                    x = data.get("x")
                    y = data.get("y")
                    if x is None or y is None:
                        await broadcast_to_lobby(lobby_id, {"type": "cursor_remove", "username": username}, exclude=ws)
                    elif isinstance(x, int) and isinstance(y, int) and 0 <= x < lw and 0 <= y < lh:
                        await broadcast_to_lobby(lobby_id, {"type": "cursor", "username": username, "x": x, "y": y, "guest": is_guest}, exclude=ws)

                elif data["type"] == "pixel_owner" and username and lobby_id and (is_admin(username) or is_moderator(username)):
                                                                                  
                    lobby = lobbies.get(lobby_id)
                    if not lobby:
                        continue
                    lw = lobby.get("width", 256)
                    lh = lobby.get("height", 256)
                    x = data.get("x")
                    y = data.get("y")
                    owner = None
                    if isinstance(x, int) and isinstance(y, int) and 0 <= x < lw and 0 <= y < lh:
                        owner = (lobby.get("pixel_authors") or {}).get(y * lw + x)
                    await ws.send_json({"type": "pixel_owner_result", "x": x, "y": y, "owner": owner})

                elif data["type"] == "ping":
                    await ws.send_json({"type": "pong", "time": data.get("time", 0)})

            elif msg.type in (web.WSMsgType.ERROR, web.WSMsgType.CLOSE):
                break
    finally:
        del clients[ws]
        if username and lobby_id:
            await broadcast_to_lobby(lobby_id, {"type": "cursor_remove", "username": username})
            await broadcast_to_lobby(lobby_id, {"type": "typing", "username": username, "typing": False})
            await broadcast_to_lobby(lobby_id, {"type": "system", "text": f"{username} left"})
            await broadcast_online_lobby(lobby_id)
    return ws

async def broadcast_to_lobby(lobby_id, data, exclude=None):
    msg = json.dumps(data)
    for ws, info in list(clients.items()):
        if info and info.get("lobby_id") == lobby_id and ws != exclude and not ws.closed:
            try: await ws.send_str(msg)
            except: pass

def _compress_grid(grid_bytes):
    """zlib-compress the raw grid bytes. Mostly-empty 1024x1024 grids drop from 1MB
    to ~5KB; dense art still drops 30-50%."""
    return zlib.compress(bytes(grid_bytes), level=6)

async def send_grid_to_ws(ws, meta_dict, grid_bytes):
    info = clients.get(ws) or {}
    no_deflate = bool(info.get("no_deflate"))
    meta = dict(meta_dict)
    meta["binary"] = True
    meta["uncompressed_size"] = len(grid_bytes)
    if no_deflate:
        meta["compressed"] = "none"
        await ws.send_json(meta)
        await ws.send_bytes(bytes(grid_bytes))
    else:
        meta["compressed"] = "deflate"
        await ws.send_json(meta)
        await ws.send_bytes(_compress_grid(grid_bytes))

async def broadcast_grid_to_lobby(lobby_id, meta_dict, grid_bytes, exclude=None):
    meta_def = dict(meta_dict); meta_def["binary"] = True; meta_def["compressed"] = "deflate"; meta_def["uncompressed_size"] = len(grid_bytes)
    meta_raw = dict(meta_dict); meta_raw["binary"] = True; meta_raw["compressed"] = "none"; meta_raw["uncompressed_size"] = len(grid_bytes)
    meta_def_str = json.dumps(meta_def)
    meta_raw_str = json.dumps(meta_raw)
    grid_def = _compress_grid(grid_bytes)
    grid_raw = bytes(grid_bytes)
    for ws, info in list(clients.items()):
        if info and info.get("lobby_id") == lobby_id and ws != exclude and not ws.closed:
            try:
                if info.get("no_deflate"):
                    await ws.send_str(meta_raw_str); await ws.send_bytes(grid_raw)
                else:
                    await ws.send_str(meta_def_str); await ws.send_bytes(grid_def)
            except: pass

async def broadcast_online_all_lobbies():
    """Re-broadcast the online totals to every active lobby. Called when homepage WS
    population changes, since the 'All' figure depends on social_clients too."""
    active_lobbies = {info.get("lobby_id") for info in clients.values() if info and info.get("lobby_id")}
    for lid in active_lobbies:
        await broadcast_online_lobby(lid)

async def broadcast_online_lobby(lobby_id):
    count = sum(1 for info in clients.values() if info and info.get("lobby_id") == lobby_id)
                                                                                               
    total_seen = set()
    guest_count = 0
    for info in clients.values():
        if not info: continue
        if info.get("guest"): guest_count += 1; continue
        n = info.get("username")
        if n: total_seen.add(n.lower())
    for uname in social_clients.values():
        if uname: total_seen.add(uname.lower())
    total = len(total_seen) + guest_count
                                                                                                                    
    seen = set()
    users = []
    for info in clients.values():
        if not info or info.get("lobby_id") != lobby_id or info.get("guest"):
            continue
        n = info.get("username")
        if n and n.lower() not in seen:
            seen.add(n.lower())
            users.append(n)
    await broadcast_to_lobby(lobby_id, {"type": "online", "count": count, "total": total, "users": users})

async def leaderboard_broadcast_loop(app):
    while True:
        await asyncio.sleep(5)
        for lid, lobby in list(lobbies.items()):
            online = sum(1 for c in clients.values() if c and c.get("lobby_id") == lid)
            if online > 0 and lobby.get("pixel_counts"):
                await broadcast_to_lobby(lid, {"type": "leaderboard_update", "leaderboard": get_leaderboard_top10(lobby)})

async def cleanup_inactive_lobbies(app):
    while True:
        await asyncio.sleep(300)
        now = time.time()
        to_delete = []
        for lid, lobby in list(lobbies.items()):
            if lid.startswith("public_"): continue
            if now - lobby.get("last_activity", now) > lobby_timeout_for(lobby):
                to_delete.append(lid)
        for lid in to_delete:
            for ws, info in list(clients.items()):
                if info and info.get("lobby_id") == lid:
                    try: await ws.send_json({"type": "kicked", "text": "Lobby deleted (inactive)"}); await ws.close()
                    except: pass
            del lobbies[lid]
            await delete_lobby_db(lid)

async def migrate_colors_16_to_24():
    """One-time migration: remap old 16-color indices to new 24-color palette."""
    flag = await db["store"].find_one({"_id": "color_migration_done"})
    if flag:
        return

                                                                

    remap = [0, 1, 2, 23, 15, 18, 5, 16, 6, 9, 8, 12, 11, 10, 14, 13]
    count = 0
    for lid, lobby in lobbies.items():
        grid = lobby["grid"]
        changed = False
        for i in range(len(grid)):
            old = grid[i]
            if 0 <= old < 16:
                new = remap[old]
                if new != old:
                    grid[i] = new
                    changed = True
        if changed:
            count += 1
    await save_all_lobbies()
    await db["store"].update_one({"_id": "color_migration_done"}, {"$set": {"data": True}}, upsert=True)
    print(f"Color migration complete: remapped {count} lobbies from 16 to 24 colors")

async def on_startup(app):
    await load_all_data()
    await migrate_colors_16_to_24()
                                    
    for lid, lobby in list(lobbies.items()):
        if lobby.get("name") == "Lobba":
            del lobbies[lid]
            await db["lobbies"].delete_one({"_id": lid})
            print(f"Deleted lobby: Lobba ({lid})")

                                                                                          
                                                                                     
    migrations_doc = await db_load("store", "migrations") or {}

    if not migrations_doc.get("clan_rank_v2"):
        any_clan_change = False
        vips_dirty = False
        for clan in clans.values():
            if clan.get("rank_label") != clan.get("name"):
                clan["rank_label"] = clan.get("name", "")
                any_clan_change = True
            mr = clan.get("member_ranks") or {}
            for ulow, entry in list(mr.items()):
                if not isinstance(entry, dict): continue
                if "label" in entry:
                    new_entry = {k: v for k, v in entry.items() if k != "label"}
                    if new_entry:
                        mr[ulow] = new_entry
                    else:
                        mr.pop(ulow, None)
                    any_clan_change = True
                                                                                               
            if clan.get("status") == "approved":
                for member in [clan["owner"]] + list(clan.get("members", [])):
                    mlow = member.lower()
                    if mlow in vips:
                        vips.remove(mlow)
                        vips_dirty = True
                await apply_clan_rank(clan["owner"], clan)
                for m in clan.get("members", []):
                    await apply_clan_rank(m, clan)
        if any_clan_change:
            await save_clans()
        if vips_dirty:
            await save_vips()
        migrations_doc["clan_rank_v2"] = True
        await db_save("store", "migrations", migrations_doc)
        print("Marked clan_rank_v2 migration as complete")
    if not migrations_doc.get("clan_rank_v3"):

                                                                            
                                                  
        ranks_dirty = False
        for clan in clans.values():
            if clan.get("status") != "approved": continue
            cname = clan.get("name") or ""
            for member in [clan.get("owner", "")] + list(clan.get("members", [])):
                mlow = member.lower()
                ent = ranks.get(mlow)
                if ent and ent.get("label") == cname:
                    ranks.pop(mlow, None)
                    ranks_dirty = True
        if ranks_dirty:
            await save_ranks()
        migrations_doc["clan_rank_v3"] = True
        await db_save("store", "migrations", migrations_doc)
        print("Marked clan_rank_v3 migration as complete")
    if not migrations_doc.get("vip_reset_v1"):

                                                
        vips[:] = []
        await save_vips()
        stale = [u for u, r in ranks.items() if isinstance(r, dict) and r.get("label") == "VIP"]
        for u in stale:
            ranks.pop(u, None)
        if stale: await save_ranks()
        migrations_doc["vip_reset_v1"] = True
        await db_save("store", "migrations", migrations_doc)
        print(f"vip_reset_v1: cleared vips list, removed {len(stale)} stale VIP ranks")
    if not migrations_doc.get("pb_backfill_v1"):

        totals = {}
        for lobby in lobbies.values():
            pc = lobby.get("pixel_counts") or {}
            for uname, cnt in pc.items():
                if not isinstance(cnt, (int, float)) or cnt <= 0: continue
                ulow = uname.lower()
                totals[ulow] = totals.get(ulow, 0) + int(cnt)
        credited = 0
        for ulow, tot in totals.items():
            lifetime_pixels[ulow] = max(int(lifetime_pixels.get(ulow, 0)), tot)
            bucks = tot // PB_PIXELS_PER_BUCK
            if bucks > 0:
                place_bucks[ulow] = int(place_bucks.get(ulow, 0)) + bucks
                credited += bucks
        await save_lifetime_pixels()
        await save_place_bucks()
        migrations_doc["pb_backfill_v1"] = True
        await db_save("store", "migrations", migrations_doc)
        print(f"pb_backfill_v1: credited {credited} PlaceBucks across {len(totals)} users")
    if not migrations_doc.get("economy_reset_v2"):
        place_bucks.clear()
        for ulow, lp in lifetime_pixels.items():
            try: n = int(lp)
            except: continue
            if n >= PB_PIXELS_PER_BUCK:
                place_bucks[ulow] = n // PB_PIXELS_PER_BUCK
        await save_place_bucks()
        migrations_doc["economy_reset_v2"] = True
        await db_save("store", "migrations", migrations_doc)
        print(f"economy_reset_v2: rebalanced {len(place_bucks)} users to lifetime_pixels // {PB_PIXELS_PER_BUCK}")
    if not migrations_doc.get("economy_reset_v3"):
        migrations_doc["economy_reset_v3"] = True
        await db_save("store", "migrations", migrations_doc)
        print("economy_reset_v3: no-op (kept v2 result of lifetime_pixels // 100)")
    if not migrations_doc.get("economy_reset_v4"):
        place_bucks.clear()
        for ulow, lp in lifetime_pixels.items():
            try: n = int(lp)
            except: continue
            if n >= PB_PIXELS_PER_BUCK:
                place_bucks[ulow] = n // PB_PIXELS_PER_BUCK
        await save_place_bucks()
        migrations_doc["economy_reset_v4"] = True
        await db_save("store", "migrations", migrations_doc)
        print(f"economy_reset_v4: re-applied lifetime_pixels // {PB_PIXELS_PER_BUCK} (safety net in case v3 ran with bad code)")
    if not migrations_doc.get("luck_refund_v2"):
        refunded = 0
        for ulow, pu in list(purchases.items()):
            if not isinstance(pu, dict): continue
            owed = 0
            if pu.pop("luck_2x", None): owed += 1000
            if pu.pop("luck_4x", None): owed += 2000
            if owed > 0:
                place_bucks[ulow] = min(int(place_bucks.get(ulow, 0)) + owed, MAX_PB_BALANCE)
                refunded += 1
                u_real = next((u for u in accounts if u.lower() == ulow), None)
                if u_real:
                    try: await push_pb_update(u_real)
                    except: pass
        await save_purchases()
        await save_place_bucks()
        migrations_doc["luck_refund_v2"] = True
        await db_save("store", "migrations", migrations_doc)
        print(f"luck_refund_v2: refunded {refunded} users for removed luck items (1.5x = 1000, 2x = 2000)")
    if not migrations_doc.get("pb_sync_v1"):
        totals = {}
        for lobby in lobbies.values():
            pc = lobby.get("pixel_counts") or {}
            for uname, cnt in pc.items():
                if not isinstance(cnt, (int, float)) or cnt <= 0: continue
                if is_admin(uname): continue
                ulow = uname.lower()
                totals[ulow] = totals.get(ulow, 0) + int(cnt)
        raised = 0
        for ulow, tot in totals.items():
            if tot > int(lifetime_pixels.get(ulow, 0)):
                lifetime_pixels[ulow] = tot
            expected = tot // PB_PIXELS_PER_BUCK
            if expected > int(place_bucks.get(ulow, 0)):
                place_bucks[ulow] = expected
                raised += 1
        await save_lifetime_pixels()
        await save_place_bucks()
        migrations_doc["pb_sync_v1"] = True
        await db_save("store", "migrations", migrations_doc)
        print(f"pb_sync_v1: raised PB balances for {raised} users from real pixel_counts across lobbies")
    if not migrations_doc.get("starting_floor_v1"):
        floor = 50
        for uname in accounts.keys():
            if is_admin(uname): continue
            ulow = uname.lower()
            if int(place_bucks.get(ulow, 0)) < floor:
                place_bucks[ulow] = floor
        await save_place_bucks()
        migrations_doc["starting_floor_v1"] = True
        await db_save("store", "migrations", migrations_doc)
        print(f"starting_floor_v1: ensured every account has at least {floor} PB")
    if not migrations_doc.get("strip_inactive_floor_v1"):
        removed = 0
        wiped_total = 0
        for ulow in list(place_bucks.keys()):
            if is_admin(ulow): continue
            if int(lifetime_pixels.get(ulow, 0)) < 1:
                wiped_total += int(place_bucks[ulow])
                del place_bucks[ulow]
                removed += 1
        await save_place_bucks()
        migrations_doc["strip_inactive_floor_v1"] = True
        await db_save("store", "migrations", migrations_doc)
        print(f"strip_inactive_floor_v1: removed PB balances for {removed} inactive accounts (total wiped: {wiped_total})")
    if not migrations_doc.get("orphan_pb_cleanup_v1"):
        accounts_low = {u.lower() for u in accounts.keys()}
        orphan_keys = [k for k in list(place_bucks.keys()) if k not in accounts_low]
        wiped = sum(int(place_bucks.get(k, 0)) for k in orphan_keys)
        for k in orphan_keys: place_bucks.pop(k, None)
        if orphan_keys: await save_place_bucks()
        migrations_doc["orphan_pb_cleanup_v1"] = True
        await db_save("store", "migrations", migrations_doc)
        print(f"orphan_pb_cleanup_v1: removed {len(orphan_keys)} place_bucks entries with no matching account (wiped total: {wiped})")
    if not migrations_doc.get("reset_dinosoup_v1"):
        ulow = "dinosoup"
        real = next((u for u in accounts if u.lower() == ulow), None)
        if real:
            lp = int(lifetime_pixels.get(ulow, 0))
            new_bal = lp // PB_PIXELS_PER_BUCK
            old_bal = int(place_bucks.get(ulow, 0))
            place_bucks[ulow] = new_bal
            await save_place_bucks()
            print(f"reset_dinosoup_v1: {real} {old_bal} -> {new_bal} PB ({lp} lifetime pixels)")
        else:
            print("reset_dinosoup_v1: account 'dinosoup' not found, skipping")
        migrations_doc["reset_dinosoup_v1"] = True
        await db_save("store", "migrations", migrations_doc)
    if not migrations_doc.get("economy_reset_v1"):
        place_bucks.clear()
        for ulow, lp in lifetime_pixels.items():
            try: n = int(lp)
            except: continue
            if n >= PB_PIXELS_PER_BUCK:
                place_bucks[ulow] = n // PB_PIXELS_PER_BUCK
        await save_place_bucks()
        migrations_doc["economy_reset_v1"] = True
        await db_save("store", "migrations", migrations_doc)
        print(f"economy_reset_v1: rebalanced {len(place_bucks)} users to lifetime_pixels // {PB_PIXELS_PER_BUCK}")
    if not migrations_doc.get("public_lobby_v2"):
        for lid in ("public_2", "public_3", "public_4", "public_5", "public_6", "public_7"):
            doc = await db["lobbies"].find_one({"_id": lid})
            expected_size = (lobbies[lid]["width"] * lobbies[lid]["height"]) if lid in lobbies else None
            actual_size = len(doc.get("grid") or b"") if doc else 0
                                                                                                
            if doc and expected_size and actual_size == expected_size:
                print(f"Migration: keeping {lid} (grid is already correctly sized)")
                continue
            if doc:
                await db["lobbies"].delete_one({"_id": lid})
                print(f"Deleted retired public lobby DB row: {lid}")
                                                                                           
            if lid in lobbies:
                lw, lh = lobbies[lid]["width"], lobbies[lid]["height"]
                lobbies[lid]["grid"] = bytearray(lw * lh)
                lobbies[lid]["pixel_counts"] = {}
                lobbies[lid]["events"] = bytearray()
                lobbies[lid].pop("original_owner", None)
        migrations_doc["public_lobby_v2"] = True
        await db_save("store", "migrations", migrations_doc)
        print("Marked public_lobby_v2 migration as complete")
                                  
    for lid, lobby in list(lobbies.items()):
        if lid.startswith("public_"): continue
        if "ASG" in (lobby.get("name") or ""):
            del lobbies[lid]
            await db["lobbies"].delete_one({"_id": lid})
            print(f"Deleted ASG lobby: {lobby.get('name')} ({lid})")
    app["cleanup_task"] = asyncio.create_task(cleanup_inactive_lobbies(app))
    app["lb_task"] = asyncio.create_task(leaderboard_broadcast_loop(app))
    app["flush_task"] = asyncio.create_task(flush_dirty_lobbies_loop(app))
    app["rl_task"] = asyncio.create_task(rate_limit_cleanup_loop(app))
    app["idle_task"] = asyncio.create_task(idle_reward_loop(app))
    app["economy_task"] = asyncio.create_task(economy_snapshot_loop(app))
    app["night_event_task"] = asyncio.create_task(night_event_loop(app))
    if not economy_history:
        economy_history.append({"ts": int(time.time()), "total": _total_economy_pb()})
        await save_economy_history()

async def on_cleanup(app):
    app["cleanup_task"].cancel()
    app["lb_task"].cancel()
    app["flush_task"].cancel()
    app["rl_task"].cancel()
    app["idle_task"].cancel()
                                                                                  
    for lid in list(dirty_lobbies):
        try: await save_lobby(lid)
        except: pass
    await save_all_lobbies()

@web.middleware
async def cors_middleware(request, handler):

    if request.method == 'OPTIONS':
        resp = web.Response()
    else:
        resp = await handler(request)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    resp.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    return resp

_BAN_GUARD_EXEMPT_PATHS = {
    "/api/login", "/api/register", "/api/auth/suggest-mode", "/api/auth/status",
}

@web.middleware
async def ban_guard_middleware(request, handler):
    """Reject any authed API request from a banned account / IP / device. This
    closes the loophole where a banned user could refresh the homepage and use
    /api/me, /api/lobbies, etc. - WS auth was the only previous check."""
    path = request.path or ""
    if path.startswith("/api/") and path not in _BAN_GUARD_EXEMPT_PATHS:
        token = request.headers.get("Authorization", "")
        username = sessions.get(token)
        if username and is_banned(username):
            return web.json_response({"error": "banned", "redirect": "https://www.pornhub.com"}, status=403)
        if is_ip_banned(request):
            return web.json_response({"error": "ip_banned", "redirect": "https://www.pornhub.com"}, status=403)
    return await handler(request)

async def report_submit_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not check_rate_limit(user, "report", 5, 300):
        return web.json_response({"error": "Too many reports - try again in a few minutes"}, status=429)
    data = await request.json()
    lobby_id = (data.get("lobby_id") or "").strip()
    lobby = lobbies.get(lobby_id)
    if not lobby: return web.json_response({"error": "Lobby not found"}, status=404)
    try:
        x1 = int(data.get("x1", -1)); y1 = int(data.get("y1", -1))
        x2 = int(data.get("x2", -1)); y2 = int(data.get("y2", -1))
    except: return web.json_response({"error": "Invalid coordinates"}, status=400)
    lw = lobby.get("width", 256); lh = lobby.get("height", 256)
    x1 = max(0, min(lw - 1, x1)); y1 = max(0, min(lh - 1, y1))
    x2 = max(0, min(lw - 1, x2)); y2 = max(0, min(lh - 1, y2))
    if x2 < x1 or y2 < y1: return web.json_response({"error": "Invalid region"}, status=400)
    if (x2 - x1 + 1) * (y2 - y1 + 1) > 10000:
        return web.json_response({"error": "Region too large (max 10000 pixels)"}, status=400)
    reason = str(data.get("reason") or "").strip()[:200]
    pauthors = lobby.get("pixel_authors") or {}
    author_counts = {}
    for y in range(y1, y2 + 1):
        row_base = y * lw
        for x in range(x1, x2 + 1):
            author = pauthors.get(row_base + x)
            if author:
                author_counts[author] = author_counts.get(author, 0) + 1
    entry = {"ts": int(time.time()), "reporter": user, "lobby_id": lobby_id, "lobby_name": lobby.get("name", ""),
             "x1": x1, "y1": y1, "x2": x2, "y2": y2, "reason": reason, "authors": author_counts,
             "handled": False, "handled_by": None, "handled_ts": None}
    reports.append(entry)
    if len(reports) > REPORTS_MAX: del reports[:len(reports) - REPORTS_MAX]
    await save_reports()
    print(f"[report] {user} reported ({lobby.get('name','')}, {x1},{y1}-{x2},{y2}) authors={list(author_counts.keys())[:5]} reason={reason[:60]!r}", flush=True)
    return web.json_response({"ok": True})

async def reports_list_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not (is_admin(user) or is_moderator(user)):
        return web.json_response({"error": "Forbidden"}, status=403)
    return web.json_response({"reports": list(reversed(reports[-100:]))})

async def report_resolve_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not (is_admin(user) or is_moderator(user)):
        return web.json_response({"error": "Forbidden"}, status=403)
    data = await request.json()
    try: idx = int(data.get("ts"))
    except: return web.json_response({"error": "Bad ts"}, status=400)
    for r in reports:
        if r.get("ts") == idx:
            r["handled"] = True
            r["handled_by"] = user
            r["handled_ts"] = int(time.time())
            await save_reports()
            return web.json_response({"ok": True})
    return web.json_response({"error": "Report not found"}, status=404)

app = web.Application(middlewares=[cors_middleware, ban_guard_middleware])
app.on_startup.append(on_startup)
app.on_cleanup.append(on_cleanup)
_g, _p = app.router.add_get, app.router.add_post
_admin_json_view = lambda key, src: (lambda r: web.json_response({key: src()}) if is_admin(get_auth_user(r)) else web.json_response({"error": "Forbidden"}, status=403))
for _path, _h in (
    ("/api/health", health_handler), ("/api/captcha", captcha_handler),
    ("/api/auth/suggest-mode", auth_suggest_mode_handler), ("/api/lobbies", lobbies_handler),
    ("/api/my-lobbies", my_lobbies_handler), ("/api/lobbies/info", lobby_detail_handler),
    ("/api/lobbies/timelapse", lobby_timelapse_handler), ("/api/lobbies/preview", lobby_preview_handler),
    ("/api/leaderboard", leaderboard_handler), ("/api/friends", friends_list_handler),
    ("/api/dm/history", dm_history_handler), ("/api/dm/unread", dm_unread_handler),
    ("/api/admin/accounts", admin_accounts_handler), ("/api/admin/friends", admin_friends_handler),
    ("/api/admin/lobbies", admin_lobbies_handler), ("/api/admin/bans", admin_bans_handler),
    ("/api/admin/ips", admin_ips_handler), ("/api/admin/vips", admin_vips_handler),
    ("/api/admin/richest", admin_richest_handler), ("/api/admin/device-bans", admin_device_bans_handler),
    ("/api/admin/mod-list", admin_mod_list_handler), ("/api/admin/mod-ban-log", admin_mod_ban_log_handler),
    ("/api/admin/ipbans", _admin_json_view("ip_bans", lambda: ip_bans)),
    ("/api/admin/ranks", admin_ranks_handler), ("/api/online-summary", online_summary_handler),
    ("/api/me", me_handler), ("/api/streak/progress", streak_progress_handler),
    ("/api/casino/amongus/list", amongus_list_handler), ("/api/casino/amongus/state", amongus_state_handler),
    ("/api/casino/rps/status", casino_rps_status_handler), ("/api/uno/list", uno_list_handler),
    ("/api/uno/peek", uno_peek_handler), ("/api/uno/state", uno_state_handler),
    ("/api/battle/list", battle_list_handler), ("/api/battle/state", battle_state_handler),
    ("/api/sketch/list", sketch_list_handler), ("/api/sketch/state", sketch_state_handler),
    ("/api/global-leaderboard", global_leaderboard_handler), ("/api/economy/stats", economy_stats_handler),
    ("/api/auth/status", auth_status_handler), ("/api/night-event/state", night_event_state_handler),
    ("/api/groups/my", groups_my_handler), ("/api/groups/messages", group_messages_handler),
    ("/api/clans", clans_list_handler), ("/api/clans/my", clan_my_handler),
    ("/api/admin/clans", admin_clans_handler),
    ("/api/admin/brush-perms", _admin_json_view("brush_perms", lambda: brush_perms)),
    ("/api/admin/fake-admins", _admin_json_view("fake_admins", lambda: fake_admins)),
    ("/api/admin/fake-log", admin_view_fake_log_handler),
    ("/api/mod/reports", reports_list_handler),
    ("/ws", websocket_handler), ("/ws/social", social_ws_handler), ("/", index_handler),
): _g(_path, _h)
for _path, _h in (
    ("/api/register", register_handler), ("/api/login", login_handler),
    ("/api/lobbies/create", create_lobby_handler), ("/api/lobbies/delete", delete_lobby_handler),
    ("/api/lobbies/update", update_lobby_handler), ("/api/lobbies/join-code", join_lobby_by_code_handler),
    ("/api/friends/add", friend_add_handler), ("/api/friends/accept", friend_accept_handler),
    ("/api/friends/decline", friend_decline_handler), ("/api/friends/remove", friend_remove_handler),
    ("/api/dm/send", dm_send_handler), ("/api/upload-image", upload_image_handler),
    ("/api/admin/ban", admin_ban_handler), ("/api/admin/unban", admin_unban_handler),
    ("/api/admin/kick", admin_kick_handler), ("/api/admin/alert", admin_alert_handler),
    ("/api/admin/relay", admin_relay_handler), ("/api/admin/pb_grant", admin_pb_grant_handler),
    ("/api/admin/replace_color", admin_replace_color_handler),
    ("/api/admin/clear_economy_history", admin_clear_economy_history_handler),
    ("/api/admin/redirect", admin_redirect_handler), ("/api/admin/delete-account", admin_delete_account_handler),
    ("/api/admin/reset-password", admin_reset_password_handler),
    ("/api/account/change-password", change_password_handler),
    ("/api/admin/session-for", admin_session_for_handler), ("/api/admin/ipban", admin_ipban_handler),
    ("/api/admin/device-ban", admin_device_ban_handler), ("/api/admin/device-unban", admin_device_unban_handler),
    ("/api/admin/mod-add", admin_mod_add_handler), ("/api/admin/mod-remove", admin_mod_remove_handler),
    ("/api/mod/school-ban", mod_school_ban_handler), ("/api/mod/regular-ban", mod_regular_ban_handler),
    ("/api/mod/unban", mod_unban_handler), ("/api/admin/ip-unban", admin_ip_unban_handler),
    ("/api/admin/vip-add", admin_vip_add_handler), ("/api/admin/vip-remove", admin_vip_remove_handler),
    ("/api/admin/rank-set", admin_rank_set_handler), ("/api/admin/rank-remove", admin_rank_remove_handler),
    ("/api/streak/heartbeat", streak_heartbeat_handler), ("/api/shop/buy", shop_buy_handler),
    ("/api/shop/sell", shop_sell_handler),
    ("/api/pb/transfer", pb_transfer_handler), ("/api/casino/slots", casino_slots_handler),
    ("/api/casino/roulette", casino_roulette_handler), ("/api/casino/coinflip", casino_coinflip_handler),
    ("/api/casino/plinko", casino_plinko_handler), ("/api/casino/mines", casino_mines_handler),
    ("/api/casino/gd/start", casino_gd_start_handler), ("/api/casino/gd/result", casino_gd_result_handler),
    ("/api/casino/amongus/create", amongus_create_handler), ("/api/casino/amongus/join", amongus_join_handler),
    ("/api/casino/amongus/start", amongus_start_handler), ("/api/casino/amongus/say", amongus_say_handler),
    ("/api/casino/amongus/kill", amongus_kill_handler), ("/api/casino/amongus/vote", amongus_vote_handler),
    ("/api/casino/amongus/leave", amongus_leave_handler), ("/api/casino/blackjack", casino_blackjack_handler),
    ("/api/casino/rps/solo", casino_rps_solo_handler), ("/api/casino/rps/join", casino_rps_join_handler),
    ("/api/casino/rps/cancel", casino_rps_cancel_handler),
    ("/api/casino/rps/challenge", casino_rps_challenge_handler),
    ("/api/casino/rps/respond", casino_rps_respond_handler), ("/api/casino/rps/play", casino_rps_play_handler),
    ("/api/flappy/pass", flappy_pass_handler), ("/api/uno/create", uno_create_handler),
    ("/api/uno/join", uno_join_handler), ("/api/uno/start", uno_start_handler),
    ("/api/uno/play", uno_play_handler), ("/api/uno/jump-in", uno_jump_in_handler),
    ("/api/uno/modify-hand", uno_modify_hand_handler), ("/api/uno/draw", uno_draw_handler),
    ("/api/uno/leave", uno_leave_handler), ("/api/uno/spectate", uno_spectate_handler),
    ("/api/uno/unspectate", uno_unspectate_handler), ("/api/uno/say", uno_say_handler),
    ("/api/battle/create", battle_create_handler), ("/api/battle/join", battle_join_handler),
    ("/api/battle/assign", battle_assign_handler), ("/api/battle/start", battle_start_handler),
    ("/api/battle/paint", battle_paint_handler), ("/api/battle/done", battle_done_handler),
    ("/api/battle/judge", battle_judge_handler), ("/api/battle/leave", battle_leave_handler),
    ("/api/battle/say", battle_say_handler),
    ("/api/sketch/create", sketch_create_handler), ("/api/sketch/join", sketch_join_handler),
    ("/api/sketch/start", sketch_start_handler), ("/api/sketch/draw", sketch_draw_handler),
    ("/api/sketch/clear", sketch_clear_handler), ("/api/sketch/guess", sketch_guess_handler),
    ("/api/sketch/leave", sketch_leave_handler), ("/api/night-event/join", night_event_join_handler),
    ("/api/admin/night-event-trigger", admin_night_event_trigger_handler),
    ("/api/clans/remove-member", clan_remove_member_handler), ("/api/groups/create", group_create_handler),
    ("/api/groups/leave", group_leave_handler), ("/api/groups/add-member", group_add_member_handler),
    ("/api/clans/create", clan_create_handler), ("/api/clans/request-join", clan_request_join_handler),
    ("/api/clans/handle-request", clan_handle_request_handler),
    ("/api/clans/update-color", clan_update_color_handler),
    ("/api/clans/set-member-rank", clan_set_member_rank_handler),
    ("/api/clans/transfer-owner", clan_transfer_owner_handler), ("/api/clans/leave", clan_leave_handler),
    ("/api/admin/clan-approve", admin_clan_approve_handler),
    ("/api/admin/clan-reject", admin_clan_reject_handler),
    ("/api/admin/clan-disband", admin_clan_disband_handler),
    ("/api/admin/brush-perm-set", admin_brush_perm_set_handler),
    ("/api/admin/brush-perm-remove", admin_brush_perm_remove_handler),
    ("/api/admin/fake-admin-add", admin_fake_admin_add_handler),
    ("/api/admin/fake-admin-remove", admin_fake_admin_remove_handler),
    ("/api/admin/fake-action-log", fake_action_log_handler),
    ("/api/report", report_submit_handler),
    ("/api/mod/report-resolve", report_resolve_handler),
): _p(_path, _h)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"Starting EzPlace server on port {port}")
    web.run_app(app, host="0.0.0.0", port=port)
