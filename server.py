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
LOBBY_TIMEOUT_PUBLIC = 48 * 60 * 60   # 48h for user-created public lobbies
LOBBY_TIMEOUT_PRIVATE = 168 * 60 * 60 # 168h (1 week) for private lobbies
# Kept for back-compat in any reference; default value is the longer of the two
LOBBY_TIMEOUT = LOBBY_TIMEOUT_PRIVATE

def lobby_timeout_for(lobby):
    return LOBBY_TIMEOUT_PUBLIC if lobby.get("public") else LOBBY_TIMEOUT_PRIVATE

MONGO_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017/ezplace")
mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
db = mongo_client.get_default_database() if "mongodb.net" in MONGO_URI else mongo_client["ezplace"]

accounts = {}
sessions = {}
captchas = {}
friends_data = {}
dms = {}
dm_last_seen = {}  # { user_lower: { peer_lower: epoch_seconds } }
bans = []
ip_bans = []
vips = []
ranks = {}  # { username_lower: { "label": "VIP", "color": "#daa520" } }
user_ips = {}
fake_admins = []
brush_perms = {}  # { username_lower: { "size": int, "drag": bool } }
clans = {}  # { clan_id: { id, name, owner, color, rank_label, status, members, pending_requests, created_at } }
groups = {}  # { group_id: { id, name, owner, members, created_at } }
group_messages = {}  # { group_id: [ {from, text?, image_url?, time}, ... ] }

# PlaceBucks economy: 1 PB per 100 pixels placed. Cumulative pixels are tracked
# separately so awarding is incremental (PB given on each crossing of a 100-mark).
place_bucks = {}      # { username_lower: int }
lifetime_pixels = {}  # { username_lower: int }
purchases = {}        # { username_lower: { "custom_wheel": bool, "vip": bool } }
PB_PIXELS_PER_BUCK = 100
SHOP_PRICES = {"custom_wheel": 5, "vip": 50, "custom_rank": 70}
LOBBY_PRICES = {(256, 256): 0, (512, 512): 10, (1024, 1024): 20}

# Whitelist of image hosts so users can't inject arbitrary URLs (which could leak IPs or load malicious content)
ALLOWED_IMAGE_HOSTS = ("https://files.catbox.moe/", "https://litter.catbox.moe/", "https://i.imgur.com/", "https://imgur.com/")
def is_safe_image_url(url):
    return isinstance(url, str) and any(url.startswith(h) for h in ALLOWED_IMAGE_HOSTS) and len(url) <= 500
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

async def save_accounts():
    await db_save("store", "accounts", accounts)

async def save_friends():
    await db_save("store", "friends", friends_data)

async def save_bans():
    await db_save("store", "bans", bans)

async def save_ip_bans():
    await db_save("store", "ip_bans", ip_bans)

async def save_vips():
    await db_save("store", "vips", vips)

async def save_fake_admins():
    await db_save("store", "fake_admins", fake_admins)

async def save_brush_perms():
    await db_save("store", "brush_perms", brush_perms)

async def save_clans():
    await db_save("store", "clans", clans)

async def save_groups():
    await db_save("store", "groups", groups)

async def save_group_messages(gid):
    msgs = group_messages.get(gid, [])
    await db_save("group_messages", gid, msgs)

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
    independent of the `ranks` dict — a clan member who ALSO has an admin
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
    # Clan tags are now computed live (get_clan_tag) and no longer occupy the
    # `ranks` dict, so an admin-given rank can coexist with the clan chip.
    # Kept as a no-op for the many existing call sites.
    return

async def remove_user_clan_rank(username):
    if not username or is_admin(username): return
    ulow = username.lower()
    ranks.pop(ulow, None)
    # Also strip from vips so the chat tag doesn't fall back to [VIP] after leaving a clan.
    # Admin-granted VIPs use a different code path (admin_vip_add) and are unaffected.
    if ulow in vips:
        vips.remove(ulow)
        await save_vips()
    await save_ranks()

def get_brush_perm(username):
    if not username:
        return {"size": 1, "drag": False}
    if is_admin(username):
        return {"size": 25, "drag": True}
    return brush_perms.get(username.lower(), {"size": 1, "drag": False})

async def save_ranks():
    await db_save("store", "ranks", ranks)

async def save_user_ips():
    await db_save("store", "user_ips", user_ips)

async def save_place_bucks():
    await db_save("store", "place_bucks", place_bucks)
async def save_lifetime_pixels():
    await db_save("store", "lifetime_pixels", lifetime_pixels)
async def save_purchases():
    await db_save("store", "purchases", purchases)

PB_UNLIMITED = 10 ** 9  # sentinel "balance" reported for the real admin

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
def credit_pb(user, amount):
    if not user or amount <= 0 or is_admin(user): return
    ulow = user.lower()
    place_bucks[ulow] = int(place_bucks.get(ulow, 0)) + amount
def has_purchase(user, item):
    if not user: return False
    if is_admin(user): return True
    return bool((purchases.get(user.lower()) or {}).get(item))

async def push_pb_update(username):
    """Send the current balance/purchases to all live sockets for this user."""
    if not username: return
    payload = {"type": "pb_update", "balance": get_pb(username), "purchases": purchases.get(username.lower()) or {}}
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
    ulow = username.lower()
    old = int(lifetime_pixels.get(ulow, 0))
    new = old + count
    lifetime_pixels[ulow] = new
    delta = (new // PB_PIXELS_PER_BUCK) - (old // PB_PIXELS_PER_BUCK)
    if delta > 0:
        place_bucks[ulow] = int(place_bucks.get(ulow, 0)) + delta
        await save_place_bucks()
        await push_pb_update(username)

# Set of lobby IDs with unsaved changes. The background task flushes them periodically.
# save_lobby() flushes immediately; mark_lobby_dirty() defers to the background loop.
dirty_lobbies = set()

def mark_lobby_dirty(lid):
    if lid and lid in lobbies:
        dirty_lobbies.add(lid)

# Sliding-window rate limiter. Keys are (identity, action) tuples; identity is a username
# for authenticated actions or an IP for anonymous/auth actions. Returns True if the action
# is allowed (and records the timestamp); False if the caller has exceeded max_count in window_sec.
_rate_limits = {}  # { (identity, action): [timestamp, ...] }

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

TIMELAPSE_WINDOW_SEC = 24 * 60 * 60  # 24 hours
EVENT_STRUCT = struct.Struct('<IHHBB')  # 10 bytes: ts (uint32 sec), x (uint16), y (uint16), new_color (uint8), old_color (uint8)
EVENT_SIZE = EVENT_STRUCT.size

def append_event(lobby, x, y, new_color, old_color):
    """Append a pixel event to the lobby's timelapse log and prune entries older than 24h."""
    log = lobby.setdefault("events", bytearray())
    now = int(time.time())
    log.extend(EVENT_STRUCT.pack(now, x, y, new_color, old_color))
    # Prune old events: scan from start and find first event within window
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
    In-memory only — intentionally NOT persisted (would bloat DB/bandwidth) so it
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
    # grid/events/pixel_authors are saved as separate binary fields, not in meta.
    # pixel_authors persists across restarts so the admin "who placed this" tool
    # survives deploys (stored compactly; never sent to clients).
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
    """Background task: every 30s, save any lobby that has unsaved pixel changes."""
    while True:
        try:
            await asyncio.sleep(30)
            for lid in list(dirty_lobbies):
                try:
                    await save_lobby(lid)
                except Exception as e:
                    print(f"flush_dirty_lobbies: failed to save {lid}: {e}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"flush_dirty_lobbies_loop error: {e}")

async def rate_limit_cleanup_loop(app):
    """Periodically prune empty/expired entries from the rate limiter so it doesn't grow unbounded."""
    while True:
        try:
            await asyncio.sleep(300)  # every 5 minutes
            now = time.time()
            # Use a generous 1-hour cutoff — anything older than the longest window we track is safe to drop
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

async def save_dm(key):
    msgs = dms.get(key, [])
    await db_save("dms", key, msgs)

async def save_dm_last_seen():
    await db_save("store", "dm_last_seen", dm_last_seen)

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
    # Find all DM threads this user is part of
    senders = {}  # peer_display_name -> {count, last_text, last_time}
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
    global accounts, friends_data, bans, ip_bans, vips, ranks, user_ips, fake_admins, brush_perms, clans, groups, group_messages, dms, dm_last_seen, place_bucks, lifetime_pixels, purchases

    accounts = await db_load("store", "accounts") or {}
    friends_data = await db_load("store", "friends") or {}
    bans = await db_load("store", "bans") or []
    ip_bans = await db_load("store", "ip_bans") or []
    vips = await db_load("store", "vips") or []
    ranks = await db_load("store", "ranks") or {}
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

    # Convert whatever MongoDB gives us back (old int-list format or new bytes format) to bytearray
    def _to_bytearray(data):
        if data is None:
            return None
        # pymongo returns BSON Binary as the `bytes` type and lists as Python lists.
        # bytearray() accepts both so this is a one-liner, but we wrap it for clarity.
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
    # Tiny, CORS-friendly endpoint used by the client to probe whether this
    # host is reachable (primary vs. proxy failover).
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
        return web.json_response({"error": "Too many registration attempts — try again later."}, status=429)
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
    # If the requesting IP has previously been seen attached to any account, default to login.
    # Otherwise default to register. This is a UX hint only — users can still switch manually.
    existing = any(v == ip for v in user_ips.values()) if ip else False
    return web.json_response({"mode": "login" if existing else "register"})

async def login_handler(request):
    data = await request.json()
    if not check_rate_limit(get_client_ip(request), "login", 10, 60):
        return web.json_response({"error": "Too many login attempts — try again in a minute."}, status=429)
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
    has_full_24h = oldest is not None and (now - oldest) >= TIMELAPSE_WINDOW_SEC - 60  # allow 1min slack
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
        return web.json_response({"error": "Slow down — max 3 lobby creates per minute."}, status=429)
    name = data.get("name", "").strip()[:30]
    is_public = data.get("public", False)
    wl = data.get("whitelist_enabled", False)
    cooldown = max(0, min(MAX_COOLDOWN, float(data.get("cooldown", DEFAULT_COOLDOWN))))
    lw = int(data.get("width", 256))
    lh = int(data.get("height", 256))
    if (lw, lh) not in VALID_SIZES:
        lw, lh = 256, 256
    if not name:
        return web.json_response({"error": "Lobby name required"}, status=400)
    if user_lobby_count(user) >= MAX_LOBBIES_PER_USER:
        return web.json_response({"error": f"Max {MAX_LOBBIES_PER_USER} lobbies"}, status=400)
    # PlaceBucks cost based on lobby size (256 free, 512 = 10 PB, 1024 = 20 PB).
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
    # Official public_ lobbies: only lobby_unban is editable
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
    return web.json_response({"friends": [{"name": f, "online": is_online(f)} for f in fd["friends"]], "incoming": fd["incoming"], "outgoing": fd["outgoing"]})

async def friend_add_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not check_rate_limit(user, "friend_add", 10, 60):
        return web.json_response({"error": "Too many friend requests — try again in a minute."}, status=429)
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
    if target:
        mark_dm_seen(user, target)
        await save_dm_last_seen()
    return web.json_response({"messages": msgs})

async def dm_unread_handler(request):
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    return web.json_response({"senders": get_unread_dm_summary(user)})

async def upload_image_handler(request):
    """Browser-CORS-safe image upload proxy. Forwards the file to catbox.moe and returns the URL.
    The image bytes pass through this server in transit but are NOT stored anywhere — neither on
    disk nor in MongoDB. Only the resulting catbox URL is later persisted in DM/group history."""
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if is_banned(user) or is_ip_banned(request):
        return web.json_response({"error": "Forbidden"}, status=403)
    if not check_rate_limit(user, "upload_image", 10, 60):
        return web.json_response({"error": "Upload rate limit — max 10 images per minute."}, status=429)
    if not check_rate_limit(get_client_ip(request), "upload_image_ip", 20, 60):
        return web.json_response({"error": "Upload rate limit — too many uploads from your IP."}, status=429)
    try:
        reader = await request.multipart()
    except Exception:
        return web.json_response({"error": "Invalid multipart"}, status=400)
    field = await reader.next()
    if field is None or field.name != "file":
        return web.json_response({"error": "Missing file field"}, status=400)
    # Read up to 6 MB (catbox accepts larger but the client already compresses to ~1-2 MB)
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
    # Upload to catbox from the server (server-to-server, no CORS to worry about)
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
        return web.json_response({"error": "Rate limited — max 5 DMs per 5 seconds."}, status=429)
    if image_url and not check_rate_limit(user, "dm_image", 5, 60):
        return web.json_response({"error": "Image rate limit — max 5 images per minute."}, status=429)
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
    return web.json_response({"lobbies": {lid: {k: v for k, v in l.items() if k != "grid"} for lid, l in lobbies.items()}})

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
    # Also account-ban and kick all connections from that IP
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

async def admin_redirect_handler(request):
    data = await request.json()
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    target = data.get("username", "").strip()
    url = data.get("url", "").strip()[:500]
    if not target or not url:
        return web.json_response({"error": "Username and url required"}, status=400)
    # Only allow http(s) URLs — blocks javascript:, data:, file:, etc. so this stays a redirect, not a code exec vector
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
    # Disconnect any active sessions
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
    # Remove from accounts
    del accounts[found]
    await save_accounts()
    # Remove from friends_data (their entry + references in others)
    if found in friends_data:
        del friends_data[found]
    for u, fd in list(friends_data.items()):
        fd["friends"] = [f for f in fd.get("friends", []) if f.lower() != tlow]
        fd["incoming"] = [f for f in fd.get("incoming", []) if f.lower() != tlow]
        fd["outgoing"] = [f for f in fd.get("outgoing", []) if f.lower() != tlow]
    await save_friends()
    # Remove from bans, vips, user_ips
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
    # Delete their owned lobbies
    owned_lids = [lid for lid, l in list(lobbies.items()) if not lid.startswith("public_") and l.get("owner", "").lower() == tlow]
    for lid in owned_lids:
        for ws, info in list(clients.items()):
            if info and info.get("lobby_id") == lid:
                try: await ws.send_json({"type": "kicked", "text": "Lobby deleted (owner account removed)"}); await ws.close()
                except: pass
        del lobbies[lid]
        await db["lobbies"].delete_one({"_id": lid})
    # Remove from pixel_counts in all remaining lobbies
    for lid, l in lobbies.items():
        pc = l.get("pixel_counts", {})
        for k in [k for k in pc if k.lower() == tlow]:
            del pc[k]
    # Delete DM threads involving this user
    keys_to_delete = [k for k in dms if tlow in k.split(":")]
    for k in keys_to_delete:
        del dms[k]
        await db["dms"].delete_one({"_id": k})
    # Remove dm_last_seen entries for and about this user
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
    # Clan cleanup: disband any clan they owned; remove from any clan they were a member of
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
    # PlaceBucks + purchases + lifetime pixels
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
    join_requests = []  # outgoing requests where I asked to join
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
        return web.json_response({"error": "Slow down — too many clan create attempts."}, status=429)
    name = data.get("name", "").strip()[:30]
    color = data.get("color", "").strip()[:32]
    if not name or not color:
        return web.json_response({"error": "Name and color required"}, status=400)
    if not (color.startswith("#") and (len(color) == 7 or len(color) == 4)):
        return web.json_response({"error": "Color must be a hex code"}, status=400)
    # One clan per user (any status)
    for clan in clans.values():
        if clan.get("owner", "").lower() == user.lower():
            return web.json_response({"error": "You already have a clan request or approved clan"}, status=400)
    cid = secrets.token_hex(6)
    # rank_label is kept in the dict for back-compat with old clients/serializers, but the
    # clan name is what's actually used as the chat rank tag (see apply_clan_rank).
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
        return web.json_response({"error": "Too many clan join requests — try again in a minute."}, status=429)
    cid = data.get("clan_id", "")
    clan = clans.get(cid)
    if not clan or clan.get("status") != "approved":
        return web.json_response({"error": "Clan not found"}, status=404)
    ulow = user.lower()
    if clan["owner"].lower() == ulow or any(m.lower() == ulow for m in clan.get("members", [])):
        return web.json_response({"error": "You are already in this clan"}, status=400)
    # Can't be in another clan
    if find_clan_by_member(user):
        return web.json_response({"error": "Leave your current clan first"}, status=400)
    if any(r.lower() == ulow for r in clan.get("pending_requests", [])):
        return web.json_response({"error": "You already requested to join"}, status=400)
    # Only ONE outgoing pending clan-join request at a time across all clans.
    for other in clans.values():
        if any(r.lower() == ulow for r in other.get("pending_requests", [])):
            return web.json_response({"error": f"You already have a pending request to join '{other.get('name', '')}'. Wait for that to be answered or cancel it first."}, status=400)
    clan.setdefault("pending_requests", []).append(user)
    await save_clans()
    # Notify owner via social WS
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
        # Make sure they're not already in another clan
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
        return web.json_response({"error": "Too many rank changes — slow down."}, status=429)
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
    # Re-apply the (possibly overridden) rank so chat tags update immediately
    if clan.get("status") == "approved":
        # Use the canonical-cased username from members/owner for the apply call
        canonical = clan["owner"] if clan["owner"].lower() == tlow else next((m for m in clan.get("members", []) if m.lower() == tlow), target)
        await apply_clan_rank(canonical, clan)
    return web.json_response({"ok": True})

async def clan_update_color_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not check_rate_limit(user, "clan_update_color", 10, 60):
        return web.json_response({"error": "Too many color changes — slow down."}, status=429)
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
    # Re-apply rank to owner + all members so their chat-tag color updates immediately
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
        # Owner leaves -> dissolve clan, strip ranks from all members
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
        return web.json_response({"error": "Too many groups created — try again later."}, status=429)
    name = data.get("name", "").strip()[:30]
    invitees = data.get("members", [])
    if not name: return web.json_response({"error": "Group name required"}, status=400)
    if not isinstance(invitees, list): invitees = []
    fd = get_friend_data(user)
    friend_set = {f.lower() for f in fd["friends"]}
    members = [user]
    for u in invitees[:19]:  # cap total members at 20 (1 owner + 19)
        if not isinstance(u, str): continue
        if u.lower() in friend_set and u.lower() != user.lower() and u not in members:
            members.append(u)
    gid = secrets.token_hex(6)
    groups[gid] = {"id": gid, "name": name, "owner": user, "members": members, "created_at": time.time()}
    await save_groups()
    # Notify each invited member
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
        # Owner leaves -> dissolve, OR no members left
        del groups[gid]
        group_messages.pop(gid, None)
        await db["group_messages"].delete_one({"_id": gid})
        await save_groups()
        # Notify remaining members that the group dissolved
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
    """Current user's balance, purchases, lifetime pixels. Real admin reports unlimited."""
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    return web.json_response({
        "username": user,
        "balance": get_pb(user),
        "unlimited": bool(is_admin(user)),
        "purchases": purchases.get(user.lower()) or {},
        "lifetime_pixels": int(lifetime_pixels.get(user.lower(), 0)),
        "prices": {"shop": SHOP_PRICES, "lobby": {f"{w}x{h}": p for (w, h), p in LOBBY_PRICES.items()}, "pixels_per_buck": PB_PIXELS_PER_BUCK},
    })

async def shop_buy_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    if not check_rate_limit(user, "shop_buy", 10, 60):
        return web.json_response({"error": "Too many purchases — slow down."}, status=429)
    item = (data.get("item") or "").strip()
    if item not in SHOP_PRICES:
        return web.json_response({"error": "Unknown item"}, status=400)
    # custom_rank is special: pay 70 PB once, then edit label/color for free anytime.
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
    if has_purchase(user, item):
        return web.json_response({"error": "You already own that"}, status=400)
    price = SHOP_PRICES[item]
    if not spend_pb(user, price):
        return web.json_response({"error": f"Not enough PlaceBucks (need {price})"}, status=400)
    pu = purchases.setdefault(user.lower(), {})
    pu[item] = True
    await save_purchases()
    await save_place_bucks()
    # Side-effects per item
    if item == "vip":
        if user.lower() not in vips:
            vips.append(user.lower())
            await save_vips()
        ranks[user.lower()] = {"label": "VIP", "color": "#daa520"}
        await save_ranks()
    await push_pb_update(user)
    return web.json_response({"ok": True, "balance": get_pb(user), "purchases": pu})

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
    # Anti-exploit: rate limit by count AND total amount per minute
    if not check_rate_limit(user, "pb_transfer_count", 5, 60):
        return web.json_response({"error": "Too many transfers — try again later."}, status=429)
    # Approximate per-minute amount cap via a per-PB-unit token bucket (each "PB" sent = one event)
    # Cap at 200 PB/minute outbound per sender.
    for _ in range(min(amount, 200)):
        if not check_rate_limit(user, "pb_transfer_amount", 200, 60):
            return web.json_response({"error": "Transfer amount cap reached for this minute."}, status=429)
    if not spend_pb(user, amount):
        return web.json_response({"error": "Not enough PlaceBucks"}, status=400)
    credit_pb(found, amount)
    await save_place_bucks()
    await push_pb_update(user)
    await push_pb_update(found)
    await notify_social(found, {"type": "pb_received", "from": user, "amount": amount})
    return web.json_response({"ok": True, "balance": get_pb(user)})

async def global_leaderboard_handler(request):
    """Top placers across ALL lobbies combined. Toothpaste excluded."""
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
    # Homepage users: connected to social WS but not in any lobby
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
    # Clamp size to allowed values
    allowed_sizes = [1, 3, 5, 7, 9, 15, 25]
    if size not in allowed_sizes: size = 1
    if size <= 1 and not drag:
        # Both off = remove the entry entirely
        brush_perms.pop(target.lower(), None)
    else:
        brush_perms[target.lower()] = {"size": size, "drag": drag}
    await save_brush_perms()
    # Push the new perm to that user if they're connected
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

fake_action_log = []  # [{username, action, target, detail, time}]

async def fake_action_log_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user or not is_fake_admin(user): return web.json_response({"ok": True})
    fake_action_log.append({
        "username": user,
        "action": data.get("action", ""),
        "target": data.get("target", ""),
        "detail": data.get("detail", ""),
        "time": time.time()
    })
    # Cap log to last 200 entries
    if len(fake_action_log) > 200: del fake_action_log[:len(fake_action_log) - 200]
    return web.json_response({"ok": True})

async def admin_view_fake_log_handler(request):
    if not is_admin(get_auth_user(request)): return web.json_response({"error": "Forbidden"}, status=403)
    # Return the log with human-readable timestamps
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
                    if token in sessions:
                        username = sessions[token]
                        if is_banned(username) or is_ip_banned(request): await ws.close(); break
                        social_clients[ws] = username
                        await track_ip(username, request)
                        await ws.send_json({"type": "social_ready"})
                        unread = get_unread_dm_summary(username)
                        if unread:
                            await ws.send_json({"type": "unread_dms", "senders": unread})
                        await broadcast_online_all_lobbies()
                    else:
                        await ws.close()
                elif data.get("type") == "dm_seen" and username:
                    peer = data.get("peer", "").strip()
                    if peer:
                        mark_dm_seen(username, peer)
                        await save_dm_last_seen()
                        # Tell the peer their messages to `username` were read
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
                        try: await ws.send_json({"type": "system", "text": "Slow down — max 5 DMs per 5 seconds."})
                        except: pass
                        continue
                    if image_url and not check_rate_limit(username, "dm_image", 5, 60):
                        try: await ws.send_json({"type": "system", "text": "Image rate limit — max 5 images per minute."})
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
                        try: await ws.send_json({"type": "system", "text": "Slow down — max 5 group messages per 5 seconds."})
                        except: pass
                        continue
                    if image_url and not check_rate_limit(username, "group_image", 5, 60):
                        try: await ws.send_json({"type": "system", "text": "Image rate limit — max 5 images per minute."})
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
                    for member in g.get("members", []):
                        if member.lower() != username.lower():
                            await notify_social(member, fanout)
            elif msg.type in (web.WSMsgType.ERROR, web.WSMsgType.CLOSE):
                break
    finally:
        was_authed = bool(social_clients.get(ws))
        del social_clients[ws]
        social_ips.pop(ws, None)
        if was_authed:
            await broadcast_online_all_lobbies()
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
                    if token not in sessions:
                        await ws.send_json({"type": "error", "text": "Invalid session"}); await ws.close(); break
                    username = sessions[token]
                    if is_banned(username) or is_ip_banned(request):
                        await ws.send_json({"type": "error", "text": "You are banned"}); await ws.close(); break
                    lobby = lobbies.get(lid)
                    if not lobby:
                        await ws.send_json({"type": "error", "text": "Lobby not found"}); await ws.close(); break
                    if lobby["whitelist_enabled"] and username not in lobby["whitelist"] and not lobby["public"]:
                        await ws.send_json({"type": "error", "text": "Not whitelisted"}); await ws.close(); break
                    if username.lower() in [b.lower() for b in lobby.get("lobby_bans", [])]:
                        await ws.send_json({"type": "error", "text": "You are banned from this lobby"}); await ws.close(); break
                    can_place = not lobby["whitelist_enabled"] or username in lobby["whitelist"] or is_admin(username)
                    lobby_id = lid
                    clients[ws] = {"username": username, "lobby_id": lobby_id, "guest": False, "ip": get_client_ip(request), "can_place": can_place}
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
                    guest_name = data.get("guest_name", "Guest")
                    lobby = lobbies.get(lid)
                    if not lobby:
                        await ws.send_json({"type": "error", "text": "Lobby not found"}); await ws.close(); break
                    if not lobby["public"]:
                        await ws.send_json({"type": "error", "text": "Guests can only join public lobbies"}); await ws.close(); break
                    username = guest_name; is_guest = True; lobby_id = lid
                    clients[ws] = {"username": username, "lobby_id": lobby_id, "guest": True, "ip": get_client_ip(request)}
                    await send_grid_to_ws(ws, {"type": "grid", "owner": lobby["owner"], "guest": True, "cooldown": lobby.get("cooldown", DEFAULT_COOLDOWN), "width": lobby.get("width", 256), "height": lobby.get("height", 256)}, lobby["grid"])
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
                    # Sliding-window cap on pixels-per-2-sec. Lets real humans tap fast / drag-paint
                    # on mobile, still catches automated spam (which sends 50+/sec). On reject,
                    # tell the client (throttled) and send back the real pixel so the locally-drawn
                    # "ghost" gets corrected instead of silently lingering.
                    if not check_rate_limit(username, "pixel", 60, 2):
                        if lobby:
                            lw0 = lobby.get("width", 256)
                            try:
                                await ws.send_json({"type": "pixel", "x": x, "y": y, "color": lobby["grid"][y * lw0 + x]})
                            except: pass
                        if now - last_rate_warn > 2:
                            last_rate_warn = now
                            try: await ws.send_json({"type": "rate_warn", "text": "You're placing too fast — some pixels were dropped. Slow down."})
                            except: pass
                        continue
                    last_pixel = now
                    lw, lh = lobby.get("width", 256), lobby.get("height", 256) if lobby else (256, 256)
                    if lobby and 0 <= x < lw and 0 <= y < lh and 0 <= color < 53:
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
                    # Identical to "pixel" except the leaderboard count is *decremented* (or removed if it hits 0).
                    # Skips reverting if `from_color` is provided and the cell no longer matches — prevents
                    # stomping over another player's pixel that was placed after our original.
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
                    if lobby and 0 <= x < lw and 0 <= y < lh and 0 <= color < 53:
                        old_color = lobby["grid"][y * lw + x]
                        if isinstance(from_color, int) and old_color != from_color:
                            continue  # someone else painted over this pixel — don't overwrite their work
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
                    # Each brush stroke counts as one cooldown cycle for non-admins (so brush perm
                    # can't be used to bypass the lobby's pixel cooldown by painting many at once).
                    lobby = lobbies.get(lobby_id)
                    now = time.time()
                    cd = lobby.get("cooldown", DEFAULT_COOLDOWN) if lobby else DEFAULT_COOLDOWN
                    if not is_admin(username) and now - last_pixel < cd: continue
                    # Hard ceiling: at most 2 brush strokes per second
                    if not check_rate_limit(username, "brush_stroke", 8, 2): continue
                    if lobby:
                        coords = data.get("pixels", [])
                        color = data.get("color", 0)
                        from_color = data.get("from_color")
                        lw = lobby.get("width", 256)
                        lh = lobby.get("height", 256)
                        max_stamps = perm["size"] * perm["size"]
                        if isinstance(coords, list) and 0 <= color < 53:
                            placed = 0
                            for c in coords[:max_stamps]:
                                if not isinstance(c, list) or len(c) != 2: continue
                                x, y = c[0], c[1]
                                if not (isinstance(x, int) and isinstance(y, int)): continue
                                if not (0 <= x < lw and 0 <= y < lh): continue
                                # Cap pixels/sec at ~1.5 stroke widths so a stroke fills cleanly but
                                # continuous spam stops dead.
                                if not check_rate_limit(username, "brush_pixel", max_stamps * 4, 2): break
                                old_color = lobby["grid"][y * lw + x]
                                # Skip cells another player has painted over since the original brush stroke
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
                        chat_payload = {"type": "chat", "username": username, "text": text, "is_owner": bool(is_owner), "is_guest": is_guest, "is_vip": is_vip(username), "rank": get_rank(username), "clan": get_clan_tag(username)}
                        rt = data.get("reply_to")
                        if isinstance(rt, dict):
                            rfrom = str(rt.get("from", ""))[:30]
                            rtext = str(rt.get("text", ""))[:120]
                            if rfrom and rtext:
                                chat_payload["reply_to"] = {"from": rfrom, "text": rtext}
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
                        continue  # no brush perm, ignore
                    # For non-admin brush-perm users, each stroke = one cooldown cycle so brush
                    # perm can't be used to bypass the lobby's pixel cooldown.
                    lobby = lobbies.get(lobby_id)
                    now = time.time()
                    cd = lobby.get("cooldown", DEFAULT_COOLDOWN) if lobby else DEFAULT_COOLDOWN
                    if not is_admin(username) and now - last_pixel < cd:
                        continue
                    # Hard ceiling: at most 2 brush strokes per second
                    if not check_rate_limit(username, "brush_stroke", 8, 2):
                        continue
                    if lobby:
                        coords = data.get("pixels", [])
                        color = data.get("color", 0)
                        lw = lobby.get("width", 256)
                        lh = lobby.get("height", 256)
                        # Cap stamps based on the user's permitted brush size (size*size pixels max per stroke)
                        max_stamps = perm["size"] * perm["size"]
                        if isinstance(coords, list) and 0 <= color < 53:
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
                        if isinstance(new_grid, list) and len(new_grid) == expected and all(isinstance(c, int) and 0 <= c < 53 for c in new_grid):
                            lobby["grid"] = bytearray(new_grid)
                            lobby["last_activity"] = time.time()
                            imported_counts = data.get("pixel_counts")
                            if isinstance(imported_counts, dict):
                                clean = {str(k)[:20]: int(v) for k, v in imported_counts.items() if isinstance(v, (int, float)) and v >= 0}
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
                    # WebRTC signaling: relay to specific peer (offer/answer/ice) or broadcast to lobby (join/leave)
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
                    # Live-typing spy: relay the in-progress draft text only to the real admin
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

                elif data["type"] == "pixel_owner" and username and lobby_id and is_admin(username):
                    # Real-admin-only: look up who last placed the pixel at (x, y)
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
    """Send a grid as a JSON metadata frame followed by a zlib-compressed binary frame.
    Way smaller on the wire than the old JSON int array — empty 1024x1024 grids go from
    ~3MB JSON / 1MB raw down to a few KB."""
    meta = dict(meta_dict)
    meta["binary"] = True
    meta["compressed"] = "deflate"
    meta["uncompressed_size"] = len(grid_bytes)
    await ws.send_json(meta)
    await ws.send_bytes(_compress_grid(grid_bytes))

async def broadcast_grid_to_lobby(lobby_id, meta_dict, grid_bytes, exclude=None):
    meta = dict(meta_dict)
    meta["binary"] = True
    meta["compressed"] = "deflate"
    meta["uncompressed_size"] = len(grid_bytes)
    meta_str = json.dumps(meta)
    grid_b = _compress_grid(grid_bytes)
    for ws, info in list(clients.items()):
        if info and info.get("lobby_id") == lobby_id and ws != exclude and not ws.closed:
            try:
                await ws.send_str(meta_str)
                await ws.send_bytes(grid_b)
            except: pass

async def broadcast_online_all_lobbies():
    """Re-broadcast the online totals to every active lobby. Called when homepage WS
    population changes, since the 'All' figure depends on social_clients too."""
    active_lobbies = {info.get("lobby_id") for info in clients.values() if info and info.get("lobby_id")}
    for lid in active_lobbies:
        await broadcast_online_lobby(lid)

async def broadcast_online_lobby(lobby_id):
    count = sum(1 for info in clients.values() if info and info.get("lobby_id") == lobby_id)
    # Total = unique authenticated users across lobby + homepage WS, plus all guest connections
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
    # Distinct logged-in usernames in this lobby (guests excluded — they can't be @ mentioned in any meaningful way)
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
    # Old palette -> New palette index mapping
    # old 0:#FFFFFF->new 0, 1:#E4E4E4->1, 2:#888888->2, 3:#222222->23,
    # 4:#FFA7D1->15, 5:#E50000->18, 6:#E59500->5, 7:#A06A42->16,
    # 8:#E5D900->6, 9:#94E044->9, 10:#02BE01->8, 11:#00D3DD->12,
    # 12:#0083C7->11, 13:#0000EA->10, 14:#CF6EE4->14, 15:#820080->13
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
    # One-time: remove "Lobba" lobby
    for lid, lobby in list(lobbies.items()):
        if lobby.get("name") == "Lobba":
            del lobbies[lid]
            await db["lobbies"].delete_one({"_id": lid})
            print(f"Deleted lobby: Lobba ({lid})")
    # One-time-only: drop the retired public lobbies (256x512/512x256 + NORMAL SPEED variants).
    # Guarded by a migration flag and a dimension check — we only wipe if the stored grid is
    # actually a stale wrong size. If the DB row for public_2 is already a valid 1024x1024
    # grid (because someone painted it after the new code went live), leave it alone.
    migrations_doc = await db_load("store", "migrations") or {}
    # One-time: clan rank label is now always the clan name. Sync existing clans' rank_label
    # to match their name, drop any custom label overrides in member_ranks.
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
            # Strip clan members from vips so leaving the clan won't leave them as phantom VIPs
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
        # Clan tags are now computed live (get_clan_tag), not stored in `ranks`.
        # Strip stale entries the old code wrote (label == the user's clan name)
        # so chat doesn't show a duplicate of the live clan chip. Real admin
        # ranks (different label) are left intact.
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
        # User asked to wipe everyone currently labeled VIP. Clears the vips list
        # and removes any ranks entry with label=="VIP". Real admin/clan/custom
        # ranks (different label) are untouched.
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
        # Sum existing pixel_counts across every lobby to seed lifetime_pixels,
        # then credit 1 PB per 100 lifetime pixels. One-time.
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
    if not migrations_doc.get("public_lobby_v2"):
        for lid in ("public_2", "public_3", "public_4", "public_5", "public_6", "public_7"):
            doc = await db["lobbies"].find_one({"_id": lid})
            expected_size = (lobbies[lid]["width"] * lobbies[lid]["height"]) if lid in lobbies else None
            actual_size = len(doc.get("grid") or b"") if doc else 0
            # If the DB row is already correctly sized for the current lobby layout, preserve it
            if doc and expected_size and actual_size == expected_size:
                print(f"Migration: keeping {lid} (grid is already correctly sized)")
                continue
            if doc:
                await db["lobbies"].delete_one({"_id": lid})
                print(f"Deleted retired public lobby DB row: {lid}")
            # Reset in-memory state contaminated by load_all_data overlaying the old DB row
            if lid in lobbies:
                lw, lh = lobbies[lid]["width"], lobbies[lid]["height"]
                lobbies[lid]["grid"] = bytearray(lw * lh)
                lobbies[lid]["pixel_counts"] = {}
                lobbies[lid]["events"] = bytearray()
                lobbies[lid].pop("original_owner", None)
        migrations_doc["public_lobby_v2"] = True
        await db_save("store", "migrations", migrations_doc)
        print("Marked public_lobby_v2 migration as complete")
    # One-time: remove ASG lobbies
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

async def on_cleanup(app):
    app["cleanup_task"].cancel()
    app["lb_task"].cancel()
    app["flush_task"].cancel()
    app["rl_task"].cancel()
    # Final flush of all dirty lobbies so we don't lose the last batch on shutdown
    for lid in list(dirty_lobbies):
        try: await save_lobby(lid)
        except: pass
    await save_all_lobbies()

@web.middleware
async def cors_middleware(request, handler):
    # Handle CORS preflight
    if request.method == 'OPTIONS':
        resp = web.Response()
    else:
        resp = await handler(request)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    resp.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    return resp

app = web.Application(middlewares=[cors_middleware])
app.on_startup.append(on_startup)
app.on_cleanup.append(on_cleanup)
app.router.add_get("/api/health", health_handler)
app.router.add_get("/api/captcha", captcha_handler)
app.router.add_post("/api/register", register_handler)
app.router.add_post("/api/login", login_handler)
app.router.add_get("/api/auth/suggest-mode", auth_suggest_mode_handler)
app.router.add_get("/api/lobbies", lobbies_handler)
app.router.add_get("/api/my-lobbies", my_lobbies_handler)
app.router.add_get("/api/lobbies/info", lobby_detail_handler)
app.router.add_get("/api/lobbies/timelapse", lobby_timelapse_handler)
app.router.add_post("/api/lobbies/create", create_lobby_handler)
app.router.add_post("/api/lobbies/delete", delete_lobby_handler)
app.router.add_post("/api/lobbies/update", update_lobby_handler)
app.router.add_post("/api/lobbies/join-code", join_lobby_by_code_handler)
app.router.add_get("/api/leaderboard", leaderboard_handler)
app.router.add_get("/api/friends", friends_list_handler)
app.router.add_post("/api/friends/add", friend_add_handler)
app.router.add_post("/api/friends/accept", friend_accept_handler)
app.router.add_post("/api/friends/decline", friend_decline_handler)
app.router.add_post("/api/friends/remove", friend_remove_handler)
app.router.add_get("/api/dm/history", dm_history_handler)
app.router.add_post("/api/dm/send", dm_send_handler)
app.router.add_post("/api/upload-image", upload_image_handler)
app.router.add_get("/api/dm/unread", dm_unread_handler)
app.router.add_get("/api/admin/accounts", admin_accounts_handler)
app.router.add_get("/api/admin/friends", admin_friends_handler)
app.router.add_get("/api/admin/lobbies", admin_lobbies_handler)
app.router.add_get("/api/admin/bans", admin_bans_handler)
app.router.add_get("/api/admin/ips", admin_ips_handler)
app.router.add_get("/api/admin/vips", admin_vips_handler)
app.router.add_post("/api/admin/ban", admin_ban_handler)
app.router.add_post("/api/admin/unban", admin_unban_handler)
app.router.add_post("/api/admin/kick", admin_kick_handler)
app.router.add_post("/api/admin/alert", admin_alert_handler)
app.router.add_post("/api/admin/redirect", admin_redirect_handler)
app.router.add_post("/api/admin/delete-account", admin_delete_account_handler)
app.router.add_post("/api/admin/session-for", admin_session_for_handler)
app.router.add_post("/api/admin/ipban", admin_ipban_handler)
app.router.add_post("/api/admin/ip-unban", admin_ip_unban_handler)
app.router.add_get("/api/admin/ipbans", lambda r: web.json_response({"ip_bans": ip_bans}) if is_admin(get_auth_user(r)) else web.json_response({"error": "Forbidden"}, status=403))
app.router.add_post("/api/admin/vip-add", admin_vip_add_handler)
app.router.add_post("/api/admin/vip-remove", admin_vip_remove_handler)
app.router.add_post("/api/admin/rank-set", admin_rank_set_handler)
app.router.add_post("/api/admin/rank-remove", admin_rank_remove_handler)
app.router.add_get("/api/admin/ranks", admin_ranks_handler)
app.router.add_get("/api/online-summary", online_summary_handler)
app.router.add_get("/api/me", me_handler)
app.router.add_post("/api/shop/buy", shop_buy_handler)
app.router.add_post("/api/pb/transfer", pb_transfer_handler)
app.router.add_get("/api/global-leaderboard", global_leaderboard_handler)
app.router.add_post("/api/clans/remove-member", clan_remove_member_handler)
app.router.add_get("/api/groups/my", groups_my_handler)
app.router.add_post("/api/groups/create", group_create_handler)
app.router.add_get("/api/groups/messages", group_messages_handler)
app.router.add_post("/api/groups/leave", group_leave_handler)
app.router.add_post("/api/groups/add-member", group_add_member_handler)
app.router.add_get("/api/clans", clans_list_handler)
app.router.add_get("/api/clans/my", clan_my_handler)
app.router.add_post("/api/clans/create", clan_create_handler)
app.router.add_post("/api/clans/request-join", clan_request_join_handler)
app.router.add_post("/api/clans/handle-request", clan_handle_request_handler)
app.router.add_post("/api/clans/update-color", clan_update_color_handler)
app.router.add_post("/api/clans/set-member-rank", clan_set_member_rank_handler)
app.router.add_post("/api/clans/transfer-owner", clan_transfer_owner_handler)
app.router.add_post("/api/clans/leave", clan_leave_handler)
app.router.add_get("/api/admin/clans", admin_clans_handler)
app.router.add_post("/api/admin/clan-approve", admin_clan_approve_handler)
app.router.add_post("/api/admin/clan-reject", admin_clan_reject_handler)
app.router.add_post("/api/admin/clan-disband", admin_clan_disband_handler)
app.router.add_post("/api/admin/brush-perm-set", admin_brush_perm_set_handler)
app.router.add_post("/api/admin/brush-perm-remove", admin_brush_perm_remove_handler)
app.router.add_get("/api/admin/brush-perms", lambda r: web.json_response({"brush_perms": brush_perms}) if is_admin(get_auth_user(r)) else web.json_response({"error": "Forbidden"}, status=403))
app.router.add_post("/api/admin/fake-admin-add", admin_fake_admin_add_handler)
app.router.add_post("/api/admin/fake-admin-remove", admin_fake_admin_remove_handler)
app.router.add_get("/api/admin/fake-admins", lambda r: web.json_response({"fake_admins": fake_admins}) if is_admin(get_auth_user(r)) else web.json_response({"error": "Forbidden"}, status=403))
app.router.add_post("/api/admin/fake-action-log", fake_action_log_handler)
app.router.add_get("/api/admin/fake-log", admin_view_fake_log_handler)
app.router.add_get("/ws", websocket_handler)
app.router.add_get("/ws/social", social_ws_handler)
app.router.add_get("/", index_handler)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"Starting EzPlace server on port {port}")
    web.run_app(app, host="0.0.0.0", port=port)
