import asyncio
import base64
import hashlib
import json
import os
import random
import secrets
import string
import time
from aiohttp import web
import motor.motor_asyncio

MAX_LOBBIES_PER_USER = 5
MAX_DM_HISTORY = 100
VALID_SIZES = [(256, 256), (512, 512), (256, 512), (512, 256)]
PUBLIC_LOBBIES = [
    {"name": "24/7 CHAOS 256x256", "cooldown": 0, "width": 256, "height": 256},
    {"name": "24/7 CHAOS 512x512", "cooldown": 0, "width": 512, "height": 512},
    {"name": "24/7 CHAOS 256x512", "cooldown": 0, "width": 256, "height": 512},
    {"name": "24/7 CHAOS 512x256", "cooldown": 0, "width": 512, "height": 256},
    {"name": "24/7 NORMAL SPEED 256x256", "cooldown": 0.5, "width": 256, "height": 256},
    {"name": "24/7 NORMAL SPEED 512x512", "cooldown": 0.5, "width": 512, "height": 512},
    {"name": "24/7 NORMAL SPEED 256x512", "cooldown": 0.5, "width": 256, "height": 512},
    {"name": "24/7 NORMAL SPEED 512x256", "cooldown": 0.5, "width": 512, "height": 256},
]
DEFAULT_COOLDOWN = 0.5
MAX_COOLDOWN = 60
ADMIN_USER = "toothpaste"
LOBBY_TIMEOUT = 172800

MONGO_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017/ezplace")
mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
db = mongo_client.get_default_database() if "mongodb.net" in MONGO_URI else mongo_client["ezplace"]

accounts = {}
sessions = {}
captchas = {}
friends_data = {}
dms = {}
bans = []
ip_bans = []
vips = []
user_ips = {}
lobbies = {}
clients = {}
social_clients = {}
social_ips = {}

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
        "expires_in": max(0, LOBBY_TIMEOUT - (time.time() - lobby.get("last_activity", time.time()))) if not lobby["id"].startswith("public_") else None,
    }
    if include_code and lobby.get("code"):
        info["code"] = lobby["code"]
    if lobby["whitelist_enabled"]:
        info["whitelist"] = lobby["whitelist"]
    return info

def user_lobby_count(username):
    return sum(1 for l in lobbies.values() if l["owner"] and l["owner"].lower() == username.lower() and not l["id"].startswith("public_"))

def get_leaderboard_top10(lobby):
    pc = lobby.get("pixel_counts", {})
    top = sorted(pc.items(), key=lambda x: x[1], reverse=True)[:10]
    return [{"name": n, "pixels": c, "online": is_online(n)} for n, c in top]

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

async def save_user_ips():
    await db_save("store", "user_ips", user_ips)

async def save_lobby(lid):
    lobby = lobbies.get(lid)
    if not lobby:
        return
    data = {k: v for k, v in lobby.items() if k != "grid"}
    grid_data = list(lobby["grid"])
    await db["lobbies"].update_one({"_id": lid}, {"$set": {"meta": data, "grid": grid_data}}, upsert=True)

async def save_all_lobbies():
    for lid in lobbies:
        await save_lobby(lid)

async def delete_lobby_db(lid):
    await db["lobbies"].delete_one({"_id": lid})

async def save_dm(key):
    msgs = dms.get(key, [])
    await db_save("dms", key, msgs)

async def track_ip(username, request):
    ip = get_client_ip(request)
    if ip and username:
        user_ips[username] = ip
        await save_user_ips()

async def load_all_data():
    global accounts, friends_data, bans, ip_bans, vips, user_ips, dms

    accounts = await db_load("store", "accounts") or {}
    friends_data = await db_load("store", "friends") or {}
    bans = await db_load("store", "bans") or []
    ip_bans = await db_load("store", "ip_bans") or []
    vips = await db_load("store", "vips") or []
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

    async for doc in db["lobbies"].find():
        lid = doc["_id"]
        meta = doc.get("meta", {})
        grid_data = doc.get("grid")
        if lid.startswith("public_") and lid in lobbies:
            expected_size = lobbies[lid]["width"] * lobbies[lid]["height"]
            if "pixel_counts" in meta:
                lobbies[lid]["pixel_counts"] = meta["pixel_counts"]
            if grid_data and len(grid_data) == expected_size:
                lobbies[lid]["grid"] = bytearray(grid_data)
        elif lid.startswith("public_") and lid not in lobbies:
            continue
        else:
            lw = meta.get("width", 256)
            lh = meta.get("height", 256)
            meta["grid"] = bytearray(grid_data) if grid_data else bytearray(lw * lh)
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

async def login_handler(request):
    data = await request.json()
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

async def create_lobby_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
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
    if not lobby or lid.startswith("public_"):
        return web.json_response({"error": "Not found"}, status=404)
    if lobby["owner"].lower() != user.lower():
        return web.json_response({"error": "Not yours"}, status=403)
    if "public" in data:
        lobby["public"] = bool(data["public"])
        if lobby["public"]: lobby["whitelist_enabled"] = False; lobby["code"] = None
        elif not lobby["code"]: lobby["code"] = secrets.token_hex(4).upper()
    if "whitelist_enabled" in data and not lobby["public"]:
        lobby["whitelist_enabled"] = bool(data["whitelist_enabled"])
        if lobby["whitelist_enabled"] and user not in lobby["whitelist"]: lobby["whitelist"].append(user)
    if "add_whitelist" in data and lobby["whitelist_enabled"]:
        n = data["add_whitelist"].strip()
        if n and n not in lobby["whitelist"]: lobby["whitelist"].append(n)
    if "remove_whitelist" in data and lobby["whitelist_enabled"]:
        n = data["remove_whitelist"].strip()
        if n in lobby["whitelist"] and n.lower() != user.lower(): lobby["whitelist"].remove(n)
    if "name" in data: lobby["name"] = data["name"].strip()[:30] or lobby["name"]
    await save_lobby(lid)
    return web.json_response({"ok": True, "lobby": lobby_info(lobby, True)})

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
    return web.json_response({"leaderboard": [{"name": n, "pixels": c, "online": is_online(n)} for n, c in top]})

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
    return web.json_response({"messages": dms.get(dm_key(user, target), [])[-MAX_DM_HISTORY:]})

async def dm_send_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user: return web.json_response({"error": "Not authenticated"}, status=401)
    target, text = data.get("to", "").strip(), data.get("text", "").strip()[:200]
    if not target or not text: return web.json_response({"error": "Missing fields"}, status=400)
    fd = get_friend_data(user)
    if target not in fd["friends"]: return web.json_response({"error": "Not friends"}, status=403)
    key = dm_key(user, target)
    msg = {"from": user, "text": text, "time": time.time()}
    dms.setdefault(key, []).append(msg)
    if len(dms[key]) > MAX_DM_HISTORY: dms[key] = dms[key][-MAX_DM_HISTORY:]
    await save_dm(key)
    await notify_social(target, {"type": "dm", "from": user, "text": text, "time": msg["time"]})
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
                    else:
                        await ws.close()
                elif data.get("type") == "dm" and username:
                    target = data.get("to", "").strip()
                    text = data.get("text", "").strip()[:200]
                    if target and text:
                        fd = get_friend_data(username)
                        if target in fd["friends"]:
                            key = dm_key(username, target)
                            m = {"from": username, "text": text, "time": time.time()}
                            dms.setdefault(key, []).append(m)
                            if len(dms[key]) > MAX_DM_HISTORY: dms[key] = dms[key][-MAX_DM_HISTORY:]
                            await save_dm(key)
                            await notify_social(target, {"type": "dm", "from": username, "text": text, "time": m["time"]})
            elif msg.type in (web.WSMsgType.ERROR, web.WSMsgType.CLOSE):
                break
    finally:
        del social_clients[ws]
        social_ips.pop(ws, None)
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
                    await ws.send_json({"type": "grid", "data": list(lobby["grid"]), "owner": lobby["owner"], "cooldown": lobby.get("cooldown", DEFAULT_COOLDOWN), "width": lobby.get("width", 256), "height": lobby.get("height", 256), "can_place": can_place})
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
                    await ws.send_json({"type": "grid", "data": list(lobby["grid"]), "owner": lobby["owner"], "guest": True, "cooldown": lobby.get("cooldown", DEFAULT_COOLDOWN), "width": lobby.get("width", 256), "height": lobby.get("height", 256)})
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
                    last_pixel = now
                    lw, lh = lobby.get("width", 256), lobby.get("height", 256) if lobby else (256, 256)
                    if lobby and 0 <= x < lw and 0 <= y < lh and 0 <= color < 32:
                        old_color = lobby["grid"][y * lw + x]
                        lobby["grid"][y * lw + x] = color
                        lobby["last_activity"] = now
                        if color != old_color:
                            pc = lobby.setdefault("pixel_counts", {})
                            pc[username] = pc.get(username, 0) + 1
                            if pc[username] % 10 == 0:
                                await save_lobby(lobby_id)
                        await broadcast_to_lobby(lobby_id, {"type": "pixel", "x": x, "y": y, "color": color}, exclude=ws)

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
                        await broadcast_to_lobby(lobby_id, {"type": "chat", "username": username, "text": text, "is_owner": bool(is_owner), "is_guest": is_guest, "is_vip": is_vip(username)})

                elif data["type"] == "lobby_kick" and username and lobby_id and not is_guest:
                    lobby = lobbies.get(lobby_id)
                    if lobby and lobby["owner"].lower() == username.lower():
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
                    if lobby and lobby["owner"].lower() == username.lower():
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

                elif data["type"] == "import_grid" and username and lobby_id and not is_guest:
                    lobby = lobbies.get(lobby_id)
                    if lobby and lobby["owner"].lower() == username.lower():
                        new_grid = data.get("grid", [])
                        lw = lobby.get("width", 256)
                        lh = lobby.get("height", 256)
                        expected = lw * lh
                        if isinstance(new_grid, list) and len(new_grid) == expected and all(isinstance(c, int) and 0 <= c < 32 for c in new_grid):
                            lobby["grid"] = bytearray(new_grid)
                            lobby["last_activity"] = time.time()
                            await save_lobby(lobby_id)
                            await broadcast_to_lobby(lobby_id, {"type": "grid", "data": list(lobby["grid"]), "owner": lobby["owner"], "cooldown": lobby.get("cooldown", DEFAULT_COOLDOWN), "width": lw, "height": lh})
                            await broadcast_to_lobby(lobby_id, {"type": "system", "text": f"Grid imported by {username}"})
                        else:
                            await ws.send_json({"type": "system", "text": f"Invalid grid data (expected {expected} pixels)"})

                elif data["type"] == "ping":
                    await ws.send_json({"type": "pong", "time": data.get("time", 0)})

            elif msg.type in (web.WSMsgType.ERROR, web.WSMsgType.CLOSE):
                break
    finally:
        del clients[ws]
        if username and lobby_id:
            await broadcast_to_lobby(lobby_id, {"type": "system", "text": f"{username} left"})
            await broadcast_online_lobby(lobby_id)
    return ws

async def broadcast_to_lobby(lobby_id, data, exclude=None):
    msg = json.dumps(data)
    for ws, info in list(clients.items()):
        if info and info.get("lobby_id") == lobby_id and ws != exclude and not ws.closed:
            try: await ws.send_str(msg)
            except: pass

async def broadcast_online_lobby(lobby_id):
    count = sum(1 for info in clients.values() if info and info.get("lobby_id") == lobby_id)
    total = sum(1 for info in clients.values() if info)
    await broadcast_to_lobby(lobby_id, {"type": "online", "count": count, "total": total})

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
            if now - lobby.get("last_activity", now) > LOBBY_TIMEOUT:
                to_delete.append(lid)
        for lid in to_delete:
            for ws, info in list(clients.items()):
                if info and info.get("lobby_id") == lid:
                    try: await ws.send_json({"type": "kicked", "text": "Lobby deleted (48hr inactivity)"}); await ws.close()
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
    # One-time: remove ASG lobbies
    for lid, lobby in list(lobbies.items()):
        if lid.startswith("public_"): continue
        if "ASG" in (lobby.get("name") or ""):
            del lobbies[lid]
            await db["lobbies"].delete_one({"_id": lid})
            print(f"Deleted ASG lobby: {lobby.get('name')} ({lid})")
    app["cleanup_task"] = asyncio.create_task(cleanup_inactive_lobbies(app))
    app["lb_task"] = asyncio.create_task(leaderboard_broadcast_loop(app))

async def on_cleanup(app):
    app["cleanup_task"].cancel()
    app["lb_task"].cancel()
    await save_all_lobbies()

app = web.Application()
app.on_startup.append(on_startup)
app.on_cleanup.append(on_cleanup)
app.router.add_get("/api/captcha", captcha_handler)
app.router.add_post("/api/register", register_handler)
app.router.add_post("/api/login", login_handler)
app.router.add_get("/api/lobbies", lobbies_handler)
app.router.add_get("/api/my-lobbies", my_lobbies_handler)
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
app.router.add_get("/api/admin/accounts", admin_accounts_handler)
app.router.add_get("/api/admin/friends", admin_friends_handler)
app.router.add_get("/api/admin/lobbies", admin_lobbies_handler)
app.router.add_get("/api/admin/bans", admin_bans_handler)
app.router.add_get("/api/admin/ips", admin_ips_handler)
app.router.add_get("/api/admin/vips", admin_vips_handler)
app.router.add_post("/api/admin/ban", admin_ban_handler)
app.router.add_post("/api/admin/unban", admin_unban_handler)
app.router.add_post("/api/admin/kick", admin_kick_handler)
app.router.add_post("/api/admin/ipban", admin_ipban_handler)
app.router.add_post("/api/admin/ip-unban", admin_ip_unban_handler)
app.router.add_get("/api/admin/ipbans", lambda r: web.json_response({"ip_bans": ip_bans}) if is_admin(get_auth_user(r)) else web.json_response({"error": "Forbidden"}, status=403))
app.router.add_post("/api/admin/vip-add", admin_vip_add_handler)
app.router.add_post("/api/admin/vip-remove", admin_vip_remove_handler)
app.router.add_get("/ws", websocket_handler)
app.router.add_get("/ws/social", social_ws_handler)
app.router.add_get("/", index_handler)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"Starting EzPlace server on port {port}")
    web.run_app(app, host="0.0.0.0", port=port)
