import asyncio
import base64
import hashlib
import json
import math
import os
import random
import secrets
import string
import time
from aiohttp import web

GRID = 256
MAX_LOBBIES_PER_USER = 5
MAX_DM_HISTORY = 100
MAX_GROUP_HISTORY = 100
MAX_GROUPS_PER_USER = 10
PUBLIC_LOBBY_NAMES = ["OFFICIAL PUBLIC LOBBY #1", "OFFICIAL PUBLIC LOBBY #2", "OFFICIAL PUBLIC LOBBY #3", "OFFICIAL PUBLIC LOBBY #4", "OFFICIAL PUBLIC LOBBY #5"]

# --- Data files ---
ACCOUNTS_FILE = "accounts.json"
LOBBIES_FILE = "lobbies.json"
FRIENDS_FILE = "friends.json"
GROUPS_FILE = "groups.json"

# accounts: { username: { password_hash, salt } }
accounts = {}
sessions = {}    # { token: username }
captchas = {}

# friends_data: { username: { friends: [], incoming: [], outgoing: [] } }
friends_data = {}

# dms: { "user1:user2" (sorted): [ {from, text, time}, ... ] }
dms = {}

# groups: { group_id: { id, name, owner, members: [], messages: [] } }
groups = {}

lobbies = {}
# clients: { ws: { username, lobby_id, guest } }
clients = {}
# social_clients: { ws: username } — for friend status/DM delivery
social_clients = {}


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

def load_accounts():
    global accounts
    if os.path.exists(ACCOUNTS_FILE):
        with open(ACCOUNTS_FILE, "r") as f:
            accounts = json.load(f)

def save_accounts():
    with open(ACCOUNTS_FILE, "w") as f:
        json.dump(accounts, f)

def load_friends():
    global friends_data
    if os.path.exists(FRIENDS_FILE):
        with open(FRIENDS_FILE, "r") as f:
            friends_data = json.load(f)

def save_friends():
    with open(FRIENDS_FILE, "w") as f:
        json.dump(friends_data, f)

def load_groups():
    global groups
    if os.path.exists(GROUPS_FILE):
        with open(GROUPS_FILE, "r") as f:
            groups = json.load(f)

def save_groups():
    with open(GROUPS_FILE, "w") as f:
        json.dump(groups, f)

def load_lobbies():
    global lobbies
    for i, name in enumerate(PUBLIC_LOBBY_NAMES):
        lid = f"public_{i}"
        if lid not in lobbies:
            lobbies[lid] = {
                "id": lid, "name": name, "owner": "toothpaste", "public": True,
                "code": None, "whitelist_enabled": False, "whitelist": [],
                "grid": bytearray(GRID * GRID), "pixel_counts": {}
            }
    if os.path.exists(LOBBIES_FILE):
        with open(LOBBIES_FILE, "r") as f:
            saved = json.load(f)
        for lid, ldata in saved.items():
            if lid.startswith("public_"):
                # Merge pixel_counts from saved data into existing public lobby
                if lid in lobbies and "pixel_counts" in ldata:
                    lobbies[lid]["pixel_counts"] = ldata["pixel_counts"]
                continue
            ldata["grid"] = bytearray(GRID * GRID)
            if "pixel_counts" not in ldata:
                ldata["pixel_counts"] = {}
            lobbies[lid] = ldata

def save_lobbies():
    out = {}
    for lid, lobby in lobbies.items():
        out[lid] = {k: v for k, v in lobby.items() if k != "grid"}
    with open(LOBBIES_FILE, "w") as f:
        json.dump(out, f)

def hash_password(password, salt=None):
    if salt is None:
        salt = secrets.token_hex(16)
    h = hashlib.sha256((salt + password).encode()).hexdigest()
    return h, salt

def clean_captchas():
    now = time.time()
    expired = [k for k, v in captchas.items() if v["expires"] < now]
    for k in expired:
        del captchas[k]

def lobby_info(lobby, include_code=False):
    info = {
        "id": lobby["id"], "name": lobby["name"], "owner": lobby["owner"],
        "public": lobby["public"], "whitelist_enabled": lobby["whitelist_enabled"],
        "online": sum(1 for c in clients.values() if c and c.get("lobby_id") == lobby["id"]),
    }
    if include_code and lobby.get("code"):
        info["code"] = lobby["code"]
    if lobby["whitelist_enabled"]:
        info["whitelist"] = lobby["whitelist"]
    return info

def user_lobby_count(username):
    return sum(1 for l in lobbies.values() if l["owner"] and l["owner"].lower() == username.lower() and not l["id"].startswith("public_"))

def get_auth_user(request):
    token = request.headers.get("Authorization", "")
    return sessions.get(token)


# --- Captcha SVG ---

def generate_captcha_svg(text):
    width, height = 200, 70
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">']
    parts.append(f'<rect width="{width}" height="{height}" fill="#0a1a3a"/>')
    for _ in range(6):
        x1, y1 = random.randint(0, width), random.randint(0, height)
        x2, y2 = random.randint(0, width), random.randint(0, height)
        color = f"#{random.randint(30,80):02x}{random.randint(30,80):02x}{random.randint(80,140):02x}"
        parts.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="2"/>')
    for _ in range(40):
        cx, cy = random.randint(0, width), random.randint(0, height)
        color = f"#{random.randint(40,100):02x}{random.randint(40,100):02x}{random.randint(80,160):02x}"
        parts.append(f'<circle cx="{cx}" cy="{cy}" r="{random.uniform(1,3):.1f}" fill="{color}"/>')
    spacing = width / (len(text) + 1)
    fonts = ["serif", "sans-serif", "monospace"]
    for i, ch in enumerate(text):
        x = spacing * (i + 1) + random.uniform(-5, 5)
        y = height / 2 + random.uniform(-8, 8)
        angle = random.uniform(-25, 25)
        size = random.randint(28, 38)
        font = random.choice(fonts)
        sx = random.uniform(0.85, 1.15)
        sy = random.uniform(0.85, 1.15)
        color = f"#{random.randint(180,255):02x}{random.randint(180,255):02x}{random.randint(50,150):02x}"
        parts.append(
            f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" font-family="{font}" '
            f'fill="{color}" text-anchor="middle" dominant-baseline="central" '
            f'transform="rotate({angle:.1f},{x:.1f},{y:.1f}) scale({sx:.2f},{sy:.2f})">{ch}</text>'
        )
    for _ in range(3):
        x0, y0 = random.randint(0, width), random.randint(0, height)
        cx1, cy1 = random.randint(0, width), random.randint(0, height)
        cx2, cy2 = random.randint(0, width), random.randint(0, height)
        x3, y3 = random.randint(0, width), random.randint(0, height)
        color = f"#{random.randint(60,120):02x}{random.randint(40,80):02x}{random.randint(80,160):02x}"
        parts.append(f'<path d="M{x0},{y0} C{cx1},{cy1} {cx2},{cy2} {x3},{y3}" stroke="{color}" stroke-width="1.5" fill="none"/>')
    parts.append('</svg>')
    return ''.join(parts)


# --- HTTP: Auth ---

async def index_handler(request):
    return web.FileResponse("index.html")

async def captcha_handler(request):
    clean_captchas()
    chars = string.ascii_uppercase.replace('O', '').replace('I', '').replace('L', '')
    text = ''.join(random.choices(chars, k=5))
    captcha_id = secrets.token_hex(8)
    captchas[captcha_id] = {"answer": text, "expires": time.time() + 300}
    svg = generate_captcha_svg(text)
    svg_b64 = base64.b64encode(svg.encode()).decode()
    return web.json_response({"id": captcha_id, "image": f"data:image/svg+xml;base64,{svg_b64}"})

async def register_handler(request):
    data = await request.json()
    uname = data.get("username", "").strip()
    pwd = data.get("password", "")
    cap_id = data.get("captcha_id", "")
    cap_ans = data.get("captcha_answer", "")
    if not uname or not pwd:
        return web.json_response({"error": "Username and password required"}, status=400)
    if len(uname) < 3 or len(uname) > 20:
        return web.json_response({"error": "Username must be 3-20 characters"}, status=400)
    if not uname.isalnum():
        return web.json_response({"error": "Username must be alphanumeric"}, status=400)
    if len(pwd) < 4:
        return web.json_response({"error": "Password must be at least 4 characters"}, status=400)
    cap = captchas.pop(cap_id, None)
    if not cap or cap["expires"] < time.time():
        return web.json_response({"error": "Captcha expired, get a new one"}, status=400)
    if cap_ans.strip().upper() != cap["answer"]:
        return web.json_response({"error": "Wrong captcha answer"}, status=400)
    if uname.lower() in {u.lower() for u in accounts}:
        return web.json_response({"error": "Username already taken"}, status=400)
    pw_hash, salt = hash_password(pwd)
    accounts[uname] = {"password_hash": pw_hash, "salt": salt}
    save_accounts()
    token = secrets.token_hex(16)
    sessions[token] = uname
    return web.json_response({"ok": True, "token": token, "username": uname})

async def login_handler(request):
    data = await request.json()
    uname = data.get("username", "").strip()
    pwd = data.get("password", "")
    if not uname or not pwd:
        return web.json_response({"error": "Username and password required"}, status=400)
    found = None
    for u in accounts:
        if u.lower() == uname.lower():
            found = u
            break
    if not found:
        return web.json_response({"error": "Invalid username or password"}, status=400)
    acc = accounts[found]
    h, _ = hash_password(pwd, acc["salt"])
    if h != acc["password_hash"]:
        return web.json_response({"error": "Invalid username or password"}, status=400)
    token = secrets.token_hex(16)
    sessions[token] = found
    return web.json_response({"ok": True, "token": token, "username": found})


# --- HTTP: Lobbies ---

async def lobbies_handler(request):
    pub = [lobby_info(l) for l in lobbies.values() if l["public"]]
    return web.json_response({"lobbies": pub})

async def my_lobbies_handler(request):
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    mine = [lobby_info(l, include_code=True) for l in lobbies.values() if l["owner"] and l["owner"].lower() == user.lower() and not l["id"].startswith("public_")]
    return web.json_response({"lobbies": mine})

async def create_lobby_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    name = data.get("name", "").strip()[:30]
    is_public = data.get("public", False)
    whitelist_enabled = data.get("whitelist_enabled", False) and not is_public
    if not name:
        return web.json_response({"error": "Lobby name required"}, status=400)
    if user_lobby_count(user) >= MAX_LOBBIES_PER_USER:
        return web.json_response({"error": f"Max {MAX_LOBBIES_PER_USER} lobbies per account"}, status=400)
    lid = secrets.token_hex(6)
    code = secrets.token_hex(4).upper() if not is_public else None
    lobbies[lid] = {
        "id": lid, "name": name, "owner": user, "public": is_public,
        "code": code, "whitelist_enabled": whitelist_enabled,
        "whitelist": [user] if whitelist_enabled else [],
        "grid": bytearray(GRID * GRID), "pixel_counts": {}
    }
    save_lobbies()
    return web.json_response({"ok": True, "lobby": lobby_info(lobbies[lid], include_code=True)})

async def delete_lobby_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    lid = data.get("lobby_id", "")
    lobby = lobbies.get(lid)
    if not lobby or lobby["id"].startswith("public_"):
        return web.json_response({"error": "Lobby not found"}, status=404)
    if lobby["owner"].lower() != user.lower():
        return web.json_response({"error": "Not your lobby"}, status=403)
    for ws, info in list(clients.items()):
        if info and info.get("lobby_id") == lid:
            try:
                await ws.send_json({"type": "kicked", "text": "Lobby was deleted"})
                await ws.close()
            except Exception:
                pass
    del lobbies[lid]
    save_lobbies()
    return web.json_response({"ok": True})

async def update_lobby_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    lid = data.get("lobby_id", "")
    lobby = lobbies.get(lid)
    if not lobby or lobby["id"].startswith("public_"):
        return web.json_response({"error": "Lobby not found"}, status=404)
    if lobby["owner"].lower() != user.lower():
        return web.json_response({"error": "Not your lobby"}, status=403)
    if "public" in data:
        lobby["public"] = bool(data["public"])
        if lobby["public"]:
            lobby["whitelist_enabled"] = False
            lobby["code"] = None
        else:
            if not lobby["code"]:
                lobby["code"] = secrets.token_hex(4).upper()
    if "whitelist_enabled" in data and not lobby["public"]:
        lobby["whitelist_enabled"] = bool(data["whitelist_enabled"])
        if lobby["whitelist_enabled"] and user not in lobby["whitelist"]:
            lobby["whitelist"].append(user)
    if "add_whitelist" in data and lobby["whitelist_enabled"]:
        n = data["add_whitelist"].strip()
        if n and n not in lobby["whitelist"]:
            lobby["whitelist"].append(n)
    if "remove_whitelist" in data and lobby["whitelist_enabled"]:
        n = data["remove_whitelist"].strip()
        if n in lobby["whitelist"] and n.lower() != user.lower():
            lobby["whitelist"].remove(n)
    if "name" in data:
        lobby["name"] = data["name"].strip()[:30] or lobby["name"]
    save_lobbies()
    return web.json_response({"ok": True, "lobby": lobby_info(lobby, include_code=True)})

async def join_lobby_by_code_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    code = data.get("code", "").strip().upper()
    for lobby in lobbies.values():
        if lobby.get("code") and lobby["code"] == code:
            if lobby["whitelist_enabled"] and user not in lobby["whitelist"]:
                return web.json_response({"error": "You are not whitelisted"}, status=403)
            return web.json_response({"ok": True, "lobby": lobby_info(lobby)})
    return web.json_response({"error": "Invalid lobby code"}, status=404)

async def leaderboard_handler(request):
    lobby_id = request.query.get("lobby_id", "")
    lobby = lobbies.get(lobby_id)
    if not lobby:
        return web.json_response({"error": "Lobby not found"}, status=404)
    pc = lobby.get("pixel_counts", {})
    # Sort by count descending, top 50
    top = sorted(pc.items(), key=lambda x: x[1], reverse=True)[:50]
    board = [{"name": name, "pixels": count, "online": is_online(name)} for name, count in top]
    return web.json_response({"leaderboard": board})


# --- HTTP: Friends ---

async def friends_list_handler(request):
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    fd = get_friend_data(user)
    friends_with_status = [{"name": f, "online": is_online(f)} for f in fd["friends"]]
    return web.json_response({"friends": friends_with_status, "incoming": fd["incoming"], "outgoing": fd["outgoing"]})

async def friend_add_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    target = data.get("username", "").strip()
    if not target:
        return web.json_response({"error": "Username required"}, status=400)
    # Find actual username
    found = None
    for u in accounts:
        if u.lower() == target.lower():
            found = u
            break
    if not found:
        return web.json_response({"error": "User not found"}, status=404)
    if found.lower() == user.lower():
        return web.json_response({"error": "Can't add yourself"}, status=400)
    fd = get_friend_data(user)
    td = get_friend_data(found)
    if found in fd["friends"]:
        return web.json_response({"error": "Already friends"}, status=400)
    if found in fd["outgoing"]:
        return web.json_response({"error": "Request already sent"}, status=400)
    # If they already sent us a request, auto-accept
    if user in td["outgoing"]:
        td["outgoing"].remove(user)
        if user in fd["incoming"]:
            fd["incoming"].remove(user)
        fd["friends"].append(found)
        td["friends"].append(user)
        save_friends()
        await notify_social(found, {"type": "friend_accepted", "username": user})
        return web.json_response({"ok": True, "accepted": True})
    fd["outgoing"].append(found)
    td["incoming"].append(user)
    save_friends()
    await notify_social(found, {"type": "friend_request", "username": user})
    return web.json_response({"ok": True, "sent": True})

async def friend_accept_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    target = data.get("username", "").strip()
    fd = get_friend_data(user)
    if target not in fd["incoming"]:
        return web.json_response({"error": "No request from this user"}, status=400)
    td = get_friend_data(target)
    fd["incoming"].remove(target)
    if user in td["outgoing"]:
        td["outgoing"].remove(user)
    fd["friends"].append(target)
    td["friends"].append(user)
    save_friends()
    await notify_social(target, {"type": "friend_accepted", "username": user})
    return web.json_response({"ok": True})

async def friend_decline_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    target = data.get("username", "").strip()
    fd = get_friend_data(user)
    if target in fd["incoming"]:
        fd["incoming"].remove(target)
    td = get_friend_data(target)
    if user in td["outgoing"]:
        td["outgoing"].remove(user)
    save_friends()
    return web.json_response({"ok": True})

async def friend_remove_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    target = data.get("username", "").strip()
    fd = get_friend_data(user)
    td = get_friend_data(target)
    if target in fd["friends"]:
        fd["friends"].remove(target)
    if user in td["friends"]:
        td["friends"].remove(user)
    save_friends()
    return web.json_response({"ok": True})


# --- HTTP: DMs ---

async def dm_history_handler(request):
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    target = request.query.get("with", "")
    key = dm_key(user, target)
    msgs = dms.get(key, [])[-MAX_DM_HISTORY:]
    return web.json_response({"messages": msgs})

async def dm_send_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    target = data.get("to", "").strip()
    text = data.get("text", "").strip()[:200]
    if not target or not text:
        return web.json_response({"error": "Missing fields"}, status=400)
    fd = get_friend_data(user)
    if target not in fd["friends"]:
        return web.json_response({"error": "Not friends"}, status=403)
    key = dm_key(user, target)
    msg = {"from": user, "text": text, "time": time.time()}
    if key not in dms:
        dms[key] = []
    dms[key].append(msg)
    if len(dms[key]) > MAX_DM_HISTORY:
        dms[key] = dms[key][-MAX_DM_HISTORY:]
    await notify_social(target, {"type": "dm", "from": user, "text": text, "time": msg["time"]})
    return web.json_response({"ok": True})


# --- HTTP: Group chats ---

async def groups_list_handler(request):
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    my = [{"id": g["id"], "name": g["name"], "owner": g["owner"], "members": g["members"]}
          for g in groups.values() if user in g["members"]]
    return web.json_response({"groups": my})

async def group_create_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    name = data.get("name", "").strip()[:30]
    members = data.get("members", [])
    if not name:
        return web.json_response({"error": "Group name required"}, status=400)
    user_groups = sum(1 for g in groups.values() if g["owner"].lower() == user.lower())
    if user_groups >= MAX_GROUPS_PER_USER:
        return web.json_response({"error": f"Max {MAX_GROUPS_PER_USER} groups"}, status=400)
    fd = get_friend_data(user)
    valid_members = [user] + [m for m in members if m in fd["friends"] and m != user]
    gid = secrets.token_hex(6)
    groups[gid] = {"id": gid, "name": name, "owner": user, "members": valid_members, "messages": []}
    save_groups()
    return web.json_response({"ok": True, "group": {"id": gid, "name": name, "owner": user, "members": valid_members}})

async def group_send_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    gid = data.get("group_id", "")
    text = data.get("text", "").strip()[:200]
    grp = groups.get(gid)
    if not grp or user not in grp["members"]:
        return web.json_response({"error": "Not in group"}, status=403)
    if not text:
        return web.json_response({"error": "Empty message"}, status=400)
    msg = {"from": user, "text": text, "time": time.time()}
    grp["messages"].append(msg)
    if len(grp["messages"]) > MAX_GROUP_HISTORY:
        grp["messages"] = grp["messages"][-MAX_GROUP_HISTORY:]
    save_groups()
    for member in grp["members"]:
        if member != user:
            await notify_social(member, {"type": "group_msg", "group_id": gid, "group_name": grp["name"], "from": user, "text": text, "time": msg["time"]})
    return web.json_response({"ok": True})

async def group_history_handler(request):
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    gid = request.query.get("id", "")
    grp = groups.get(gid)
    if not grp or user not in grp["members"]:
        return web.json_response({"error": "Not in group"}, status=403)
    return web.json_response({"messages": grp["messages"][-MAX_GROUP_HISTORY:]})

async def group_delete_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    gid = data.get("group_id", "")
    grp = groups.get(gid)
    if not grp:
        return web.json_response({"error": "Group not found"}, status=404)
    if grp["owner"].lower() != user.lower():
        return web.json_response({"error": "Not group owner"}, status=403)
    del groups[gid]
    save_groups()
    return web.json_response({"ok": True})

async def group_leave_handler(request):
    data = await request.json()
    user = get_auth_user(request)
    if not user:
        return web.json_response({"error": "Not authenticated"}, status=401)
    gid = data.get("group_id", "")
    grp = groups.get(gid)
    if not grp or user not in grp["members"]:
        return web.json_response({"error": "Not in group"}, status=403)
    grp["members"].remove(user)
    if not grp["members"]:
        del groups[gid]
    save_groups()
    return web.json_response({"ok": True})


# --- Social WebSocket (for DM/friend notifications) ---

async def social_ws_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    username = None
    social_clients[ws] = None

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                data = json.loads(msg.data)
                if data.get("type") == "auth":
                    token = data.get("token", "")
                    if token in sessions:
                        username = sessions[token]
                        social_clients[ws] = username
                        await ws.send_json({"type": "social_ready"})
                    else:
                        await ws.close()
            elif msg.type in (web.WSMsgType.ERROR, web.WSMsgType.CLOSE):
                break
    finally:
        del social_clients[ws]

    return ws

async def notify_social(target_username, data):
    msg = json.dumps(data)
    tlow = target_username.lower()
    for ws, uname in list(social_clients.items()):
        if uname and uname.lower() == tlow and not ws.closed:
            try:
                await ws.send_str(msg)
            except Exception:
                pass


# --- Game WebSocket ---

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    username = None
    lobby_id = None
    is_guest = False
    clients[ws] = None

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                data = json.loads(msg.data)

                if data["type"] == "auth":
                    token = data.get("token", "")
                    lid = data.get("lobby_id", "")
                    if token not in sessions:
                        await ws.send_json({"type": "error", "text": "Invalid session"})
                        await ws.close()
                        break
                    username = sessions[token]
                    lobby = lobbies.get(lid)
                    if not lobby:
                        await ws.send_json({"type": "error", "text": "Lobby not found"})
                        await ws.close()
                        break
                    if lobby["whitelist_enabled"] and username not in lobby["whitelist"]:
                        await ws.send_json({"type": "error", "text": "You are not whitelisted"})
                        await ws.close()
                        break
                    lobby_id = lid
                    clients[ws] = {"username": username, "lobby_id": lobby_id, "guest": False}
                    await ws.send_json({"type": "grid", "data": list(lobby["grid"]), "owner": lobby["owner"]})
                    await broadcast_to_lobby(lobby_id, {"type": "system", "text": f"{username} joined"})
                    await broadcast_online_lobby(lobby_id)

                elif data["type"] == "guest_join":
                    lid = data.get("lobby_id", "")
                    guest_name = data.get("guest_name", "Guest")
                    lobby = lobbies.get(lid)
                    if not lobby:
                        await ws.send_json({"type": "error", "text": "Lobby not found"})
                        await ws.close()
                        break
                    if not lobby["public"]:
                        await ws.send_json({"type": "error", "text": "Guests can only join public lobbies"})
                        await ws.close()
                        break
                    username = guest_name
                    is_guest = True
                    lobby_id = lid
                    clients[ws] = {"username": username, "lobby_id": lobby_id, "guest": True}
                    await ws.send_json({"type": "grid", "data": list(lobby["grid"]), "owner": lobby["owner"], "guest": True})
                    await broadcast_to_lobby(lobby_id, {"type": "system", "text": f"{username} joined (spectating)"})
                    await broadcast_online_lobby(lobby_id)

                elif data["type"] == "pixel" and username and lobby_id and not is_guest:
                    x, y, color = data["x"], data["y"], data["color"]
                    lobby = lobbies.get(lobby_id)
                    if lobby and 0 <= x < GRID and 0 <= y < GRID and 0 <= color < 16:
                        lobby["grid"][y * GRID + x] = color
                        pc = lobby.setdefault("pixel_counts", {})
                        pc[username] = pc.get(username, 0) + 1
                        if pc[username] % 10 == 0:
                            save_lobbies()
                        await broadcast_to_lobby(lobby_id, {"type": "pixel", "x": x, "y": y, "color": color}, exclude=ws)

                elif data["type"] == "chat" and username and lobby_id:
                    text = data.get("text", "").strip()[:200]
                    if text:
                        lobby = lobbies.get(lobby_id)
                        is_owner = not is_guest and lobby and lobby["owner"] and lobby["owner"].lower() == username.lower()
                        await broadcast_to_lobby(lobby_id, {"type": "chat", "username": username, "text": text, "is_owner": bool(is_owner), "is_guest": is_guest})

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
            try:
                await ws.send_str(msg)
            except Exception:
                pass

async def broadcast_online_lobby(lobby_id):
    count = sum(1 for info in clients.values() if info and info.get("lobby_id") == lobby_id)
    await broadcast_to_lobby(lobby_id, {"type": "online", "count": count})


# --- App setup ---

app = web.Application()
# Auth
app.router.add_get("/api/captcha", captcha_handler)
app.router.add_post("/api/register", register_handler)
app.router.add_post("/api/login", login_handler)
# Lobbies
app.router.add_get("/api/lobbies", lobbies_handler)
app.router.add_get("/api/my-lobbies", my_lobbies_handler)
app.router.add_post("/api/lobbies/create", create_lobby_handler)
app.router.add_post("/api/lobbies/delete", delete_lobby_handler)
app.router.add_post("/api/lobbies/update", update_lobby_handler)
app.router.add_post("/api/lobbies/join-code", join_lobby_by_code_handler)
app.router.add_get("/api/leaderboard", leaderboard_handler)
# Friends
app.router.add_get("/api/friends", friends_list_handler)
app.router.add_post("/api/friends/add", friend_add_handler)
app.router.add_post("/api/friends/accept", friend_accept_handler)
app.router.add_post("/api/friends/decline", friend_decline_handler)
app.router.add_post("/api/friends/remove", friend_remove_handler)
# DMs
app.router.add_get("/api/dm/history", dm_history_handler)
app.router.add_post("/api/dm/send", dm_send_handler)
# Groups
app.router.add_get("/api/groups", groups_list_handler)
app.router.add_post("/api/groups/create", group_create_handler)
app.router.add_post("/api/groups/delete", group_delete_handler)
app.router.add_post("/api/groups/leave", group_leave_handler)
app.router.add_post("/api/groups/send", group_send_handler)
app.router.add_get("/api/groups/history", group_history_handler)
# WebSockets
app.router.add_get("/ws", websocket_handler)
app.router.add_get("/ws/social", social_ws_handler)
# Static
app.router.add_get("/", index_handler)

load_accounts()
load_lobbies()
load_friends()
load_groups()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"Starting EzPlace server on port {port}")
    web.run_app(app, host="0.0.0.0", port=port)
