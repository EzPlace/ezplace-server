/* EzWorld: the 3D voxel world. Loaded only when somebody opens EzWorld from the menu.
   The server decides everything that matters (which blocks exist, who may change them, where you may stand);
   this file draws the world, moves your camera and sends clicks. */
(function () {
  'use strict';
  var HMAX = 400, WMIN = -1000, WMAX = 1000, GROUND = 40;
  var REACH = 6, HALF = 0.3, PH = 1.8, EYE = 1.62;
  var WALK = 4.4, SPRINT = 6.9, JUMP_V = 8.6, GRAV = 28, TERMINAL = 55;
  var RDIST = 7;                       // chunks drawn in every direction
  var FOV = 75 * Math.PI / 180;

  // ------------------------------------------------------------------ world data (blocks that differ from the flat ground)
  var chunks = new Map();              // key -> chunk
  function ckey(cx, cz) { return (cx + 64) * 128 + (cz + 64); }       // chunk coordinates run from -63 to 62
  function newChunk(cx, cz) { return { cx: cx, cz: cz, edits: new Map(), loaded: false, reqAt: 0, minY: GROUND, maxY: GROUND, mesh: null, dirty: false }; }
  function getBlock(x, y, z) {
    if (y < 0) return 201;
    if (y >= HMAX) return 0;
    if (x < WMIN || x >= WMAX || z < WMIN || z >= WMAX) return 0;
    var c = chunks.get(ckey(x >> 4, z >> 4));
    if (c) { var v = c.edits.get((y << 8) | ((x & 15) << 4) | (z & 15)); if (v !== undefined) return v; }
    return y <= GROUND ? 201 : 0;
  }
  function markDirty(c) { if (c && c.loaded) c.dirty = true; }
  function markAround(cx, cz, lx, lz) {
    markDirty(chunks.get(ckey(cx, cz)));
    if (lx === 0) markDirty(chunks.get(ckey(cx - 1, cz)));
    if (lx === 15) markDirty(chunks.get(ckey(cx + 1, cz)));
    if (lz === 0) markDirty(chunks.get(ckey(cx, cz - 1)));
    if (lz === 15) markDirty(chunks.get(ckey(cx, cz + 1)));
  }
  // store one block change (c = 0 for air). Returns the chunk it landed in, or null if that chunk is not loaded.
  function setBlock(x, y, z, c) {
    if (x < WMIN || x >= WMAX || z < WMIN || z >= WMAX || y < 0 || y >= HMAX) return null;
    var ch = chunks.get(ckey(x >> 4, z >> 4));
    if (!ch || !ch.loaded) return null;
    var k = (y << 8) | ((x & 15) << 4) | (z & 15);
    var base = y <= GROUND ? 201 : 0;
    if (c === base) ch.edits.delete(k); else ch.edits.set(k, c);
    if (y < ch.minY) ch.minY = y;
    if (y > ch.maxY) ch.maxY = y;
    markAround(ch.cx, ch.cz, x & 15, z & 15);
    return ch;
  }
  function loadChunkFrame(dv) {          // 0x10 | cx i16 | cz i16 | n u16 | n * (lxz u8, y u16, c u8)
    var cx = dv.getInt16(1, true), cz = dv.getInt16(3, true), n = dv.getUint16(5, true);
    var ch = chunks.get(ckey(cx, cz));
    if (!ch) { ch = newChunk(cx, cz); chunks.set(ckey(cx, cz), ch); }
    ch.edits = new Map(); ch.minY = GROUND; ch.maxY = GROUND;
    var o = 7;
    for (var i = 0; i < n; i++, o += 4) {
      var lxz = dv.getUint8(o), y = dv.getUint16(o + 1, true), c = dv.getUint8(o + 3);
      ch.edits.set((y << 8) | lxz, c);
      if (y < ch.minY) ch.minY = y;
      if (y > ch.maxY) ch.maxY = y;
    }
    ch.loaded = true; ch.dirty = true;
    if (n > 0) for (var dx = -1; dx <= 1; dx++) for (var dz = -1; dz <= 1; dz++) if (dx || dz) markDirty(chunks.get(ckey(cx + dx, cz + dz)));   // their border faces may change
    return ch;
  }

  // ------------------------------------------------------------------ meshing: only faces that touch air, with soft corner shading
  var PAL = [];                          // palette index -> [r,g,b]
  function baseColor(y) { return y === 0 ? [62, 62, 68] : y <= 36 ? [137, 141, 144] : y <= 39 ? [124, 84, 50] : [84, 160, 60]; }
  var AX = [0, 0, 1, 1, 2, 2], SG = [1, -1, 1, -1, 1, -1];
  var TU = [2, 2, 0, 0, 0, 0], TV = [1, 1, 2, 2, 1, 1];
  var CU = [-1, 1, 1, -1], CV = [-1, -1, 1, 1];
  var AOB = [0.5, 0.68, 0.84, 1.0];
  function faceShade(a, s) { return a === 1 ? (s > 0 ? 1.0 : 0.5) : (a === 0 ? 0.8 : 0.66); }
  function meshChunk(c) {
    var x0 = c.cx * 16, z0 = c.cz * 16, minY = c.minY, maxY = c.maxY;
    for (var dx = -1; dx <= 1; dx++) for (var dz = -1; dz <= 1; dz++) {
      var n = chunks.get(ckey(c.cx + dx, c.cz + dz));
      if (n && n.loaded) { if (n.minY < minY) minY = n.minY; if (n.maxY > maxY) maxY = n.maxY; }
    }
    var yLo = Math.max(0, minY - 1), yHi = Math.min(HMAX - 1, maxY + 1);
    var pos = [], col = [];
    var b = [0, 0, 0], nb = [0, 0, 0], base = [0, 0, 0], q = [0, 0, 0], ao = [0, 0, 0, 0], vx = [], vy = [], vz = [];
    function solidAt(x, y, z) { return getBlock(x, y, z) !== 0 ? 1 : 0; }
    for (var lx = 0; lx < 16; lx++) for (var lz = 0; lz < 16; lz++) for (var y = yLo; y <= yHi; y++) {
      var wx = x0 + lx, wz = z0 + lz;
      var code = getBlock(wx, y, wz);
      if (code === 0) continue;
      var rgb = code > 200 ? baseColor(y) : (PAL[code - 1] || [255, 0, 255]);
      b[0] = wx; b[1] = y; b[2] = wz;
      for (var d = 0; d < 6; d++) {
        var a = AX[d], s = SG[d], u = TU[d], v = TV[d];
        base[0] = b[0]; base[1] = b[1]; base[2] = b[2]; base[a] += s;
        if (getBlock(base[0], base[1], base[2]) !== 0) continue;
        var sh = faceShade(a, s);
        for (var i = 0; i < 4; i++) {
          var du = CU[i], dv = CV[i];
          q[0] = base[0]; q[1] = base[1]; q[2] = base[2]; q[u] += du;
          var s1 = solidAt(q[0], q[1], q[2]);
          q[0] = base[0]; q[1] = base[1]; q[2] = base[2]; q[v] += dv;
          var s2 = solidAt(q[0], q[1], q[2]);
          q[0] = base[0]; q[1] = base[1]; q[2] = base[2]; q[u] += du; q[v] += dv;
          var s3 = solidAt(q[0], q[1], q[2]);
          ao[i] = (s1 && s2) ? 0 : 3 - (s1 + s2 + s3);
          nb[a] = b[a] + (s > 0 ? 1 : 0); nb[u] = b[u] + (du > 0 ? 1 : 0); nb[v] = b[v] + (dv > 0 ? 1 : 0);
          vx[i] = nb[0] - x0; vy[i] = nb[1]; vz[i] = nb[2] - z0;
        }
        var order = (ao[0] + ao[2] > ao[1] + ao[3]) ? [0, 1, 2, 0, 2, 3] : [1, 2, 3, 1, 3, 0];
        for (var t = 0; t < 6; t++) {
          var k = order[t], br = AOB[ao[k]] * sh;
          pos.push(vx[k], vy[k], vz[k]);
          col.push(Math.min(255, Math.round(rgb[0] * br)), Math.min(255, Math.round(rgb[1] * br)), Math.min(255, Math.round(rgb[2] * br)));
        }
      }
    }
    var nv = pos.length / 3;
    var buf = new ArrayBuffer(nv * 16), f = new Float32Array(buf), u8 = new Uint8Array(buf);
    for (var j = 0; j < nv; j++) {
      f[j * 4] = pos[j * 3]; f[j * 4 + 1] = pos[j * 3 + 1]; f[j * 4 + 2] = pos[j * 3 + 2];
      u8[j * 16 + 12] = col[j * 3]; u8[j * 16 + 13] = col[j * 3 + 1]; u8[j * 16 + 14] = col[j * 3 + 2]; u8[j * 16 + 15] = 255;
    }
    return { data: buf, count: nv };
  }

  // ------------------------------------------------------------------ aiming, collision, movement
  function raycast(ox, oy, oz, dx, dy, dz, maxD) {
    var x = Math.floor(ox), y = Math.floor(oy), z = Math.floor(oz);
    var sx = dx > 0 ? 1 : -1, sy = dy > 0 ? 1 : -1, sz = dz > 0 ? 1 : -1;
    var tdx = dx !== 0 ? Math.abs(1 / dx) : Infinity, tdy = dy !== 0 ? Math.abs(1 / dy) : Infinity, tdz = dz !== 0 ? Math.abs(1 / dz) : Infinity;
    var tmx = dx > 0 ? (x + 1 - ox) / dx : dx < 0 ? (ox - x) / -dx : Infinity;
    var tmy = dy > 0 ? (y + 1 - oy) / dy : dy < 0 ? (oy - y) / -dy : Infinity;
    var tmz = dz > 0 ? (z + 1 - oz) / dz : dz < 0 ? (oz - z) / -dz : Infinity;
    var nx = 0, ny = 0, nz = 0, t = 0;
    for (var i = 0; i < 400; i++) {
      if (getBlock(x, y, z) !== 0) return { x: x, y: y, z: z, nx: nx, ny: ny, nz: nz, t: t };
      if (tmx < tmy && tmx < tmz) { x += sx; t = tmx; tmx += tdx; nx = -sx; ny = 0; nz = 0; }
      else if (tmy < tmz) { y += sy; t = tmy; tmy += tdy; nx = 0; ny = -sy; nz = 0; }
      else { z += sz; t = tmz; tmz += tdz; nx = 0; ny = 0; nz = -sz; }
      if (t > maxD) return null;
    }
    return null;
  }
  function lineBlocked(ox, oy, oz, tx, ty, tz) {      // is there a solid block between two points?
    var dx = tx - ox, dy = ty - oy, dz = tz - oz, len = Math.sqrt(dx * dx + dy * dy + dz * dz);
    if (len < 0.01) return false;
    var h = raycast(ox, oy, oz, dx / len, dy / len, dz / len, len);
    return !!h && h.t < len - 0.01;
  }
  function collidesAt(x, y, z) {
    var x0 = Math.floor(x - HALF), x1 = Math.floor(x + HALF - 1e-7), z0 = Math.floor(z - HALF), z1 = Math.floor(z + HALF - 1e-7);
    var y0 = Math.floor(y), y1 = Math.floor(y + PH - 1e-7);
    for (var ix = x0; ix <= x1; ix++) for (var iz = z0; iz <= z1; iz++) for (var iy = y0; iy <= y1; iy++) if (getBlock(ix, iy, iz) !== 0) return true;
    return false;
  }
  function moveAxis(pl, ax, d) {         // slide along one axis; returns true if something stopped us
    if (d === 0) return false;
    var steps = Math.ceil(Math.abs(d) / 0.4), sd = d / steps;
    for (var i = 0; i < steps; i++) {
      var nx = pl.x + (ax === 0 ? sd : 0), ny = pl.y + (ax === 1 ? sd : 0), nz = pl.z + (ax === 2 ? sd : 0);
      if (!collidesAt(nx, ny, nz)) { pl.x = nx; pl.y = ny; pl.z = nz; continue; }
      if (ax === 0) pl.x = sd > 0 ? Math.floor(nx + HALF) - HALF - 1e-5 : Math.floor(nx - HALF) + 1 + HALF + 1e-5;
      else if (ax === 2) pl.z = sd > 0 ? Math.floor(nz + HALF) - HALF - 1e-5 : Math.floor(nz - HALF) + 1 + HALF + 1e-5;
      else pl.y = sd > 0 ? Math.floor(ny + PH) - PH - 1e-5 : Math.floor(ny) + 1;
      return true;
    }
    return false;
  }
  // If a block ends up on top of you (another player placed it as you walked into it, or the server moved you into a spot that has
  // since been built on) you are pushed out to the nearest free spot, the way Minecraft does, instead of being stuck inside it.
  function escapeBlocks(pl) {
    if (!collidesAt(pl.x, pl.y, pl.z)) return false;
    for (var r = 0.1; r <= 4.01; r += 0.1) {
      if (pl.y + r < HMAX + 6 && !collidesAt(pl.x, pl.y + r, pl.z)) { pl.y += r; return true; }
      for (var a = 0; a < 16; a++) {
        var nx = pl.x + r * Math.cos(a * Math.PI / 8), nz = pl.z + r * Math.sin(a * Math.PI / 8);
        if (nx > WMIN + HALF && nx < WMAX - HALF && nz > WMIN + HALF && nz < WMAX - HALF && !collidesAt(nx, pl.y, nz)) { pl.x = nx; pl.z = nz; return true; }
      }
    }
    return false;
  }
  function stepPlayer(pl, inp, dt) {
    if (collidesAt(pl.x, pl.y, pl.z)) { if (escapeBlocks(pl)) { pl.vy = 0; pl.stuck = 0; } else pl.stuck = (pl.stuck || 0) + dt; } else pl.stuck = 0;
    var speed = inp.sprint ? SPRINT : WALK;
    var fwd = (inp.f ? 1 : 0) - (inp.b ? 1 : 0), side = (inp.r ? 1 : 0) - (inp.l ? 1 : 0);
    var len = Math.sqrt(fwd * fwd + side * side);
    var tx = 0, tz = 0;
    if (len > 0) {
      fwd /= len; side /= len;
      tx = (-Math.sin(pl.yaw) * fwd + Math.cos(pl.yaw) * side) * speed;
      tz = (-Math.cos(pl.yaw) * fwd - Math.sin(pl.yaw) * side) * speed;
    }
    var k = Math.min(1, dt * (pl.onGround ? 14 : 4));
    pl.vx += (tx - pl.vx) * k; pl.vz += (tz - pl.vz) * k;
    if (inp.jump && pl.onGround) { pl.vy = JUMP_V; pl.onGround = false; }
    pl.vy = Math.max(-TERMINAL, pl.vy - GRAV * dt);
    moveAxis(pl, 0, pl.vx * dt);
    moveAxis(pl, 2, pl.vz * dt);
    var wasGround = pl.onGround;
    var hit = moveAxis(pl, 1, pl.vy * dt);
    if (hit) { pl.onGround = pl.vy < 0; pl.vy = 0; } else pl.onGround = false;
    pl.x = Math.min(Math.max(pl.x, WMIN + HALF), WMAX - HALF);
    pl.z = Math.min(Math.max(pl.z, WMIN + HALF), WMAX - HALF);
    if (pl.y > HMAX + 6) { pl.y = HMAX + 6; if (pl.vy > 0) pl.vy = 0; }
    return wasGround !== pl.onGround;
  }
  function overlapsPlayer(pl, bx, by, bz) {
    return bx < pl.x + HALF && bx + 1 > pl.x - HALF && bz < pl.z + HALF && bz + 1 > pl.z - HALF && by < pl.y + PH && by + 1 > pl.y;
  }

  // ------------------------------------------------------------------ matrices (column-major, like WebGL wants)
  function perspective(fov, aspect, near, far) {
    var f = 1 / Math.tan(fov / 2), m = new Float32Array(16);
    m[0] = f / aspect; m[5] = f; m[10] = (far + near) / (near - far); m[11] = -1; m[14] = 2 * far * near / (near - far);
    return m;
  }
  function viewMatrix(ex, ey, ez, yaw, pitch) {
    var fx = -Math.sin(yaw) * Math.cos(pitch), fy = Math.sin(pitch), fz = -Math.cos(yaw) * Math.cos(pitch);
    var rx = Math.cos(yaw), ry = 0, rz = -Math.sin(yaw);
    var ux = ry * fz - rz * fy, uy = rz * fx - rx * fz, uz = rx * fy - ry * fx;
    var m = new Float32Array(16);
    m[0] = rx; m[1] = ux; m[2] = -fx; m[4] = ry; m[5] = uy; m[6] = -fy; m[8] = rz; m[9] = uz; m[10] = -fz;
    m[12] = -(rx * ex + ry * ey + rz * ez); m[13] = -(ux * ex + uy * ey + uz * ez); m[14] = fx * ex + fy * ey + fz * ez; m[15] = 1;
    return m;
  }
  function mul(a, b) {
    var o = new Float32Array(16);
    for (var c = 0; c < 4; c++) for (var r = 0; r < 4; r++) {
      var s = 0; for (var k = 0; k < 4; k++) s += a[k * 4 + r] * b[c * 4 + k];
      o[c * 4 + r] = s;
    }
    return o;
  }
  function project(vp, x, y, z, w, h) {   // world point -> screen pixels, or null if behind the camera
    var cx = vp[0] * x + vp[4] * y + vp[8] * z + vp[12], cy = vp[1] * x + vp[5] * y + vp[9] * z + vp[13], cw = vp[3] * x + vp[7] * y + vp[11] * z + vp[15];
    if (cw <= 0.05) return null;
    return { x: (cx / cw * 0.5 + 0.5) * w, y: (1 - (cy / cw * 0.5 + 0.5)) * h, w: cw };
  }

  // ------------------------------------------------------------------ pixel art for the rocket launcher, the rocket and the explosion
  // (each letter is one pixel; the pictures are turned into small crisp images when EzWorld opens)
  var PIXC = { k: '#14161a', o: '#55683f', O: '#7b9159', d: '#3a4a2b', s: '#6b7280', S: '#a3acb9', t: '#3f444d', r: '#d9381e', R: '#ff6a3d', q: '#a8281a',
               y: '#f0a020', Y: '#ffe06b', w: '#fff7e0', b: '#6b4423', B: '#45290f', z: '#6e4a40' };
  var ART_LAUNCHER = [
    "....................................................",
    "....................................................",
    "...................kkkkkkk..........................",
    "..................kSSSSSSSk.................kkkk....",
    ".......kkkkk......ktttktttk..............kkkssssk...",
    "......kSSSSSkkkkkkktttttttkkkkkkkkkkkkkkkSSSttttk...",
    "...kkkksssttOOOOYYOOOOOOOOOOOOOOYYOOOOOOOssstkkk....",
    "..kRRRRsssttooooyyooooooooooooooyyooooooossstkkk....",
    ".kRRRrrsssttooooyyooooooooooooooyyooooooossstkkk....",
    ".kRrrrrsssttooooyyoooSoooSoooSooyyooooooossstkkk....",
    ".kRrrrrsssttooooyyooodrrwwwwwdooyyooooooossstkkk....",
    ".kRrrrrsssttooooyyooodddddddddooyyooooooossstkkk....",
    "..krrqqsssttddddyyddddddddddddddyydddddddssstkkk....",
    "...kkkksssttddddyyddddddddddddddyydddddddssstkkk....",
    "......ktttttkkkkkkkkkkkkkkkkkkkkkkkkkkkkktttttttk...",
    ".......kkkkk.kbbbbk...kttttbbbBk.........kkkttttk...",
    ".............kbbbBk...ktkkkbbbBk............kkkk....",
    ".............kbbbBk...ktkkkbbbBk....................",
    ".............kbbbBk...kttttkbbbBk...................",
    ".............kbbbBk....kkkkkbbbBk...................",
    "..............kkkk.........kbbbBk...................",
    "............................kbbbBk..................",
    "............................kbbbBk..................",
    ".............................kkkk...................",
    "....................................................",
    "...................................................."
  ];
  var ART_ROCKET = [
    "....kkkk..............",
    "...kttttk.............",
    ".kkkstttkkkkkkkkkkk...",
    "kryyySSSSrrSSSSRRRrk..",
    "kYwYyssssrrssssrrrRRk.",
    "kryyyttttrrttttrrrrk..",
    ".kkkstttkkkkkkkkkkk...",
    "...kttttk.............",
    "....kkkk.............."
  ];
  var ART_BOOM_FRAMES = [[
    "................",
    "................",
    "................",
    "................",
    ".....yyyyYY.....",
    "....yyYYYYY.....",
    ".....YYwwYY.....",
    "....yyYwwYYy....",
    "....YYwwwwYy....",
    "....YYYwwYYYy...",
    ".......YYYyy....",
    ".......yY.......",
    ".......yyy......",
    "................",
    "................",
    "................"
  ], [
    "........rr......",
    ".......rrrr.....",
    "...rr.rryryrr...",
    "...rrryyyyrr....",
    ".rryyyryYYyrr...",
    "..ryYYYYYYyr....",
    "..yyYYYwwYyyyrrr",
    "...yYYwwwwYYyyrr",
    "...ryYwwwwYYyyyr",
    "..ryyYwwwYyYyrr.",
    "..yyyYYYYYyrr...",
    ".rryyyyyyYyy....",
    "...ryr.ryyyr....",
    "....r.rryyyr....",
    ".........rrr....",
    "........r.r....."
  ], [
    "......zzzzz.....",
    ".....zzrrzz.....",
    ".....zzrrr......",
    ".....zrryrzzz...",
    "z.zzrzryyyzrrrz.",
    "zzrrrryyyyyrrrz.",
    "zrrryyyYYyyyrrrz",
    ".rrryyYYYYyyrzzz",
    "..rrryyYYyyyrrz.",
    "...rryYYYyyyrzz.",
    ".zzzryyyyyyrr...",
    "..zrryyyrrrrzz..",
    "...rrrrrrrrrzzz.",
    "...zrrz.zzrrzzz.",
    "...zzzzz..zzz...",
    "....zz.z..z.zz.."
  ], [
    "....St.S........",
    "..S...ttS..SSSS.",
    "...Stt.t.tttSSS.",
    "..S..z.zzttttttS",
    "..Sttzzzzz.zztt.",
    "..tttzzzzzzzt.t.",
    ".t.zzzzrrrzz....",
    "tt..zzrrrrzzttS.",
    "tttzzrrrrrzzt.t.",
    "Sttzz.rrrrzzz.tS",
    "SSt...zrzzzzzt.S",
    ".Stttz.zztz....S",
    "...Stzzzztt.t.t.",
    "....Stz.ttSt.S..",
    "...SS...tt......",
    "...S.Sttt......."
  ]];
  function pixUrl(rows) {
    var out = '', w = rows[0].length;
    for (var y = 0; y < rows.length; y++) {
      var x = 0;
      while (x < w) {
        var c = rows[y].charAt(x);
        if (c === '.' || c === ' ') { x++; continue; }
        var x2 = x; while (x2 < w && rows[y].charAt(x2) === c) x2++;
        out += '<rect x="' + x + '" y="' + y + '" width="' + (x2 - x) + '" height="1" fill="' + (PIXC[c] || '#f0f') + '"/>'; x = x2;
      }
    }
    var svg = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ' + w + ' ' + rows.length + '" shape-rendering="crispEdges">' + out + '</svg>';
    return 'url("data:image/svg+xml,' + encodeURIComponent(svg) + '")';
  }
  function boomSheet() {                       // the four explosion frames side by side
    var rows = [];
    for (var y = 0; y < ART_BOOM_FRAMES[0].length; y++) rows.push(ART_BOOM_FRAMES.map(function (f) { return f[y]; }).join(''));
    return rows;
  }

  // ------------------------------------------------------------------ the FPV drone: pixel art and flight physics
  var ART_DRONE = [[
    "....................................",
    "....................................",
    ".kkkkkkkkk...............kkkkkkkkkk.",
    "kSwwwwwwwSk.............krSwwwwwwwSk",
    "kSSSSSSSSSk..............kSSSSSSSSSk",
    ".kkkkkkkkk...............kkkkkkkkkk.",
    "..ksssssk...kkkkkkkkkkkk.k.ksssssk..",
    "..ktttttkkkkoOOOOOOOOOOokkkktttttk..",
    "..ktttttssssoooooooooooosssstttttk..",
    "..ktttttttttdddttttttdddtttttttttk..",
    "..ktrtttkkkkdddtkccktdddkkkktttgtk..",
    "...kkkkk...kyyyyyyyyyyyyk...kkkkk...",
    "...........kdddtkkkktdddk...........",
    "...........kdddttttttdddk...........",
    "...........kttttttttttttk...........",
    "............kttkkkkkkttk............",
    "...........kkttkk..kkttkk...........",
    "..........ksssssskkssssssk.........."
  ], [
    "....................................",
    "....................................",
    ".........................k..........",
    "..kkkkkkk...............krkkkkkkkk..",
    ".kSwwwwwSk...............kkSwwwwwSk.",
    ".kSSSSSSSk...............kkSSSSSSSk.",
    "..ksssssk...kkkkkkkkkkkk.k.ksssssk..",
    "..ktttttkkkkoOOOOOOOOOOokkkktttttk..",
    "..ktttttssssoooooooooooosssstttttk..",
    "..ktttttttttdddttttttdddtttttttttk..",
    "..ktrtttkkkkdddtkccktdddkkkktttgtk..",
    "...kkkkk...kyyyyyyyyyyyyk...kkkkk...",
    "...........kdddtkkkktdddk...........",
    "...........kdddttttttdddk...........",
    "...........kttttttttttttk...........",
    "............kttkkkkkkttk............",
    "...........kkttkk..kkttkk...........",
    "..........ksssssskkssssssk.........."
  ]];
  PIXC.c = '#4fc3f7'; PIXC.g = '#42d36a';
  var DR_ACCEL = 75, DR_DRAG = 1.65, DR_HALF = 0.22, DR_CRASH = 3, DR_MAXY = 430;       // top speed is accel / drag, about 45 blocks a second
  function droneHits(x, y, z) {
    for (var a = -1; a <= 1; a += 2) for (var b = -1; b <= 1; b += 2) for (var c = -1; c <= 1; c += 2)
      if (getBlock(Math.floor(x + a * DR_HALF), Math.floor(y + b * DR_HALF), Math.floor(z + c * DR_HALF)) !== 0) return true;
    return false;
  }
  // One physics step. The drone flies where you look (W/S), strafes (A/D) and climbs (Space) or dives (Shift). Returns 'ok', 'bump' (touched something
  // slowly) or 'crash' (hit something at speed; the server decides what breaks).
  function stepDrone(d, inp, dt) {
    var M = d.mult || 1;                                            // each speed upgrade raises the top speed (and the punch of the motors)
    var cp = Math.cos(d.pitch), fx = -Math.sin(d.yaw) * cp, fy = Math.sin(d.pitch), fz = -Math.cos(d.yaw) * cp, rx = Math.cos(d.yaw), rz = -Math.sin(d.yaw);
    var f = (inp.f ? 1 : 0) - (inp.b ? 1 : 0), s = (inp.r ? 1 : 0) - (inp.l ? 1 : 0), u = (inp.up ? 1 : 0) - (inp.down ? 1 : 0);
    var wx = fx * f + rx * s, wy = fy * f + u, wz = fz * f + rz * s, wl = Math.sqrt(wx * wx + wy * wy + wz * wz);
    if (wl > 1) { wx /= wl; wy /= wl; wz /= wl; }
    d.vx += (wx * DR_ACCEL * M - d.vx * DR_DRAG) * dt; d.vy += (wy * DR_ACCEL * M - d.vy * DR_DRAG) * dt; d.vz += (wz * DR_ACCEL * M - d.vz * DR_DRAG) * dt;
    var speed = Math.sqrt(d.vx * d.vx + d.vy * d.vy + d.vz * d.vz), steps = Math.max(1, Math.ceil(speed * dt / 0.3)), res = 'ok';
    for (var i = 0; i < steps; i++) {
      var sx = d.vx * dt / steps, sy = d.vy * dt / steps, sz = d.vz * dt / steps;
      if (!droneHits(d.x + sx, d.y, d.z)) d.x += sx; else { res = speed >= DR_CRASH ? 'crash' : 'bump'; d.vx = 0; }
      if (!droneHits(d.x, d.y + sy, d.z)) d.y += sy; else { res = speed >= DR_CRASH ? 'crash' : 'bump'; d.vy = 0; }
      if (!droneHits(d.x, d.y, d.z + sz)) d.z += sz; else { res = speed >= DR_CRASH ? 'crash' : 'bump'; d.vz = 0; }
      if (res === 'crash') break;
    }
    d.x = Math.min(Math.max(d.x, WMIN + 0.5), WMAX - 0.5); d.z = Math.min(Math.max(d.z, WMIN + 0.5), WMAX - 0.5);
    if (d.y > DR_MAXY) { d.y = DR_MAXY; if (d.vy > 0) d.vy = 0; }
    if (d.y < 1) { d.y = 1; if (d.vy < 0) d.vy = 0; }
    return res;
  }


  // ------------------------------------------------------------------ helicopters: five real ones at real size (1 block is 1 metre), and their flight physics
  // vmax: top speed in blocks per second; climb: best climb rate; rr: main rotor radius; top: rotor height above the skids; f / b: nose / tail reach from the mast;
  // hw: half width; hard: the fastest sink rate a landing survives; yawRate: pedal turn rate; chase: camera distance; eye: where the pilot's eyes are (right, up, back is positive)
  var HELIS = {
    r22:  { name: 'Robinson R22', price: 2500, vmax: 36, climb: 8, rr: 3.84, top: 2.7, f: 2.1, b: 4.7, hw: 0.6, hard: 3.6, yawRate: 1.5, chase: 14, eye: [0.28, 1.28, -0.8],
            facts: 'Two-seat piston trainer, the helicopter most pilots learn on. 7.7 m rotor, cruises near 100 knots. Light and twitchy: easy to fly, easy to crash.' },
    b206: { name: 'Bell 206 JetRanger', price: 6000, vmax: 44, climb: 10, rr: 5.08, top: 2.9, f: 2.6, b: 6.6, hw: 0.75, hard: 4.0, yawRate: 1.3, chase: 17, eye: [0.36, 1.25, -1.2],
            facts: 'Five-seat turbine helicopter flown since 1967 by news crews and police. 10.2 m rotor, smooth and forgiving.' },
    uh1:  { name: 'Bell UH-1H Huey', price: 12000, vmax: 46, climb: 11, rr: 7.3, top: 4.4, f: 3.6, b: 8.6, hw: 1.2, hard: 4.8, yawRate: 1.1, chase: 21, eye: [0.5, 1.6, -2.2],
            facts: 'The Vietnam-era workhorse. One 14.6 m two-blade rotor makes the famous whop-whop. Slow to turn, strong in a climb.' },
    uh60: { name: 'Sikorsky UH-60 Black Hawk', price: 25000, vmax: 56, climb: 13, rr: 8.18, top: 5.1, f: 4.1, b: 10.0, hw: 1.35, hard: 5.6, yawRate: 1.0, chase: 24, eye: [0.5, 1.9, -2.6],
            facts: 'The US Army utility helicopter since 1979. Twin turbines, 16.4 m four-blade rotor, crash-absorbing landing gear. The fastest of the five.' },
    ch47: { name: 'Boeing CH-47 Chinook', price: 40000, vmax: 50, climb: 12, rr: 9.1, top: 5.7, f: 7.8, b: 8.4, hw: 1.9, hard: 5.0, yawRate: 0.75, chase: 34, eye: [0.5, 2.3, -6.3],
            facts: 'Twin-rotor heavy lifter that carries 33 troops. Two 18.3 m rotors, 30 m long in flight. Huge, steady and slow to turn: it needs a big clearing.' }
  };
  var HELI_IDS = ['r22', 'b206', 'uh1', 'uh60', 'ch47'];
  var HELI_MAXY = 430, HELI_SPOOL = 4.0, HELI_READY = 0.92, HELI_TILT = 0.36;
  function hspec(id) {
    var s = HELIS[id], g = typeof window !== 'undefined' && window.HeliGL && window.HeliGL.SPECS && window.HeliGL.SPECS[id];
    if (g && g.eye && !s._g) { s._g = 1; if (g.chaseDist) s.chase = g.chaseDist; s.eye = g.eye; }
    return s;
  }
  function hpt(h, sp, lx, lz, ly) {            // a point on the helicopter (right, back, up) as a world point
    var c = Math.cos(h.yaw), s = Math.sin(h.yaw);
    return [h.x + lx * c + lz * s, h.y + ly, h.z - lx * s + lz * c];
  }
  function rotorDisc(sp) {                       // one sample point per block over the whole rotor disc (cached)
    if (sp._disc) return sp._disc;
    var pts = [], r = sp.rr, dx, dz;
    for (dx = -r; dx <= r + 0.01; dx += 0.9) for (dz = -r; dz <= r + 0.01; dz += 0.9) if (dx * dx + dz * dz <= r * r) pts.push([dx, dz]);
    return (sp._disc = pts);
  }
  // What is touching a helicopter placed like h: 1 = the spinning rotor, 2 = the body or tail, 4 = the skids.
  function heliHit(h, sp) {
    var m = 0, i, p, ry = Math.floor(h.y + sp.top - 0.2), disc = rotorDisc(sp);
    for (i = 0; i < disc.length; i++) if (getBlock(Math.floor(h.x + disc[i][0]), ry, Math.floor(h.z + disc[i][1])) !== 0) { m |= 1; break; }
    var lz, hs = [0.5, sp.top * 0.55];
    for (lz = -sp.f; lz <= sp.b + 0.01; lz += 1.2) {
      var xs = lz > 2.2 ? [0] : [-sp.hw, 0, sp.hw];
      for (var xi = 0; xi < xs.length; xi++) for (var hi = 0; hi < 2; hi++) {
        p = hpt(h, sp, xs[xi], lz, hs[hi]);
        if (getBlock(Math.floor(p[0]), Math.floor(p[1]), Math.floor(p[2])) !== 0) { m |= 2; xi = 9; break; }
      }
      if (m & 2) break;
    }
    var fz = [-sp.f * 0.3, sp.b * 0.12];
    for (var k = 0; k < 4; k++) {
      p = hpt(h, sp, (k & 1 ? 1 : -1) * sp.hw * 1.25, fz[k >> 1], -0.02);
      if (getBlock(Math.floor(p[0]), Math.floor(p[1]), Math.floor(p[2])) !== 0) { m |= 4; break; }
    }
    return m;
  }
  function heliGround(h) {                       // how far the skids are above the ground below (up to 12 blocks), or -1
    var by = Math.floor(h.y - 0.02);
    for (var k = 0; k < 13; k++) if (getBlock(Math.floor(h.x), by - k, Math.floor(h.z)) !== 0) return h.y - (by - k + 1);
    return -1;
  }
  // One physics step. inp: f/b pitch, l/r bank, yl/yr pedals, up/down collective. Returns 'ok', 'bump', 'land' (touched down softly) or 'crash' (h.why says how).
  function stepHeli(h, inp, dt, sp) {
    var up = !!inp.up, down = !!inp.down, ready = h.rpm >= HELI_READY;
    h.rpm = Math.min(1, h.rpm + dt / HELI_SPOOL);
    var tp = ready && !h.onGround ? Math.max(-1, Math.min(1, (inp.f ? 1 : 0) - (inp.b ? 1 : 0) + (inp.ay || 0))) * HELI_TILT : 0, tr = ready && !h.onGround ? ((inp.r ? 1 : 0) - (inp.l ? 1 : 0)) * HELI_TILT * 0.8 : 0;
    var kt = Math.min(1, dt * 2.4);
    h.pt += (tp - h.pt) * kt; h.rl += (tr - h.rl) * kt;
    var wy = h.rpm > 0.5 ? Math.max(-1, Math.min(1, (inp.yl ? 1 : 0) - (inp.yr ? 1 : 0) - (inp.ax || 0))) * sp.yawRate : 0;
    h.wy += (wy - h.wy) * Math.min(1, dt * 3); h.yaw += h.wy * dt;
    if (h.wy && (heliHit(h, sp) & 2)) { h.yaw -= h.wy * dt; h.wy = 0; }               // the tail swung into something: the pedals stop turning it
    var vmaxA = sp.vmax * 0.5, fwx = -Math.sin(h.yaw), fwz = -Math.cos(h.yaw), rgx = Math.cos(h.yaw), rgz = -Math.sin(h.yaw);
    var af = h.pt / HELI_TILT * vmaxA, ar = h.rl / (HELI_TILT * 0.8) * vmaxA * 0.8, k2 = h.rpm * h.rpm;
    if (h.onGround) { var fr = Math.exp(-3.2 * dt); h.vx *= fr; h.vz *= fr; }
    else { h.vx += ((fwx * af + rgx * ar) * k2 - h.vx * 0.5) * dt; h.vz += ((fwz * af + rgz * ar) * k2 - h.vz * 0.5) * dt; }
    var vt = (up ? sp.climb : 0) - (down ? sp.climb * 0.6 : 0);
    var cushion = false;
    if (!h.onGround && vt < 0) { var gd = heliGround(h); if (gd >= 0 && gd < 4) { vt = Math.max(vt, -(1.5 + gd * 0.9)); cushion = true; } }       // the cushion of air under the rotor near the ground
    if (!ready) { if (!h.onGround) h.vy = Math.max(h.vy - 20 * dt, -45); }
    else if (h.onGround) { if (vt > 0) { h.onGround = false; h.vy = 0.4; } else h.vy = 0; }
    else h.vy += (vt - h.vy) * Math.min(1, dt * (cushion ? 3.5 : 1.8));
    var spd = Math.sqrt(h.vx * h.vx + h.vy * h.vy + h.vz * h.vz), steps = Math.max(1, Math.ceil(spd * dt / 0.25)), res = 'ok', ax, mk, t;
    function fail(why) { h.why = why; h.cv = [h.vx, h.vy, h.vz]; h.vx = h.vy = h.vz = 0; res = 'crash'; }
    for (var i = 0; i < steps && res !== 'crash'; i++) {
      for (ax = 0; ax < 3 && res !== 'crash'; ax++) {
        var d = (ax === 0 ? h.vx : ax === 1 ? h.vy : h.vz) * dt / steps;
        if (!d) continue;
        var ox = h.x, oy = h.y, oz = h.z;
        if (ax === 0) h.x += d; else if (ax === 1) h.y += d; else h.z += d;
        mk = heliHit(h, sp);
        if (!mk) continue;
        h.x = ox; h.y = oy; h.z = oz;                                              // blocked: stay where we were
        var down1 = ax === 1 && h.vy < 0;
        if ((mk & 1) && h.rpm > 0.3) { fail('The rotor hit something.'); break; }
        if (mk === 4 && down1) {                                                  // the skids touched down
          var sink = -h.vy, hs = Math.sqrt(h.vx * h.vx + h.vz * h.vz), tilt = Math.max(Math.abs(h.pt), Math.abs(h.rl));
          if (sink > sp.hard) { fail('Hard landing.'); break; }
          if (hs > 9) { fail('You landed too fast.'); break; }
          if (tilt > 0.22 && sink > 2.2) { fail('You landed on one skid.'); break; }
          h.y = Math.floor(h.y + d - 0.02) + 1.03; h.vy = 0; h.onGround = true; res = 'land';       // rests 3 cm up, so the skids are not inside the block
        } else {
          t = Math.sqrt(h.vx * h.vx + h.vy * h.vy + h.vz * h.vz);
          if (t >= (mk === 4 ? 6 : 4)) { fail(mk === 4 ? 'The skids caught on something.' : 'You flew into something.'); break; }
          if (ax === 0) h.vx = 0; else if (ax === 1) h.vy = 0; else h.vz = 0;
          if (res === 'ok') res = 'bump';
        }
      }
    }
    if (res !== 'crash' && h.rpm > 0.3 && (heliHit(h, sp) & 1)) fail('The rotor hit something.');      // the blades sweep into a block even when nothing moves
    if (h.onGround && !(heliHit({ x: h.x, y: h.y - 0.05, z: h.z, yaw: h.yaw }, sp) & 4)) h.onGround = false;       // the ground went away
    h.x = Math.min(Math.max(h.x, WMIN + 1), WMAX - 1); h.z = Math.min(Math.max(h.z, WMIN + 1), WMAX - 1);
    if (h.y > HELI_MAXY) { h.y = HELI_MAXY; if (h.vy > 0) h.vy = 0; }
    return res;
  }
  function heliModelMat(x, y, z, yaw, pt, rl) {      // model space (nose toward -z, up +y) to world, column-major
    var c = Math.cos(yaw), s = Math.sin(yaw), cx = Math.cos(-pt), sx = Math.sin(-pt), cz = Math.cos(-rl), sz = Math.sin(-rl);
    var rx = [[1, 0, 0], [0, cx, -sx], [0, sx, cx]], rz = [[cz, -sz, 0], [sz, cz, 0], [0, 0, 1]], ry = [[c, 0, s], [0, 1, 0], [-s, 0, c]], a = [[0, 0, 0], [0, 0, 0], [0, 0, 0]], r = [[0, 0, 0], [0, 0, 0], [0, 0, 0]], i, j, k;
    for (i = 0; i < 3; i++) for (j = 0; j < 3; j++) { for (k = 0; k < 3; k++) a[i][j] += rx[i][k] * rz[k][j]; }
    for (i = 0; i < 3; i++) for (j = 0; j < 3; j++) { for (k = 0; k < 3; k++) r[i][j] += ry[i][k] * a[k][j]; }
    return new Float32Array([r[0][0], r[1][0], r[2][0], 0, r[0][1], r[1][1], r[2][1], 0, r[0][2], r[1][2], r[2][2], 0, x, y, z, 1]);
  }


  // ------------------------------------------------------------------ fighter jets: five real ones at real size, and their flight physics
  // vmax: top speed in blocks per second; stall: below this the wings stop lifting; len / span / h: real size in metres; mach: real top speed in Mach
  var JETS = {
    f16xl:   { name: 'General Dynamics F-16XL', price: 30000, vmax: 105, stall: 30, len: 16.5, span: 10.4, h: 5.1, mach: 2.0, chase: 26,
               facts: 'An experimental F-16 with a huge cranked-arrow delta wing, built for NASA. Only two were made. Carries more fuel and flies a long way, fast, with a very high roll rate.' },
    f15:     { name: 'McDonnell Douglas F-15 Eagle', price: 45000, vmax: 100, stall: 31, len: 19.43, span: 13.05, h: 5.63, mach: 2.5, chase: 30,
               facts: 'The air-superiority fighter that has never been shot down in air combat. Two big engines, two tail fins and a Mach 2.5 top speed. Huge wing, stable and strong.' },
    rafale:  { name: 'Dassault Rafale', price: 60000, vmax: 100, stall: 29, len: 15.27, span: 10.8, h: 5.34, mach: 1.8, chase: 24,
               facts: 'French delta-wing fighter with canards. Flies from land and from aircraft carriers, and slows down well for short landings. Nimble and light.' },
    typhoon: { name: 'Eurofighter Typhoon', price: 75000, vmax: 108, stall: 29, len: 15.96, span: 10.95, h: 5.28, mach: 2.0, chase: 25,
               facts: 'Four European nations built this delta-canard fighter. It is deliberately unstable so it turns on a coin; the computer holds it steady. Fastest of the five here.' },
    su30:    { name: 'Sukhoi Su-30SM2', price: 90000, vmax: 95, stall: 27, len: 21.9, span: 14.7, h: 6.4, mach: 2.0, chase: 34,
               facts: 'Russian two-seat twin-engine heavy fighter with thrust-vectoring nozzles. It is famous for flying at slow speed and still turning hard. Biggest and slowest of the five.' }
  };
  var JET_IDS = ['f16xl', 'f15', 'rafale', 'typhoon', 'su30'];
  var JET_MAXY = 430;
  function jspec(id) {
    var s = JETS[id]; if (s._r) return s;
    var g = typeof window !== 'undefined' && window.JetGL && window.JetGL.SPECS && window.JetGL.SPECS[id], L = s.len, sp = s.span;
    s.noseZ = g && g.noseZ || -0.55 * L; s.tailZ = g && g.tailZ || 0.45 * L;
    s.tips = g && g.wingTips || [[-sp / 2, 1.3, 0.2 * L], [sp / 2, 1.3, 0.2 * L]];
    s.fin = g && g.finTop || [0, s.h, 0.35 * L];
    s.wheels = g && g.wheels || [[0, 0, -0.3 * L], [-1.6, 0, 0.1 * L], [1.6, 0, 0.1 * L]];
    s.eye = g && g.eye || [0, 1.7, -0.3 * L]; s.nozzles = g && g.nozzles || [[0, 1.3, s.tailZ]];
    if (g && g.chaseDist) s.chase = g.chaseDist;
    s.chaseUp = g && g.chaseUp || 0.2 * L;
    var pts = [[0, 1.3, s.noseZ], [0, 1.4, s.tailZ], s.fin, [0, 1.9, 0]], wx;
    for (wx = 0; wx <= 1.0001; wx += 1.5 / Math.abs(s.tips[1][0] || 5)) { pts.push([s.tips[0][0] * wx, s.tips[0][1], s.tips[0][2]]); pts.push([s.tips[1][0] * wx, s.tips[1][1], s.tips[1][2]]); }
    pts.push(s.tips[0], s.tips[1]);
    for (var z = s.noseZ + 2; z < s.tailZ - 1; z += 2.5) pts.push([0, 1.3, z]);
    s.body = pts; s.vrot = s.stall * 1.12;
    if (g) s._r = true;
    return s;
  }
  function jv(x, y, z) { return [x, y, z]; }
  function jnorm(v) { var l = Math.sqrt(v[0] * v[0] + v[1] * v[1] + v[2] * v[2]) || 1; v[0] /= l; v[1] /= l; v[2] /= l; return v; }
  function jcross(a, b) { return [a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]]; }
  // A world point from model coordinates (right, up, back-positive z) for a jet with forward f, up u at position p
  function jpt(j, x, y, z) { var r = jcross(j.f, j.u); return [j.x + r[0] * x + j.u[0] * y - j.f[0] * z, j.y + r[1] * x + j.u[1] * y - j.f[1] * z, j.z + r[2] * x + j.u[2] * y - j.f[2] * z]; }
  function jetBasis(yaw, pitch, roll) {              // forward / up vectors from the three angles sent over the wire
    var sy = Math.sin(yaw), cy = Math.cos(yaw), sp = Math.sin(pitch), cp = Math.cos(pitch);
    var f = [-sy * cp, sp, -cy * cp], u0 = [sy * sp, cp, cy * sp], r0 = jcross(f, u0), cr = Math.cos(roll), sr = Math.sin(roll);
    return { f: f, u: [u0[0] * cr + r0[0] * sr, u0[1] * cr + r0[1] * sr, u0[2] * cr + r0[2] * sr] };
  }
  function jetAngles(f, u) {                          // and back: yaw, pitch, roll
    var pitch = Math.asin(Math.max(-1, Math.min(1, f[1]))), yaw = Math.atan2(-f[0], -f[2]);
    var b = jetBasis(yaw, pitch, 0), r0 = jcross(f, b.u);
    var roll = Math.atan2(u[0] * r0[0] + u[1] * r0[1] + u[2] * r0[2], u[0] * b.u[0] + u[1] * b.u[1] + u[2] * b.u[2]);
    return { yaw: yaw, pitch: pitch, roll: roll };
  }
  function jetMat(x, y, z, f, u) {                    // model space to world, column-major: columns are right, up, back
    var r = jcross(f, u);
    return new Float32Array([r[0], r[1], r[2], 0, u[0], u[1], u[2], 0, -f[0], -f[1], -f[2], 0, x, y, z, 1]);
  }
  function jetHit(j, sp) {                            // 1 = something touches the airframe, 2 = a wheel touches something
    var m = 0, i, p, pts = sp.body;
    for (i = 0; i < pts.length; i++) { p = jpt(j, pts[i][0], pts[i][1], pts[i][2]); if (getBlock(Math.floor(p[0]), Math.floor(p[1]), Math.floor(p[2])) !== 0) { m |= 1; break; } }
    for (i = 0; i < sp.wheels.length; i++) { p = jpt(j, sp.wheels[i][0], sp.wheels[i][1] - 0.02, sp.wheels[i][2]); if (getBlock(Math.floor(p[0]), Math.floor(p[1]), Math.floor(p[2])) !== 0) { m |= 2; break; } }
    return m;
  }
  function jetRotate(j, axis, ang) {                  // axis 0: pitch (about the right wing), 1: roll (about the nose), 2: yaw (about the up axis)
    var c = Math.cos(ang), s = Math.sin(ang), f = j.f, u = j.u, r = jcross(f, u), i;
    if (axis === 0) { for (i = 0; i < 3; i++) { var nf = f[i] * c + u[i] * s, nu = u[i] * c - f[i] * s; f[i] = nf; u[i] = nu; } }
    else if (axis === 1) { for (i = 0; i < 3; i++) u[i] = u[i] * c + r[i] * s; }
    else { for (i = 0; i < 3; i++) f[i] = f[i] * c - r[i] * s; }
    jnorm(f); var d = f[0] * u[0] + f[1] * u[1] + f[2] * u[2]; for (i = 0; i < 3; i++) u[i] -= f[i] * d; jnorm(u);
  }
  // One physics step. inp: f/b nose down / up, l/r roll, yl/yr rudder, up/down throttle. Returns 'ok', 'bump', 'land' or 'crash' (j.why says how).
  function stepJet(j, inp, dt, sp) {
    var res = 'ok', air = !j.onGround, i;
    j.thr = Math.max(0, Math.min(1, j.thr + ((inp.up ? 1 : 0) - (inp.down ? 1 : 0)) * 0.6 * dt));
    var target = j.thr * sp.vmax;
    var dv = (target - j.speed) * (target > j.speed ? 0.55 : (air ? 0.18 : 0.5));
    j.speed += Math.min(dv, air ? 24 : 11) * dt;                           // an engine can only push so hard: a fighter needs a runway
    if (!air && j.thr < 0.05) j.speed = Math.max(0, j.speed - 14 * dt);
    if (air) j.speed -= 12 * j.f[1] * dt;                                  // climbing costs speed, diving gains it
    j.speed = Math.max(0, Math.min(sp.vmax * 1.25, j.speed));
    var pull = Math.max(-1, Math.min(1, (inp.b ? 1 : 0) - (inp.f ? 1 : 0) + (inp.ay || 0))), rollIn = Math.max(-1, Math.min(1, (inp.r ? 1 : 0) - (inp.l ? 1 : 0) + (inp.ax || 0))), yawIn = (inp.yl ? 1 : 0) - (inp.yr ? 1 : 0);
    var auth = air ? Math.max(0.05, Math.min(1, (j.speed - 0.35 * sp.stall) / (0.9 * sp.stall))) : 0;
    var lf = Math.max(0, Math.min(1, (j.speed - 0.7 * sp.stall) / (0.4 * sp.stall)));
    j.stalled = air && lf < 0.6;
    var k = Math.min(1, dt * 7);
    j.rp += ((pull * 1.15 - (air ? (1 - lf) * 0.9 : 0)) * auth - j.rp) * k; j.rr += (rollIn * 2.6 * auth - j.rr) * k; j.ry += (yawIn * 0.45 * auth - j.ry) * k;
    if (air) {
      jetRotate(j, 0, j.rp * dt); jetRotate(j, 1, j.rr * dt); jetRotate(j, 2, j.ry * dt);
      if (!rollIn && j.u[1] > 0.1 && Math.abs(j.f[1]) < 0.85) {          // a gentle self-levelling when the stick is centred
        var bank = jcross(j.f, j.u)[1];                                  // right wing's height: positive means rolled left
        jetRotate(j, 1, Math.max(-1, Math.min(1, bank * 2.2)) * 0.8 * auth * dt);
      }
      j.sink = (j.sink || 0) - 26 * (1 - lf) * dt; j.sink *= Math.exp(-2.5 * lf * dt);
    } else {
      var turn = ((inp.yl ? 1 : 0) - (inp.yr ? 1 : 0)) * 0.5 * Math.min(1, j.speed / 10);
      var ang = Math.atan2(-j.f[0], -j.f[2]) + turn * dt; j.f = [-Math.sin(ang), 0, -Math.cos(ang)]; j.u = [0, 1, 0]; j.sink = 0;
      if (j.speed >= sp.vrot && pull > 0) { j.onGround = false; air = true; jetRotate(j, 0, 0.12); j.sink = 0.5; }
    }
    var vx = j.f[0] * j.speed, vy = j.f[1] * j.speed + (air ? j.sink : 0), vz = j.f[2] * j.speed;
    j.vx = vx; j.vy = vy; j.vz = vz;
    var tot = Math.sqrt(vx * vx + vy * vy + vz * vz), steps = Math.max(1, Math.ceil(tot * dt / 0.5));
    for (i = 0; i < steps && res !== 'crash'; i++) {
      var ox = j.x, oy = j.y, oz = j.z;
      j.x += vx * dt / steps; j.y += vy * dt / steps; j.z += vz * dt / steps;
      var mk = jetHit(j, sp);
      if (!mk) continue;
      if (mk === 2 && vy >= 0) {                                          // wheels scraping while rising or rotating: lift out of the ground instead of stopping
        for (var lt = 0; lt < 12 && jetHit(j, sp) === 2; lt++) j.y += 0.08;
        if (jetHit(j, sp) === 0) continue;
      }
      j.x = ox; j.y = oy; j.z = oz;
      if (mk === 2 && vy < 0 && air) {                                    // the wheels touched down
        var pitch = Math.asin(Math.max(-1, Math.min(1, j.f[1])));
        if (-vy > 7) { j.why = 'You hit the ground too hard.'; res = 'crash'; }
        else if (j.u[1] < 0.93) { j.why = 'You landed with a wing down.'; res = 'crash'; }
        else if (pitch > 0.4 || pitch < -0.14) { j.why = 'You landed at a bad angle.'; res = 'crash'; }
        else if (j.speed > sp.vmax * 0.8) { j.why = 'You landed far too fast.'; res = 'crash'; }
        else {
          var ang2 = Math.atan2(-j.f[0], -j.f[2]); j.f = [-Math.sin(ang2), 0, -Math.cos(ang2)]; j.u = [0, 1, 0];
          var top = -1;
          for (var wi = 0; wi < sp.wheels.length; wi++) { var wp = jpt({ x: ox, y: oy, z: oz, f: [-Math.sin(ang2), 0, -Math.cos(ang2)], u: [0, 1, 0] }, sp.wheels[wi][0], 0, sp.wheels[wi][2]), by = Math.floor(wp[1] + 0.3); for (var q = 0; q < 4 && getBlock(Math.floor(wp[0]), by, Math.floor(wp[2])) === 0; q++) by--; if (getBlock(Math.floor(wp[0]), by, Math.floor(wp[2])) !== 0) top = Math.max(top, by + 1); }
          if (top >= 0) j.y = top + 1.03 - 1;
          j.onGround = true; j.sink = 0; res = 'land'; air = false; break;
        }
      } else if (j.speed >= 8 || (mk & 1)) { j.why = (mk & 1) ? 'You flew into something.' : 'The landing gear caught on something.'; res = 'crash'; }
      else { j.speed = 0; if (res === 'ok') res = 'bump'; break; }
    }
    if (res === 'crash') { j.cv = [vx, vy, vz]; j.speed = 0; j.vx = j.vy = j.vz = 0; }
    if (j.onGround) { var gp = jetHit({ x: j.x, y: j.y - 0.05, z: j.z, f: j.f, u: j.u }, sp); if (!(gp & 2)) j.onGround = false; }       // the ground went away
    j.x = Math.min(Math.max(j.x, WMIN + 1), WMAX - 1); j.z = Math.min(Math.max(j.z, WMIN + 1), WMAX - 1);
    if (j.y > JET_MAXY) { j.y = JET_MAXY; if (j.f[1] > 0) jetRotate(j, 0, -0.5 * dt); }
    return res;
  }


  // ------------------------------------------------------------------ cars: seven real ones at real size, and their driving physics
  // vmax: top speed in blocks per second (about 3.6 km/h each); accel: how hard it pulls from a standstill; grip: how quickly sideways sliding dies away;
  // wb: wheelbase; len / wid / h: real size; chase: camera distance
  var CARS = {
    yaris:   { name: 'Toyota Yaris', price: 6000, vmax: 28, accel: 11, brake: 22, grip: 6.5, wb: 2.55, len: 3.95, wid: 1.70, h: 1.50, chase: 7.5, pitchF: 1.0,
               facts: 'The small Toyota hatchback: light, cheap and tough. Easy to park and quick to turn, but the slowest of the lot. 175 km/h in real life.' },
    civic:   { name: 'Honda Civic', price: 10000, vmax: 34, accel: 12, brake: 24, grip: 6.8, wb: 2.70, len: 4.67, wid: 1.80, h: 1.41, chase: 8, pitchF: 1.0,
               facts: 'The compact sedan that put Honda on the map. Sharp steering and plenty of grip for the money. Around 200 km/h.' },
    f150:    { name: 'Ford F-150', price: 20000, vmax: 31, accel: 10, brake: 20, grip: 5.6, wb: 3.69, len: 5.89, wid: 2.03, h: 1.97, chase: 10, pitchF: 0.8,
               facts: "America's best-selling pickup for decades. Tall, heavy and wide: it shrugs off bumps but it wallows through corners. 180 km/h and a long wheelbase." },
    c5:      { name: 'Chevrolet Corvette C5', price: 28000, vmax: 48, accel: 19, brake: 28, grip: 7.4, wb: 2.65, len: 4.56, wid: 1.87, h: 1.21, chase: 8, pitchF: 1.1,
               facts: 'The 1997 to 2004 front-engine Corvette: a V8 and a long hood in a low fibreglass body. Built to go fast in a straight line. Up to 270 km/h.' },
    m3:      { name: 'BMW M3', price: 40000, vmax: 46, accel: 18, brake: 28, grip: 7.2, wb: 2.73, len: 4.49, wid: 1.78, h: 1.37, chase: 8, pitchF: 1.0,
               facts: 'The E46 M3 coupe, loved for its straight-six and balance. It grips hard and turns eagerly. Electronically limited at 250 km/h.' },
    '911':   { name: 'Porsche 911 Carrera', price: 70000, vmax: 52, accel: 20, brake: 30, grip: 7.8, wb: 2.35, len: 4.45, wid: 1.80, h: 1.30, chase: 8, pitchF: 1.1,
               facts: 'The rear-engined icon: the engine hangs behind the axle, so it launches hard and the tail can swing. Short wheelbase, huge grip. About 285 km/h.' },
    cls63:   { name: 'Mercedes-Benz CLS 63 AMG', price: 55000, vmax: 50, accel: 17, brake: 27, grip: 6.9, wb: 2.95, len: 4.99, wid: 1.88, h: 1.42, chase: 8.5, pitchF: 1.0,
               facts: 'A four-door coupe with a hand-built twin-turbo V8. Luxury on the inside, a monster on the road. Limited to about 250 km/h.' }
  };
  var CAR_IDS = ['yaris', 'civic', 'f150', 'c5', 'm3', '911', 'cls63'];
  var CAR_CRASH_MIN = 8;
  function cspec(id) {
    var s = CARS[id]; if (s._r) return s;
    var g = typeof window !== 'undefined' && window.CarGL && window.CarGL.SPECS && window.CarGL.SPECS[id], hw = s.wid / 2, wbh = s.wb / 2;
    s.wheels = g && g.wheels || [[-hw * 0.85, 0.32, -wbh], [hw * 0.85, 0.32, -wbh], [-hw * 0.85, 0.32, wbh], [hw * 0.85, 0.32, wbh]];
    s.eye = g && g.eye || [-0.35, 1.15, -0.2]; s.noseZ = g && g.noseZ || -s.len / 2; s.tailZ = g && g.tailZ || s.len / 2;
    s.wr = g && g.wheelRadius || 0.32;
    if (g && g.chaseDist) s.chase = g.chaseDist; s.chaseUp = g && g.chaseUp || 2.4;
    var hl = Math.max(Math.abs(s.noseZ), Math.abs(s.tailZ)) * 0.98, hwid = s.wid / 2 * 0.98;
    s.pts = [[-hwid, -hl], [hwid, -hl], [-hwid, hl], [hwid, hl], [0, -hl], [0, hl], [-hwid, 0], [hwid, 0]];
    if (g) s._r = true;
    return s;
  }
  function carGroundAt(x, y, z) {                  // the top of the ground under a point, looking down from just above y; null when there is nothing for 14 blocks
    var bx = Math.floor(x), bz = Math.floor(z), by = Math.floor(y + 1.1);
    for (var k = 0; k < 15; k++) if (getBlock(bx, by - k, bz) !== 0) return by - k + 1;
    return null;
  }
  function carDirs(yaw) { return [-Math.sin(yaw), -Math.cos(yaw), Math.cos(yaw), -Math.sin(yaw)]; }       // forward x, z and right x, z
  function carWheelXZ(c, sp, w) { var d = carDirs(c.yaw); return [c.x + d[2] * w[0] - d[0] * w[2], c.z + d[3] * w[0] - d[1] * w[2]]; }
  function carBodyHit(c, sp, x, z) {               // is any part of the body (above a one block step) inside a block?
    var d = carDirs(c.yaw), yy = Math.floor(c.y + 1.2);
    for (var i = 0; i < sp.pts.length; i++) {
      var p = sp.pts[i], px = x + d[2] * p[0] - d[0] * p[1], pz = z + d[3] * p[0] - d[1] * p[1];
      if (getBlock(Math.floor(px), yy, Math.floor(pz)) !== 0) return true;
    }
    return false;
  }
  // One physics step. inp: f throttle, b brake/reverse, l/r steer, hb handbrake. Returns null, or { res: 'crash' | 'bump' | 'land', spd, dx, dz } for something that touched.
  function stepCar(c, inp, dt, sp) {
    var thr = !!inp.f, brk = !!inp.b, hb = !!inp.hb, ev = null;
    var sm = 0.58 - 0.38 * Math.min(1, Math.abs(c.vf) / sp.vmax), st = ((inp.r ? 1 : 0) - (inp.l ? 1 : 0)) * sm;
    c.steer += (st - c.steer) * Math.min(1, dt * (st ? 6 : 10));
    if (c.onGround) {
      var acc = 0;
      if (thr) acc = c.vf < -0.5 ? sp.brake : sp.accel * (1 - c.vf / sp.vmax);
      if (brk) acc = c.vf > 0.5 ? -sp.brake : -sp.accel * 0.7 * Math.max(0, 1 + c.vf / (sp.vmax * 0.3));
      if (!thr && !brk) { var coast = 2.2 + Math.abs(c.vf) * 0.08; acc = Math.abs(c.vf) < coast * dt ? -c.vf / dt : -Math.sign(c.vf) * coast; }
      c.vf += acc * dt;
      if (hb) c.vf *= Math.exp(-0.7 * dt);
    }
    var wlim = Math.min(2.4, (9 + sp.grip) / Math.max(Math.abs(c.vf), 3)) * (hb ? 1.4 : 1), wyT = Math.max(-wlim, Math.min(wlim, -c.vf / sp.wb * Math.tan(c.steer) * (hb ? 1.4 : 1)));      // and no turn can be tighter than the tyres hold          // steering right (positive) turns the heading clockwise
    if (!c.onGround) wyT = c.wy;
    c.wy += (wyT - c.wy) * Math.min(1, dt * 9);
    var d0 = carDirs(c.yaw), vx = d0[0] * c.vf + d0[2] * c.vl, vz = d0[1] * c.vf + d0[3] * c.vl;     // the world velocity, then re-aimed at the new heading
    c.yaw += c.wy * dt;
    var d1 = carDirs(c.yaw);
    c.vf = vx * d1[0] + vz * d1[1]; c.vl = vx * d1[2] + vz * d1[3];
    if (c.onGround) { c.vl *= Math.exp(-(hb ? 1.3 : sp.grip) * dt); c.vf *= Math.exp(-Math.abs(c.vl) * 0.012 * dt); }
    vx = d1[0] * c.vf + d1[2] * c.vl; vz = d1[1] * c.vf + d1[3] * c.vl;
    c.speed = Math.sqrt(vx * vx + vz * vz);
    var steps = Math.max(1, Math.ceil(c.speed * dt / 0.35)), h = dt / steps, i;
    for (i = 0; i < steps; i++) {
      var nx = c.x + vx * h, nz = c.z + vz * h, hit = false;
      var ys = c.y;
      // the wheels: a step up of a block or less is driven over, anything taller is a wall
      var gmax = -1e9, gsum = [0, 0, 0, 0], w, gw;
      for (w = 0; w < 4; w++) {
        var wp = carWheelXZ({ x: nx, z: nz, yaw: c.yaw }, sp, sp.wheels[w]); gw = carGroundAt(wp[0], ys, wp[1]);
        if (gw === null) gw = ys - 15;
        if (gw > ys + 1.06) hit = true;
        gsum[w] = gw; if (gw > gmax) gmax = gw;
      }
      if (!hit && carBodyHit(c, sp, nx, nz)) hit = true;
      if (hit) {
        var spd = Math.sqrt(vx * vx + vz * vz) || 1;
        ev = { res: spd >= CAR_CRASH_MIN ? 'crash' : 'bump', spd: spd, dx: vx / spd, dz: vz / spd };
        vx *= -0.28; vz *= -0.28;                                         // it bounces back off what it hit
        c.vf = vx * d1[0] + vz * d1[1]; c.vl = vx * d1[2] + vz * d1[3]; c.wy += (c.vf > 0 ? -1 : 1) * (Math.random() - 0.3) * Math.min(2.5, spd * 0.08);
        break;
      }
      c.x = nx; c.z = nz;
      // the height: ride on the highest wheel, fall when there is nothing under them
      var gt = gmax;
      if (c.y < gt + 0.03 - 1e-6 || (c.onGround && c.y - gt < 0.4)) {
        c.y += Math.min(gt + 0.03 - c.y, 9 * h + 0.05); if (c.y > gt + 0.03) c.y = gt + 0.03;
        if (!c.onGround && c.y >= gt + 0.03 - 1e-6) { c.onGround = true; if (c.vy < -9) ev = ev || { res: 'crash', spd: -c.vy * 0.7, dx: 0, dz: 0 }; else if (c.vy < -3) ev = ev || { res: 'land', spd: -c.vy, dx: 0, dz: 0 }; c.vy = 0; }
        else if (c.onGround) c.vy = 0;
        c.gw = gsum;
      } else {
        c.onGround = false; c.vy = Math.max(c.vy - 28 * h, -60); c.y += c.vy * h;
        if (c.y <= gt + 0.03) { c.y = gt + 0.03; c.onGround = true; if (c.vy < -9) ev = ev || { res: 'crash', spd: -c.vy * 0.7, dx: 0, dz: 0 }; else if (c.vy < -3) ev = ev || { res: 'land', spd: -c.vy, dx: 0, dz: 0 }; c.vy = 0; }
        c.gw = gsum;
      }
    }
    // body attitude from how high the four wheels sit: nose up or down on a slope, one side up on a kerb
    var g = c.gw || [c.y, c.y, c.y, c.y], pf = ((g[0] + g[1]) / 2 - (g[2] + g[3]) / 2) / sp.wb, rf = ((g[0] + g[2]) / 2 - (g[1] + g[3]) / 2) / (sp.wid * 0.8);
    var tp = c.onGround ? Math.atan(pf) : Math.max(-0.5, Math.min(0.5, c.vy * 0.03)), tr = c.onGround ? Math.atan(rf) : 0;
    tp += (c.vf - (c.pvf === undefined ? c.vf : c.pvf)) / Math.max(dt, 0.001) * 0.0035 * (sp.pitchF || 1);        // braking dips the nose, accelerating squats the tail
    c.pvf = c.vf;
    tr += c.wy * c.vf * 0.0065;                                                                                  // leaning out in a corner
    c.pitch += (tp - c.pitch) * Math.min(1, dt * 7); c.roll += (tr - c.roll) * Math.min(1, dt * 7);
    c.x = Math.min(Math.max(c.x, WMIN + 2), WMAX - 2); c.z = Math.min(Math.max(c.z, WMIN + 2), WMAX - 2);
    c.wheel += c.vf * dt / sp.wr;
    return ev;
  }

  // ------------------------------------------------------------------ compass
  var WIND = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW'];
  function headingOf(yaw) { return (((-yaw * 180 / Math.PI) % 360) + 360) % 360; }   // 0 = north (-Z), 90 = east (+X), 180 = south, 270 = west
  function windName(h) { return WIND[Math.round(h / 45) % 8]; }
  // Draws the scrolling strip (about 120 degrees visible, faded at both ends). cv/off are canvases, colours come from the page theme.
  function drawCompass(cv, off, heading, accent, text) {
    var ctx = cv.getContext('2d');
    if (!ctx) return;                                  // no 2D canvas available: skip the compass, never break the 3D view
    var W = cv.width, H = cv.height, k = W / 380, w = 380, h = 40, ppd = 3.4;
    ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.clearRect(0, 0, W, H);
    off.width = W; off.height = H;
    var o = off.getContext('2d'); o.setTransform(k, 0, 0, k, 0, 0); o.clearRect(0, 0, w, h);
    o.textAlign = 'center'; o.textBaseline = 'middle';
    var first = Math.floor((heading - 70) / 5) * 5;
    for (var deg = first; deg <= heading + 70; deg += 5) {
      var x = w / 2 + (deg - heading) * ppd, n = ((deg % 360) + 360) % 360;
      if (n % 45 === 0) {
        o.fillStyle = n === 0 ? accent : text; o.font = (n % 90 === 0 ? '700 16px' : '600 12px') + ' Inter, system-ui, sans-serif';
        o.fillText(WIND[n / 45], x, 15);
        o.fillRect(x - 0.75, 28, 1.5, 9);
      } else if (n % 15 === 0) {
        o.fillStyle = text; o.globalAlpha = 0.55; o.font = '600 9px Inter, system-ui, sans-serif'; o.fillText(String(n), x, 14); o.globalAlpha = 1;
        o.fillRect(x - 0.5, 30, 1, 7);
      } else { o.fillStyle = text; o.globalAlpha = 0.4; o.fillRect(x - 0.5, 33, 1, 4); o.globalAlpha = 1; }
    }
    var g = o.createLinearGradient(0, 0, w, 0);
    g.addColorStop(0, 'rgba(0,0,0,0)'); g.addColorStop(0.18, 'rgba(0,0,0,1)'); g.addColorStop(0.82, 'rgba(0,0,0,1)'); g.addColorStop(1, 'rgba(0,0,0,0)');
    o.globalCompositeOperation = 'destination-in'; o.fillStyle = g; o.fillRect(0, 0, w, h); o.globalCompositeOperation = 'source-over';
    ctx.setTransform(k, 0, 0, k, 0, 0);
    ctx.fillStyle = 'rgba(10,10,14,0.55)'; ctx.beginPath();
    if (ctx.roundRect) ctx.roundRect(0, 0, w, h, 10); else ctx.rect(0, 0, w, h);
    ctx.fill();
    ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.drawImage(off, 0, 0);
    ctx.setTransform(k, 0, 0, k, 0, 0);
    ctx.fillStyle = accent; ctx.beginPath(); ctx.moveTo(w / 2 - 5, 0); ctx.lineTo(w / 2 + 5, 0); ctx.lineTo(w / 2, 7); ctx.closePath(); ctx.fill();
  }

  // ------------------------------------------------------------------ the running game
  var G = null;
  var VS = 'attribute vec3 aPos;attribute vec4 aCol;uniform mat4 uVP;uniform vec3 uOff;uniform vec3 uCam;varying vec3 vCol;varying float vDist;' +
    'void main(){vec3 w=aPos+uOff;gl_Position=uVP*vec4(w,1.0);vCol=aCol.rgb;vDist=length(w-uCam);}';
  var FS = 'precision mediump float;uniform vec3 uFogCol;uniform vec2 uFog;varying vec3 vCol;varying float vDist;' +
    'void main(){float f=clamp((vDist-uFog.x)/(uFog.y-uFog.x),0.0,1.0);gl_FragColor=vec4(mix(vCol,uFogCol,f),1.0);}';
  var SKY = [0.56, 0.75, 0.94];

  function $(id) { return document.getElementById(id); }
  function hexRgb(h) { return [parseInt(h.slice(1, 3), 16), parseInt(h.slice(3, 5), 16), parseInt(h.slice(5, 7), 16)]; }

  function open(opts) {
    if (G) close(true);
    var view = $('ew-view'), old = $('ew-canvas'), canvas = document.createElement('canvas');
    canvas.id = 'ew-canvas'; old.parentNode.replaceChild(canvas, old);
    G = { opts: opts, view: view, canvas: canvas, ws: null, me: null, others: new Map(), keys: {}, sel: 0, lastTap: 0, sprint: false,
          raf: 0, last: 0, lastSend: 0, lastSent: '', lastEdit: 0, hover: null, handlers: [], timers: [], ready: false, closed: false, myId: -1,
          compassCv: $('ew-compass-cv'), compassOff: null, lastHeading: -999, compassDirty: true, t0: performance.now(), failed: false, inv: { rockets: 0, armed: false, price: 5000, per: 5, max: 50, drone: false, dprice: 300, flying: false, heli: {}, hp: {}, hrep: 0.1, hfly: null, jet: {}, jp: {}, jfly: null, car: {}, cp: {}, ccid: null, cdrive: null }, rings: [], cars: new Map(), car: null, csel: '', cArm: '', cArmT: 0, ccam: null, carpose: null, hijack: null, mouseFly: true, mouseInv: false, mstick: { x: 0, y: 0 }, jet: null, jets: new Map(), jsel: '', jArm: '', jArmT: 0, jcam: null, jpose: null, jcrashState: null, heli: null, helis: new Map(), wrecks: [], hsel: '', hArm: '', hArmT: 0, hcam: null, hpose: null, hcrashState: null, rks: [], booms: [], drone: null, drones: new Map(), dbuyArm: 0, bodyWait: false, dsent: 0, dlast: '', lastFire: -1e9, shake: 0, shaking: false, buyArm: 0, shopTimer: 0, msgEls: new Map(), typers: new Map(), replyTo: null, myTyping: false, typeTimer: 0, mention: null, hintAt: 0 };
    PAL = opts.colors.map(hexRgb);
    view.style.display = 'flex';
    document.body.classList.add('ew-open');
    if (menuOpen() && window.hideMenu) window.hideMenu();           // the menu you pressed Join in must not stay on top of the world
    $('ew-leave').onclick = function () { close(false); };
    $('ew-typing').style.display = 'none'; $('ew-reply-bar').style.display = 'none'; $('ew-mention').style.display = 'none';
    view.style.setProperty('--ew-launcher', pixUrl(ART_LAUNCHER)); view.style.setProperty('--ew-rocket', pixUrl(ART_ROCKET)); view.style.setProperty('--ew-boom', pixUrl(boomSheet())); view.style.setProperty('--ew-drone0', pixUrl(ART_DRONE[0])); view.style.setProperty('--ew-drone1', pixUrl(ART_DRONE[1]));
    heliCss(); carCss();
    $('ew-shop').style.display = 'none'; $('ew-fx').innerHTML = ''; $('ew-hotbar').style.display = 'none'; $('ew-hotbar2').style.display = 'none'; $('ew-osd').style.display = 'none'; ['ew-hhud', 'ew-hhelp', 'ew-jhud', 'ew-jhelp', 'ew-fpv', 'ew-chud', 'ew-carprompt'].forEach(function (k) { var e0 = $(k); if (e0) e0.style.display = 'none'; }); view.classList.remove('fpv'); $('ew-held').style.display = 'none'; $('ew-online-total').textContent = '0';
    $('ew-me-name').textContent = opts.username || '';
    var me = $('ew-me-img'); me.onerror = function () { me.onerror = null; me.src = opts.defaultAvatar; }; me.src = opts.avatarUrl(opts.username || '');
    syncUserBar();
    $('ew-filter').checked = window.EzChat ? window.EzChat.getFilter() : true;
    $('ew-messages').innerHTML = '';
    $('ew-tags').innerHTML = '';
    $('ew-load').style.display = 'flex'; $('ew-load').textContent = 'Connecting...';
    $('ew-hint').style.display = 'none';
    var logo = document.querySelector('#header h1 img'); if (logo && $('ew-logo')) $('ew-logo').src = logo.src;
    if (!initGL()) { $('ew-load').textContent = 'EzWorld could not start 3D graphics (WebGL). Turn on hardware acceleration in your browser settings, close other heavy tabs, then press Leave and try again.'; return; }
    if (window.matchMedia && window.matchMedia('(pointer: coarse)').matches && !window.matchMedia('(pointer: fine)').matches) {
      $('ew-load').textContent = 'EzWorld needs a keyboard and a mouse. Open it on a computer.'; return;
    }
    buildPalette();
    bindInput();
    connect();
    G.last = performance.now();
    G.raf = requestAnimationFrame(frame);
  }

  function initGL() {
    var gl = null, tries = [{ antialias: true, alpha: false }, { alpha: false }, {}];
    for (var i = 0; i < tries.length && !gl; i++) { try { gl = G.canvas.getContext('webgl', tries[i]) || G.canvas.getContext('experimental-webgl', tries[i]); } catch (e) { gl = null; } }
    if (!gl || (gl.isContextLost && gl.isContextLost())) return false;
    function sh(type, src) { var s = gl.createShader(type); if (!s) return null; gl.shaderSource(s, src); gl.compileShader(s); return s; }
    var vs = sh(gl.VERTEX_SHADER, VS), fs = sh(gl.FRAGMENT_SHADER, FS), p = gl.createProgram();
    if (!vs || !fs || !p) return false;
    gl.attachShader(p, vs); gl.attachShader(p, fs); gl.linkProgram(p);
    if (!gl.getProgramParameter(p, gl.LINK_STATUS)) { try { console.warn('EzWorld shader link failed', gl.getProgramInfoLog(p)); } catch (e) {} return false; }
    G.gl = gl; G.prog = p;
    G.loc = { pos: gl.getAttribLocation(p, 'aPos'), col: gl.getAttribLocation(p, 'aCol'), vp: gl.getUniformLocation(p, 'uVP'), off: gl.getUniformLocation(p, 'uOff'),
              cam: gl.getUniformLocation(p, 'uCam'), fogCol: gl.getUniformLocation(p, 'uFogCol'), fog: gl.getUniformLocation(p, 'uFog') };
    // outline of the block you are aiming at (12 edges of a cube a hair larger than the block)
    var e = -0.003, E = 1.003, v = [[e, e, e], [E, e, e], [E, E, e], [e, E, e], [e, e, E], [E, e, E], [E, E, E], [e, E, E]];
    var ed = [0, 1, 1, 2, 2, 3, 3, 0, 4, 5, 5, 6, 6, 7, 7, 4, 0, 4, 1, 5, 2, 6, 3, 7];
    var lb = new Float32Array(24 * 4);
    for (var i = 0; i < 24; i++) { lb[i * 4] = v[ed[i]][0]; lb[i * 4 + 1] = v[ed[i]][1]; lb[i * 4 + 2] = v[ed[i]][2]; }
    G.lineBuf = gl.createBuffer(); gl.bindBuffer(gl.ARRAY_BUFFER, G.lineBuf); gl.bufferData(gl.ARRAY_BUFFER, lb, gl.STATIC_DRAW);
    gl.enable(gl.DEPTH_TEST);
    resize();
    return true;
  }

  function resize() {
    if (!G || !G.gl) return;
    var r = G.canvas.parentNode.getBoundingClientRect(), dpr = Math.min(window.devicePixelRatio || 1, 1.5);
    G.cw = Math.max(1, Math.floor(r.width)); G.ch = Math.max(1, Math.floor(r.height));
    G.canvas.width = Math.floor(G.cw * dpr); G.canvas.height = Math.floor(G.ch * dpr);
    G.canvas.style.width = G.cw + 'px'; G.canvas.style.height = G.ch + 'px';
    G.gl.viewport(0, 0, G.canvas.width, G.canvas.height);
  }

  function on(target, ev, fn, o) { target.addEventListener(ev, fn, o); G.handlers.push([target, ev, fn, o]); }

  function bindInput() {
    var canvas = G.canvas;
    on(window, 'resize', resize);
    on(canvas, 'contextmenu', function (e) { e.preventDefault(); });
    on(canvas, 'webglcontextlost', function (e) { e.preventDefault(); if (G) { G.ready = false; G.failed = true; $('ew-load').style.display = 'flex'; $('ew-load').textContent = 'Your graphics card reset EzWorld. Press Leave, then join again.'; } });
    on($('ew-hint'), 'click', function () { if (G.ready && !locked()) { try { canvas.requestPointerLock(); } catch (er) {} } });
    on(canvas, 'click', function () { if (!locked() && !chatFocused() && G.ready) canvas.requestPointerLock && canvas.requestPointerLock(); });
    on(document, 'pointerlockchange', function () { $('ew-hint').style.display = (G.ready && !locked() && !chatFocused() && !menuOpen()) ? 'flex' : 'none'; if (!locked()) { G.keys = {}; G.sprint = false; } });
    on(document, 'mousemove', function (e) {
      if (!locked() || !G.me) return;
      if (G.car) {
        var c0 = G.ccam; c0.yaw -= (e.movementX || 0) * 0.0023; c0.pitch = Math.max(-1.2, Math.min(1.2, c0.pitch - (e.movementY || 0) * 0.0023)); c0.idle = 0;
        return;
      }
      if (G.jet && G.mouseFly) {                              // the mouse is the stick: it deflects, and settles back to centre
        var ms = G.mstick;
        ms.x = Math.max(-1, Math.min(1, ms.x + (e.movementX || 0) * 0.0042)); ms.y = Math.max(-1, Math.min(1, ms.y - (e.movementY || 0) * 0.0042));
        return;
      }
      if (G.jet) {
        var jc0 = G.jcam, cock0 = G.jet.view === 1;
        jc0.yaw -= (e.movementX || 0) * 0.0023; jc0.pitch = Math.max(-1.2, Math.min(1.2, jc0.pitch - (e.movementY || 0) * 0.0023)); jc0.idle = 0;
        if (cock0) jc0.yaw = Math.max(-2.4, Math.min(2.4, jc0.yaw));
        return;
      }
      if (G.heli) {
        var hc = G.hcam, cock = G.heli.view === 1;
        hc.yaw -= (e.movementX || 0) * 0.0023; if (cock) hc.yaw = Math.max(-2.2, Math.min(2.2, hc.yaw));
        hc.pitch = Math.max(cock ? -1.1 : -1.4, Math.min(cock ? 0.9 : 0.6, hc.pitch - (e.movementY || 0) * 0.0023)); hc.idle = 0;
        return;
      }
      var who = G.drone || G.me;
      who.yaw -= (e.movementX || 0) * 0.0023;
      who.pitch = Math.max(-1.55, Math.min(1.55, who.pitch - (e.movementY || 0) * 0.0023));
    });
    on(canvas, 'mousedown', function (e) {            // one action per click: holding the button does nothing
      if (!locked() || !G.ready) return;
      e.preventDefault();
      if (G.drone || G.heli || G.jet || G.car) return;           // no placing, breaking or rockets while flying
      if (e.button === 0) { if (G.inv.armed) fireRocket(); else doBreak(); } else if (e.button === 2) doPlace(); else if (e.button === 1) doPick();
    });
    on(canvas, 'wheel', function (e) { e.preventDefault(); if (G.car) { G.ccam.dist = Math.max(3, Math.min(30, G.ccam.dist * (e.deltaY > 0 ? 1.1 : 0.9))); return; } if (G.jet) { G.jcam.dist = Math.max(6, Math.min(120, G.jcam.dist * (e.deltaY > 0 ? 1.1 : 0.9))); return; } if (G.heli) { G.hcam.dist = Math.max(4, Math.min(80, G.hcam.dist * (e.deltaY > 0 ? 1.1 : 0.9))); return; } selectColor(G.sel + (e.deltaY > 0 ? 1 : -1)); }, { passive: false });
    on(window, 'keydown', function (e) {
      if (!G || menuOpen() || typingElsewhere()) return;
      audio();                                                                  // browsers only allow sound after a key press or click
      if (shopOpen()) { if (e.code === 'Escape' || e.code === 'KeyB') { closeShop(); e.preventDefault(); } return; }
      if (chatFocused()) {
        if (e.code === 'Escape') { $('ew-chat-input').blur(); e.preventDefault(); }
        return;
      }
      if (e.code === 'Enter' || e.code === 'KeyT' || e.code === 'Slash') {
        e.preventDefault(); if (document.exitPointerLock) document.exitPointerLock();
        var inp = $('ew-chat-input'); inp.focus(); if (e.code === 'Slash') inp.value = '/';
        return;
      }
      if (!locked()) return;
      if (e.code === 'KeyW' && !e.repeat) {
        var now = performance.now();
        if (now - G.lastTap < 320) G.sprint = true;
        G.lastTap = now;
      }
      if (e.code === 'Digit1' && !G.drone && !G.heli && !G.jet && !G.car) setArmed(!G.inv.armed);
      if (e.code === 'Digit2') toggleDrone();
      if (e.code === 'Digit3' && !e.repeat) toggleHeli();
      if (e.code === 'Digit4' && !e.repeat) toggleJet();
      if (e.code === 'Digit5' && !e.repeat) summonCar();
      if (e.code === 'KeyF' && !e.repeat) carUse();
      if (e.code === 'KeyH' && G.car && !e.repeat) { send({ t: 'horn' }); hornSound(1, 0, G.car.m); }
      if (e.code === 'KeyL' && G.car && !e.repeat) { G.car.head = !G.car.head; }
      if (e.code === 'KeyV' && G.car && !e.repeat) { G.car.view = 1 - G.car.view; G.ccam.yaw = 0; G.ccam.pitch = 0; }
      if (e.code === 'KeyN' && !e.repeat) toggleMute();
      if (e.code === 'KeyI' && G.jet && !e.repeat) { G.mouseInv = !G.mouseInv; sys(G.mouseInv ? 'Mouse up now pulls the nose up (inverted).' : 'Mouse up now pushes the nose down, like a flight stick.'); }
      if (e.code === 'KeyM' && G.jet && !e.repeat) { G.mouseFly = !G.mouseFly; G.mstick.x = G.mstick.y = 0; sys(G.mouseFly ? 'Mouse steering on: the mouse works like a stick (M to switch to a free-look camera).' : 'Mouse steering off: the mouse looks around (M to steer with it again).'); }
      if (e.code === 'KeyV' && G.jet && !e.repeat) { G.jet.view = 1 - G.jet.view; G.jcam.yaw = 0; G.jcam.pitch = 0; }
      if (e.code === 'KeyV' && G.heli && !e.repeat) { G.heli.view = 1 - G.heli.view; G.hcam.yaw = 0; G.hcam.pitch = G.heli.view ? 0 : -0.22; }
      if (e.code === 'KeyB') { openShop(); return; }
      if (e.code === 'KeyQ' && !G.heli && !G.jet && !G.car) selectColor(G.sel - 1);
      if (e.code === 'KeyE' && !G.heli && !G.jet && !G.car) selectColor(G.sel + 1);
      G.keys[e.code] = true;
      if (e.code === 'Space' || e.code.indexOf('Arrow') === 0) e.preventDefault();
    });
    on(window, 'keyup', function (e) {
      if (!G) return;
      G.keys[e.code] = false;
      if (e.code === 'KeyW') G.sprint = false;
    });
    on(window, 'blur', function () { if (G) { G.keys = {}; G.sprint = false; } });
    var inp = $('ew-chat-input');
    on(inp, 'keydown', function (e) {
      if (G.mention) {
        var mn = G.mention;
        if (e.key === 'ArrowDown') { e.preventDefault(); mn.i = (mn.i + 1) % mn.names.length; updateMention(); return; }
        if (e.key === 'ArrowUp') { e.preventDefault(); mn.i = (mn.i - 1 + mn.names.length) % mn.names.length; updateMention(); return; }
        if (e.key === 'Enter' || e.key === 'Tab') { e.preventDefault(); insertMention(mn.names[mn.i]); return; }
        if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); hideMention(); return; }
      }
      if (e.code === 'Enter') {
        sendChat(); this.blur();
        try { G.canvas.requestPointerLock(); } catch (er) {}
        e.preventDefault();
      }
    });
    on(inp, 'input', function () { notifyTyping(); updateMention(); });
    on(inp, 'focus', function () { $('ew-hint').style.display = 'none'; });
    on(inp, 'blur', function () { stopTyping(); hideMention(); if (G && G.ready && !locked() && !menuOpen()) $('ew-hint').style.display = 'flex'; });
    on($('ew-chat-send'), 'click', function () { sendChat(); });
    on($('ew-reply-cancel'), 'click', clearReply);
    on($('ew-filter'), 'change', function () { if (window.EzChat) window.EzChat.setFilter(this.checked); });
    on($('ew-menu'), 'click', openMenu);
    on($('ew-shop-btn'), 'click', openShop);
    on($('ew-shop-close'), 'click', closeShop);
    on($('ew-shop-buy'), 'click', shopBuy);
    on($('ew-shop-equip'), 'click', function () { setArmed(!G.inv.armed); });
    on($('ew-shop-dbuy'), 'click', shopBuyDrone);
    on($('ew-shop-dlaunch'), 'click', function () { closeShop(); toggleDrone(); });
    on($('ew-shop-dupg'), 'click', shopUpgradeDrone);
    on($('ew-online'), 'click', toggleOnlinePanel);
    on(document, 'click', function (e) { var p = $('ew-online-panel'); if (p && p.style.display === 'block' && !p.contains(e.target)) p.style.display = 'none'; });
    on($('ew-shop-helis'), 'click', heliShopClick);
    on($('ew-shop-jets'), 'click', jetShopClick);
    on($('ew-shop-cars'), 'click', carShopClick);
    G.timers.push(setInterval(function () { renderTyping(); syncUserBar(); }, 1000));
    on($('ew-leave'), 'click', function () { close(false); });
    G.timers.push(setInterval(ensureChunks, 200));
  }
  function menuOpen() { return document.body.classList.contains('menu-open'); }
  function typingElsewhere() { var a = document.activeElement, t = a && a.tagName; return !!a && a !== $('ew-chat-input') && (t === 'INPUT' || t === 'TEXTAREA' || t === 'SELECT' || a.isContentEditable); }
  function openMenu() { if (document.exitPointerLock) document.exitPointerLock(); if (window.openMenuView) window.openMenuView('home'); }
  function syncUserBar() {
    var lv = document.getElementById('level-text'), pb = document.getElementById('pb-balance');
    $('ew-me-level').textContent = lv ? lv.textContent : ''; $('ew-me-pb').textContent = pb ? pb.textContent : '';
  }
  function locked() { return document.pointerLockElement === G.canvas; }
  function chatFocused() { return document.activeElement === $('ew-chat-input'); }

  function buildPalette() {
    var box = $('ew-palette'); box.innerHTML = '';
    G.opts.colors.forEach(function (hex, i) {
      var b = document.createElement('button');
      b.className = 'ew-sw'; b.style.background = hex; b.title = hex.toUpperCase(); b.dataset.i = i;
      b.onclick = function () { selectColor(i); };
      box.appendChild(b);
    });
    selectColor(Math.min(G.sel, G.opts.colors.length - 1));
  }
  function selectColor(i) {
    var n = G.opts.colors.length; G.sel = ((i % n) + n) % n;
    var sw = $('ew-palette').children;
    for (var k = 0; k < sw.length; k++) sw[k].classList.toggle('sel', k === G.sel);
    if (sw[G.sel] && sw[G.sel].scrollIntoView) { var p = $('ew-palette'), t = sw[G.sel]; if (t.offsetTop < p.scrollTop || t.offsetTop + t.offsetHeight > p.scrollTop + p.clientHeight) p.scrollTop = t.offsetTop - 20; }
    $('ew-sel-chip').style.background = G.opts.colors[G.sel];
    $('ew-sel-hex').textContent = G.opts.colors[G.sel].toUpperCase();
  }

  // ---- talking to the server
  function connect() {
    var ws = new WebSocket(G.opts.wsUrl); ws.binaryType = 'arraybuffer'; G.ws = ws;
    ws.onopen = function () { ws.send(JSON.stringify({ t: 'auth', token: G.opts.token })); };
    ws.onmessage = function (ev) { if (G && G.ws === ws) onMessage(ev.data); };
    ws.onclose = function () { if (G && G.ws === ws && !G.closed) { sys('Disconnected from EzWorld. Press Leave and join again.'); G.ready = false; G.failed = true; if (document.exitPointerLock) document.exitPointerLock(); $('ew-load').style.display = 'flex'; $('ew-load').textContent = 'Disconnected.'; } };
  }
  function onMessage(d) {
    if (d instanceof ArrayBuffer) {
      var dv = new DataView(d), t = dv.getUint8(0);
      if (t === 0x10) { loadChunkFrame(dv); }
      else if (t === 0x13) onHeliFrame(dv);
      else if (t === 0x14) onJetFrame(dv);
      else if (t === 0x15) onCarFrame(dv);
      else if (t === 0x12) {                                 // positions of flying drones
        var dn = dv.getUint16(1, true), dop = 3;
        for (var di = 0; di < dn; di++, dop += 10) {
          var did = dv.getUint16(dop, true), dd = G.drones.get(did);
          if (!dd) { var pl = G.others.get(did); dd = { id: did, name: pl ? pl.name : '', x: 0, y: 0, z: 0, tx: 0, ty: 0, tz: 0, el: null, fresh: true }; G.drones.set(did, dd); }
          dd.tx = dv.getInt16(dop + 2, true) / 16; dd.ty = dv.getInt16(dop + 4, true) / 16; dd.tz = dv.getInt16(dop + 6, true) / 16;
          if (dd.fresh) { dd.x = dd.tx; dd.y = dd.ty; dd.z = dd.tz; dd.fresh = false; }
        }
      }
      else if (t === 0x11) {
        var n = dv.getUint16(1, true), o = 3;
        for (var i = 0; i < n; i++, o += 10) {
          var id = dv.getUint16(o, true), p = G.others.get(id);
          if (!p) continue;
          p.tx = dv.getInt16(o + 2, true) / 16; p.ty = dv.getInt16(o + 4, true) / 16; p.tz = dv.getInt16(o + 6, true) / 16;
        }
      }
      return;
    }
    var m; try { m = JSON.parse(d); } catch (e) { return; }
    switch (m.t) {
      case 'hello':
        if (m.min != null && m.max != null) { WMIN = m.min; WMAX = m.max; }       // the server says how big the world is
        G.myId = m.id;
        if (m.total != null) setTotal(m.total);
        G.me = { x: m.x, y: m.y, z: m.z, vx: 0, vy: 0, vz: 0, yaw: 0, pitch: 0, onGround: false };
        (m.players || []).forEach(function (p) { addOther(p[0], p[1], p[2], p[3], p[4]); });
        updateOnline();
        $('ew-load').textContent = 'Loading the world...';
        ensureChunks();
        break;
      case 'dstart': startDrone(m); break;
      case 'dend': endDrone(m); break;
      case 'dhp': if (G.drone) {
          var hp0 = G.dhp; G.dhp = m.hp;
          if (m.hp < hp0) { G.dhurt = performance.now(); if (G.dview) G.dview.jolt = 1; if (G.aud && m.x !== undefined) noiseHit(G.aud, 0.7, 0.35, 1800, 200, 120); if (m.x !== undefined) G.shake = performance.now() + 250; }
          if (m.x !== undefined) { G.drone.x = m.x; G.drone.y = m.y; G.drone.z = m.z; G.drone.vx = G.drone.vy = G.drone.vz = 0; G.drone.crashAt = 0; if (m.v) { G.drone.vx = m.v[0]; G.drone.vy = m.v[1]; G.drone.vz = m.v[2]; } }
        } break;
      case 'dtp': if (G.drone) { G.drone.x = m.x; G.drone.y = m.y; G.drone.z = m.z; G.drone.vx = G.drone.vy = G.drone.vz = 0; G.drone.crashAt = 0; if (m.v) { G.drone.vx = m.v[0]; G.drone.vy = m.v[1]; G.drone.vz = m.v[2]; } } break;
      case 'dr': if (m.on) { G.drones.set(m.id, { id: m.id, name: m.name || '', x: m.x, y: m.y, z: m.z, tx: m.x, ty: m.y, tz: m.z, el: null, fresh: false }); } else removeDrone(m.id); break;
      case 'hno': G.hno = (G.hno || 0) + 1; if (G.hno >= 2) { G.hno = 0; sys('Stuck? Moving you out of the helicopter.'); send({ t: 'chat', text: '/unstuck' }); } break;
      case 'hstart': startHeli(m); break;
      case 'hend': endHeli(m); break;
      case 'htp': if (G.heli) { G.heli.x = m.x; G.heli.y = m.y; G.heli.z = m.z; G.heli.vx = G.heli.vy = G.heli.vz = 0; G.heli.crashAt = 0; } break;
      case 'hr': if (m.on) setOtherHeli(m.id, m.m, m.name, m.x, m.y, m.z); else removeOtherHeli(m.id); break;
      case 'hx': addWreck(m); break;
      case 'cstart': startCar(m); break;
      case 'cend': endCar(m); break;
      case 'ctp': if (G.car) { var cd = carDirs(G.car.yaw); G.car.x = m.x; G.car.y = m.y; G.car.z = m.z; G.car.vf = (m.v ? m.v[0] * cd[0] + m.v[2] * cd[1] : 0); G.car.vl = (m.v ? m.v[0] * cd[2] + m.v[2] * cd[3] : 0); G.car.vy = 0; G.car.onGround = true; } break;
      case 'cdmg': if (G.car && G.car.id === m.id) { G.car.dmg = m.dmg; G.car.dents = (m.dents || G.car.dents).slice(); } break;
      case 'chj': G.hijack = { id: m.id, t0: performance.now(), ms: m.ms || 1500 }; break;
      case 'cx': addCarWreck(m); break;
      case 'cgone': G.cars.delete(m.id); break;
      case 'horn': { var hc = G.cars.get(m.id), cmv = G.drone || G.heli || G.jet || G.car || G.me; if (hc && cmv) hornSound(1 / (1 + Math.hypot(hc.x - cmv.x, hc.z - cmv.z) / 15), 0, hc.m); break; }
      case 'cno': break;
      case 'jstart': startJet(m); break;
      case 'jend': endJet(m); break;
      case 'jtp': if (G.jet) { G.jet.x = m.x; G.jet.y = m.y; G.jet.z = m.z; G.jet.speed = 0; G.jet.vx = G.jet.vy = G.jet.vz = 0; G.jet.crashAt = 0; } break;
      case 'jr': if (m.on) setOtherJet(m.id, m.m, m.name, m.x, m.y, m.z); else removeOtherJet(m.id); break;
      case 'jx': addJetWreck(m); break;
      case 'jno': G.jno = (G.jno || 0) + 1; if (G.jno >= 2) { G.jno = 0; sys('Stuck? Moving you out of the jet.'); send({ t: 'chat', text: '/unstuck' }); } break;
      case 'inv': applyInv(m); break;
      case 'rk': addRocket(m); break;
      case 'bs': { (m.d || []).forEach(function (b) { setBlock(b[0], b[1], b[2], b[3]); }); if (m.e) addBoom(m.e, m.s); break; }
      case 'online': setTotal(m.total); if (m.here != null) { G.hereN = m.here; updateOnline(); } break;
      case 'sys': sys(m.text); break;
      case 'chat': chatLine(m); break;
      case 'typing': { var k = String(m.name).toLowerCase(); if (m.typing) G.typers.set(k, { name: m.name, exp: Date.now() + 7000 }); else G.typers.delete(k); renderTyping(); break; }
      case 'reaction': { var rel = G.msgEls.get(m.mid); if (rel && window.EzChat) window.EzChat.apply(rel, m.emoji, m.user, m.on); break; }
      case 'join': G.hereN = null; addOther(m.id, m.name, m.x, m.y, m.z); sys(m.name + ' joined EzWorld.'); updateOnline(); break;
      case 'leave': { G.hereN = null; var o2 = G.others.get(m.id); if (o2) { sys(o2.name + ' left EzWorld.'); o2.el.remove(); G.others.delete(m.id); removeDrone(m.id); removeOtherHeli(m.id); removeOtherJet(m.id); G.typers.delete(o2.name.toLowerCase()); renderTyping(); updateOnline(); } break; }
      case 'b': {
        var x = m.d[0], y = m.d[1], z = m.d[2], c = m.d[3];
        var ch = setBlock(x, y, z, c);
        if (ch) meshNow(ch, x & 15, z & 15);
        break;
      }
      case 'tp': if (G.me) { G.me.x = m.x; G.me.y = m.y; G.me.z = m.z; G.me.vx = G.me.vy = G.me.vz = 0; sendPos(true); } break;
      case 'error': sys(m.text || 'Error'); G.ready = false; G.failed = true; $('ew-load').style.display = 'flex'; $('ew-load').textContent = m.text || 'Error'; break;
    }
  }
  function addOther(id, name, x, y, z) {
    if (G.others.has(id)) return;
    var el = document.createElement('div'); el.className = 'ew-tag';
    var nm = document.createElement('span'); nm.className = 'ew-tag-name'; nm.textContent = name;
    var img = document.createElement('img'); img.className = 'ew-tag-img'; img.alt = ''; img.draggable = false;
    img.onerror = function () { img.onerror = null; img.src = G.opts.defaultAvatar; };
    img.src = G.opts.avatarUrl(name);
    el.appendChild(nm); el.appendChild(img); $('ew-tags').appendChild(el);
    G.others.set(id, { id: id, name: name, x: x, y: y, z: z, tx: x, ty: y, tz: z, el: el, img: img });
  }
  function updateOnline() { $('ew-online-n').textContent = String(G.hereN != null ? G.hereN : G.others.size + 1); }
  function toggleOnlinePanel(e) {                  // the same panel as the 2D lobbies: who is here, and who is online
    e.stopPropagation();
    var p = $('ew-online-panel');
    if (!p) {
      p = document.createElement('div'); p.id = 'ew-online-panel';
      p.style.cssText = 'display:none;position:fixed;top:52px;right:14px;z-index:1000;background:var(--bg-elevated);border:1px solid var(--accent);border-radius:var(--radius-md);box-shadow:0 8px 24px rgba(0,0,0,0.6);padding:10px 12px;min-width:220px;max-width:280px;max-height:420px;overflow-y:auto;font-size:12px;';
      G.view.appendChild(p);
    }
    if (p.style.display === 'block') { p.style.display = 'none'; return; }
    p.style.display = 'block';
    var meName = String(G.opts.username || ''), here = [meName].concat(Array.from(G.others.values()).map(function (o) { return o.name; })).filter(Boolean), g0 = G;
    function row(n) { var me = n.toLowerCase() === meName.toLowerCase(); return '<div style="padding:3px 4px;border-radius:3px;' + (me ? 'background:var(--accent-dim);color:var(--accent);font-weight:700' : '') + '">' + esc(n) + '</div>'; }
    function draw(all) {
      p.innerHTML = '<div style="font-weight:700;color:var(--accent);margin-bottom:4px;">In EzWorld (' + here.length + ')</div><div style="margin-bottom:8px;">' + here.map(row).join('') + '</div>' +
        '<div style="font-weight:700;color:var(--accent);margin-bottom:4px;border-top:1px solid var(--border);padding-top:6px;">All Online' + (all ? ' (' + all.length + ')' : '') + '</div><div>' + (all ? (all.length ? all.map(row).join('') : '<div style="color:var(--text-muted);font-size:11px;">No one</div>') : '<div style="color:var(--text-muted);font-size:11px;">Loading...</div>') + '</div>';
    }
    draw(null);
    try {
      var base = String(G.opts.wsUrl || '').replace(/^ws/, 'http').replace(/\/ws\/.*$/, '');
      fetch(base + '/api/online-summary').then(function (r) { return r.json(); }).then(function (d) { if (G === g0 && p.style.display === 'block') draw(Array.isArray(d.users) ? d.users : []); }).catch(function () { if (G === g0 && p.style.display === 'block') draw([]); });
    } catch (er) { draw([]); }
  }
  function sendPos(force) {
    if (!G.me || G.heli || G.jet || G.car || !G.ws || G.ws.readyState !== 1) return;
    var now = performance.now();
    if (now - G.lastSend < (force ? 40 : 100)) return;
    var b = new ArrayBuffer(9), dv = new DataView(b), m = G.me;
    var qx = Math.round(m.x * 16), qy = Math.round(m.y * 16), qz = Math.round(m.z * 16);
    var yaw = Math.floor(((m.yaw % (2 * Math.PI) + 2 * Math.PI) % (2 * Math.PI)) / (2 * Math.PI) * 256) & 255;
    var pit = Math.max(0, Math.min(255, Math.round((m.pitch + Math.PI / 2) / Math.PI * 255)));
    var key = qx + ',' + qy + ',' + qz + ',' + yaw + ',' + pit;
    if (key === G.lastSent && !force) return;
    G.lastSent = key; G.lastSend = now;
    dv.setUint8(0, 1); dv.setInt16(1, qx, true); dv.setInt16(3, qy, true); dv.setInt16(5, qz, true); dv.setUint8(7, yaw); dv.setUint8(8, pit);
    G.ws.send(b);
  }

  // ---- chat box: messages are drawn by the same code as the 2D lobby chat (rank, VIP, clan tag, name colour, links, @mentions,
  // reply quotes), plus replies, reactions, a typing indicator and @name suggestions
  function sysLine(text) { var d = document.createElement('div'); d.className = 'msg ew-sys'; d.textContent = text; $('ew-messages').appendChild(d); trimScroll(); }
  function sys(text) { sysLine(text); }
  function trimScroll() { var box = $('ew-messages'); while (box.children.length > 200) box.removeChild(box.firstChild); box.scrollTop = box.scrollHeight; }
  function send(o) { if (G && G.ws && G.ws.readyState === 1) G.ws.send(JSON.stringify(o)); }
  function chatLine(m) {
    var el;
    if (window.EzChat) {
      el = window.EzChat.build(m.from, m.text, false, m.is_vip, m.rank, m.clan, m.reply_to, m.name_color, !!m.discord, {
        noModerate: true, onReply: setReply,
        onReact: m.mid != null ? function (emoji) { send({ t: 'react', mid: m.mid, emoji: emoji }); } : null });
    } else { el = document.createElement('div'); el.className = 'msg'; el.textContent = m.from + ': ' + m.text; }
    if (m.mid != null) { G.msgEls.set(m.mid, el); if (G.msgEls.size > 200) G.msgEls.delete(G.msgEls.keys().next().value); }
    $('ew-messages').appendChild(el); trimScroll();
    if (String(m.from).toLowerCase() !== String(G.opts.username || '').toLowerCase()) {
      G.typers.delete(String(m.from).toLowerCase()); renderTyping();
      if (el.classList && el.classList.contains('mention-self')) { try { if (window.playNotifSound) window.playNotifSound(); } catch (e) {} }
      try { if (window.ezpMarkUnseen) window.ezpMarkUnseen(); } catch (e) {}
    }
  }
  function setReply(from, text) {
    G.replyTo = { from: from, text: text };
    $('ew-reply-text').textContent = 'Replying to ' + from + ': ' + String(text).slice(0, 80);
    $('ew-reply-bar').style.display = 'flex';
    if (document.exitPointerLock) document.exitPointerLock();
    $('ew-chat-input').focus();
  }
  function clearReply() { G.replyTo = null; $('ew-reply-bar').style.display = 'none'; }
  function sendChat() {
    var inp = $('ew-chat-input'), t = inp.value.trim(); inp.value = '';
    hideMention(); stopTyping();
    if (!t) return;
    var p = { t: 'chat', text: t };
    if (G.replyTo && t.charAt(0) !== '/') p.reply_to = { from: G.replyTo.from, text: G.replyTo.text.slice(0, 120) };
    send(p); clearReply();
  }
  // typing indicator
  function sendTyping(state) { if (state === G.myTyping) return; G.myTyping = state; send({ t: 'typing', typing: state }); }
  function stopTyping() { if (!G) return; clearTimeout(G.typeTimer); sendTyping(false); }
  function notifyTyping() {
    var v = $('ew-chat-input').value;
    if (!v.trim() || v.charAt(0) === '/') { stopTyping(); return; }
    sendTyping(true); clearTimeout(G.typeTimer); G.typeTimer = setTimeout(stopTyping, 4000);
  }
  function renderTyping() {
    if (!G) return;
    var now = Date.now(), names = [];
    G.typers.forEach(function (v, k) { if (v.exp < now) G.typers.delete(k); else names.push(v.name); });
    var el = $('ew-typing');
    if (!names.length) { el.style.display = 'none'; return; }
    $('ew-typing-text').textContent = names.length === 1 ? names[0] + ' is typing...' : names.length === 2 ? names[0] + ' and ' + names[1] + ' are typing...' : 'Several people are typing...';
    el.style.display = '';
  }
  // @name suggestions, same look as the lobby chat
  function updateMention() {
    var inp = $('ew-chat-input'), v = inp.value, caret = inp.selectionStart == null ? v.length : inp.selectionStart;
    var mm = /@([A-Za-z0-9]{0,20})$/.exec(v.slice(0, caret));
    if (!mm) { hideMention(); return; }
    var q = mm[1].toLowerCase(), names = [];
    G.others.forEach(function (p) { if (p.name.toLowerCase().indexOf(q) === 0) names.push(p.name); });
    names.sort(); names = names.slice(0, 8);
    if (!names.length) { hideMention(); return; }
    var keep = G.mention && G.mention.names.join() === names.join() ? G.mention.i : 0;
    G.mention = { start: caret - mm[0].length, names: names, i: Math.min(keep, names.length - 1) };
    var box = $('ew-mention'); box.innerHTML = '';
    names.forEach(function (n, k) {
      var it = document.createElement('div'); it.className = 'mention-item' + (k === G.mention.i ? ' active' : '');
      var at = document.createElement('span'); at.className = 'at'; at.textContent = '@'; it.appendChild(at); it.appendChild(document.createTextNode(n));
      it.onmousedown = function (e) { e.preventDefault(); insertMention(n); };
      box.appendChild(it);
    });
    box.style.display = 'block';
  }
  function hideMention() { if (!G) return; G.mention = null; $('ew-mention').style.display = 'none'; }
  function insertMention(name) {
    if (!G.mention) return;
    var inp = $('ew-chat-input'), v = inp.value, caret = inp.selectionStart == null ? v.length : inp.selectionStart;
    var before = v.slice(0, G.mention.start), after = v.slice(caret), ins = '@' + name + ' ';
    inp.value = before + ins + after;
    var pos = (before + ins).length; inp.setSelectionRange(pos, pos);
    hideMention(); inp.focus();
  }

  // ---- rocket launcher: the shop, equipping, firing, and drawing rockets and explosions. The server owns the money, the ammo and the damage.
  function fmt(n) { return String(Math.round(n)).replace(/\B(?=(\d{3})+(?!\d))/g, ','); }
  function setTotal(n) { $('ew-online-total').textContent = String(n); }
  function applyInv(m) {
    var i = G.inv; i.rockets = m.rockets | 0; i.armed = !!m.armed && i.rockets > 0 && !G.drone && !G.heli && !G.jet && !G.car;
    if (m.price) i.price = m.price; if (m.per) i.per = m.per; if (m.max) i.max = m.max;
    i.drone = !!m.drone; if (m.dprice) i.dprice = m.dprice; i.flying = !!m.flying;
    i.dlevel = m.dlevel | 0; i.dupg = m.dupg === undefined ? null : m.dupg; i.dmult = m.dmult || 1;
    i.heli = m.heli || {}; if (m.hp) i.hp = m.hp; i.hfly = m.hfly || null;
    i.jet = m.jet || {}; if (m.jp) i.jp = m.jp; i.jfly = m.jfly || null;
    i.car = m.car || {}; if (m.cp) i.cp = m.cp; i.ccid = m.ccid === undefined ? null : m.ccid; i.cdrive = m.cdrive || null;
    renderInv();
  }
  function renderInv() {
    var i = G.inv;
    $('ew-hotbar').style.display = i.rockets > 0 ? 'flex' : 'none';
    $('ew-hotbar').classList.toggle('sel', i.armed);
    $('ew-hotbar-count').textContent = String(i.rockets);
    $('ew-hotbar2').style.display = i.drone ? 'flex' : 'none';
    $('ew-hotbar2').classList.toggle('sel', !!G.drone);
    $('ew-held').style.display = i.armed && !G.drone ? 'block' : 'none';
    $('ew-shop-price').textContent = fmt(i.price) + ' PlaceBux';
    $('ew-shop-have').textContent = i.rockets > 0 ? 'You have ' + i.rockets + ' rocket' + (i.rockets === 1 ? '' : 's') + '.' : 'You have no rockets yet.';
    var eq = $('ew-shop-equip'); eq.textContent = i.armed ? 'Put away' : 'Equip'; eq.disabled = i.rockets <= 0;
    var buy = $('ew-shop-buy'); buy.disabled = i.rockets + i.per > i.max;
    if (!G.buyArm) buy.textContent = buy.disabled ? 'Carrying the most' : 'Buy';
    $('ew-shop-dprice').textContent = fmt(i.dprice) + ' PlaceBux';
    $('ew-shop-dhave').textContent = i.drone ? 'You own one. Press 2 to launch it.' : '';
    var db = $('ew-shop-dbuy'); db.disabled = i.drone;
    if (!G.dbuyArm) db.textContent = i.drone ? 'Owned' : 'Buy';
    var dl = $('ew-shop-dlaunch'); dl.disabled = !i.drone; dl.textContent = G.drone ? 'Land' : 'Launch';
    var du = $('ew-shop-dupg'), dv2 = $('ew-shop-dlvl');
    if (du && dv2) {
      var lvl = i.dlevel | 0, top = Math.round(45 * (i.dmult || 1) * 3.6);
      dv2.textContent = i.drone ? 'Speed level ' + lvl + ' (top speed ' + top + ' km/h)' : '';
      du.style.display = i.drone ? '' : 'none'; du.disabled = i.dupg === null || i.dupg === undefined;
      if (!G.dupgArm) du.textContent = i.dupg == null ? 'Fully upgraded' : 'Upgrade speed: ' + fmt(i.dupg);
    }
    var hb3 = $('ew-hotbar3');
    if (hb3) {
      var hid = G.hsel && (i.heli || {})[G.hsel] ? G.hsel : (HELI_IDS.filter(function (k) { return (i.heli || {})[k]; }).pop() || '');
      hb3.style.display = hid ? 'flex' : 'none'; hb3.classList.toggle('sel', !!G.heli);
      $('ew-hotbar3-name').textContent = hid ? HELIS[hid].name : '';
    }
    var hb4 = $('ew-hotbar4');
    if (hb4) {
      var jid = G.jsel && (i.jet || {})[G.jsel] ? G.jsel : (JET_IDS.filter(function (k) { return (i.jet || {})[k]; }).pop() || '');
      hb4.style.display = jid ? 'flex' : 'none'; hb4.classList.toggle('sel', !!G.jet);
      $('ew-hotbar4-name').textContent = jid ? JETS[jid].name : '';
    }
    var hb5 = $('ew-hotbar5');
    if (hb5) {
      var cid = G.csel && (i.car || {})[G.csel] ? G.csel : (CAR_IDS.filter(function (k) { return (i.car || {})[k]; }).pop() || '');
      hb5.style.display = cid ? 'flex' : 'none'; hb5.classList.toggle('sel', !!G.car);
      $('ew-hotbar5-name').textContent = cid ? CARS[cid].name : '';
    }
    if (shopOpen()) { renderHeliShop(); renderJetShop(); renderCarShop(); }
  }
  function balance() { try { return typeof myBalance !== 'undefined' ? myBalance : 0; } catch (e) { return 0; } }
  function shopOpen() { return $('ew-shop').style.display === 'flex'; }
  function refreshShop() {
    $('ew-shop-bal').textContent = 'PlaceBux: ' + fmt(balance());
    if (G.buyArm && performance.now() > G.buyArm) { G.buyArm = 0; renderInv(); }
    if (G.dbuyArm && performance.now() > G.dbuyArm) { G.dbuyArm = 0; renderInv(); }
    if (G.hArm && performance.now() > G.hArmT) { G.hArm = ''; renderInv(); }
    if (G.jArm && performance.now() > G.jArmT) { G.jArm = ''; renderInv(); }
    if (G.cArm && performance.now() > G.cArmT) { G.cArm = ''; renderInv(); }
    if (G.dupgArm && performance.now() > G.dupgArm) { G.dupgArm = 0; renderInv(); }
  }
  function shopUpgradeDrone() {                                  // another level, two clicks, as long as you can pay
    var i = G.inv, now = performance.now();
    if (!i.drone || i.dupg == null) return;
    if (G.dupgArm && now < G.dupgArm) { G.dupgArm = 0; send({ t: 'buy', item: 'drone_speed' }); renderInv(); return; }
    if (balance() < i.dupg) { $('ew-shop-note').textContent = 'The next upgrade costs ' + fmt(i.dupg) + ' PlaceBux (you have ' + fmt(balance()) + ').'; return; }
    $('ew-shop-note').textContent = ''; G.dupgArm = now + 3000; $('ew-shop-dupg').textContent = 'Click again to confirm';
  }
  function shopBuyDrone() {                                      // two clicks here too
    var i = G.inv, now = performance.now();
    if (i.drone) return;
    if (G.dbuyArm && now < G.dbuyArm) { G.dbuyArm = 0; send({ t: 'buy', item: 'drone' }); renderInv(); return; }
    if (balance() < i.dprice) { $('ew-shop-note').textContent = 'You need ' + fmt(i.dprice) + ' PlaceBux (you have ' + fmt(balance()) + ').'; return; }
    $('ew-shop-note').textContent = ''; G.dbuyArm = now + 3000; $('ew-shop-dbuy').textContent = 'Click again to confirm';
  }
  function openShop() {
    if (document.exitPointerLock) document.exitPointerLock();
    $('ew-shop').style.display = 'flex'; $('ew-shop-note').textContent = '';
    renderInv(); refreshShop(); clearInterval(G.shopTimer); G.shopTimer = setInterval(refreshShop, 500);
  }
  function closeShop() { $('ew-shop').style.display = 'none'; clearInterval(G.shopTimer); G.buyArm = 0; G.dbuyArm = 0; G.dupgArm = 0; G.hArm = ''; G.jArm = ''; G.cArm = ''; renderInv(); }
  function shopBuy() {                                           // two clicks, so 5,000 PlaceBux can't go on one slip
    var i = G.inv, now = performance.now();
    if (i.rockets + i.per > i.max) return;
    if (G.buyArm && now < G.buyArm) { G.buyArm = 0; send({ t: 'buy', item: 'launcher' }); renderInv(); return; }
    if (balance() < i.price) { $('ew-shop-note').textContent = 'You need ' + fmt(i.price) + ' PlaceBux (you have ' + fmt(balance()) + ').'; return; }
    $('ew-shop-note').textContent = ''; G.buyArm = now + 3000; $('ew-shop-buy').textContent = 'Click again to confirm';
  }
  function setArmed(on) {
    if (on && G.inv.rockets <= 0) { sys('You have no rockets. Buy a rocket launcher in the shop (B).'); return; }
    G.inv.armed = !!on; renderInv(); send({ t: 'equip', on: !!on });
  }
  function fireRocket() {
    var m = G.me, now = performance.now(), i = G.inv;
    if (!i.armed || i.rockets <= 0 || now - G.lastFire < 1200) return;
    G.lastFire = now;
    var cp = Math.cos(m.pitch);
    send({ t: 'fire', d: [-Math.sin(m.yaw) * cp, Math.sin(m.pitch), -Math.cos(m.yaw) * cp] });
    i.rockets -= 1; if (i.rockets <= 0) i.armed = false;          // the server confirms with the real numbers right after
    renderInv();
    var h = $('ew-held'); h.classList.add('fire'); setTimeout(function () { h.classList.remove('fire'); }, 130);
  }
  function addRocket(m) { G.rks.push({ o: m.o, e: m.e, s: m.s || 45, t0: performance.now(), el: null }); }
  function addBoom(e, scale) {
    G.booms.push({ p: e, t0: performance.now(), el: null, s: scale || 1 });
    if (G.aud && G.cw) { var bc = G.drone || G.hpose || G.jpose || G.cpose || G.me; if (bc) boomSound(Math.hypot(e[0] - bc.x, e[1] - bc.y, e[2] - bc.z), scale); }
    if (G.me) { var dx = e[0] - G.me.x, dz = e[2] - G.me.z; if (dx * dx + dz * dz < 900) G.shake = performance.now() + 350; }
  }
  function drawFx(vp, focal) {
    var now = performance.now(), layer = $('ew-fx'), i, sp;
    for (i = G.rks.length - 1; i >= 0; i--) {
      var r = G.rks[i], dx = r.e[0] - r.o[0], dy = r.e[1] - r.o[1], dz = r.e[2] - r.o[2], len = Math.sqrt(dx * dx + dy * dy + dz * dz) || 1;
      var f = (now - r.t0) / (len / r.s * 1000);
      if (f >= 1) { if (r.el) r.el.remove(); G.rks.splice(i, 1); continue; }
      var off = Math.min(0.8 / len, 0.5), g = off + f * (1 - off);   // starts a little in front of the shooter, so it is seen leaving the launcher
      sp = project(vp, r.o[0] + dx * g, r.o[1] + dy * g - (1 - g) * 0.25, r.o[2] + dz * g, G.cw, G.ch);
      if (!sp) { if (r.el) r.el.style.display = 'none'; continue; }
      if (!r.el) { r.el = document.createElement('div'); r.el.className = 'ew-rocket'; layer.appendChild(r.el); }
      var g2 = Math.min(1, g + 0.02), sp2 = project(vp, r.o[0] + dx * g2, r.o[1] + dy * g2 - (1 - g2) * 0.25, r.o[2] + dz * g2, G.cw, G.ch);
      if (sp2 && Math.hypot(sp2.x - sp.x, sp2.y - sp.y) > 0.4) r.ang = Math.atan2(sp2.y - sp.y, sp2.x - sp.x);
      var rw = Math.max(16, Math.min(150, focal * 1.7 / sp.w));
      r.el.style.display = 'block'; r.el.style.width = rw.toFixed(1) + 'px'; r.el.style.height = (rw * 9 / 22).toFixed(1) + 'px';
      r.el.style.transform = 'translate(' + (sp.x | 0) + 'px,' + (sp.y | 0) + 'px) translate(-50%,-50%) rotate(' + (r.ang || 0).toFixed(3) + 'rad)';
    }
    for (i = G.booms.length - 1; i >= 0; i--) {
      var b = G.booms[i], bf = (now - b.t0) / 700;
      if (bf >= 1) { if (b.el) b.el.remove(); G.booms.splice(i, 1); continue; }
      sp = project(vp, b.p[0], b.p[1], b.p[2], G.cw, G.ch);
      if (!sp) { if (b.el) b.el.style.display = 'none'; continue; }
      if (!b.el) { b.el = document.createElement('div'); b.el.className = 'ew-boom'; layer.appendChild(b.el); }
      var bsc = b.s || 1, bs = Math.max(40 * bsc, Math.min(900, focal * 7.5 * bsc / sp.w)), frame = Math.min(3, Math.floor(bf * 4));
      b.el.style.display = 'block'; b.el.style.width = b.el.style.height = bs.toFixed(0) + 'px';
      b.el.style.setProperty('--f', String(frame)); b.el.style.opacity = bf > 0.75 ? String(1 - (bf - 0.75) / 0.25) : '1';
      b.el.style.transform = 'translate(' + (sp.x | 0) + 'px,' + (sp.y | 0) + 'px) translate(-50%,-50%)';
    }
  }

  // ---- flying helicopters: starting, controls, camera, instruments, other pilots, wrecks and the shop cards
  var HTHUMBS = {};
  function impactFx(x, y, z, big) {
    var near = !G.me || Math.hypot(x - G.me.x, z - G.me.z) < 70 || (G.jet && G.jet.crashAt) || (G.heli && G.heli.crashAt);
    addBoom([x, y + 1, z], big ? 2.4 : 1.7); if (near) { G.flash = performance.now(); G.shake = performance.now() + (big ? 900 : 700); }
    var g0 = G;
    [150, 320, 520].forEach(function (d, i) { setTimeout(function () { if (G === g0) addBoom([x + (Math.random() - 0.5) * 8, y + 0.5 + Math.random() * 3, z + (Math.random() - 0.5) * 8], (big ? 1.5 : 1.0) - i * 0.15); }, d); });
    G.rings.push({ p: [x, y + 0.5, z], t0: performance.now(), el: null, r: big ? 46 : 30 });
    for (var i = 0; i < 8; i++) { var a = i / 8 * 6.283; G.rings.push({ p: [x + Math.cos(a) * 2, y + 0.4, z + Math.sin(a) * 2], t0: performance.now(), el: null, r: 0, dust: [Math.cos(a), Math.sin(a)] }); }
  }
  function mouseCurve(v) { return v * (0.45 + 0.55 * Math.abs(v)); }        // small movements are gentle, big ones are firm
  function heliCss() {
    if (document.getElementById('ew-heli-css')) return;
    var st = document.createElement('style'); st.id = 'ew-heli-css';
    st.textContent = '#ew-hhud{display:none;position:absolute;left:50%;bottom:92px;transform:translateX(-50%);gap:14px;padding:6px 14px;font:700 12px "JetBrains Mono","Fira Code",monospace;color:#7dff9a;text-shadow:0 0 6px rgba(0,0,0,.9);background:rgba(8,12,10,.55);border:1px solid rgba(125,255,154,.35);border-radius:8px;pointer-events:none;flex-wrap:wrap;justify-content:center;max-width:94%}' +
      '#ew-hhud b{color:#fff}#ew-hhud .st{color:#ffd166}#ew-hhud .bad{color:#ff6b5e}#ew-hhud.cockpit{background:rgba(8,12,10,.8)}' +
      '#ew-hhelp{display:none;position:absolute;left:50%;bottom:62px;transform:translateX(-50%);font-size:11px;color:#fff;opacity:.75;text-shadow:0 0 4px #000;pointer-events:none;text-align:center;max-width:94%}' +
      '.ew-htag{position:absolute;left:0;top:0;font-size:12px;font-weight:700;color:#fff;text-shadow:0 0 4px #000;pointer-events:none;white-space:nowrap}' +
      '.ew-hudbox{display:none;position:absolute;left:50%;bottom:92px;transform:translateX(-50%);gap:14px;padding:6px 14px;font:700 12px "JetBrains Mono","Fira Code",monospace;color:#7dff9a;text-shadow:0 0 6px rgba(0,0,0,.9);background:rgba(8,12,10,.55);border:1px solid rgba(125,255,154,.35);border-radius:8px;pointer-events:none;flex-wrap:wrap;justify-content:center;max-width:94%}' +
      '.ew-hudbox b{color:#fff}.ew-hudbox .st{color:#ffd166}.ew-hudbox .bad{color:#ff6b5e}' +
      '.ew-hhelp2{display:none;position:absolute;left:50%;bottom:62px;transform:translateX(-50%);font-size:11px;color:#fff;opacity:.75;text-shadow:0 0 4px #000;pointer-events:none;text-align:center;max-width:94%}' +
      '#ew-hotbar4{display:none;align-items:center;gap:8px;padding:6px 12px;background:rgba(12,12,16,.62);border:2px solid rgba(255,255,255,.18);border-radius:10px;color:#fff;font-weight:700;font-size:13px}#ew-hotbar4.sel{border-color:var(--accent);box-shadow:0 0 0 2px var(--accent-dim)}#ew-hotbar4 .key{font-size:11px;opacity:.7}' +
      '#ew-shop-jets{padding:0 16px 6px;display:flex;flex-direction:column;gap:10px}#ew-shop-jets h4{margin:6px 0 0;font-size:13px;color:var(--text-secondary);text-transform:uppercase;letter-spacing:.06em}' +
      '.ew-debris.burn{background:#ff8a1e;border-color:#ffd070;box-shadow:0 0 10px 3px rgba(255,120,20,.7)}' +
      '#ew-fpv{position:absolute;inset:0;pointer-events:none;z-index:2;overflow:hidden;font:700 13px "JetBrains Mono","Fira Code",monospace;color:#fff;text-shadow:1px 1px 0 #000,-1px 1px 0 #000,1px -1px 0 #000,-1px -1px 0 #000}' +
      '#ew-fpv .fp-noise{position:absolute;inset:-20px;opacity:.05;background:url("data:image/svg+xml;utf8,<svg xmlns=\'http://www.w3.org/2000/svg\' width=\'160\' height=\'160\'><filter id=\'n\'><feTurbulence type=\'fractalNoise\' baseFrequency=\'.85\' numOctaves=\'2\' stitchTiles=\'stitch\'/></filter><rect width=\'100%25\' height=\'100%25\' filter=\'url(%23n)\'/></svg>")}' +
      '#ew-fpv .fp-lines{position:absolute;inset:0;background:repeating-linear-gradient(0deg,rgba(0,0,0,.10) 0,rgba(0,0,0,.10) 1px,transparent 1px,transparent 3px)}' +
      '#ew-fpv .fp-hurt{position:absolute;inset:0;opacity:0;background:radial-gradient(ellipse at center,rgba(255,0,0,0) 28%,rgba(255,30,20,.8) 100%)}' +
      '#ew-fpv .fp-vig{position:absolute;inset:0;background:radial-gradient(ellipse at center,transparent 48%,rgba(0,0,0,.55) 82%,rgba(0,0,0,.92) 100%)}' +
      '#ew-fpv .fp-hor{position:absolute;left:50%;top:50%;width:0;height:0}' +
      '#ew-fpv .fp-hor i{position:absolute;left:0;height:2px;background:#fff;box-shadow:0 0 0 1px #000}' +
      '#ew-fpv .fp-top{position:absolute;left:0;right:0;top:12px;display:flex;justify-content:space-between;padding:0 28px}' +
      '#ew-fpv .fp-bot{position:absolute;left:0;right:0;bottom:16px;display:flex;justify-content:center;gap:26px;padding:0 20px;flex-wrap:wrap}' +
      '#ew-fpv .fp-bat{position:absolute;left:28px;bottom:16px}#ew-fpv .fp-rec{color:#ff4d4d}#ew-fpv .fp-bad{color:#ff5a4d}' +
      '.ew-ring{position:absolute;left:0;top:0;border-radius:50%;border:5px solid rgba(255,225,170,.9);box-shadow:0 0 30px 8px rgba(255,150,50,.5),inset 0 0 30px rgba(255,150,50,.4);pointer-events:none}' +
      '.ew-debris{position:absolute;left:0;top:0;background:#2a2a2d;border:1px solid #0a0a0b;pointer-events:none}' +
      '.ew-smoke{position:absolute;left:0;top:0;border-radius:50%;background:radial-gradient(circle,rgba(40,40,44,.85),rgba(40,40,44,0) 70%);pointer-events:none}' +
      '#ew-hotbar3{display:none;align-items:center;gap:8px;padding:6px 12px;background:rgba(12,12,16,.62);border:2px solid rgba(255,255,255,.18);border-radius:10px;color:#fff;font-weight:700;font-size:13px}' +
      '#ew-hotbar3.sel{border-color:var(--accent);box-shadow:0 0 0 2px var(--accent-dim)}#ew-hotbar3 .key{font-size:11px;opacity:.7}' +
      '#ew-shop-helis{padding:0 16px 6px;display:flex;flex-direction:column;gap:10px}' +
      '#ew-shop-helis h4{margin:6px 0 0;font-size:13px;color:var(--text-secondary);text-transform:uppercase;letter-spacing:.06em}' +
      '.ew-hcard{display:flex;gap:12px;align-items:center;flex-wrap:wrap;padding:10px;border:1px solid var(--border);border-radius:10px;background:var(--bg-secondary)}' +
      '.ew-hcard img{width:150px;height:80px;object-fit:contain;flex-shrink:0;border-radius:8px;background:rgba(0,0,0,.18)}' +
      '.ew-hcard .nfo{flex:1;min-width:190px;display:flex;flex-direction:column;gap:3px}.ew-hcard .nfo b{font-size:14px}.ew-hcard .nfo small{font-size:11.5px;color:var(--text-secondary);line-height:1.35}' +
      '.ew-hcard .nfo span{font-size:12px;color:var(--gold);font-weight:600}.ew-hcard .act{display:flex;flex-direction:column;gap:6px;min-width:128px;text-align:center}' +
      '.ew-hcard .act .win-btn:disabled{opacity:.45;cursor:not-allowed}#ew-shop .ew-shop-card{max-height:92vh;overflow-y:auto}';
    document.head.appendChild(st);
  }
  var HELI_SRC = (typeof document !== 'undefined' && document.currentScript && document.currentScript.src || '').replace(/ezworld.js.*$/, 'heli_gl.js');
  function heliRenderer() {                       // the helicopter models are a separate file, fetched the first time a helicopter matters (the shop, flying, or seeing one)
    if (G.hr) return G.hr;
    if (G.hrFail) return null;
    if (window.HeliGL) {
      try { G.hr = window.HeliGL.create(G.gl); } catch (e) { G.hr = null; G.hrFail = true; }
      return G.hr;
    }
    if (!G.hrLoading && HELI_SRC) {
      G.hrLoading = true; var g0 = G, sc = document.createElement('script');
      sc.src = HELI_SRC; sc.onload = function () { if (G === g0) { g0.hrLoading = false; renderInv(); } };
      sc.onerror = function () { g0.hrFail = true; };
      document.head.appendChild(sc);
    }
    return null;
  }
  function myHeliBest() {                       // the one the 3 key starts when you have not picked another
    var own = G.inv.heli || {}, best = null;
    HELI_IDS.forEach(function (id) { if (own[id] === 1) best = id; });
    return best;
  }
  function toggleHeli() {
    if (G.heli) { if (!G.heli.onGround) { sys('Land the helicopter first, then press 3 to get out.'); return; } send({ t: 'heli', on: false }); return; }
    if (G.drone || G.jet || G.car) return;
    var own = G.inv.heli || {}, id = G.hsel && own[G.hsel] === 1 ? G.hsel : myHeliBest();
    if (!id) { sys('You do not own a helicopter. Buy one in the shop (B).'); return; }
    if (!G.me || !G.me.onGround) { sys('Stand on the ground to start a helicopter.'); return; }
    G.hsel = id; send({ t: 'heli', on: true, m: id });
  }
  function startHeli(m) {
    var sp = hspec(m.m);
    G.heli = { m: m.m, x: m.x, y: m.y + 0.03, z: m.z, vx: 0, vy: 0, vz: 0, yaw: G.me.yaw, wy: 0, pt: 0, rl: 0, rpm: 0, onGround: true, ang: 0, crashAt: 0, why: '', lastSent: '', sent: 0, view: 0 };
    G.hcam = { yaw: 0, pitch: -0.22, dist: sp.chase, idle: 0 };
    G.hpose = { x: m.x, y: m.y + 3, z: m.z, yaw: G.me.yaw, pitch: -0.22 };
    G.inv.armed = false; G.hsel = m.m;
    heliCss(); heliHud(true);
    $('ew-cross').style.display = 'none';
    sys('Starting the ' + sp.name + '. When the rotor is up to speed press Space to lift. W/S pitch, A/D bank, Q/E turn, Space up, Shift down, V changes the camera, 3 gets you out once landed.');
    renderInv();
  }
  function endHeli(m) {
    var h = G.heli, eye0 = G.hpose ? [G.hpose.x, G.hpose.y, G.hpose.z] : null; G.heli = null; G.hpose = null;
    var hw = h && (h.wreck || (m && m.crash ? G.wrecks[G.wrecks.length - 1] : null));       // a crash the server decided (a collision) has its wreck from the broadcast
    if (m && m.crash && hw) G.cfollow = { w: hw, until: performance.now() + 3800, eye: eye0 };
    heliHud(false); $('ew-cross').style.display = '';
    if (G.me && m) { G.me.x = m.x; G.me.y = m.y; G.me.z = m.z; G.me.vx = G.me.vy = G.me.vz = 0; G.me.onGround = false; }
    G.bodyWait = true;
    if (m && m.crash) { sys('You crashed the ' + (HELIS[m.m] ? HELIS[m.m].name : 'helicopter') + (h && h.why ? ' (' + h.why.replace(/\.$/, '').toLowerCase() + ')' : '') + '. It is destroyed and gone from your inventory.'); G.shake = performance.now() + 450; }
    renderInv();
  }
  function heliHud(on) {
    var el = $('ew-hhud');
    if (!el) {
      el = document.createElement('div'); el.id = 'ew-hhud'; G.view.appendChild(el);
      var hp = document.createElement('div'); hp.id = 'ew-hhelp'; hp.textContent = 'W/S pitch   A/D bank   Q/E turn   Space up   Shift down   V camera   Wheel zoom   3 exit'; G.view.appendChild(hp);
    }
    el.style.display = on ? 'flex' : 'none'; $('ew-hhelp').style.display = on ? 'block' : 'none';
  }
  function sendHeliPos(force) {
    var h = G.heli; if (!h || !G.ws || G.ws.readyState !== 1) return;
    var qx = Math.round(h.x * 16), qy = Math.round(h.y * 16), qz = Math.round(h.z * 16);
    var yaw = Math.floor(((h.yaw % (2 * Math.PI) + 2 * Math.PI) % (2 * Math.PI)) / (2 * Math.PI) * 256) & 255;
    var pit = Math.max(1, Math.min(255, Math.round(h.pt / (Math.PI / 2) * 127 + 128))), rol = Math.max(1, Math.min(255, Math.round(h.rl / (Math.PI / 2) * 127 + 128)));
    var key = qx + ',' + qy + ',' + qz + ',' + yaw + ',' + pit + ',' + rol;
    if (key === h.lastSent && !force) return;
    h.lastSent = key; h.sent = performance.now();
    var b = new ArrayBuffer(10), dv = new DataView(b);
    dv.setUint8(0, 3); dv.setInt16(1, qx, true); dv.setInt16(3, qy, true); dv.setInt16(5, qz, true); dv.setUint8(7, yaw); dv.setUint8(8, pit); dv.setUint8(9, rol);
    G.ws.send(b);
  }
  function flyHeli(dt) {
    var h = G.heli, k = G.keys, now = performance.now(), sp = hspec(h.m);
    if (h.crashAt && now - h.crashAt > 1500) {                         // the server did not agree it was a crash: carry on, and if that keeps happening get you out
      h.crashAt = 0; h.tries = (h.tries || 0) + 1; if (h.wreck) { dropWreck(h.wreck); h.wreck = null; }
      if (h.tries >= 2) { h.tries = 0; sys('Stuck? Moving you out of the helicopter.'); send({ t: 'chat', text: '/unstuck' }); }
    }
    var inp = (h.crashAt || !locked()) ? {} : { f: k.KeyW || k.ArrowUp, b: k.KeyS || k.ArrowDown, l: k.KeyA || k.ArrowLeft, r: k.KeyD || k.ArrowRight, yl: k.KeyQ, yr: k.KeyE, up: k.Space, down: k.ShiftLeft || k.ShiftRight };
    var res = h.crashAt ? 'ok' : stepHeli(h, inp, dt, sp);
    if (res === 'crash' && !h.crashAt) {
      h.crashAt = now; var hv = h.cv || [0, 0, 0]; G.hcrashState = { vx: hv[0], vy: hv[1], vz: hv[2] };
      sendHeliPos(true); send({ t: 'hcrash' });
      h.wreck = addWreck({ id: G.myId, m: h.m, x: h.x, y: h.y + 0.3, z: h.z, local: true });
      impactFx(h.x, h.y, h.z, false);
    } else if (now - h.sent >= 50 || (now - h.sent >= 40 && res !== 'ok')) sendHeliPos(false);
    h.ang += dt * 40 * h.rpm;
    var me = G.me; me.x = h.x; me.y = h.y; me.z = h.z; me.yaw = h.yaw;
    // camera: chase (orbits the helicopter) or cockpit (the pilot's eyes)
    var hc = G.hcam, hp = G.hpose;
    hc.idle += dt; if (hc.idle > 1.2 && h.view === 0) hc.yaw *= Math.max(0, 1 - dt * 1.2);
    if (h.view === 0) {
      var ya = h.yaw + hc.yaw, cp = Math.cos(hc.pitch), tg = h.wreck || h, tx = tg.x, ty = tg.y + sp.top * 0.55, tz = tg.z;
      var dx = Math.sin(ya) * cp, dy = -Math.sin(hc.pitch), dz = Math.cos(ya) * cp, want = hc.dist;      // from the helicopter toward the camera
      var hit = raycast(tx, ty, tz, dx, dy, dz, want + 0.5); if (hit && hit.t < want) want = Math.max(1.2, hit.t - 0.5);
      hp.x = tx + dx * want; hp.y = ty + dy * want; hp.z = tz + dz * want; hp.yaw = ya; hp.pitch = hc.pitch;
    } else {
      var e = hpt(h, sp, sp.eye[0], sp.eye[2] * -1 * -1, sp.eye[1]);
      hp.x = e[0]; hp.y = e[1]; hp.z = e[2]; hp.yaw = h.yaw + hc.yaw; hp.pitch = hc.pitch;
    }
    // instruments
    var el = $('ew-hhud'), spd = Math.sqrt(h.vx * h.vx + h.vz * h.vz), gd = h.onGround ? 0 : heliGround(h), status = h.crashAt ? '<span class="bad">CRASH</span>' : h.rpm < HELI_READY ? '<span class="st">ENGINE START ' + Math.round(h.rpm * 100) + '%</span>' : h.onGround ? '<span class="st">READY: Space to lift</span>' : '';
    var html = '<span>' + esc(sp.name) + '</span><span>IAS <b>' + Math.round(spd * 1.944) + '</b> kt</span><span>ALT <b>' + Math.round(h.y) + '</b></span><span>AGL <b>' + (gd < 0 ? '--' : Math.round(gd)) + '</b></span><span>VS <b>' + (h.vy >= 0 ? '+' : '') + h.vy.toFixed(1) + '</b></span><span>RPM <b>' + Math.round(h.rpm * 100) + '</b>%</span>' + (status ? '<span>' + status + '</span>' : '');
    if (html !== h.hudHtml) { h.hudHtml = html; el.innerHTML = html; }
    el.classList.toggle('cockpit', h.view === 1);
  }
  function esc(s) { return String(s).replace(/[&<>"]/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]; }); }
  // other pilots' helicopters
  function setOtherHeli(id, m, name, x, y, z) {
    var pl = G.others.get(id);
    var o = { id: id, m: m, name: name || (pl ? pl.name : ''), x: x, y: y, z: z, tx: x, ty: y, tz: z, yaw: 0, tyaw: 0, pt: 0, rl: 0, tpt: 0, trl: 0, ang: 0, el: null, fresh: true };
    G.helis.set(id, o); if (pl) pl.hidden = true;
  }
  function removeOtherHeli(id) {
    var o = G.helis.get(id); if (o && o.el) o.el.remove(); G.helis.delete(id);
    var pl = G.others.get(id); if (pl) { pl.hidden = false; if (o) { pl.x = pl.tx = o.x; pl.y = pl.ty = o.y; pl.z = pl.tz = o.z; } }
  }
  function onHeliFrame(dv) {
    var n = dv.getUint16(1, true), p = 3;
    for (var i = 0; i < n; i++, p += 12) {
      var id = dv.getUint16(p, true), o = G.helis.get(id), mid = HELI_IDS[dv.getUint8(p + 11)];
      if (!o) { setOtherHeli(id, mid, '', 0, 0, 0); o = G.helis.get(id); }
      o.m = mid || o.m;
      o.tx = dv.getInt16(p + 2, true) / 16; o.ty = dv.getInt16(p + 4, true) / 16; o.tz = dv.getInt16(p + 6, true) / 16;
      o.tyaw = dv.getUint8(p + 8) / 256 * 2 * Math.PI; o.tpt = (dv.getUint8(p + 9) - 128) / 127 * Math.PI / 2; o.trl = (dv.getUint8(p + 10) - 128) / 127 * Math.PI / 2;
      if (o.fresh) { o.x = o.tx; o.y = o.ty; o.z = o.tz; o.yaw = o.tyaw; o.pt = o.tpt; o.rl = o.trl; o.fresh = false; }
    }
  }
  function angDiff(a, b) { var d = (b - a) % (2 * Math.PI); if (d > Math.PI) d -= 2 * Math.PI; if (d < -Math.PI) d += 2 * Math.PI; return d; }
  // wrecks
  function wreckKick(w, a) { w.wy += (Math.random() - 0.5) * a * 2; w.wp += (Math.random() - 0.5) * a * 2; w.wr += (Math.random() - 0.5) * a * 3; }
  function newWreck(kind, m, v, a) {
    var w = { kind: kind, m: m.m, x: m.x, y: m.y, z: m.z, yaw: a.yaw, pt: a.pt, rl: a.rl, vx: v[0], vy: v[1], vz: v[2], wy: 0, wp: 0, wr: 0, down: false, t0: performance.now(), puffs: [], fire: null, nextPuff: 0, deb: [] };
    wreckKick(w, 1.2 + Math.min(3, Math.sqrt(v[0] * v[0] + v[1] * v[1] + v[2] * v[2]) * 0.04));
    for (var i = 0; i < 34; i++) w.deb.push({ x: m.x + (Math.random() - 0.5) * 4, y: m.y + Math.random() * 2, z: m.z + (Math.random() - 0.5) * 4, vx: v[0] * 0.5 + (Math.random() - 0.5) * 26, vy: 5 + Math.random() * 20, vz: v[2] * 0.5 + (Math.random() - 0.5) * 26, s: i < 7 ? 1.8 + Math.random() * 1.6 : 0.5 + Math.random() * 1.1, burn: Math.random() < 0.4, el: null, rest: false });      // wings, tail and engine pieces first, then small bits
    G.wrecks.push(w); while (G.wrecks.length > 6) killWreck(G.wrecks.shift());
    if (!m.local) impactFx(m.x, m.y, m.z, kind === 'j');                 // everybody sees the blast, however the crash happened
    return w;
  }
  function addWreck(m) {
    var own = m.id === G.myId;
    if (own && !m.local && (G.cfollow || (G.heli && G.heli.wreck))) return null;       // already shown the moment it happened
    var h = own && G.heli ? G.heli : null, st = own && m.local ? G.hcrashState : null;
    return newWreck('h', m, st ? [st.vx, st.vy, st.vz] : [0, 0, 0], { yaw: h ? h.yaw : 0, pt: h ? h.pt : 0, rl: h ? h.rl : 0 });
  }
  function wsolid(x, y, z) { return getBlock(Math.floor(x), Math.floor(y), Math.floor(z)) !== 0; }
  // A wreck is a falling, spinning body: it hits the ground and bounces, hits walls and rebounds, slides, loses speed and spin, and finally settles.
  function stepWreck(w, dt) {
    var i, d;
    for (i = 0; i < w.deb.length; i++) {
      d = w.deb[i]; if (d.rest) continue;
      d.vy -= 26 * dt; d.x += d.vx * dt; d.y += d.vy * dt; d.z += d.vz * dt;
      if (wsolid(d.x, d.y - 0.1, d.z)) { d.y = Math.floor(d.y - 0.1) + 1.1; if (d.vy < -3) { d.vy *= -0.35; d.vx *= 0.6; d.vz *= 0.6; } else d.rest = true; }
    }
    if (w.down) return;
    var R = w.kind === 'j' ? 2.2 : 1.6, spd = Math.sqrt(w.vx * w.vx + w.vy * w.vy + w.vz * w.vz), steps = Math.max(1, Math.ceil(spd * dt / 0.4)), h = dt / steps, grounded = false;
    for (i = 0; i < steps; i++) {
      w.vy = Math.max(w.vy - 26 * h, -80);
      var nx = w.x + w.vx * h, nz = w.z + w.vz * h, ny = w.y + w.vy * h;
      if (wsolid(nx + (w.vx > 0 ? R : -R), w.y + 0.9, w.z)) { w.vx *= -0.3; w.vz *= 0.7; wreckKick(w, 0.8); } else w.x = nx;
      if (wsolid(w.x, w.y + 0.9, nz + (w.vz > 0 ? R : -R))) { w.vz *= -0.3; w.vx *= 0.7; wreckKick(w, 0.8); } else w.z = nz;
      if (w.vy < 0 && wsolid(w.x, ny - 0.1, w.z)) {
        var impact = -w.vy; w.y = Math.floor(ny - 0.1) + 1.1;
        if (impact > 4) { w.vy = impact * 0.3; w.vx *= 0.8; w.vz *= 0.8; wreckKick(w, Math.min(2.5, impact * 0.08)); } else { w.vy = 0; grounded = true; }
      } else if (w.vy > 0 && wsolid(w.x, ny + 1.8, w.z)) w.vy = 0;
      else w.y = ny;
      if (w.y < 1.1) { w.y = 1.1; w.vy = 0; grounded = true; }
    }
    var fr = grounded ? Math.exp(-2.4 * dt) : 1, af = Math.exp((grounded ? -2.4 : -0.3) * dt);
    w.vx *= fr; w.vz *= fr; w.wy *= af; w.wp *= af; w.wr *= af;
    w.yaw += w.wy * dt; w.pt += w.wp * dt; w.rl += w.wr * dt;
    if (grounded && Math.sqrt(w.vx * w.vx + w.vz * w.vz) < 0.8) {
      w.down = true; w.vx = w.vz = w.wy = w.wp = w.wr = 0;
      w.pt = Math.max(-0.35, Math.min(0.35, w.pt));
      var rl = ((w.rl + Math.PI) % (2 * Math.PI) + 2 * Math.PI) % (2 * Math.PI) - Math.PI;
      w.rl = Math.abs(rl) > 1.4 ? (rl > 0 ? Math.PI : -Math.PI) : Math.max(-0.5, Math.min(0.5, rl));        // on its belly, on its side, or upside down
    }
  }
  function dropWreck(w) { killWreck(w); var i = G.wrecks.indexOf(w); if (i >= 0) G.wrecks.splice(i, 1); }
  function updateCrashCam(dt, now) {               // after a crash the camera stays with the wreck for a few seconds
    var c = G.cfollow; if (!c) return;
    if (now > c.until || G.jet || G.heli || G.drone || G.wrecks.indexOf(c.w) < 0) { G.cfollow = null; G.cpose = null; return; }
    var w = c.w, tx = w.x, ty = w.y + 1.5, tz = w.z;
    if (!c.eye) c.eye = [tx + 14, ty + 6, tz + 14];
    var dx = c.eye[0] - tx, dy = c.eye[1] - ty, dz = c.eye[2] - tz, dl = Math.sqrt(dx * dx + dy * dy + dz * dz) || 1, want = 18, k = Math.min(1, dt * 3);
    var gx = tx + dx / dl * want, gy = ty + Math.max(4, dy / dl * want), gz = tz + dz / dl * want;
    c.eye[0] += (gx - c.eye[0]) * k; c.eye[1] += (gy - c.eye[1]) * k; c.eye[2] += (gz - c.eye[2]) * k;
    var f = [tx - c.eye[0], ty - c.eye[1], tz - c.eye[2]]; jnorm(f);
    G.cpose = { x: c.eye[0], y: c.eye[1], z: c.eye[2], yaw: Math.atan2(-f[0], -f[2]), pitch: Math.asin(Math.max(-1, Math.min(1, f[1]))), f: f, u: Math.abs(f[1]) > 0.97 ? [0, 0, 1] : [0, 1, 0] };
  }
  function killWreck(w) { if (w.fire) w.fire.remove(); w.puffs.forEach(function (p) { p.el.remove(); }); (w.deb || []).forEach(function (d) { if (d.el) d.el.remove(); }); }
  function drawHelis(vp, ex, ey, ez, dt, now) {
    var needH = G.heli || G.helis.size || G.wrecks.some(function (w) { return w.kind === 'h'; });
    if (!needH && !G.wrecks.length) return;
    var r = needH ? heliRenderer() : null; if (needH && !r) return;
    var fn = RDIST * 16 - 44, ff = RDIST * 16 - 6, cam = [ex, ey, ez];
    if (G.heli && G.heli.view === 0 && !G.heli.crashAt) { var h = G.heli; r.draw(h.m, heliModelMat(h.x, h.y, h.z, h.yaw, h.pt, h.rl), vp, cam, fn, ff, h.ang, Math.max(0, Math.min(1, (h.rpm - 0.25) / 0.55)), 0); }
    G.helis.forEach(function (o) {
      var k = Math.min(1, dt * 10);
      o.x += (o.tx - o.x) * k; o.y += (o.ty - o.y) * k; o.z += (o.tz - o.z) * k;
      o.yaw += angDiff(o.yaw, o.tyaw) * k; o.pt += (o.tpt - o.pt) * k; o.rl += (o.trl - o.rl) * k; o.ang += dt * 40;
      var dx = o.x - ex, dz = o.z - ez; if (dx * dx + dz * dz > 170 * 170 || !HELIS[o.m]) return;
      r.draw(o.m, heliModelMat(o.x, o.y, o.z, o.yaw, o.pt, o.rl), vp, cam, fn, ff, o.ang, 0.9, 0);
    });
    for (var i = G.wrecks.length - 1; i >= 0; i--) {
      var w = G.wrecks[i], age = (now - w.t0) / 1000;
      if (age > 120) { killWreck(w); G.wrecks.splice(i, 1); continue; }
      var ddx = w.x - ex, ddz = w.z - ez; if (r && w.kind === 'h' && ddx * ddx + ddz * ddz < 170 * 170 && HELIS[w.m]) r.draw(w.m, heliModelMat(w.x, w.y, w.z, w.yaw, w.pt, w.rl), vp, cam, fn, ff, 0.6, 0, 0.82);
    }
  }
  function drawHeliFx(vp, focal, ex, ey, ez, dt, now) {
    var layer = $('ew-fx');
    G.helis.forEach(function (o) {              // names above the other pilots' helicopters
      var sp = project(vp, o.x, o.y + (HELIS[o.m] ? HELIS[o.m].top + 1.2 : 3), o.z, G.cw, G.ch);
      if (!sp || sp.w > 170) { if (o.el) o.el.style.display = 'none'; return; }
      if (!o.el) { o.el = document.createElement('div'); o.el.className = 'ew-htag'; layer.appendChild(o.el); }
      var nm = o.name || (G.others.get(o.id) || {}).name || ''; if (o.el.textContent !== nm) o.el.textContent = nm;
      o.el.style.display = 'block'; o.el.style.transform = 'translate(' + (sp.x | 0) + 'px,' + (sp.y | 0) + 'px) translate(-50%,-100%)';
    });
    for (var ri = G.rings.length - 1; ri >= 0; ri--) {   // the shockwave ring and the dust thrown outward
      var rg = G.rings[ri], ra = (now - rg.t0) / (rg.dust ? 1500 : 650);
      if (ra >= 1) { if (rg.el) rg.el.remove(); G.rings.splice(ri, 1); continue; }
      var px = rg.p[0] + (rg.dust ? rg.dust[0] * ra * 22 : 0), pz = rg.p[2] + (rg.dust ? rg.dust[1] * ra * 22 : 0), rs = project(vp, px, rg.p[1] + (rg.dust ? ra * 2 : 0), pz, G.cw, G.ch);
      if (!rs) { if (rg.el) rg.el.style.display = 'none'; continue; }
      if (!rg.el) { rg.el = document.createElement('div'); rg.el.className = rg.dust ? 'ew-smoke' : 'ew-ring'; layer.appendChild(rg.el); }
      var rsz = rg.dust ? Math.max(20, Math.min(420, focal * (4 + ra * 8) / rs.w)) : Math.max(20, Math.min(1600, focal * rg.r * ra * 2 / rs.w));
      rg.el.style.display = 'block'; rg.el.style.width = rg.el.style.height = rsz.toFixed(0) + 'px'; rg.el.style.opacity = String(rg.dust ? 0.7 * (1 - ra) : 0.9 * (1 - ra));
      rg.el.style.transform = 'translate(' + (rs.x | 0) + 'px,' + (rs.y | 0) + 'px) translate(-50%,-50%)';
    }
    G.wrecks.forEach(function (w) {              // flames for a while, and smoke that rises and fades
      var age = (now - w.t0) / 1000, sp = project(vp, w.x, w.y + 1.1, w.z, G.cw, G.ch);
      w.deb.forEach(function (d) {
        var ps = project(vp, d.x, d.y, d.z, G.cw, G.ch);
        if (!ps || ps.w > 200) { if (d.el) d.el.style.display = 'none'; return; }
        if (!d.el) { d.el = document.createElement('div'); d.el.className = d.burn ? 'ew-debris burn' : 'ew-debris'; layer.appendChild(d.el); }
        var sz = Math.max(3, Math.min(60, focal * 0.5 * d.s / ps.w));
        d.el.style.display = 'block'; d.el.style.width = d.el.style.height = sz.toFixed(1) + 'px';
        d.el.style.transform = 'translate(' + (ps.x | 0) + 'px,' + (ps.y | 0) + 'px) translate(-50%,-50%) rotate(' + ((d.x + d.y + d.z) * 2).toFixed(2) + 'rad)';
      });
      var live = sp && sp.w < 220 && age > 0.1 && age < 45;
      if (live) {
        if (!w.fire) { w.fire = document.createElement('div'); w.fire.className = 'ew-boom'; layer.appendChild(w.fire); }
        var fs = Math.max(30, Math.min(500, focal * 5 / sp.w));
        w.fire.style.display = 'block'; w.fire.style.width = w.fire.style.height = fs.toFixed(0) + 'px'; w.fire.style.setProperty('--f', String(Math.floor(now / 140) % 4)); w.fire.style.opacity = String(Math.max(0, 0.9 - age / 50));
        w.fire.style.transform = 'translate(' + (sp.x | 0) + 'px,' + (sp.y | 0) + 'px) translate(-50%,-70%)';
      } else if (w.fire) w.fire.style.display = 'none';
      if (age < 75 && now > w.nextPuff && w.puffs.length < 7 && sp && sp.w < 160) {
        w.nextPuff = now + 600; var pe = document.createElement('div'); pe.className = 'ew-smoke'; layer.appendChild(pe); w.puffs.push({ el: pe, t0: now });
      }
      for (var i = w.puffs.length - 1; i >= 0; i--) {
        var pf = w.puffs[i], pa = (now - pf.t0) / 4200;
        if (pa >= 1) { pf.el.remove(); w.puffs.splice(i, 1); continue; }
        var ps = project(vp, w.x + Math.sin(pa * 3 + i) * 0.7, w.y + 1.5 + pa * 9, w.z + Math.cos(pa * 2 + i) * 0.7, G.cw, G.ch);
        if (!ps || ps.w > 170) { pf.el.style.display = 'none'; continue; }
        var sz = Math.max(14, Math.min(380, focal * (3 + pa * 6) / ps.w));
        pf.el.style.display = 'block'; pf.el.style.width = pf.el.style.height = sz.toFixed(0) + 'px'; pf.el.style.opacity = String(Math.max(0, 0.8 * (1 - pa)));
        pf.el.style.transform = 'translate(' + (ps.x | 0) + 'px,' + (ps.y | 0) + 'px) translate(-50%,-50%)';
      }
    });
  }
  // shop cards
  function heliThumb(id) {
    if (HTHUMBS[id]) return HTHUMBS[id];
    var r = heliRenderer(); if (!r || !r.thumbnail) return '';
    try { var u = r.thumbnail(id, 150, 80); if (u) HTHUMBS[id] = u; } catch (e) {}
    return HTHUMBS[id] || '';
  }
  function renderHeliShop() {
    var box = $('ew-shop-helis'); if (!box) return;
    heliCss();
    var own = G.inv.heli || {}, prices = G.inv.hp || {}, html = '<h4>Helicopters</h4>';
    HELI_IDS.forEach(function (id) {
      var sp = HELIS[id], price = prices[id] || sp.price, st = own[id], img = heliThumb(id);
      var btn1 = st === 1 ? '<button class="win-btn" data-h="launch" data-id="' + id + '"' + (G.heli ? ' disabled' : '') + '>' + (G.heli && G.heli.m === id ? 'Flying' : 'Launch') + '</button>'
        : '<button class="win-btn" data-h="buy" data-id="' + id + '">' + (G.hArm === id ? 'Click again to confirm' : 'Buy') + '</button>';
      html += '<div class="ew-hcard">' + (img ? '<img alt="" src="' + img + '">' : '<img alt="">') + '<div class="nfo"><b>' + esc(sp.name) + '</b><small>' + esc(sp.facts) + '</small><span>' + fmt(price) + ' PlaceBux' + (st === 1 ? ' · owned' : '') + ' · top speed ' + sp.vmax + ' b/s</span></div><div class="act">' + btn1 + '</div></div>';
    });
    if (box.dataset.sig !== html) { box.dataset.sig = html; box.innerHTML = html; }
  }
  function heliShopClick(e) {
    var t = e.target; if (!t || !t.getAttribute || !t.getAttribute('data-h')) return;
    var id = t.getAttribute('data-id'), kind = t.getAttribute('data-h'), prices = G.inv.hp || {}, price = prices[id] || HELIS[id].price, now = performance.now();
    if (kind === 'launch') { closeShop(); G.hsel = id; toggleHeli(); return; }
    var item = id, cost = price;
    if (G.hArm === item && now < G.hArmT) { G.hArm = ''; send({ t: 'buy', item: item }); renderInv(); return; }
    if (balance() < cost) { $('ew-shop-note').textContent = 'You need ' + fmt(cost) + ' PlaceBux (you have ' + fmt(balance()) + ').'; return; }
    $('ew-shop-note').textContent = ''; G.hArm = item; G.hArmT = now + 3000; renderInv();
  }

  // ---- flying fighter jets: starting, controls, camera, instruments, other pilots, wrecks and the shop cards
  var JET_SRC = (typeof document !== 'undefined' && document.currentScript && document.currentScript.src || '').replace(/ezworld\.js.*$/, 'jet_gl.js');
  var JTHUMBS = {};
  function jetRenderer() {                       // the jet models are another separate file, fetched the first time a jet matters
    if (G.jr) return G.jr;
    if (G.jrFail) return null;
    if (window.JetGL) {
      try { G.jr = window.JetGL.create(G.gl); } catch (e) { G.jr = null; G.jrFail = true; }
      return G.jr;
    }
    if (!G.jrLoading && JET_SRC) {
      G.jrLoading = true; var g0 = G, sc = document.createElement('script');
      sc.src = JET_SRC; sc.onload = function () { if (G === g0) { g0.jrLoading = false; renderInv(); } };
      sc.onerror = function () { g0.jrFail = true; };
      document.head.appendChild(sc);
    }
    return null;
  }
  function toggleJet() {
    if (G.jet) { if (!G.jet.onGround || G.jet.speed > 4) { sys('Land and stop the jet first, then press 4 to get out.'); return; } send({ t: 'jet', on: false }); return; }
    if (G.drone || G.heli || G.car) return;
    var own = G.inv.jet || {}, id = G.jsel && own[G.jsel] ? G.jsel : JET_IDS.filter(function (k) { return own[k]; }).pop();
    if (!id) { sys('You do not own a jet. Buy one in the shop (B).'); return; }
    if (!G.me || !G.me.onGround) { sys('Stand on the ground to get into a jet.'); return; }
    G.jsel = id; send({ t: 'jet', on: true, m: id });
  }
  function startJet(m) {
    var sp = jspec(m.m), yaw = G.me.yaw;
    G.jet = { m: m.m, x: m.x, y: m.y + 0.03, z: m.z, f: [-Math.sin(yaw), 0, -Math.cos(yaw)], u: [0, 1, 0], speed: 0, thr: 0, sink: 0, vx: 0, vy: 0, vz: 0, rp: 0, rr: 0, ry: 0, onGround: true, crashAt: 0, why: '', lastSent: '', sent: 0, view: 0, gear: 1, yaw: yaw, tries: 0 };
    G.jcam = { yaw: 0, pitch: 0, dist: sp.chase, idle: 0, cf: [-Math.sin(yaw), 0, -Math.cos(yaw)], cu: [0, 1, 0] };
    G.jpose = { x: m.x, y: m.y + 3, z: m.z, yaw: yaw, pitch: 0, f: G.jet.f.slice(), u: [0, 1, 0] };
    G.inv.armed = false; G.jsel = m.m;
    heliCss(); jetHud(true);
    $('ew-cross').style.display = 'none';
    sys('Starting the ' + sp.name + '. Hold Space for throttle, roll down the strip until about ' + Math.round(sp.vrot) + ' b/s then pull back (S) to take off. W/S pitch, A/D roll, Q/E rudder, Space/Shift throttle, V camera, 4 to get out once stopped.');
    renderInv();
  }
  function endJet(m) {
    var j = G.jet, eye0 = G.jpose ? [G.jpose.x, G.jpose.y, G.jpose.z] : null; G.jet = null; G.jpose = null;
    var jw = j && (j.wreck || (m && m.crash ? G.wrecks[G.wrecks.length - 1] : null));
    if (m && m.crash && jw) G.cfollow = { w: jw, until: performance.now() + 3800, eye: eye0 };
    jetHud(false); $('ew-cross').style.display = '';
    if (G.me && m) { G.me.x = m.x; G.me.y = m.y; G.me.z = m.z; G.me.vx = G.me.vy = G.me.vz = 0; G.me.onGround = false; }
    G.bodyWait = true;
    if (m && m.crash) { sys('You crashed the ' + (JETS[m.m] ? JETS[m.m].name : 'jet') + (j && j.why ? ' (' + j.why.replace(/\.$/, '').toLowerCase() + ')' : '') + '. It is destroyed and gone from your inventory.'); G.shake = performance.now() + 500; }
    renderInv();
  }
  function jetHud(on) {
    var el = $('ew-jhud');
    if (!el) {
      el = document.createElement('div'); el.id = 'ew-jhud'; el.className = 'ew-hudbox'; G.view.appendChild(el);
      var hp = document.createElement('div'); hp.id = 'ew-jhelp'; hp.className = 'ew-hhelp2'; hp.textContent = 'Mouse steers (M: free look, I: invert)   W/S pitch   A/D roll   Q/E rudder   Space/Shift throttle   V camera   Wheel zoom   4 exit'; G.view.appendChild(hp);
    }
    el.style.display = on ? 'flex' : 'none'; $('ew-jhelp').style.display = on ? 'block' : 'none';
  }
  function sendJetPos(force) {
    var j = G.jet; if (!j || !G.ws || G.ws.readyState !== 1) return;
    var qx = Math.round(j.x * 16), qy = Math.round(j.y * 16), qz = Math.round(j.z * 16), a = jetAngles(j.f, j.u);
    var yaw = Math.floor(((a.yaw % (2 * Math.PI) + 2 * Math.PI) % (2 * Math.PI)) / (2 * Math.PI) * 256) & 255;
    var pit = Math.max(1, Math.min(255, Math.round(a.pitch / (Math.PI / 2) * 127 + 128))), rol = Math.max(1, Math.min(255, Math.round(a.roll / Math.PI * 127 + 128)));
    var key = qx + ',' + qy + ',' + qz + ',' + yaw + ',' + pit + ',' + rol;
    if (key === j.lastSent && !force) return;
    j.lastSent = key; j.sent = performance.now();
    var b = new ArrayBuffer(10), dv = new DataView(b);
    dv.setUint8(0, 4); dv.setInt16(1, qx, true); dv.setInt16(3, qy, true); dv.setInt16(5, qz, true); dv.setUint8(7, yaw); dv.setUint8(8, pit); dv.setUint8(9, rol);
    G.ws.send(b);
  }
  function viewLook(ex, ey, ez, f, u) {            // a view matrix from a forward and an up vector (so a rolling cockpit view works)
    var r = jcross(f, u); jnorm(r);
    var m = new Float32Array(16);
    m[0] = r[0]; m[1] = u[0]; m[2] = -f[0]; m[4] = r[1]; m[5] = u[1]; m[6] = -f[1]; m[8] = r[2]; m[9] = u[2]; m[10] = -f[2];
    m[12] = -(r[0] * ex + r[1] * ey + r[2] * ez); m[13] = -(u[0] * ex + u[1] * ey + u[2] * ez); m[14] = f[0] * ex + f[1] * ey + f[2] * ez; m[15] = 1;
    return m;
  }
  function flyJet(dt) {
    var j = G.jet, k = G.keys, now = performance.now(), sp = jspec(j.m);
    if (j.crashAt && now - j.crashAt > 1500) {                         // the server did not agree it was a crash: carry on, and if that keeps happening get you out
      j.crashAt = 0; j.tries++; if (j.wreck) { dropWreck(j.wreck); j.wreck = null; }
      if (j.tries >= 2) { j.tries = 0; sys('Stuck? Moving you out of the jet.'); send({ t: 'chat', text: '/unstuck' }); }
    }
    var inp = (j.crashAt || !locked()) ? { down: false } : { f: k.KeyW || k.ArrowUp, b: k.KeyS || k.ArrowDown, l: k.KeyA || k.ArrowLeft, r: k.KeyD || k.ArrowRight, yl: k.KeyQ, yr: k.KeyE, up: k.Space, down: k.ShiftLeft || k.ShiftRight };
    if (G.mouseFly) { inp.ax = mouseCurve(G.mstick.x); inp.ay = (G.mouseInv ? 1 : -1) * mouseCurve(G.mstick.y); }
    G.mstick.x *= Math.exp(-0.9 * dt); G.mstick.y *= Math.exp(-0.9 * dt);
    var res = j.crashAt ? 'ok' : stepJet(j, inp, dt, sp);
    if (res === 'crash' && !j.crashAt) {
      j.crashAt = now; var cv = j.cv || [0, 0, 0]; G.jcrashState = { vx: cv[0], vy: cv[1], vz: cv[2], a: jetAngles(j.f, j.u) };
      sendJetPos(true); send({ t: 'jcrash', v: cv });
      j.wreck = addJetWreck({ id: G.myId, m: j.m, x: j.x, y: j.y + 0.5, z: j.z, v: cv, local: true });
      impactFx(j.x, j.y, j.z, true);
    } else if (now - j.sent >= 50 || (now - j.sent >= 40 && res !== 'ok')) sendJetPos(false);
    var me = G.me; me.x = j.x; me.y = j.y; me.z = j.z;
    var an = jetAngles(j.f, j.u); j.yaw = an.yaw; me.yaw = an.yaw;
    // the gear and the burner
    var agl = j.onGround ? 0 : -1;
    var tg = (j.onGround || (j.speed < sp.stall * 1.5 && (agl = jetAglNow(j)) >= 0 && agl < 45)) ? 1 : 0;
    j.gear += (tg - j.gear) * Math.min(1, dt * 1.6);
    j.burner = j.speed < 1 && j.thr < 0.05 ? 0 : Math.min(1, 0.15 + j.thr * 0.85);
    // camera: chase (behind, level) or cockpit (the pilot's eyes)
    var jc = G.jcam, jp = G.jpose;
    jc.idle += dt; if (jc.idle > 1.5 && j.view === 0) { jc.yaw *= Math.max(0, 1 - dt * 1.5); jc.pitch *= Math.max(0, 1 - dt * 1.5); }
    if (j.view === 0) {
      var kf = Math.min(1, dt * 5), ku = Math.min(1, dt * 2.5), q;
      for (q = 0; q < 3; q++) { jc.cf[q] += (j.f[q] - jc.cf[q]) * kf; jc.cu[q] += (j.u[q] - jc.cu[q]) * ku; }
      jnorm(jc.cf); var dd = jc.cf[0] * jc.cu[0] + jc.cf[1] * jc.cu[1] + jc.cf[2] * jc.cu[2]; for (q = 0; q < 3; q++) jc.cu[q] -= jc.cf[q] * dd; jnorm(jc.cu);
      var cr = jcross(jc.cf, jc.cu), cY = Math.cos(jc.yaw), sY = Math.sin(jc.yaw), cP = Math.cos(jc.pitch), sP = Math.sin(jc.pitch);
      var lf0 = [jc.cf[0] * cY - cr[0] * sY, jc.cf[1] * cY - cr[1] * sY, jc.cf[2] * cY - cr[2] * sY];
      var lf1 = jnorm([lf0[0] * cP + jc.cu[0] * sP, lf0[1] * cP + jc.cu[1] * sP, lf0[2] * cP + jc.cu[2] * sP]), lu1 = [jc.cu[0] * cP - lf0[0] * sP, jc.cu[1] * cP - lf0[1] * sP, jc.cu[2] * cP - lf0[2] * sP];
      var tg = j.wreck || j, tx = tg.x, ty = tg.y + sp.h * 0.4, tz = tg.z, up0 = sp.chaseUp * 0.5;
      var ex2 = tx - lf1[0] * jc.dist + lu1[0] * up0, ey2 = ty - lf1[1] * jc.dist + lu1[1] * up0, ez2 = tz - lf1[2] * jc.dist + lu1[2] * up0;
      var bx = ex2 - tx, by = ey2 - ty, bz = ez2 - tz, bl = Math.sqrt(bx * bx + by * by + bz * bz) || 1, hit = raycast(tx, ty, tz, bx / bl, by / bl, bz / bl, bl + 0.5);
      if (hit && hit.t < bl) { var cl = Math.max(2, hit.t - 0.5); ex2 = tx + bx / bl * cl; ey2 = ty + by / bl * cl; ez2 = tz + bz / bl * cl; }
      if (j.wreck) { var sk = Math.min(1, dt * 3); ex2 = jp.x + (ex2 - jp.x) * sk; ey2 = jp.y + (ey2 - jp.y) * sk; ez2 = jp.z + (ez2 - jp.z) * sk; }
      jp.x = ex2; jp.y = ey2; jp.z = ez2;
      var lf = jnorm([tx - jp.x, ty - jp.y, tz - jp.z]), uu = lu1.slice(), du = lf[0] * uu[0] + lf[1] * uu[1] + lf[2] * uu[2];
      for (q = 0; q < 3; q++) uu[q] -= lf[q] * du; jnorm(uu);
      jp.f = lf; jp.u = uu; jp.yaw = Math.atan2(-lf[0], -lf[2]); jp.pitch = Math.asin(Math.max(-1, Math.min(1, lf[1])));
    } else {
      var e = jpt(j, sp.eye[0], sp.eye[1], sp.eye[2]); jp.x = e[0]; jp.y = e[1]; jp.z = e[2];
      var f2 = j.f.slice(), u2 = j.u.slice(); jc.pitch = Math.max(-1.1, Math.min(1.1, jc.pitch)); jc.yaw = Math.max(-2.4, Math.min(2.4, jc.yaw));
      var sY = Math.sin(jc.yaw), cY = Math.cos(jc.yaw), r2 = jcross(f2, u2);              // look left and right about the up axis, up and down about the wing axis
      var f3 = [f2[0] * cY - r2[0] * sY, f2[1] * cY - r2[1] * sY, f2[2] * cY - r2[2] * sY], cP = Math.cos(jc.pitch), sP = Math.sin(jc.pitch);
      var f4 = [f3[0] * cP + u2[0] * sP, f3[1] * cP + u2[1] * sP, f3[2] * cP + u2[2] * sP], u4 = [u2[0] * cP - f3[0] * sP, u2[1] * cP - f3[1] * sP, u2[2] * cP - f3[2] * sP];
      jp.f = jnorm(f4); jp.u = jnorm(u4); jp.yaw = Math.atan2(-f4[0], -f4[2]); jp.pitch = Math.asin(Math.max(-1, Math.min(1, f4[1])));
    }
    // instruments
    if (!j.hudAt || now - j.hudAt > 90) {
      j.hudAt = now;
      var agl2 = j.onGround ? 0 : jetAglNow(j), vsp = j.vy, mach = j.speed / sp.vmax * sp.mach;
      var warn = j.crashAt ? '<span class="bad">CRASH</span>' : (!j.onGround && agl2 >= 0 && agl2 < 40 && vsp < -16) ? '<span class="bad">PULL UP</span>' : j.stalled ? '<span class="bad">STALL</span>' : (j.onGround && j.speed >= sp.vrot) ? '<span class="st">ROTATE: pull S</span>' : (j.onGround && j.thr < 0.05 && j.speed < 1) ? '<span class="st">Space: throttle</span>' : '';
      var html = '<span>' + esc(sp.name) + '</span><span>SPD <b>' + Math.round(j.speed * 3.6) + '</b> km/h</span><span>MACH <b>' + mach.toFixed(2) + '</b></span><span>ALT <b>' + Math.round(j.y) + '</b></span><span>AGL <b>' + (agl2 < 0 ? '--' : Math.round(agl2)) + '</b></span><span>THR <b>' + Math.round(j.thr * 100) + '</b>%' + (j.thr > 0.9 ? ' AB' : '') + '</span><span>GEAR <b>' + (j.gear > 0.5 ? 'DOWN' : 'UP') + '</b></span>' + (warn ? '<span>' + warn + '</span>' : '');
      if (html !== j.hudHtml) { j.hudHtml = html; $('ew-jhud').innerHTML = html; }
    }
  }
  function jetAglNow(j) {
    var h = raycast(j.x, j.y + 0.3, j.z, 0, -1, 0, 400); return h ? j.y - (h.y + 1) : -1;
  }
  // other pilots' jets
  function setOtherJet(id, m, name, x, y, z) {
    var pl = G.others.get(id);
    G.jets.set(id, { id: id, m: m, name: name || (pl ? pl.name : ''), x: x, y: y, z: z, tx: x, ty: y, tz: z, yaw: 0, tyaw: 0, pt: 0, rl: 0, tpt: 0, trl: 0, el: null, fresh: true, lx: x, lz: z, spd: 0 });
    if (pl) pl.hidden = true;
  }
  function removeOtherJet(id) {
    var o = G.jets.get(id); if (o && o.el) o.el.remove(); G.jets.delete(id);
    var pl = G.others.get(id); if (pl) { pl.hidden = false; if (o) { pl.x = pl.tx = o.x; pl.y = pl.ty = o.y; pl.z = pl.tz = o.z; } }
  }
  function onJetFrame(dv) {
    var n = dv.getUint16(1, true), p = 3;
    for (var i = 0; i < n; i++, p += 12) {
      var id = dv.getUint16(p, true), o = G.jets.get(id), mid = JET_IDS[dv.getUint8(p + 11)];
      if (!o) { setOtherJet(id, mid, '', 0, 0, 0); o = G.jets.get(id); }
      o.m = mid || o.m;
      o.tx = dv.getInt16(p + 2, true) / 16; o.ty = dv.getInt16(p + 4, true) / 16; o.tz = dv.getInt16(p + 6, true) / 16;
      o.tyaw = dv.getUint8(p + 8) / 256 * 2 * Math.PI; o.tpt = (dv.getUint8(p + 9) - 128) / 127 * Math.PI / 2; o.trl = (dv.getUint8(p + 10) - 128) / 127 * Math.PI;
      if (o.fresh) { o.x = o.tx; o.y = o.ty; o.z = o.tz; o.yaw = o.tyaw; o.pt = o.tpt; o.rl = o.trl; o.fresh = false; o.lx = o.x; o.lz = o.z; }
    }
  }
  function addJetWreck(m) {
    var own = m.id === G.myId;
    if (own && !m.local && (G.cfollow || (G.jet && G.jet.wreck))) return null;
    var st = own && m.local ? G.jcrashState : null, v = st ? [st.vx, st.vy, st.vz] : (m.v || [0, 0, 0]);
    return newWreck('j', m, v, st ? { yaw: st.a.yaw, pt: st.a.pitch, rl: st.a.roll } : { yaw: 0, pt: 0, rl: 0 });
  }
  function drawJets(vp, ex, ey, ez, dt, now) {
    if (!G.jet && !G.jets.size && !G.wrecks.some(function (w) { return w.kind === 'j'; })) return;
    var r = jetRenderer(); if (!r) return;
    var fn = RDIST * 16 - 44, ff = RDIST * 16 - 6, cam = [ex, ey, ez];
    if (G.jet && G.jet.view === 0 && !G.jet.crashAt) { var j = G.jet; r.draw(j.m, jetMat(j.x, j.y, j.z, j.f, j.u), vp, cam, fn, ff, j.burner || 0, 0, j.gear); }
    G.jets.forEach(function (o) {
      var k = Math.min(1, dt * 10);
      o.x += (o.tx - o.x) * k; o.y += (o.ty - o.y) * k; o.z += (o.tz - o.z) * k;
      o.yaw += angDiff(o.yaw, o.tyaw) * k; o.pt += (o.tpt - o.pt) * k; o.rl += angDiff(o.rl, o.trl) * k;
      var sp = Math.hypot(o.x - o.lx, o.z - o.lz) / Math.max(dt, 0.001); o.lx = o.x; o.lz = o.z; o.spd += (sp - o.spd) * Math.min(1, dt * 3);
      var dx = o.x - ex, dz = o.z - ez; if (dx * dx + dz * dz > 200 * 200 || !JETS[o.m]) return;
      var b = jetBasis(o.yaw, o.pt, o.rl), s2 = jspec(o.m);
      r.draw(o.m, jetMat(o.x, o.y, o.z, b.f, b.u), vp, cam, fn, ff, Math.min(1, 0.2 + o.spd / s2.vmax), 0, o.spd < s2.stall * 1.4 && Math.abs(o.ty - Math.round(o.ty)) < 1.2 ? 1 : 0);
    });
    G.wrecks.forEach(function (w) {
      if (w.kind !== 'j' || !JETS[w.m]) return;
      var ddx = w.x - ex, ddz = w.z - ez; if (ddx * ddx + ddz * ddz > 200 * 200) return;
      var b = jetBasis(w.yaw, w.pt, w.rl); r.draw(w.m, jetMat(w.x, w.y, w.z, b.f, b.u), vp, cam, fn, ff, 0, 0.82, 0);
    });
  }
  function drawJetFx(vp, focal, ex, ey, ez, dt, now) {
    var layer = $('ew-fx');
    G.jets.forEach(function (o) {              // names above the other pilots' jets
      var sp = project(vp, o.x, o.y + 4, o.z, G.cw, G.ch);
      if (!sp || sp.w > 220) { if (o.el) o.el.style.display = 'none'; return; }
      if (!o.el) { o.el = document.createElement('div'); o.el.className = 'ew-htag'; layer.appendChild(o.el); }
      var nm = o.name || (G.others.get(o.id) || {}).name || ''; if (o.el.textContent !== nm) o.el.textContent = nm;
      o.el.style.display = 'block'; o.el.style.transform = 'translate(' + (sp.x | 0) + 'px,' + (sp.y | 0) + 'px) translate(-50%,-100%)';
    });
  }
  // shop cards
  function jetThumb(id) {
    if (JTHUMBS[id]) return JTHUMBS[id];
    var r = jetRenderer(); if (!r || !r.thumbnail) return '';
    try { var u = r.thumbnail(id, 150, 80); if (u) JTHUMBS[id] = u; } catch (e) {}
    return JTHUMBS[id] || '';
  }
  function renderJetShop() {
    var box = $('ew-shop-jets'); if (!box) return;
    heliCss();
    var own = G.inv.jet || {}, prices = G.inv.jp || {}, html = '<h4>Fighter jets</h4>';
    JET_IDS.forEach(function (id) {
      var sp = JETS[id], price = prices[id] || sp.price, st = own[id], img = jetThumb(id);
      var btn1 = st ? '<button class="win-btn" data-j="launch" data-id="' + id + '"' + (G.jet || G.heli || G.drone ? ' disabled' : '') + '>' + (G.jet && G.jet.m === id ? 'Flying' : 'Get in') + '</button>'
        : '<button class="win-btn" data-j="buy" data-id="' + id + '">' + (G.jArm === id ? 'Click again to confirm' : 'Buy') + '</button>';
      html += '<div class="ew-hcard">' + (img ? '<img alt="" src="' + img + '">' : '<img alt="">') + '<div class="nfo"><b>' + esc(sp.name) + '</b><small>' + esc(sp.facts) + '</small><span>' + fmt(price) + ' PlaceBux' + (st ? ' · owned' : '') + ' · top speed ' + Math.round(sp.vmax * 3.6) + ' km/h (Mach ' + sp.mach + ' real)</span></div><div class="act">' + btn1 + '</div></div>';
    });
    if (box.dataset.sig !== html) { box.dataset.sig = html; box.innerHTML = html; }
  }
  function jetShopClick(e) {
    var t = e.target; if (!t || !t.getAttribute || !t.getAttribute('data-j')) return;
    var id = t.getAttribute('data-id'), kind = t.getAttribute('data-j'), prices = G.inv.jp || {}, price = prices[id] || JETS[id].price, now = performance.now();
    if (kind === 'launch') { closeShop(); G.jsel = id; toggleJet(); return; }
    if (G.jArm === id && now < G.jArmT) { G.jArm = ''; send({ t: 'buy', item: id }); renderInv(); return; }
    if (balance() < price) { $('ew-shop-note').textContent = 'You need ' + fmt(price) + ' PlaceBux (you have ' + fmt(balance()) + ').'; return; }
    $('ew-shop-note').textContent = ''; G.jArm = id; G.jArmT = now + 3000; renderInv();
  }

  // ---- sound: made with the browser's audio synth, nothing is downloaded. A drone is four whining motors (slightly out of tune, so they beat against each other
  // like the real buzz), a higher whine, and prop-wash hiss; other drones are heard in 3D with distance, left and right, a Doppler shift, and muffling behind walls.
  function audio() {
    if (G.aud === undefined) {
      G.aud = null;
      try {
        var AC = window.AudioContext || window.webkitAudioContext; if (!AC) return null;
        var ctx = new AC(), master = ctx.createGain(), comp = ctx.createDynamicsCompressor(), len = ctx.sampleRate * 2, nb = ctx.createBuffer(1, len, ctx.sampleRate), ch = nb.getChannelData(0);
        for (var i = 0; i < len; i++) ch[i] = Math.random() * 2 - 1;
        master.gain.value = G.mute ? 0 : 0.7; master.connect(comp); comp.connect(ctx.destination);
        G.aud = { ctx: ctx, master: master, noise: nb, voices: new Map(), own: null };
      } catch (e) { G.aud = null; }
    }
    var a = G.aud;
    if (a && a.ctx.state === 'suspended') { try { a.ctx.resume(); } catch (e) {} }
    return a;
  }
  function toggleMute() {
    G.mute = !G.mute; var a = G.aud;
    if (a) a.master.gain.setTargetAtTime(G.mute ? 0 : 0.7, a.ctx.currentTime, 0.05);
    sys(G.mute ? 'Sound off (N to turn it back on).' : 'Sound on (N to turn it off).');
  }
  function makeDroneVoice(a) {
    var ctx = a.ctx, v = { ctx: ctx, oscs: [] }, i;
    var mix = ctx.createGain(); mix.gain.value = 0.2;
    var detune = [-7, 5, -3, 8];
    for (i = 0; i < 4; i++) {
      var o = ctx.createOscillator(); o.type = i < 2 ? 'sawtooth' : 'triangle'; o.frequency.value = 200; o.detune.value = detune[i];
      var g = ctx.createGain(); g.gain.value = i < 2 ? 0.45 : 0.3; o.connect(g); g.connect(mix); o.start(); v.oscs.push(o);
    }
    v.wh = ctx.createOscillator(); v.wh.type = 'triangle'; v.wh.frequency.value = 600; var wg = ctx.createGain(); wg.gain.value = 0.07; v.wh.connect(wg); wg.connect(mix); v.wh.start();
    v.nz = ctx.createBufferSource(); v.nz.buffer = a.noise; v.nz.loop = true;
    v.bp = ctx.createBiquadFilter(); v.bp.type = 'bandpass'; v.bp.frequency.value = 1800; v.bp.Q.value = 0.8;
    v.ng = ctx.createGain(); v.ng.gain.value = 0.015; v.nz.connect(v.bp); v.bp.connect(v.ng); v.ng.connect(mix); v.nz.start();
    v.lp = ctx.createBiquadFilter(); v.lp.type = 'lowpass'; v.lp.frequency.value = 1800; v.lp.Q.value = 0.6;
    v.trem = ctx.createGain(); v.trem.gain.value = 1;
    v.lfo = ctx.createOscillator(); v.lfo.frequency.value = 13; v.lg = ctx.createGain(); v.lg.gain.value = 0; v.lfo.connect(v.lg); v.lg.connect(v.trem.gain); v.lfo.start();   // a damaged motor stutters
    v.out = ctx.createGain(); v.out.gain.value = 0;
    v.pan = ctx.createStereoPanner ? ctx.createStereoPanner() : null;
    mix.connect(v.lp); v.lp.connect(v.trem); v.trem.connect(v.out);
    if (v.pan) { v.out.connect(v.pan); v.pan.connect(a.master); } else v.out.connect(a.master);
    return v;
  }
  function setDroneVoice(v, p) {
    var t = v.ctx.currentTime, i;
    for (i = 0; i < v.oscs.length; i++) v.oscs[i].frequency.setTargetAtTime(p.freq * (i === 3 ? 0.995 : 1), t, 0.05);
    v.wh.frequency.setTargetAtTime(p.freq * 2.013, t, 0.05);
    v.out.gain.setTargetAtTime(p.gain, t, 0.05);
    if (v.pan) v.pan.pan.setTargetAtTime(p.pan, t, 0.05);
    v.lp.frequency.setTargetAtTime(p.cut, t, 0.08);
    v.bp.frequency.setTargetAtTime(p.freq * 5, t, 0.1);
    v.ng.gain.setTargetAtTime(p.wash, t, 0.1);
    v.lg.gain.setTargetAtTime(p.dmg, t, 0.1);
  }
  function stopDroneVoice(v, falling) {
    if (!v) return;
    var t = v.ctx.currentTime;
    if (falling) { v.oscs.forEach(function (o) { o.frequency.cancelScheduledValues(t); o.frequency.setTargetAtTime(40, t, 0.12); }); v.out.gain.setTargetAtTime(0, t + 0.15, 0.08); }
    else v.out.gain.setTargetAtTime(0, t, 0.05);
    setTimeout(function () { try { v.oscs.forEach(function (o) { o.stop(); }); v.wh.stop(); v.nz.stop(); v.lfo.stop(); v.out.disconnect(); } catch (e) {} }, 700);
  }
  function noiseHit(a, vol, dur, f0, f1, boom) {            // a burst of filtered noise (and an optional low thump): impacts, crashes and explosions
    var ctx = a.ctx, t = ctx.currentTime, src = ctx.createBufferSource(), g = ctx.createGain(), f = ctx.createBiquadFilter();
    src.buffer = a.noise; src.loop = true; f.type = 'lowpass'; f.frequency.setValueAtTime(f0, t); f.frequency.exponentialRampToValueAtTime(Math.max(40, f1), t + dur);
    g.gain.setValueAtTime(vol, t); g.gain.exponentialRampToValueAtTime(0.001, t + dur);
    src.connect(f); f.connect(g); g.connect(a.master); src.start(t, Math.random()); src.stop(t + dur + 0.05);
    if (boom) {
      var o = ctx.createOscillator(), og = ctx.createGain(); o.type = 'sine'; o.frequency.setValueAtTime(boom, t); o.frequency.exponentialRampToValueAtTime(28, t + dur);
      og.gain.setValueAtTime(vol * 1.1, t); og.gain.exponentialRampToValueAtTime(0.001, t + dur); o.connect(og); og.connect(a.master); o.start(t); o.stop(t + dur + 0.05);
    }
  }
  function boomSound(dist, scale) {
    var a = G.aud; if (!a) return;
    var vol = Math.min(1, 0.9 / (1 + dist / 25)) * Math.min(1.4, 0.6 + (scale || 1) * 0.4);
    if (vol > 0.02) noiseHit(a, vol, 1.1 + (scale || 1) * 0.3, 2400, 90, 90);
  }
  function ownDroneSound(d, inp, dt, spn, hp) {
    var a = G.aud; if (!a) return;
    if (!a.own) a.own = makeDroneVoice(a);
    var thr = Math.min(1, 0.22 + spn * 0.55 + (inp.up ? 0.2 : 0) + (inp.f ? 0.12 : 0));
    setDroneVoice(a.own, { freq: 140 + 230 * thr, gain: 0.3, pan: 0, cut: 900 + 1500 * thr, wash: 0.012 + 0.04 * spn, dmg: hp < 40 ? (40 - hp) / 40 * 0.55 : 0 });
  }
  function otherDroneSound(dr, vp, ex, ey, ez, dt, blocked) {
    var a = G.aud; if (!a) return;
    var dx = dr.x - ex, dy = dr.y - ey, dz = dr.z - ez, dist = Math.sqrt(dx * dx + dy * dy + dz * dz), v = a.voices.get(dr.id);
    if (dist > 150) { if (v) { stopDroneVoice(v, false); a.voices.delete(dr.id); } dr.lastD = undefined; return; }
    if (!v) { if (a.voices.size >= 6) return; v = makeDroneVoice(a); a.voices.set(dr.id, v); }
    var rdot = dr.lastD === undefined ? 0 : (dist - dr.lastD) / Math.max(dt, 0.001); dr.lastD = dist;
    dr.spdA = (dr.spdA || 0) + ((dr.lx === undefined ? 0 : Math.hypot(dr.x - dr.lx, dr.y - dr.ly, dr.z - dr.lz) / Math.max(dt, 0.001)) - (dr.spdA || 0)) * Math.min(1, dt * 4);
    dr.lx = dr.x; dr.ly = dr.y; dr.lz = dr.z;
    var thr = Math.min(1, 0.22 + Math.min(1, dr.spdA / 45) * 0.6), dopp = 1 / (1 + Math.max(-60, Math.min(60, rdot)) / 140);
    var sp = project(vp, dr.x, dr.y, dr.z, G.cw, G.ch), pan = sp ? Math.max(-1, Math.min(1, (sp.x / G.cw * 2 - 1) * 1.1)) : 0;
    setDroneVoice(v, { freq: (140 + 230 * thr) * dopp, gain: 0.3 * Math.pow(1 / (1 + dist / 14), 1.5), pan: pan, cut: (blocked ? 450 : 2300 - Math.min(1500, dist * 14)) * (sp ? 1 : 0.6), wash: 0.015, dmg: 0 });
  }

  // ---- cars: bringing one out, getting in and out, hijacking, driving, the cameras, other people's cars, wrecks, sound and the shop cards
  var CAR_SRC = (typeof document !== 'undefined' && document.currentScript && document.currentScript.src || '').replace(/ezworld\.js.*$/, 'car_gl.js');
  var CTHUMBS = {}, ENG = { yaris: 1.2, civic: 1.1, f150: 0.85, c5: 0.9, m3: 1.1, '911': 1.15, cls63: 0.88 };
  function carRenderer() {                       // the car models are another separate file, fetched the first time a car matters
    if (G.cr) return G.cr;
    if (G.crFail) return null;
    if (window.CarGL) {
      try { G.cr = window.CarGL.create(G.gl); } catch (e) { G.cr = null; G.crFail = true; }
      return G.cr;
    }
    if (!G.crLoading && CAR_SRC) {
      G.crLoading = true; var g0 = G, sc = document.createElement('script');
      sc.src = CAR_SRC; sc.onload = function () { if (G === g0) { g0.crLoading = false; renderInv(); } };
      sc.onerror = function () { g0.crFail = true; };
      document.head.appendChild(sc);
    }
    return null;
  }
  function carCss() {
    if (document.getElementById('ew-car-css')) return;
    var st = document.createElement('style'); st.id = 'ew-car-css';
    st.textContent = '#ew-chud{display:none;position:absolute;left:50%;bottom:78px;transform:translateX(-50%);gap:16px;padding:8px 18px;font:700 14px "JetBrains Mono","Fira Code",monospace;color:#fff;text-shadow:0 0 6px rgba(0,0,0,.9);background:rgba(8,10,14,.62);border:1px solid rgba(255,255,255,.22);border-radius:10px;pointer-events:none;align-items:center}' +
      '#ew-chud b{font-size:22px;color:#7dff9a}#ew-chud .bar{width:90px;height:8px;border-radius:4px;background:rgba(255,255,255,.18);overflow:hidden;display:inline-block;vertical-align:middle}#ew-chud .bar i{display:block;height:100%;background:#7dff9a}' +
      '#ew-chud .bad{color:#ff6b5e}#ew-chud .st{color:#ffd166}' +
      '#ew-carprompt{display:none;position:absolute;left:50%;bottom:140px;transform:translateX(-50%);padding:6px 14px;border-radius:8px;background:rgba(8,10,14,.7);border:1px solid rgba(255,255,255,.25);color:#fff;font-weight:700;font-size:13px;pointer-events:none;text-align:center}' +
      '#ew-carprompt .pb{display:none;margin-top:5px;width:160px;height:7px;border-radius:4px;background:rgba(255,255,255,.2);overflow:hidden}#ew-carprompt .pb i{display:block;height:100%;width:0;background:#ff6b5e}' +
      '#ew-hotbar5{display:none;align-items:center;gap:8px;padding:6px 12px;background:rgba(12,12,16,.62);border:2px solid rgba(255,255,255,.18);border-radius:10px;color:#fff;font-weight:700;font-size:13px}#ew-hotbar5.sel{border-color:var(--accent);box-shadow:0 0 0 2px var(--accent-dim)}#ew-hotbar5 .key{font-size:11px;opacity:.7}' +
      '#ew-shop-cars{padding:0 16px 6px;display:flex;flex-direction:column;gap:10px}#ew-shop-cars h4{margin:6px 0 0;font-size:13px;color:var(--text-secondary);text-transform:uppercase;letter-spacing:.06em}#ew-shop-cars .warn{font-size:11.5px;color:var(--red)}';
    document.head.appendChild(st);
  }
  function carBasis(c) { return jetBasis(c.yaw, c.pitch, c.roll); }
  function carMat(c) { var b = carBasis(c); return jetMat(c.x, c.y - 0.03, c.z, b.f, b.u); }
  function nearestCar() {
    var best = null, bd = 5.5 * 5.5, m = G.me; if (!m) return null;
    G.cars.forEach(function (o) { var dx = o.x - m.x, dz = o.z - m.z, d = dx * dx + dz * dz; if (d < bd && Math.abs(o.y - m.y) < 3) { bd = d; best = o; } });
    return best;
  }
  function summonCar() {
    if (G.car || G.drone || G.heli || G.jet) return;
    var own = G.inv.car || {}, id = G.csel && own[G.csel] ? G.csel : CAR_IDS.filter(function (k) { return own[k]; }).pop();
    if (!id) { sys('You do not own a car. Buy one in the shop (B).'); return; }
    if (!G.me || !G.me.onGround) { sys('Stand on the ground to bring out a car.'); return; }
    G.csel = id; send({ t: 'car', a: 'summon', m: id });
  }
  function carUse() {                              // F: get out of your car, or into the nearest one (somebody else's: hold F to hijack it)
    if (G.car) {
      if (Math.abs(G.car.vf) > 6) { sys('Slow down before you get out.'); return; }
      send({ t: 'car', a: 'exit' }); return;
    }
    if (G.drone || G.heli || G.jet) return;
    var o = nearestCar(); if (!o) return;
    send({ t: 'car', a: 'enter', id: o.id });
  }
  function startCar(m) {
    var sp = cspec(m.m), yaw = m.yaw / 256 * 2 * Math.PI;
    G.car = { id: m.id, m: m.m, x: m.x, y: m.y, z: m.z, yaw: yaw, vf: 0, vl: 0, vy: 0, wy: 0, steer: 0, speed: 0, onGround: true, pitch: 0, roll: 0, wheel: 0, dmg: m.dmg || 0, dents: (m.dents || [0, 0, 0, 0]).slice(), sent: 0, lastSent: '', view: 0, head: false, gw: null };
    G.ccam = { yaw: 0, pitch: 0, idle: 0, cyaw: yaw, dist: sp.chase };
    G.carpose = { x: m.x, y: m.y + 3, z: m.z, yaw: yaw, pitch: 0, f: [-Math.sin(yaw), 0, -Math.cos(yaw)], u: [0, 1, 0] };
    G.cars.delete(m.id); G.inv.armed = false; G.csel = m.m; G.hijack = null;
    carCss(); carHud(true); $('ew-cross').style.display = 'none';
    sys('Driving the ' + sp.name + '. W gas, S brake and reverse, A/D steer, Space handbrake, V camera, H horn, L lights, F get out.' + (m.mine ? '' : ' (Hijacked!)'));
    doorSound();
    renderInv();
  }
  function endCar(m) {
    var c = G.car, eye0 = G.carpose ? [G.carpose.x, G.carpose.y, G.carpose.z] : null; G.car = null; G.carpose = null;
    carHud(false); $('ew-cross').style.display = '';
    stopOwnEngine(); doorSound();
    if (G.me && m) { G.me.x = m.x; G.me.y = m.y; G.me.z = m.z; G.me.vx = G.me.vy = G.me.vz = 0; G.me.onGround = false; }
    G.bodyWait = true;
    if (m && m.why === 'totaled') { sys('Your ' + (c ? CARS[c.m].name : 'car') + ' was totaled. It is gone for good.'); G.shake = performance.now() + 500; }
    else if (m && m.forced && m.why) sys(m.why);
    renderInv();
  }
  function carHud(on) {
    var el = $('ew-chud');
    if (!el) { el = document.createElement('div'); el.id = 'ew-chud'; G.view.appendChild(el); }
    el.style.display = on ? 'flex' : 'none';
  }
  function carHints(now) {                          // the prompt next to a car, and the hijack progress bar
    var pr = $('ew-carprompt');
    if (!pr) { carCss(); pr = document.createElement('div'); pr.id = 'ew-carprompt'; pr.innerHTML = '<span class="tx"></span><div class="pb"><i></i></div>'; G.view.appendChild(pr); }
    if (G.car || G.drone || G.heli || G.jet || !G.ready) { pr.style.display = 'none'; return; }
    var hj = G.hijack;
    if (hj) {
      var f = Math.min(1, (now - hj.t0) / hj.ms);
      pr.style.display = 'block'; pr.querySelector('.tx').textContent = 'Hijacking... keep holding F'; pr.querySelector('.pb').style.display = 'block'; pr.querySelector('.pb i').style.width = (f * 100).toFixed(0) + '%';
      if (!G.keys.KeyF) { G.hijack = null; return; }
      if (f >= 1) { send({ t: 'car', a: 'enter', id: hj.id, confirm: true }); G.hijack = null; }
      return;
    }
    if (now - (G.cphAt || 0) < 150) return; G.cphAt = now;
    var o = nearestCar();
    if (!o) { pr.style.display = 'none'; return; }
    var mine = o.id === G.inv.ccid;
    pr.style.display = 'block'; pr.querySelector('.pb').style.display = 'none';
    pr.querySelector('.tx').textContent = (CARS[CAR_IDS[o.mi]] ? CARS[CAR_IDS[o.mi]].name : 'Car') + (o.flags & 4 ? ' (somebody is driving)' : '') + ': ' + (mine && !(o.flags & 4) ? 'press F to get in' : 'hold F to hijack');
  }
  function sendCarPos(inp, force) {
    var c = G.car; if (!c || !G.ws || G.ws.readyState !== 1) return;
    var qx = Math.round(c.x * 16), qy = Math.round(c.y * 16), qz = Math.round(c.z * 16);
    var yaw = Math.floor(((c.yaw % (2 * Math.PI) + 2 * Math.PI) % (2 * Math.PI)) / (2 * Math.PI) * 256) & 255, st = Math.max(-127, Math.min(127, Math.round(c.steer / 0.6 * 127)));
    var flags = ((inp.b && c.vf > 0.5) || inp.hb ? 1 : 0) | (c.head ? 2 : 0), key = qx + ',' + qy + ',' + qz + ',' + yaw + ',' + st + ',' + flags;
    if (key === c.lastSent && !force) return;
    c.lastSent = key; c.sent = performance.now();
    var b = new ArrayBuffer(10), dv = new DataView(b);
    dv.setUint8(0, 5); dv.setInt16(1, qx, true); dv.setInt16(3, qy, true); dv.setInt16(5, qz, true); dv.setUint8(7, yaw); dv.setInt8(8, st); dv.setUint8(9, flags);
    G.ws.send(b);
  }
  function carImpact(c, ev, sp) {                    // something was hit: tell the server, dent the car at once, shake and crunch
    send({ t: 'ccrash', v: ev.spd, dx: ev.dx, dz: ev.dz });
    var dmg = (ev.spd - CAR_CRASH_MIN) * 2.4 + 2, d = carDirs(c.yaw), along = ev.dx * d[0] + ev.dz * d[1], side = ev.dx * d[2] + ev.dz * d[3];
    if (Math.abs(along) >= Math.abs(side)) c.dents[along > 0 ? 0 : 1] = Math.min(1, c.dents[along > 0 ? 0 : 1] + dmg / 70); else c.dents[side > 0 ? 3 : 2] = Math.min(1, c.dents[side > 0 ? 3 : 2] + dmg / 80);
    c.dmg = Math.min(99, c.dmg + dmg);
    G.shake = performance.now() + Math.min(600, 150 + ev.spd * 10);
    crashSound(ev.spd);
  }
  function driveCar(dt) {
    var c = G.car, k = G.keys, now = performance.now(), sp = cspec(c.m), me = G.me;
    var inp = locked() ? { f: k.KeyW || k.ArrowUp, b: k.KeyS || k.ArrowDown, l: k.KeyA || k.ArrowLeft, r: k.KeyD || k.ArrowRight, hb: k.Space } : {};
    var ev = stepCar(c, inp, dt, sp);
    if (ev) {
      if (ev.res === 'crash') carImpact(c, ev, sp);
      else if (ev.res === 'bump' && G.aud && ev.spd > 2) noiseHit(G.aud, 0.15, 0.15, 900, 200, 0);
      sendCarPos(inp, true);
    } else if (now - c.sent >= 50) sendCarPos(inp, false);
    me.x = c.x; me.y = c.y; me.z = c.z; me.yaw = c.yaw;
    // cameras
    var cc = G.ccam, cp = G.carpose, b = carBasis(c);
    cc.idle += dt; if (cc.idle > 1.5 && c.view === 0) { cc.yaw *= Math.max(0, 1 - dt * 2); cc.pitch *= Math.max(0, 1 - dt * 2); }
    if (c.view === 0) {
      cc.cyaw += angDiff(cc.cyaw, c.yaw + Math.max(-0.5, Math.min(0.5, c.vl * 0.05))) * Math.min(1, dt * 4);
      var y0 = cc.cyaw + cc.yaw, tx = c.x, ty = c.y + sp.h * 0.7, tz = c.z, hgt = sp.chaseUp + cc.pitch * cc.dist * 0.8;
      var ex2 = tx + Math.sin(y0) * cc.dist, ey2 = ty + hgt, ez2 = tz + Math.cos(y0) * cc.dist;
      var bx = ex2 - tx, by = ey2 - ty, bz = ez2 - tz, bl = Math.sqrt(bx * bx + by * by + bz * bz) || 1, hit = raycast(tx, ty, tz, bx / bl, by / bl, bz / bl, bl + 0.4);
      if (hit && hit.t < bl) { var cl = Math.max(1.5, hit.t - 0.4); ex2 = tx + bx / bl * cl; ey2 = ty + by / bl * cl; ez2 = tz + bz / bl * cl; }
      cp.x = ex2; cp.y = ey2; cp.z = ez2;
      var lf = jnorm([tx - ex2, ty - ey2, tz - ez2]); cp.f = lf; cp.u = [0, 1, 0]; cp.yaw = Math.atan2(-lf[0], -lf[2]); cp.pitch = Math.asin(Math.max(-1, Math.min(1, lf[1])));
    } else {
      var e = jpt({ x: c.x, y: c.y - 0.03, z: c.z, f: b.f, u: b.u }, sp.eye[0], sp.eye[1], sp.eye[2]); cp.x = e[0]; cp.y = e[1]; cp.z = e[2];
      cc.pitch = Math.max(-0.7, Math.min(0.7, cc.pitch)); cc.yaw = Math.max(-2.2, Math.min(2.2, cc.yaw));
      var sY = Math.sin(cc.yaw), cY = Math.cos(cc.yaw), r2 = jcross(b.f, b.u), cPp = Math.cos(cc.pitch), sP = Math.sin(cc.pitch);
      var f3 = [b.f[0] * cY - r2[0] * sY, b.f[1] * cY - r2[1] * sY, b.f[2] * cY - r2[2] * sY], f4 = [f3[0] * cPp + b.u[0] * sP, f3[1] * cPp + b.u[1] * sP, f3[2] * cPp + b.u[2] * sP], u4 = [b.u[0] * cPp - f3[0] * sP, b.u[1] * cPp - f3[1] * sP, b.u[2] * cPp - f3[2] * sP];
      cp.f = jnorm(f4); cp.u = jnorm(u4); cp.yaw = Math.atan2(-f4[0], -f4[2]); cp.pitch = Math.asin(Math.max(-1, Math.min(1, f4[1])));
    }
    // instruments
    if (!c.hudAt || now - c.hudAt > 90) {
      c.hudAt = now;
      var kmh = Math.round(Math.abs(c.vf) * 3.6), hp = Math.max(0, 100 - c.dmg), warn = c.dmg > 85 ? '<span class="bad">CRITICAL</span>' : c.dmg > 60 ? '<span class="bad">HEAVY DAMAGE</span>' : '';
      var html = '<span><b>' + kmh + '</b> km/h</span><span>' + (c.vf < -0.5 ? 'R' : kmh < 1 ? 'N' : String(Math.min(6, 1 + Math.floor(Math.abs(c.vf) / sp.vmax * 6)))) + '</span><span>' + esc(sp.name) + '</span><span>CAR <span class="bar"><i style="width:' + hp.toFixed(0) + '%;background:' + (hp > 50 ? '#7dff9a' : hp > 25 ? '#ffd166' : '#ff6b5e') + '"></i></span> ' + Math.round(hp) + '%</span>' + (c.head ? '<span class="st">LIGHTS</span>' : '') + (inp.hb ? '<span class="st">HANDBRAKE</span>' : '') + warn;
      if (html !== c.hudHtml) { c.hudHtml = html; $('ew-chud').innerHTML = html; }
    }
    c.brakeL = !!((inp.b && c.vf > 0.5) || inp.hb); c.headL = c.head;
    ownEngine(c, inp, sp, dt);
  }
  // ---- sound: an engine that climbs through the gears, tyre squeal when it slides, a horn, and other drivers heard in 3D
  // Each car has its own engine, built from sound waves: how many cylinders it has sets the pitch for the revs, a table of harmonics sets the tone (a V8's deep burble,
  // a straight-six's smooth howl, a flat-six's rasp), pulses in the volume stand in for the cylinders firing, and extras add a VTEC-style howl, a turbo whistle with a blow-off
  // valve, exhaust crackles when you lift off, wind and road noise, tyre squeal, and a starter motor when you get in.
  var EP = {
    yaris:  { cyl: 3, idle: 900, red: 6600, h: [0.9, 1, 0.5, 0.45, 0.25, 0.2, 0.12, 0.1], am: 0.22, amIrr: 0.0, sub: 0.25, intake: 0.6, whine: 0.0, crack: 0.0, turbo: 0, lp: 1.0, bov: 0, clatter: 0 },
    civic:  { cyl: 4, idle: 800, red: 7200, h: [0.45, 1, 0.3, 0.6, 0.18, 0.3, 0.1, 0.16], am: 0.1, amIrr: 0.0, sub: 0.2, intake: 0.85, whine: 0.55, crack: 0.05, turbo: 0, lp: 1.1, bov: 0, clatter: 0 },
    f150:   { cyl: 8, idle: 650, red: 5600, h: [1, 0.85, 0.55, 0.4, 0.3, 0.2, 0.15, 0.1], am: 0.5, amIrr: 0.38, sub: 0.7, intake: 0.35, whine: 0.0, crack: 0.15, turbo: 0, lp: 0.7, bov: 0, clatter: 0 },
    c5:     { cyl: 8, idle: 700, red: 6300, h: [1, 0.95, 0.65, 0.55, 0.4, 0.3, 0.22, 0.16], am: 0.46, amIrr: 0.45, sub: 0.6, intake: 0.4, whine: 0.0, crack: 0.9, turbo: 0, lp: 0.85, bov: 0, clatter: 0 },
    m3:     { cyl: 6, idle: 850, red: 8000, h: [0.6, 1, 0.4, 0.65, 0.32, 0.45, 0.22, 0.28], am: 0.07, amIrr: 0.0, sub: 0.25, intake: 1.0, whine: 0.3, crack: 0.3, turbo: 0, lp: 1.25, bov: 0, clatter: 0 },
    '911':  { cyl: 6, idle: 800, red: 7600, h: [0.75, 0.95, 0.8, 0.85, 0.55, 0.6, 0.4, 0.34], am: 0.24, amIrr: 0.18, sub: 0.2, intake: 0.7, whine: 0.0, crack: 0.35, turbo: 0, lp: 1.15, bov: 0, clatter: 0.35 },
    cls63:  { cyl: 8, idle: 700, red: 6600, h: [1, 0.9, 0.62, 0.45, 0.3, 0.22, 0.16, 0.1], am: 0.36, amIrr: 0.22, sub: 0.55, intake: 0.5, whine: 0.0, crack: 0.9, turbo: 1, lp: 0.9, bov: 1, clatter: 0 }
  };
  var HORN = { yaris: [466, 587], civic: [415, 494], f150: [311, 370], c5: [392, 311], m3: [392, 494], '911': [440, 370], cls63: [370, 466] };
  var GEAR_TOP = [0.16, 0.31, 0.48, 0.67, 0.85, 1.0];
  function waveFor(ctx, h) {
    var re = new Float32Array(h.length + 1), im = new Float32Array(h.length + 1);
    for (var i = 0; i < h.length; i++) im[i + 1] = h[i] / (1 + i * 0.05);
    return ctx.createPeriodicWave(re, im);
  }
  function makeEngineVoice(a, model) {
    var ctx = a.ctx, ep = EP[model] || EP.civic, v = { ctx: ctx, ep: ep, model: model, gear: 0, shiftUntil: 0, boost: 0, lastThr: 0, rpmN: 0, smRpm: 0, startT: ctx.currentTime };
    var mix = ctx.createGain(); mix.gain.value = 0.3;
    var wave = waveFor(ctx, ep.h);
    function osc(det, g, type) { var o = ctx.createOscillator(); if (type) o.type = type; else o.setPeriodicWave(wave); o.frequency.value = 60; o.detune.value = det; var gn = ctx.createGain(); gn.gain.value = g; o.connect(gn); gn.connect(v.am); o.start(); return [o, gn]; }
    v.am = ctx.createGain(); v.am.gain.value = 1 - ep.am * 0.5;                                          // the cylinders: the volume pulses at the firing rate
    var o1 = osc(0, 0.6), o2 = osc(5, 0.4), o3 = osc(0, ep.sub, 'sine');
    v.o1 = o1[0]; v.o2 = o2[0]; v.o3 = o3[0];
    v.lfo = ctx.createOscillator(); v.lfo.frequency.value = 30; v.lg = ctx.createGain(); v.lg.gain.value = ep.am * 0.5; v.lfo.connect(v.lg); v.lg.connect(v.am.gain); v.lfo.start();
    v.lfo2 = ctx.createOscillator(); v.lfo2.frequency.value = 17; v.lg2 = ctx.createGain(); v.lg2.gain.value = ep.amIrr * 0.35; v.lfo2.connect(v.lg2); v.lg2.connect(v.am.gain); v.lfo2.start();   // uneven firing: the V8 burble
    v.cut = ctx.createBiquadFilter(); v.cut.type = 'lowpass'; v.cut.frequency.value = 900; v.cut.Q.value = 0.9;
    v.res = ctx.createBiquadFilter(); v.res.type = 'peaking'; v.res.frequency.value = 140; v.res.Q.value = 2.2; v.res.gain.value = 6;       // the exhaust note
    v.am.connect(v.cut); v.cut.connect(v.res); v.res.connect(mix);
    v.nz = ctx.createBufferSource(); v.nz.buffer = a.noise; v.nz.loop = true; v.nz.start();
    function noiseTo(type, f, q, g) { var b = ctx.createBiquadFilter(); b.type = type; b.frequency.value = f; b.Q.value = q; var gn = ctx.createGain(); gn.gain.value = g; v.nz.connect(b); b.connect(gn); return [b, gn]; }
    var it = noiseTo('bandpass', 1200, 1.2, 0), rd = noiseTo('lowpass', 260, 0.7, 0), wd = noiseTo('bandpass', 1500, 0.5, 0), sk = noiseTo('bandpass', 2200, 5, 0);
    v.itB = it[0]; v.itG = it[1]; v.rdG = rd[1]; v.wdG = wd[1]; v.skB = sk[0]; v.skG = sk[1];
    v.itG.connect(mix); v.rdG.connect(v.out = ctx.createGain()); v.wdG.connect(v.out); v.skG.connect(v.out); mix.connect(v.out);
    v.out.gain.value = 0;
    v.wh = ctx.createOscillator(); v.wh.type = ep.turbo ? 'sine' : 'triangle'; v.wh.frequency.value = 800; v.whG = ctx.createGain(); v.whG.gain.value = 0; v.wh.connect(v.whG); v.whG.connect(v.out); v.wh.start();   // VTEC howl or turbo whistle
    v.skLfo = ctx.createOscillator(); v.skLfo.frequency.value = 9; v.skLg = ctx.createGain(); v.skLg.gain.value = 0; v.skLfo.connect(v.skLg); v.skLg.connect(v.skG.gain); v.skLfo.start();
    v.pan = ctx.createStereoPanner ? ctx.createStereoPanner() : null;
    if (v.pan) { v.out.connect(v.pan); v.pan.connect(a.master); } else v.out.connect(a.master);
    return v;
  }
  // From the car's speed work out the gear, the revs and what the driver is doing; fire the one-off sounds (shifts, crackles, blow-off); return what to set on the voice.
  function engineState(v, vfa, vmax, thr, slip, hb, dt, now) {
    var ep = v.ep, frac = Math.min(1, vfa / vmax), g = 0;
    while (g < 5 && frac > GEAR_TOP[g]) g++;
    var lo = g ? GEAR_TOP[g - 1] : 0, gp = Math.max(0, Math.min(1, (frac - lo) / (GEAR_TOP[g] - lo)));
    var rpmN = vfa < 0.8 ? (thr ? 0.12 : 0.02) : (0.24 + 0.7 * gp);
    if (thr && rpmN < 0.95) rpmN = Math.min(1, rpmN + 0.05);
    var a = G ? G.aud : null, shifting = now < v.shiftUntil;
    if (g !== v.gear && vfa > 3) {                                                    // a gear change: a dip in the revs and a blip of the throttle
      var up = g > v.gear; v.shiftUntil = now + 220; v.gear = g;
      if (up && thr && a) noiseHit(a, 0.05, 0.08, 900, 300, 0);
      v.dropRpm = up ? 0.55 : 0.9;
    } else if (g !== v.gear) v.gear = g;
    if (shifting) rpmN = Math.min(rpmN, v.dropRpm || rpmN) * 0.9;
    v.smRpm += (rpmN - v.smRpm) * Math.min(1, dt * (rpmN > v.smRpm ? 9 : 5));
    var st = (v.ctx.currentTime - v.startT);                                          // the starter motor: the engine cranks, coughs and catches
    var rpmR = ep.idle + (ep.red - ep.idle) * v.smRpm, starting = st < 1.0, crank = 1;
    if (starting) { var k = st / 1.0; rpmR = k < 0.55 ? 140 + 90 * Math.sin(st * 38) : 140 + (ep.idle - 140) * Math.pow((k - 0.55) / 0.45, 0.6); crank = k < 0.55 ? 0.6 + 0.4 * Math.sin(st * 70) : 1; }
    // turbo
    v.boost += ((thr && v.smRpm > 0.3 ? 1 : 0) * Math.min(1, v.smRpm * 1.3) - v.boost) * Math.min(1, dt * (thr ? 2.5 : 6));
    if (ep.bov && v.lastThr && !thr && v.boost > 0.55 && a) { bovSound(a, v); v.boost = 0.2; }
    // exhaust pops when you lift off
    if (!thr && v.smRpm > 0.5 && vfa > 8 && Math.random() < ep.crack * dt * 9 && a) popSound(a, v);
    v.lastThr = thr ? 1 : 0;
    var f = rpmR / 60 * ep.cyl / 2;
    return { f: f, cut: (380 + 1900 * v.smRpm) * ep.lp * (thr ? 1.15 : 0.85), res: f * 0.5, vol: (0.11 + (thr ? 0.12 : 0.03) + 0.1 * frac) * crank, am: f,
             itK: ep.intake * (thr ? 1 : 0.25) * (0.02 + 0.07 * v.smRpm * v.smRpm), road: 0.02 + 0.12 * frac, wind: 0.2 * frac * frac, skid: slip,
             wh: ep.turbo ? 900 + 3200 * v.boost : (ep.whine && v.smRpm > 0.6 ? 1500 + 3500 * (v.smRpm - 0.6) : 0), whG: ep.turbo ? 0.025 * v.boost * (thr ? 1 : 0.4) : (ep.whine && v.smRpm > 0.6 ? ep.whine * 0.05 * (v.smRpm - 0.6) / 0.4 : 0) };
  }
  function setEngineVoice(v, p) {                  // p comes from engineState, plus vol (distance) pan, dopp, cutK from the caller
    var t = v.ctx.currentTime, dp = p.dopp || 1, ep = v.ep, f = p.f * dp, vol = p.volK === undefined ? 1 : p.volK, i;
    v.o1.frequency.setTargetAtTime(f, t, 0.03); v.o2.frequency.setTargetAtTime(f, t, 0.03); v.o3.frequency.setTargetAtTime(f * 0.5, t, 0.03);
    v.lfo.frequency.setTargetAtTime(f * 0.5, t, 0.03); v.lfo2.frequency.setTargetAtTime(f * 0.37, t, 0.03);
    v.cut.frequency.setTargetAtTime(p.cut * (p.cutK === undefined ? 1 : p.cutK), t, 0.05); v.res.frequency.setTargetAtTime(Math.max(60, p.res * 2.2), t, 0.05);
    v.out.gain.setTargetAtTime(p.vol * 1.1 * vol, t, 0.04);
    v.itB.frequency.setTargetAtTime(500 + f * 6, t, 0.06); v.itG.gain.setTargetAtTime(p.itK, t, 0.06);
    v.rdG.gain.setTargetAtTime(p.road, t, 0.1); v.wdG.gain.setTargetAtTime(p.wind, t, 0.1);
    v.skG.gain.setTargetAtTime(p.skid, t, 0.05); v.skB.frequency.setTargetAtTime(1500 + p.skid * 3500, t, 0.08); v.skLg.gain.setTargetAtTime(p.skid * 0.3, t, 0.08);
    if (p.wh) v.wh.frequency.setTargetAtTime(p.wh * dp, t, 0.05); v.whG.gain.setTargetAtTime(p.whG, t, 0.05);
    if (v.pan) v.pan.pan.setTargetAtTime(p.pan || 0, t, 0.05);
  }
  function stopEngineVoice(v) {
    if (!v) return; var t = v.ctx.currentTime;
    [v.o1, v.o2].forEach(function (o) { o.frequency.cancelScheduledValues(t); o.frequency.setTargetAtTime(14, t, 0.12); });    // the engine dies away
    v.out.gain.setTargetAtTime(0, t + 0.05, 0.12);
    setTimeout(function () { try { [v.o1, v.o2, v.o3, v.lfo, v.lfo2, v.wh, v.skLfo, v.nz].forEach(function (o) { o.stop(); }); v.out.disconnect(); } catch (e) {} }, 800);
  }
  function popSound(a, v) {                         // a crackle out of the exhaust
    var ctx = a.ctx, t = ctx.currentTime, src = ctx.createBufferSource(), g = ctx.createGain(), f = ctx.createBiquadFilter();
    src.buffer = a.noise; f.type = 'bandpass'; f.frequency.value = 700 + Math.random() * 1500; f.Q.value = 1.5;
    g.gain.setValueAtTime(0.5 + Math.random() * 0.4, t); g.gain.exponentialRampToValueAtTime(0.001, t + 0.05 + Math.random() * 0.05);
    src.connect(f); f.connect(g); g.connect(v.out); src.start(t, Math.random()); src.stop(t + 0.14);
  }
  function bovSound(a, v) {                         // the turbo's blow-off valve
    var ctx = a.ctx, t = ctx.currentTime, src = ctx.createBufferSource(), g = ctx.createGain(), f = ctx.createBiquadFilter();
    src.buffer = a.noise; f.type = 'highpass'; f.frequency.setValueAtTime(2600, t); f.frequency.exponentialRampToValueAtTime(1100, t + 0.55);
    g.gain.setValueAtTime(0.0, t); g.gain.linearRampToValueAtTime(0.35, t + 0.03); g.gain.exponentialRampToValueAtTime(0.001, t + 0.6);
    src.connect(f); f.connect(g); g.connect(v.out); src.start(t, Math.random()); src.stop(t + 0.7);
  }
  function ownEngine(c, inp, sp, dt) {
    var a = G.aud; if (!a) return;
    if (!a.ownCar || a.ownCar.model !== c.m) { if (a.ownCar) stopEngineVoice(a.ownCar); a.ownCar = makeEngineVoice(a, c.m); }
    var slip = (Math.abs(c.vl) > 2.5 && c.speed > 4) || (inp.hb && c.speed > 6) ? Math.min(0.4, 0.08 + (Math.abs(c.vl) - 2) * 0.05 + (inp.hb ? 0.1 : 0)) : 0;
    setEngineVoice(a.ownCar, engineState(a.ownCar, Math.abs(c.vf), sp.vmax, !!inp.f, slip, !!inp.hb, dt, performance.now()));
  }
  function stopOwnEngine() { var a = G.aud; if (a && a.ownCar) { stopEngineVoice(a.ownCar); a.ownCar = null; } }
  function doorSound() { var a = G.aud; if (a) { noiseHit(a, 0.3, 0.1, 700, 160, 95); setTimeout(function () { if (G && G.aud) noiseHit(G.aud, 0.1, 0.04, 3000, 900, 0); }, 90); } }
  function crashSound(spd) {                        // metal, a low thump, ringing panels and, in a hard one, breaking glass
    var a = G.aud; if (!a) return; var ctx = a.ctx, t = ctx.currentTime, k = Math.min(1, spd / 40);
    noiseHit(a, 0.35 + 0.65 * k, 0.4 + 0.5 * k, 2600, 140, 70);
    [520, 1180, 1900].forEach(function (fr, i) {
      var o = ctx.createOscillator(), g = ctx.createGain(), f = ctx.createBiquadFilter(); o.type = 'square'; o.frequency.value = fr * (0.9 + Math.random() * 0.2); f.type = 'bandpass'; f.frequency.value = fr; f.Q.value = 8;
      g.gain.setValueAtTime(0.06 * (0.5 + k), t + 0.01 * i); g.gain.exponentialRampToValueAtTime(0.001, t + 0.3 + 0.2 * k); o.connect(f); f.connect(g); g.connect(a.master); o.start(t); o.stop(t + 0.6);
    });
    if (spd > 22) for (var j = 0; j < 7; j++) setTimeout(function () { if (G && G.aud) noiseHit(G.aud, 0.12 * Math.random() + 0.04, 0.07, 9000, 4500, 0); }, 40 + j * (30 + Math.random() * 60));
  }
  function hornSound(gain, pan, model) {
    var a = G.aud; if (!a || gain < 0.02) return; var ctx = a.ctx, t = ctx.currentTime, g = ctx.createGain(), p = ctx.createStereoPanner ? ctx.createStereoPanner() : null, hf = HORN[model] || HORN.civic;
    g.gain.setValueAtTime(0, t); g.gain.linearRampToValueAtTime(0.16 * gain, t + 0.02); g.gain.setValueAtTime(0.16 * gain, t + 0.42); g.gain.linearRampToValueAtTime(0, t + 0.5);
    hf.forEach(function (f) { var o = ctx.createOscillator(); o.type = 'sawtooth'; o.frequency.value = f; var lp = ctx.createBiquadFilter(); lp.type = 'lowpass'; lp.frequency.value = 2000; o.connect(lp); lp.connect(g); o.start(t); o.stop(t + 0.55); });
    if (p) { p.pan.value = pan || 0; g.connect(p); p.connect(a.master); } else g.connect(a.master);
  }
  // ---- other people's cars
  function onCarFrame(dv) {
    var n = dv.getUint16(1, true), p = 3;
    for (var i = 0; i < n; i++, p += 17) {
      var id = dv.getUint16(p, true), o = G.cars.get(id);
      if (G.car && G.car.id === id) continue;
      if (!o) { o = { id: id, el: null, fresh: true, wheel: 0, lx: 0, lz: 0, spd: 0, pitch: 0, roll: 0, dents: [0, 0, 0, 0], x: 0, y: 0, z: 0, yaw: 0 }; G.cars.set(id, o); }
      o.tx = dv.getInt16(p + 2, true) / 16; o.ty = dv.getInt16(p + 4, true) / 16; o.tz = dv.getInt16(p + 6, true) / 16;
      o.tyaw = dv.getUint8(p + 8) / 256 * 2 * Math.PI; o.steerR = dv.getInt8(p + 9) / 127 * 0.6; o.mi = dv.getUint8(p + 10); o.m = CAR_IDS[o.mi] || 'civic';
      o.dmg = dv.getUint8(p + 11) / 2.55; o.flags = dv.getUint8(p + 12);
      for (var q = 0; q < 4; q++) o.dents[q] = dv.getUint8(p + 13 + q) / 255;
      if (o.fresh) { o.x = o.tx; o.y = o.ty; o.z = o.tz; o.yaw = o.tyaw; o.lx = o.x; o.lz = o.z; o.fresh = false; }
    }
  }
  function carDrawOpts(c, cockpit) { return { steer: c.steerR !== undefined ? c.steerR : c.steer, wheelSpin: c.wheel, damage: Math.min(1, (c.dmg || 0) / 100), dentF: c.dents[0], dentB: c.dents[1], dentL: c.dents[2], dentR: c.dents[3], brake: c.brakeL ? 1 : 0, head: c.headL ? 1 : 0, cockpit: !!cockpit, tint: 0 }; }
  function addCarWreck(m) {
    var w = newWreck('c', m, [0, 0, 0], { yaw: m.yaw / 256 * 2 * Math.PI, pt: 0, rl: 0 });
    w.wy = w.wp = w.wr = 0; w.dents = m.dents || [1, 0.6, 0.4, 0.4]; w.cm = m.m;
    if (G.car && G.car.id === m.id) { /* the driver is already out */ }
    return w;
  }
  function drawCars(vp, ex, ey, ez, dt, now) {
    if (!G.car && !G.cars.size && !G.wrecks.some(function (w) { return w.kind === 'c'; })) return;
    var r = carRenderer(); if (!r) return;
    var fn = RDIST * 16 - 44, ff = RDIST * 16 - 6, cam = [ex, ey, ez], a = G.aud;
    if (G.car) { var c = G.car; r.draw(c.m, carMat(c), vp, cam, fn, ff, carDrawOpts(c, c.view === 1)); }
    G.cars.forEach(function (o) {
      var k = Math.min(1, dt * 12);
      o.x += (o.tx - o.x) * k; o.y += (o.ty - o.y) * k; o.z += (o.tz - o.z) * k; o.yaw += angDiff(o.yaw, o.tyaw) * k;
      var sp0 = Math.hypot(o.x - o.lx, o.z - o.lz) / Math.max(dt, 0.001); o.lx = o.x; o.lz = o.z; o.spd += (sp0 - o.spd) * Math.min(1, dt * 3); o.wheel += o.spd * dt / 0.33;
      o.brakeL = !!(o.flags & 1); o.headL = !!(o.flags & 2);
      var dx = o.x - ex, dz = o.z - ez, d2 = dx * dx + dz * dz;
      // other drivers' engines
      if (a) {
        var key = 'c' + o.id, v = a.voices.get(key);
        if ((o.flags & 4) && d2 < 110 * 110) {
          if (!v) { if (a.voices.size >= 7) { v = null; } else { v = makeEngineVoice(a, o.m); v.isCar = true; a.voices.set(key, v); } }
          if (v) {
            var dist = Math.sqrt(d2), rdot = o.rd === undefined ? 0 : (dist - o.rd) / Math.max(dt, 0.001); o.rd = dist;
            var spc = cspec(o.m), st = engineState(v, o.spd, spc.vmax, o.spd > 1.5, 0, false, dt, now), psp = project(vp, o.x, o.y + 1, o.z, G.cw, G.ch);
            st.dopp = 1 / (1 + Math.max(-60, Math.min(60, rdot)) / 200); st.volK = 1.2 * Math.pow(1 / (1 + dist / 12), 1.4); st.cutK = lineBlocked(ex, ey, ez, o.x, o.y + 1, o.z) ? 0.35 : 1; st.pan = psp ? Math.max(-1, Math.min(1, (psp.x / G.cw * 2 - 1) * 1.1)) : 0;
            setEngineVoice(v, st);
          }
        } else if (v) { stopEngineVoice(v); a.voices.delete(key); }
      }
      if (d2 > 230 * 230 || !CARS[o.m]) return;
      var cb = { x: o.x, y: o.y, z: o.z, yaw: o.yaw, pitch: 0, roll: 0 };
      r.draw(o.m, carMat(cb), vp, cam, fn, ff, carDrawOpts(o, false));
    });
    G.wrecks.forEach(function (w) {
      if (w.kind !== 'c' || !CARS[w.cm]) return;
      var ddx = w.x - ex, ddz = w.z - ez; if (ddx * ddx + ddz * ddz > 230 * 230) return;
      var o2 = { steer: 0, wheel: 0, dmg: 100, dents: w.dents, brakeL: false, headL: false };
      var opt = carDrawOpts(o2, false); opt.tint = 0.85; opt.damage = 1;
      r.draw(w.cm, carMat({ x: w.x, y: w.y - 1.07, z: w.z, yaw: w.yaw, pitch: w.pt, roll: w.rl }), vp, cam, fn, ff, opt);
    });
  }
  // ---- shop cards
  function carThumb(id) {
    if (CTHUMBS[id]) return CTHUMBS[id];
    var r = carRenderer(); if (!r || !r.thumbnail) return '';
    try { var u = r.thumbnail(id, 150, 80); if (u) CTHUMBS[id] = u; } catch (e) {}
    return CTHUMBS[id] || '';
  }
  function renderCarShop() {
    var box = $('ew-shop-cars'); if (!box) return;
    carCss(); heliCss();
    var own = G.inv.car || {}, prices = G.inv.cp || {}, html = '<h4>Cars</h4><div class="warn">Cars take damage. Crash one until it is totaled and it is gone for good. Anyone can hijack a parked car.</div>';
    CAR_IDS.forEach(function (id) {
      var sp = CARS[id], price = prices[id] || sp.price, st = own[id], img = carThumb(id);
      var btn1 = st ? '<button class="win-btn" data-c="launch" data-id="' + id + '"' + (G.car || G.drone || G.heli || G.jet ? ' disabled' : '') + '>' + (G.car && G.car.m === id ? 'Driving' : 'Bring out') + '</button>'
        : '<button class="win-btn" data-c="buy" data-id="' + id + '">' + (G.cArm === id ? 'Click again to confirm' : 'Buy') + '</button>';
      html += '<div class="ew-hcard">' + (img ? '<img alt="" src="' + img + '">' : '<img alt="">') + '<div class="nfo"><b>' + esc(sp.name) + '</b><small>' + esc(sp.facts) + '</small><span>' + fmt(price) + ' PlaceBux' + (st ? ' · owned' : '') + ' · top speed ' + Math.round(sp.vmax * 3.6) + ' km/h</span></div><div class="act">' + btn1 + '</div></div>';
    });
    if (box.dataset.sig !== html) { box.dataset.sig = html; box.innerHTML = html; }
  }
  function carShopClick(e) {
    var t = e.target; if (!t || !t.getAttribute || !t.getAttribute('data-c')) return;
    var id = t.getAttribute('data-id'), kind = t.getAttribute('data-c'), prices = G.inv.cp || {}, price = prices[id] || CARS[id].price, now = performance.now();
    if (kind === 'launch') { closeShop(); G.csel = id; summonCar(); return; }
    if (G.cArm === id && now < G.cArmT) { G.cArm = ''; send({ t: 'buy', item: id }); renderInv(); return; }
    if (balance() < price) { $('ew-shop-note').textContent = 'You need ' + fmt(price) + ' PlaceBux (you have ' + fmt(balance()) + ').'; return; }
    $('ew-shop-note').textContent = ''; G.cArm = id; G.cArmT = now + 3000; renderInv();
  }

  // ---- flying the drone
  function toggleDrone() {
    if (G.heli || G.jet || G.car) return;
    if (G.drone) { send({ t: 'drone', on: false }); return; }
    if (!G.inv.drone) { sys('You do not own an FPV drone. Buy one in the shop (B).'); return; }
    if (!G.me || !G.me.onGround) { sys('Stand on the ground to launch the drone.'); return; }
    send({ t: 'drone', on: true });
  }
  function fpvOverlay(on) {                       // the goggles: noise, scan lines, a vignette, a horizon and the flight controller's on-screen display
    var el = $('ew-fpv');
    if (!el) {
      el = document.createElement('div'); el.id = 'ew-fpv';
      var hor = ''; for (var d = -40; d <= 40; d += 10) { if (d === 0) hor += '<i style="top:-1px;left:-90px;width:70px"></i><i style="top:-1px;left:20px;width:70px"></i>'; else hor += '<i style="top:' + (-d * 4.6 - 1).toFixed(0) + 'px;left:' + (d > 0 ? -34 : -34) + 'px;width:68px;opacity:.75;height:1px"></i>'; }
      el.innerHTML = '<div class="fp-noise"></div><div class="fp-lines"></div><div class="fp-vig"></div><div class="fp-hurt"></div><div class="fp-hor" id="fp-hor">' + hor + '</div>' +
        '<div class="fp-top"><span id="fp-rssi"></span><span id="fp-time"></span><span class="fp-rec">&#9679; REC</span></div>' +
        '<div class="fp-bat"><span id="fp-bat"></span> <span id="fp-warn"></span></div>' +
        '<div class="fp-bot"><span id="fp-mode">ANGLE</span><span id="fp-spd"></span><span id="fp-alt"></span><span id="fp-home"></span><span id="fp-hp"></span></div>';
      ($('ew-stage') || G.view).appendChild(el);
    }
    el.style.display = on ? 'block' : 'none';
    G.canvas.style.filter = on ? 'contrast(1.14) saturate(1.3) brightness(1.04)' : '';
  }
  function startDrone(m) {
    G.drone = { x: m.x, y: m.y, z: m.z, vx: 0, vy: 0, vz: 0, yaw: G.me.yaw, pitch: 0, crashAt: 0, mult: m.mult || 1 };
    G.dhp = m.hp || 100; G.dhurt = 0; G.dview = { roll: 0, lean: 0, t0: performance.now(), jolt: 0 }; heliCss(); fpvOverlay(true);
    G.inv.armed = false; G.dlast = ''; G.dsent = 0;
    G.view.classList.add('fpv'); $('ew-osd').style.display = 'flex'; $('ew-cross').style.display = 'block';
    renderInv();
  }
  function endDrone(m) {
    G.drone = null; G.bodyWait = true;                             // back in your body; wait for the ground under it to be loaded before it moves
    G.view.classList.remove('fpv'); $('ew-osd').style.display = 'none'; fpvOverlay(false);
    if (G.aud && G.aud.own) { stopDroneVoice(G.aud.own, !!(m && m.crash)); G.aud.own = null; if (m && m.crash) noiseHit(G.aud, 0.9, 0.9, 3000, 120, 110); }
    if (m && m.crash) { sys('Signal lost: your drone crashed.'); G.shake = performance.now() + 250; }
    renderInv();
  }
  function removeDrone(id) { if (G.aud) { var av = G.aud.voices.get(id); if (av) { stopDroneVoice(av, true); G.aud.voices.delete(id); } } var d = G.drones.get(id); if (d && d.el) d.el.remove(); G.drones.delete(id); }
  function sendDronePos(force) {
    var d = G.drone; if (!d || !G.ws || G.ws.readyState !== 1) return;
    var qx = Math.round(d.x * 16), qy = Math.round(d.y * 16), qz = Math.round(d.z * 16);
    var yaw = Math.floor(((d.yaw % (2 * Math.PI) + 2 * Math.PI) % (2 * Math.PI)) / (2 * Math.PI) * 256) & 255;
    var pit = Math.max(0, Math.min(255, Math.round((d.pitch + Math.PI / 2) / Math.PI * 255)));
    var key = qx + ',' + qy + ',' + qz + ',' + yaw + ',' + pit;
    if (key === G.dlast && !force) return;
    G.dlast = key; G.dsent = performance.now();
    var b = new ArrayBuffer(9), dv = new DataView(b);
    dv.setUint8(0, 2); dv.setInt16(1, qx, true); dv.setInt16(3, qy, true); dv.setInt16(5, qz, true); dv.setUint8(7, yaw); dv.setUint8(8, pit);
    G.ws.send(b);
  }
  function flyDrone(dt) {
    var d = G.drone, k = G.keys, now = performance.now();
    if (d.crashAt && now - d.crashAt > 1200) d.crashAt = 0;       // the server did not agree it was a crash: carry on flying
    var inp = (d.crashAt || !locked()) ? {} : { f: k.KeyW || k.ArrowUp, b: k.KeyS || k.ArrowDown, l: k.KeyA || k.ArrowLeft, r: k.KeyD || k.ArrowRight, up: k.Space, down: k.ShiftLeft || k.ShiftRight };
    var res = stepDrone(d, inp, dt);
    if (res === 'crash' && !d.crashAt) { d.crashAt = now; d.vx = d.vy = d.vz = 0; sendDronePos(true); send({ t: 'dcrash' }); }
    else if (now - G.dsent >= 50) sendDronePos();
    // what an FPV pilot sees: the camera leans into the flight, banks sideways, shakes with the motors, and the picture breaks up far from home
    var dv = G.dview, fxh = -Math.sin(d.yaw), fzh = -Math.cos(d.yaw), vf = d.vx * fxh + d.vz * fzh, vr = d.vx * Math.cos(d.yaw) - d.vz * Math.sin(d.yaw);
    var yr = angDiff(d.pyaw === undefined ? d.yaw : d.pyaw, d.yaw) / Math.max(dt, 0.001); d.pyaw = d.yaw;
    var kk = Math.min(1, dt * 7);
    dv.roll += (Math.max(-0.75, Math.min(0.75, vr / 45 * 0.6 + Math.max(-5, Math.min(5, yr)) * 0.05)) - dv.roll) * kk;
    dv.lean += (-Math.max(-1, Math.min(1, vf / 45)) * 0.32 - dv.lean) * kk;
    var spn = Math.min(1, Math.sqrt(d.vx * d.vx + d.vy * d.vy + d.vz * d.vz) / 45), amp = 0.003 + 0.009 * spn + (dv.jolt || 0) * 0.06, tt = now * 0.001; dv.jolt = (dv.jolt || 0) * Math.exp(-6 * dt);
    var jr = (Math.sin(tt * 61) + Math.sin(tt * 97.3) * 0.7) * amp, jp = (Math.sin(tt * 73.1) + Math.sin(tt * 113) * 0.6) * amp * 0.8;
    var bs = jetBasis(d.yaw, Math.max(-1.5, Math.min(1.5, d.pitch + dv.lean + jp)), dv.roll + jr); d.f = bs.f; d.u = bs.u;
    var me = G.me, sp = Math.sqrt(d.vx * d.vx + d.vy * d.vy + d.vz * d.vz), home = Math.hypot(d.x - me.x, d.y - me.y, d.z - me.z);
    if (!G.dfpvAt || now - G.dfpvAt > 60) {
      G.dfpvAt = now;
      var el = $('ew-fpv'), secs = (now - dv.t0) / 1000, volts = Math.max(13.2, 16.8 - 2.4 * secs / 240), sig = Math.max(0, Math.min(1, (home - 70) / 230));
      if (el) {
        var hpv = G.dhp === undefined ? 100 : G.dhp, hurtA = Math.max(0, 1 - (now - (G.dhurt || 0)) / 700);
        el.querySelector('.fp-noise').style.opacity = Math.min(0.9, 0.05 + sig * 0.75 + (hpv < 50 ? (50 - hpv) / 50 * 0.45 : 0)).toFixed(2); el.querySelector('.fp-hurt').style.opacity = Math.max(hurtA * 0.85, hpv < 30 ? 0.18 + 0.12 * Math.sin(now * 0.012) : 0).toFixed(2);
        $('fp-hp').textContent = 'HP ' + '█'.repeat(Math.max(0, Math.round(hpv / 10))) + '░'.repeat(10 - Math.max(0, Math.min(10, Math.round(hpv / 10)))) + ' ' + Math.max(0, Math.round(hpv)); $('fp-hp').className = hpv < 30 ? 'fp-bad' : ''; el.querySelector('.fp-noise').style.backgroundPosition = ((Math.random() * 160) | 0) + 'px ' + ((Math.random() * 160) | 0) + 'px';
        $('fp-hor').style.transform = 'rotate(' + (-dv.roll).toFixed(3) + 'rad) translateY(' + ((d.pitch + dv.lean + jp) * 270).toFixed(0) + 'px)';
        $('fp-rssi').textContent = 'LINK ' + Math.round(100 - sig * 90) + '%'; $('fp-time').textContent = Math.floor(secs / 60) + ':' + ('0' + Math.floor(secs % 60)).slice(-2);
        $('fp-bat').textContent = volts.toFixed(1) + 'V'; $('fp-warn').textContent = volts < 14.2 ? 'LOW BATTERY' : (sig > 0.6 ? 'LOW SIGNAL' : '');
        $('fp-warn').className = 'fp-bad'; $('fp-spd').textContent = Math.round(sp * 3.6) + ' km/h'; $('fp-alt').textContent = 'ALT ' + Math.round(d.y); $('fp-home').textContent = 'HOME ' + Math.round(home) + 'm';
      }
    }
    ownDroneSound(d, inp, dt, spn, G.dhp === undefined ? 100 : G.dhp);
    $('ew-osd-spd').textContent = sp.toFixed(1); $('ew-osd-alt').textContent = String(Math.round(d.y));
    $('ew-osd-dist').textContent = String(Math.round(home));
  }
  function drawDrones(vp, focal, ex, ey, ez, dt, now) {
    var layer = $('ew-fx'), f1 = Math.floor(now / 90) % 2 === 1;
    G.drones.forEach(function (dr) {
      var k = Math.min(1, dt * 14);
      dr.x += (dr.tx - dr.x) * k; dr.y += (dr.ty - dr.y) * k; dr.z += (dr.tz - dr.z) * k;
      if (G.aud) otherDroneSound(dr, vp, ex, ey, ez, dt, lineBlocked(ex, ey, ez, dr.x, dr.y, dr.z));
      var sp = project(vp, dr.x, dr.y, dr.z, G.cw, G.ch);
      if (!sp || sp.w > 170 || lineBlocked(ex, ey, ez, dr.x, dr.y, dr.z)) { if (dr.el) dr.el.style.display = 'none'; return; }
      if (!dr.el) {
        dr.el = document.createElement('div'); dr.el.className = 'ew-drone';
        var nm = document.createElement('span'); nm.className = 'ew-drone-name'; dr.nm = nm; dr.el.appendChild(nm); layer.appendChild(dr.el);
      }
      if (dr.nm.textContent !== dr.name) dr.nm.textContent = dr.name;
      var w = Math.max(22, Math.min(260, focal * 1.1 / sp.w));
      dr.el.style.display = 'block'; dr.el.style.width = w.toFixed(1) + 'px'; dr.el.style.height = (w / 2).toFixed(1) + 'px';
      dr.el.style.transform = 'translate(' + (sp.x | 0) + 'px,' + (sp.y | 0) + 'px) translate(-50%,-50%)';
      dr.el.classList.toggle('f1', f1);
    });
  }

  // ---- placing and breaking
  function targetNow() {
    var m = G.me, cp = Math.cos(m.pitch);
    return raycast(m.x, m.y + EYE, m.z, -Math.sin(m.yaw) * cp, Math.sin(m.pitch), -Math.cos(m.yaw) * cp, REACH);
  }
  function canAct() { var n = performance.now(); if (n - G.lastEdit < 110) return false; G.lastEdit = n; return true; }
  function sendEdit(x, y, z, c) {
    var ch = setBlock(x, y, z, c);                       // guess locally so it feels instant; the server's answer overrides it
    if (ch) meshNow(ch, x & 15, z & 15);
    if (G.ws && G.ws.readyState === 1) G.ws.send(JSON.stringify({ t: 'edit', x: x, y: y, z: z, c: c }));
  }
  function doBreak() {
    var h = targetNow(); if (!h || h.y === 0 || !canAct()) return;
    sendEdit(h.x, h.y, h.z, 0);
  }
  function doPlace() {
    var h = targetNow(); if (!h) return;
    var x = h.x + h.nx, y = h.y + h.ny, z = h.z + h.nz;
    if (y < 1 || y >= HMAX || x < WMIN || x >= WMAX || z < WMIN || z >= WMAX) return;
    if (getBlock(x, y, z) !== 0 || overlapsPlayer(G.me, x, y, z) || !canAct()) return;
    sendEdit(x, y, z, G.sel + 1);
  }
  function doPick() {
    var h = targetNow(); if (!h) return;
    var c = getBlock(h.x, h.y, h.z);
    if (c >= 1 && c <= 200) selectColor(c - 1);
  }

  // ---- chunk streaming and mesh upload
  function ensureChunks() {
    if (!G || !G.me || !G.ws || G.ws.readyState !== 1) return;
    var cv = G.drone || G.me;                                       // load the world around the camera, which is the drone while you fly
    var pcx = Math.floor(cv.x / 16), pcz = Math.floor(cv.z / 16), now = performance.now(), want = [], pending = 0;
    chunks.forEach(function (c, k) {
      if (Math.max(Math.abs(c.cx - pcx), Math.abs(c.cz - pcz)) > RDIST + 2) { freeMesh(c); chunks.delete(k); }
      else if (!c.loaded && now - c.reqAt < 6000) pending++;
    });
    for (var dx = -RDIST; dx <= RDIST; dx++) for (var dz = -RDIST; dz <= RDIST; dz++) {
      var cx = pcx + dx, cz = pcz + dz;
      if (cx < (WMIN >> 4) || cx > ((WMAX - 1) >> 4) || cz < (WMIN >> 4) || cz > ((WMAX - 1) >> 4)) continue;
      var c = chunks.get(ckey(cx, cz));
      if (c && (c.loaded || now - c.reqAt < 6000)) continue;
      want.push([dx * dx + dz * dz, cx, cz]);
    }
    want.sort(function (a, b) { return a[0] - b[0]; });
    var first = chunks.size < 30, room = Math.max(0, (first ? 64 : 48) - pending), list = [];
    for (var i = 0; i < want.length && list.length < Math.min(first ? 40 : 24, room); i++) {
      var w = want[i], ch = chunks.get(ckey(w[1], w[2]));
      if (!ch) { ch = newChunk(w[1], w[2]); chunks.set(ckey(w[1], w[2]), ch); }
      ch.reqAt = now; list.push([w[1], w[2]]);
    }
    if (list.length) G.ws.send(JSON.stringify({ t: 'chunks', list: list }));
  }
  function freeMesh(c) { if (c.mesh && c.mesh.buf && G && G.gl) G.gl.deleteBuffer(c.mesh.buf); c.mesh = null; }
  function upload(c) {
    var m = meshChunk(c), gl = G.gl;
    if (!c.mesh) c.mesh = { buf: null, count: 0 };
    if (m.count === 0) { if (c.mesh.buf) gl.deleteBuffer(c.mesh.buf); c.mesh = { buf: null, count: 0 }; c.dirty = false; return; }
    if (!c.mesh.buf) c.mesh.buf = gl.createBuffer();
    gl.bindBuffer(gl.ARRAY_BUFFER, c.mesh.buf); gl.bufferData(gl.ARRAY_BUFFER, m.data, gl.STATIC_DRAW);
    c.mesh.count = m.count; c.dirty = false;
  }
  function meshNow(ch, lx, lz) {                       // a block changed: rebuild at once (this chunk and any it borders)
    var list = [ch];
    if (lx === 0) list.push(chunks.get(ckey(ch.cx - 1, ch.cz))); if (lx === 15) list.push(chunks.get(ckey(ch.cx + 1, ch.cz)));
    if (lz === 0) list.push(chunks.get(ckey(ch.cx, ch.cz - 1))); if (lz === 15) list.push(chunks.get(ckey(ch.cx, ch.cz + 1)));
    list.forEach(function (c) { if (c && c.loaded) upload(c); });
  }

  // ---- one frame
  function frame(now) {
    if (!G || G.closed) return;
    G.raf = requestAnimationFrame(frame);
    var dt = Math.min(0.05, (now - G.last) / 1000); G.last = now;
    if (!G.me && !G.failed) { var wsecs = (performance.now() - G.t0) / 1000; $('ew-load').textContent = wsecs > 6 ? 'Still connecting... the server may be waking up (' + Math.floor(wsecs) + 's)' : 'Connecting...'; }
    if (menuOpen() && locked() && document.exitPointerLock) document.exitPointerLock();
    if (G.shake > now) { G.canvas.style.transform = 'translate(' + ((Math.random() - 0.5) * 8).toFixed(1) + 'px,' + ((Math.random() - 0.5) * 8).toFixed(1) + 'px)'; G.shaking = true; } else if (G.shaking) { G.canvas.style.transform = ''; G.shaking = false; }
    if (G.me && G.ready) {
      var hd = headingOf((G.drone || G.heli || G.jet || G.car || G.me).yaw);
      if (Math.abs(hd - G.lastHeading) > 0.2 || G.compassDirty) {
        G.lastHeading = hd; G.compassDirty = false;
        if (!G.compassOff) G.compassOff = document.createElement('canvas');
        var cs = getComputedStyle(G.view);
        try { drawCompass(G.compassCv, G.compassOff, hd, (cs.getPropertyValue('--accent') || '#ee5a52').trim(), (cs.getPropertyValue('--text-primary') || '#ece6d8').trim()); } catch (e) {}
        $('ew-compass-text').textContent = windName(hd) + '  ' + Math.round(hd) % 360 + '°';
      }
    }
    if (now - G.hintAt > 250) { G.hintAt = now; if (G.ready) $('ew-hint').style.display = (!locked() && !chatFocused() && !menuOpen() && !shopOpen()) ? 'flex' : 'none'; $('ew-compass').style.display = G.ready ? 'block' : 'none'; }
    var me = G.me;
    if (me) {
      // wait until the ground under you has arrived before letting physics run
      var under = chunks.get(ckey(Math.floor(me.x) >> 4, Math.floor(me.z) >> 4));
      var near = 0, total = 0;
      chunks.forEach(function (c) { total++; if (c.loaded) near++; });
      var need = Math.min(9, total);                       // the 3 x 3 chunks around you are enough to start; the rest streams in as you play
      if (!G.ready && !G.failed) $('ew-load').textContent = 'Loading the world... (' + near + ' of ' + need + ' chunks)';
      if (!G.ready && under && under.loaded && near >= need) {
        G.ready = true; $('ew-load').style.display = 'none';
        try { console.log('[EzWorld] ready after ' + Math.round(performance.now() - G.t0) + ' ms'); } catch (e) {}
        if (!locked() && !chatFocused()) $('ew-hint').style.display = 'flex';
      }
      if (G.ready) {
        var k = G.keys, inp = { f: k.KeyW || k.ArrowUp, b: k.KeyS || k.ArrowDown, l: k.KeyA || k.ArrowLeft, r: k.KeyD || k.ArrowRight, jump: k.Space, sprint: G.sprint && (k.KeyW || k.ArrowUp) };
        var changed = false;
        if (G.car) driveCar(dt);
        else if (G.jet) flyJet(dt);
        else if (G.heli) flyHeli(dt);
        else if (G.drone) flyDrone(dt);                                    // the body stands still while the drone flies
        else if (G.bodyWait) { var bu = chunks.get(ckey(Math.floor(me.x) >> 4, Math.floor(me.z) >> 4)); if (bu && bu.loaded) G.bodyWait = false; }
        else changed = stepPlayer(me, inp, dt);
        if (me.stuck > 1.5) { me.stuck = -4; send({ t: 'chat', text: '/unstuck' }); }       // could not push out on our own: ask the server for the nearest free spot
        sendPos(changed);
        $('ew-coords').textContent = 'X ' + me.x.toFixed(1) + '  Y ' + me.y.toFixed(1) + '  Z ' + me.z.toFixed(1);
      }
    }
    // rebuild a few changed chunks per frame, nearest first
    var budget = G.ready ? 3 : 8, t0 = performance.now(), cap = G.ready ? 8 : 20;      // build faster while the loading screen is up
    if (me) {
      var pend = [];
      chunks.forEach(function (c) { if (c.dirty && c.loaded) pend.push(c); });
      pend.sort(function (a, b) { return Math.abs(a.cx * 16 + 8 - me.x) + Math.abs(a.cz * 16 + 8 - me.z) - Math.abs(b.cx * 16 + 8 - me.x) - Math.abs(b.cz * 16 + 8 - me.z); });
      for (var i = 0; i < pend.length && budget > 0 && performance.now() - t0 < cap; i++, budget--) upload(pend[i]);
    }
    if (G.ready) carHints(now);
    draw(dt);
  }

  function draw(dt) {
    var gl = G.gl, me = G.me;
    gl.clearColor(SKY[0], SKY[1], SKY[2], 1); gl.clear(gl.COLOR_BUFFER_BIT | gl.DEPTH_BUFFER_BIT);
    if (!me) return;
    if (G.cfollow) updateCrashCam(dt, performance.now());
    var fe = $('ew-flash');
    if (G.flash) {                                                                   // a white-orange flash that fades as the aircraft blows up
      if (!fe) { fe = document.createElement('div'); fe.id = 'ew-flash'; fe.style.cssText = 'position:absolute;inset:0;pointer-events:none;z-index:4;opacity:0;background:radial-gradient(circle,#fff6d0 0%,#ff9a3a 55%,#a01000 100%)'; G.view.appendChild(fe); }
      var fa = 1 - (performance.now() - G.flash) / 700; fe.style.opacity = fa > 0 ? (fa * 0.85).toFixed(2) : '0'; if (fa <= 0) G.flash = 0;
    } else if (fe && fe.style.opacity !== '0') fe.style.opacity = '0';
    var cam = G.drone || G.hpose || G.jpose || G.carpose || G.cpose, fovNow = G.drone ? FOV + 0.68 : G.jpose ? FOV + 0.1 + Math.min(0.3, G.jet ? G.jet.speed / 330 : 0) : G.carpose ? FOV + (G.car && G.car.view === 1 ? 0.2 : 0.05 + Math.min(0.25, G.car ? Math.abs(G.car.vf) / cspec(G.car.m).vmax * 0.25 : 0)) : G.hpose || G.cpose ? FOV + 0.1 : FOV + (G.sprint && G.keys.KeyW ? 0.12 : 0);        // a wide lens while flying
    var ex = cam ? cam.x : me.x, ey = cam ? cam.y : me.y + EYE, ez = cam ? cam.z : me.z;
    var vp = mul(perspective(fovNow, G.cw / G.ch, 0.1, 600), (cam && cam.f ? viewLook(ex, ey, ez, cam.f, cam.u) : viewMatrix(ex, ey, ez, cam ? cam.yaw : me.yaw, cam ? cam.pitch : me.pitch)));
    G.vp = vp;
    gl.useProgram(G.prog);
    gl.uniformMatrix4fv(G.loc.vp, false, vp);
    gl.uniform3f(G.loc.cam, ex, ey, ez);
    gl.uniform3f(G.loc.fogCol, SKY[0], SKY[1], SKY[2]);
    gl.uniform2f(G.loc.fog, RDIST * 16 - 44, RDIST * 16 - 6);
    gl.enableVertexAttribArray(G.loc.pos); gl.enableVertexAttribArray(G.loc.col);
    var fx = -Math.sin(cam ? cam.yaw : me.yaw), fz = -Math.cos(cam ? cam.yaw : me.yaw);
    chunks.forEach(function (c) {
      if (!c.mesh || !c.mesh.count) return;
      var dx = c.cx * 16 + 8 - ex, dz = c.cz * 16 + 8 - ez;
      if (dx * dx + dz * dz > 1300 && dx * fx + dz * fz < 0) return;       // behind you and not close
      gl.bindBuffer(gl.ARRAY_BUFFER, c.mesh.buf);
      gl.vertexAttribPointer(G.loc.pos, 3, gl.FLOAT, false, 16, 0);
      gl.vertexAttribPointer(G.loc.col, 4, gl.UNSIGNED_BYTE, true, 16, 12);
      gl.uniform3f(G.loc.off, c.cx * 16, 0, c.cz * 16);
      gl.drawArrays(gl.TRIANGLES, 0, c.mesh.count);
    });
    G.wrecks.forEach(function (w) { stepWreck(w, dt); });
    drawHelis(vp, ex, ey, ez, dt, performance.now());
    drawJets(vp, ex, ey, ez, dt, performance.now());
    drawCars(vp, ex, ey, ez, dt, performance.now());
    // outline of what you are aiming at
    if (G.ready && !G.drone && !G.heli && !G.jet && !G.car) {
      var h = targetNow();
      if (h) {
        gl.bindBuffer(gl.ARRAY_BUFFER, G.lineBuf);
        gl.vertexAttribPointer(G.loc.pos, 3, gl.FLOAT, false, 16, 0);
        gl.disableVertexAttribArray(G.loc.col); gl.vertexAttrib4f(G.loc.col, 0, 0, 0, 1);
        gl.uniform3f(G.loc.off, h.x, h.y, h.z);
        gl.drawArrays(gl.LINES, 0, 24);
        gl.enableVertexAttribArray(G.loc.col);
      }
    }
    // other players: profile picture + name, as page elements so any picture works
    var focal = (G.ch / 2) / Math.tan(fovNow / 2);
    drawFx(vp, focal);
    drawDrones(vp, focal, ex, ey, ez, dt, performance.now());
    drawHeliFx(vp, focal, ex, ey, ez, dt, performance.now());
    drawJetFx(vp, focal, ex, ey, ez, dt, performance.now());
    G.others.forEach(function (p) {
      if (p.hidden) { p.el.style.display = 'none'; return; }
      var k = Math.min(1, dt * 12);
      p.x += (p.tx - p.x) * k; p.y += (p.ty - p.y) * k; p.z += (p.tz - p.z) * k;
      var cy = p.y + 1.0, sp = project(vp, p.x, cy, p.z, G.cw, G.ch);
      if (!sp || sp.w > 150 || lineBlocked(ex, ey, ez, p.x, cy, p.z)) { p.el.style.display = 'none'; return; }
      var size = Math.max(16, Math.min(220, focal * 1.0 / sp.w));
      p.el.style.display = 'flex';
      p.el.style.transform = 'translate(' + (sp.x | 0) + 'px,' + (sp.y | 0) + 'px) translate(-50%,-50%)';
      p.img.style.width = p.img.style.height = size.toFixed(0) + 'px';
      p.el.style.opacity = sp.w > 100 ? '0.6' : '1';
    });
  }

  function close(silent) {
    if (!G) return;
    var g = G; g.closed = true; G = null;
    cancelAnimationFrame(g.raf);
    g.timers.forEach(clearInterval); clearInterval(g.shopTimer);
    g.handlers.forEach(function (h) { h[0].removeEventListener(h[1], h[2], h[3]); });
    try { if (document.pointerLockElement && document.exitPointerLock) document.exitPointerLock(); } catch (e) {}
    try { if (g.ws) { g.ws.onclose = null; g.ws.close(); } } catch (e) {}
    chunks.forEach(function (c) { if (c.mesh && c.mesh.buf && g.gl) g.gl.deleteBuffer(c.mesh.buf); });
    chunks.clear();
    try { if (g.hr) g.hr.dispose(); } catch (e) {}
    try { if (g.aud) g.aud.ctx.close(); } catch (e) {}
    try { if (g.jr) g.jr.dispose(); } catch (e) {}
    try { if (g.cr) g.cr.dispose(); } catch (e) {}
    try { var ext = g.gl && g.gl.getExtension('WEBGL_lose_context'); if (ext) ext.loseContext(); } catch (e) {}
    g.view.style.display = 'none';
    $('ew-shop').style.display = 'none'; $('ew-fx').innerHTML = '';
    document.body.classList.remove('ew-open');
    if (!silent && window.showMenu) window.showMenu('home');       // leaving EzWorld returns to the main menu
    $('ew-leave').onclick = null;
    $('ew-tags').innerHTML = '';
    if (!silent && g.opts.onClose) g.opts.onClose();
  }

  window.EzWorld = { open: open, close: function () { close(false); } };
  window.EzWorld._t = { EP: EP, engineState: engineState, stepCar: stepCar, CARS: CARS, cspec: cspec, carGroundAt: carGroundAt, stepWreck: stepWreck, stepJet: stepJet, jetHit: jetHit, JETS: JETS, jspec: jspec, jetBasis: jetBasis, jetAngles: jetAngles, jetMat: jetMat, stepHeli: stepHeli, heliHit: heliHit, HELIS: HELIS, heliModelMat: heliModelMat, stepDrone: stepDrone, droneHits: droneHits, escapeBlocks: escapeBlocks, headingOf: headingOf, windName: windName, drawCompass: drawCompass, getBlock: getBlock, setBlock: setBlock, loadChunkFrame: loadChunkFrame, meshChunk: meshChunk, raycast: raycast, stepPlayer: stepPlayer,
                        collidesAt: collidesAt, perspective: perspective, viewMatrix: viewMatrix, mul: mul, project: project, chunks: chunks, ckey: ckey, setPal: function (p) { PAL = p; },
                        newChunk: newChunk, overlapsPlayer: overlapsPlayer };
})();
