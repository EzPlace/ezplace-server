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
    var cp = Math.cos(d.pitch), fx = -Math.sin(d.yaw) * cp, fy = Math.sin(d.pitch), fz = -Math.cos(d.yaw) * cp, rx = Math.cos(d.yaw), rz = -Math.sin(d.yaw);
    var f = (inp.f ? 1 : 0) - (inp.b ? 1 : 0), s = (inp.r ? 1 : 0) - (inp.l ? 1 : 0), u = (inp.up ? 1 : 0) - (inp.down ? 1 : 0);
    var wx = fx * f + rx * s, wy = fy * f + u, wz = fz * f + rz * s, wl = Math.sqrt(wx * wx + wy * wy + wz * wz);
    if (wl > 1) { wx /= wl; wy /= wl; wz /= wl; }
    d.vx += (wx * DR_ACCEL - d.vx * DR_DRAG) * dt; d.vy += (wy * DR_ACCEL - d.vy * DR_DRAG) * dt; d.vz += (wz * DR_ACCEL - d.vz * DR_DRAG) * dt;
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
  var HELI_REPAIR = 0.10, HELI_MAXY = 430, HELI_SPOOL = 4.0, HELI_READY = 0.92, HELI_TILT = 0.36;
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
    var tp = ready && !h.onGround ? ((inp.f ? 1 : 0) - (inp.b ? 1 : 0)) * HELI_TILT : 0, tr = ready && !h.onGround ? ((inp.r ? 1 : 0) - (inp.l ? 1 : 0)) * HELI_TILT * 0.8 : 0;
    var kt = Math.min(1, dt * 2.4);
    h.pt += (tp - h.pt) * kt; h.rl += (tr - h.rl) * kt;
    var wy = h.rpm > 0.5 ? ((inp.yl ? 1 : 0) - (inp.yr ? 1 : 0)) * sp.yawRate : 0;
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
    function fail(why) { h.why = why; h.vx = h.vy = h.vz = 0; res = 'crash'; }
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
          compassCv: $('ew-compass-cv'), compassOff: null, lastHeading: -999, compassDirty: true, t0: performance.now(), failed: false, inv: { rockets: 0, armed: false, price: 5000, per: 5, max: 50, drone: false, dprice: 300, flying: false, heli: {}, hp: {}, hrep: 0.1, hfly: null }, heli: null, helis: new Map(), wrecks: [], hsel: '', hArm: '', hArmT: 0, hcam: null, hpose: null, hcrashState: null, rks: [], booms: [], drone: null, drones: new Map(), dbuyArm: 0, bodyWait: false, dsent: 0, dlast: '', lastFire: -1e9, shake: 0, shaking: false, buyArm: 0, shopTimer: 0, msgEls: new Map(), typers: new Map(), replyTo: null, myTyping: false, typeTimer: 0, mention: null, hintAt: 0 };
    PAL = opts.colors.map(hexRgb);
    view.style.display = 'flex';
    document.body.classList.add('ew-open');
    if (menuOpen() && window.hideMenu) window.hideMenu();           // the menu you pressed Join in must not stay on top of the world
    $('ew-leave').onclick = function () { close(false); };
    $('ew-typing').style.display = 'none'; $('ew-reply-bar').style.display = 'none'; $('ew-mention').style.display = 'none';
    view.style.setProperty('--ew-launcher', pixUrl(ART_LAUNCHER)); view.style.setProperty('--ew-rocket', pixUrl(ART_ROCKET)); view.style.setProperty('--ew-boom', pixUrl(boomSheet())); view.style.setProperty('--ew-drone0', pixUrl(ART_DRONE[0])); view.style.setProperty('--ew-drone1', pixUrl(ART_DRONE[1]));
    heliCss();
    $('ew-shop').style.display = 'none'; $('ew-fx').innerHTML = ''; $('ew-hotbar').style.display = 'none'; $('ew-hotbar2').style.display = 'none'; $('ew-osd').style.display = 'none'; ['ew-hhud', 'ew-hhelp'].forEach(function (k) { var e0 = $(k); if (e0) e0.style.display = 'none'; }); view.classList.remove('fpv'); $('ew-held').style.display = 'none'; $('ew-online-total').textContent = '0';
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
      if (G.drone || G.heli) return;                         // no placing, breaking or rockets while flying
      if (e.button === 0) { if (G.inv.armed) fireRocket(); else doBreak(); } else if (e.button === 2) doPlace(); else if (e.button === 1) doPick();
    });
    on(canvas, 'wheel', function (e) { e.preventDefault(); if (G.heli) { G.hcam.dist = Math.max(4, Math.min(80, G.hcam.dist * (e.deltaY > 0 ? 1.1 : 0.9))); return; } selectColor(G.sel + (e.deltaY > 0 ? 1 : -1)); }, { passive: false });
    on(window, 'keydown', function (e) {
      if (!G || menuOpen() || typingElsewhere()) return;
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
      if (e.code === 'Digit1' && !G.drone && !G.heli) setArmed(!G.inv.armed);
      if (e.code === 'Digit2') toggleDrone();
      if (e.code === 'Digit3' && !e.repeat) toggleHeli();
      if (e.code === 'KeyV' && G.heli && !e.repeat) { G.heli.view = 1 - G.heli.view; G.hcam.yaw = 0; G.hcam.pitch = G.heli.view ? 0 : -0.22; }
      if (e.code === 'KeyB') { openShop(); return; }
      if (e.code === 'KeyQ' && !G.heli) selectColor(G.sel - 1);
      if (e.code === 'KeyE' && !G.heli) selectColor(G.sel + 1);
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
    on($('ew-shop-helis'), 'click', heliShopClick);
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
      case 'dtp': if (G.drone) { G.drone.x = m.x; G.drone.y = m.y; G.drone.z = m.z; G.drone.vx = G.drone.vy = G.drone.vz = 0; G.drone.crashAt = 0; if (m.v) { G.drone.vx = m.v[0]; G.drone.vy = m.v[1]; G.drone.vz = m.v[2]; } } break;
      case 'dr': if (m.on) { G.drones.set(m.id, { id: m.id, name: m.name || '', x: m.x, y: m.y, z: m.z, tx: m.x, ty: m.y, tz: m.z, el: null, fresh: false }); } else removeDrone(m.id); break;
      case 'hstart': startHeli(m); break;
      case 'hend': endHeli(m); break;
      case 'htp': if (G.heli) { G.heli.x = m.x; G.heli.y = m.y; G.heli.z = m.z; G.heli.vx = G.heli.vy = G.heli.vz = 0; G.heli.crashAt = 0; } break;
      case 'hr': if (m.on) setOtherHeli(m.id, m.m, m.name, m.x, m.y, m.z); else removeOtherHeli(m.id); break;
      case 'hx': addWreck(m); break;
      case 'inv': applyInv(m); break;
      case 'rk': addRocket(m); break;
      case 'bs': { (m.d || []).forEach(function (b) { setBlock(b[0], b[1], b[2], b[3]); }); if (m.e) addBoom(m.e, m.s); break; }
      case 'online': setTotal(m.total); break;
      case 'sys': sys(m.text); break;
      case 'chat': chatLine(m); break;
      case 'typing': { var k = String(m.name).toLowerCase(); if (m.typing) G.typers.set(k, { name: m.name, exp: Date.now() + 7000 }); else G.typers.delete(k); renderTyping(); break; }
      case 'reaction': { var rel = G.msgEls.get(m.mid); if (rel && window.EzChat) window.EzChat.apply(rel, m.emoji, m.user, m.on); break; }
      case 'join': addOther(m.id, m.name, m.x, m.y, m.z); sys(m.name + ' joined EzWorld.'); updateOnline(); break;
      case 'leave': { var o2 = G.others.get(m.id); if (o2) { sys(o2.name + ' left EzWorld.'); o2.el.remove(); G.others.delete(m.id); removeDrone(m.id); removeOtherHeli(m.id); G.typers.delete(o2.name.toLowerCase()); renderTyping(); updateOnline(); } break; }
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
  function updateOnline() { $('ew-online-n').textContent = String(G.others.size + 1); }
  function sendPos(force) {
    if (!G.me || G.heli || !G.ws || G.ws.readyState !== 1) return;
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
    var i = G.inv; i.rockets = m.rockets | 0; i.armed = !!m.armed && i.rockets > 0 && !G.drone && !G.heli;
    if (m.price) i.price = m.price; if (m.per) i.per = m.per; if (m.max) i.max = m.max;
    i.drone = !!m.drone; if (m.dprice) i.dprice = m.dprice; i.flying = !!m.flying;
    i.heli = m.heli || {}; if (m.hp) i.hp = m.hp; if (m.hrep) i.hrep = m.hrep; i.hfly = m.hfly || null;
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
    var hb3 = $('ew-hotbar3');
    if (hb3) {
      var hid = G.hsel && (i.heli || {})[G.hsel] ? G.hsel : (HELI_IDS.filter(function (k) { return (i.heli || {})[k]; }).pop() || '');
      hb3.style.display = hid ? 'flex' : 'none'; hb3.classList.toggle('sel', !!G.heli);
      $('ew-hotbar3-name').textContent = hid ? HELIS[hid].name + ((i.heli || {})[hid] === 2 ? ' (wrecked)' : '') : '';
    }
    if (shopOpen()) renderHeliShop();
  }
  function balance() { try { return typeof myBalance !== 'undefined' ? myBalance : 0; } catch (e) { return 0; } }
  function shopOpen() { return $('ew-shop').style.display === 'flex'; }
  function refreshShop() {
    $('ew-shop-bal').textContent = 'PlaceBux: ' + fmt(balance());
    if (G.buyArm && performance.now() > G.buyArm) { G.buyArm = 0; renderInv(); }
    if (G.dbuyArm && performance.now() > G.dbuyArm) { G.dbuyArm = 0; renderInv(); }
    if (G.hArm && performance.now() > G.hArmT) { G.hArm = ''; renderInv(); }
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
  function closeShop() { $('ew-shop').style.display = 'none'; clearInterval(G.shopTimer); G.buyArm = 0; G.dbuyArm = 0; G.hArm = ''; renderInv(); }
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
  function heliCss() {
    if (document.getElementById('ew-heli-css')) return;
    var st = document.createElement('style'); st.id = 'ew-heli-css';
    st.textContent = '#ew-hhud{display:none;position:absolute;left:50%;bottom:92px;transform:translateX(-50%);gap:14px;padding:6px 14px;font:700 12px "JetBrains Mono","Fira Code",monospace;color:#7dff9a;text-shadow:0 0 6px rgba(0,0,0,.9);background:rgba(8,12,10,.55);border:1px solid rgba(125,255,154,.35);border-radius:8px;pointer-events:none;flex-wrap:wrap;justify-content:center;max-width:94%}' +
      '#ew-hhud b{color:#fff}#ew-hhud .st{color:#ffd166}#ew-hhud .bad{color:#ff6b5e}#ew-hhud.cockpit{background:rgba(8,12,10,.8)}' +
      '#ew-hhelp{display:none;position:absolute;left:50%;bottom:62px;transform:translateX(-50%);font-size:11px;color:#fff;opacity:.75;text-shadow:0 0 4px #000;pointer-events:none;text-align:center;max-width:94%}' +
      '.ew-htag{position:absolute;left:0;top:0;font-size:12px;font-weight:700;color:#fff;text-shadow:0 0 4px #000;pointer-events:none;white-space:nowrap}' +
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
    if (G.drone) return;
    var own = G.inv.heli || {}, id = G.hsel && own[G.hsel] === 1 ? G.hsel : myHeliBest();
    if (!id) { var anyW = HELI_IDS.some(function (k) { return own[k] === 2; }); sys('You do not own a helicopter. Buy one in the shop (B).'); return; }
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
    var h = G.heli; G.heli = null; G.hpose = null;
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
    if (h.crashAt && now - h.crashAt > 1500) h.crashAt = 0;           // the server did not agree it was a crash: carry on
    var inp = (h.crashAt || !locked()) ? {} : { f: k.KeyW || k.ArrowUp, b: k.KeyS || k.ArrowDown, l: k.KeyA || k.ArrowLeft, r: k.KeyD || k.ArrowRight, yl: k.KeyQ, yr: k.KeyE, up: k.Space, down: k.ShiftLeft || k.ShiftRight };
    var res = h.crashAt ? 'ok' : stepHeli(h, inp, dt, sp);
    if (res === 'crash' && !h.crashAt) {
      h.crashAt = now; G.hcrashState = { vx: h.vx, vy: h.vy, vz: h.vz };
      sendHeliPos(true); send({ t: 'hcrash' });
    } else if (now - h.sent >= 50 || (now - h.sent >= 40 && res !== 'ok')) sendHeliPos(false);
    h.ang += dt * 40 * h.rpm;
    var me = G.me; me.x = h.x; me.y = h.y; me.z = h.z; me.yaw = h.yaw;
    // camera: chase (orbits the helicopter) or cockpit (the pilot's eyes)
    var hc = G.hcam, hp = G.hpose;
    hc.idle += dt; if (hc.idle > 1.2 && h.view === 0) hc.yaw *= Math.max(0, 1 - dt * 1.2);
    if (h.view === 0) {
      var ya = h.yaw + hc.yaw, cp = Math.cos(hc.pitch), tx = h.x, ty = h.y + sp.top * 0.55, tz = h.z;
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
  function addWreck(m) {
    var own = m.id === G.myId, h = own && G.heli ? G.heli : null, st = own ? G.hcrashState : null;
    var w = { m: m.m, x: m.x, y: m.y, z: m.z, yaw: h ? h.yaw : 0, pt: h ? h.pt : 0, rl: h ? h.rl : 0, vx: st ? st.vx : 0, vy: st ? Math.min(st.vy, 0) : 0, vz: st ? st.vz : 0, wr: (Math.random() - 0.5) * 2.2, wp: (Math.random() - 0.5) * 0.8, down: false, t0: performance.now(), puffs: [], fire: null, nextPuff: 0 };
    G.wrecks.push(w); while (G.wrecks.length > 6) killWreck(G.wrecks.shift());
  }
  function killWreck(w) { if (w.fire) w.fire.remove(); w.puffs.forEach(function (p) { p.el.remove(); }); }
  function drawHelis(vp, ex, ey, ez, dt, now) {
    if (!G.heli && !G.helis.size && !G.wrecks.length) return;
    var r = heliRenderer(); if (!r) return;
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
      if (!w.down) {
        w.vy = Math.max(w.vy - 24 * dt, -40); w.x += w.vx * dt; w.y += w.vy * dt; w.z += w.vz * dt; w.yaw += w.wr * dt; w.rl += w.wr * 0.4 * dt; w.pt += w.wp * dt;
        if (getBlock(Math.floor(w.x), Math.floor(w.y - 0.05), Math.floor(w.z)) !== 0) { w.y = Math.floor(w.y - 0.05) + 1; w.down = true; w.rl = Math.max(-0.5, Math.min(0.5, w.rl)) + 0.25; w.pt = Math.max(-0.4, Math.min(0.4, w.pt)); w.vx = w.vy = w.vz = 0; }
        if (w.y < 1) { w.y = 1; w.down = true; }
      }
      var ddx = w.x - ex, ddz = w.z - ez; if (ddx * ddx + ddz * ddz < 170 * 170 && HELIS[w.m]) r.draw(w.m, heliModelMat(w.x, w.y, w.z, w.yaw, w.pt, w.rl), vp, cam, fn, ff, 0.6, 0, 0.82);
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
    G.wrecks.forEach(function (w) {              // flames for a while, and smoke that rises and fades
      var age = (now - w.t0) / 1000, sp = project(vp, w.x, w.y + 1.1, w.z, G.cw, G.ch);
      var live = sp && sp.w < 160 && w.down && age < 45;
      if (live) {
        if (!w.fire) { w.fire = document.createElement('div'); w.fire.className = 'ew-boom'; layer.appendChild(w.fire); }
        var fs = Math.max(30, Math.min(500, focal * 5 / sp.w));
        w.fire.style.display = 'block'; w.fire.style.width = w.fire.style.height = fs.toFixed(0) + 'px'; w.fire.style.setProperty('--f', String(Math.floor(now / 140) % 4)); w.fire.style.opacity = String(Math.max(0, 0.9 - age / 50));
        w.fire.style.transform = 'translate(' + (sp.x | 0) + 'px,' + (sp.y | 0) + 'px) translate(-50%,-70%)';
      } else if (w.fire) w.fire.style.display = 'none';
      if (w.down && age < 75 && now > w.nextPuff && w.puffs.length < 7 && sp && sp.w < 160) {
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
      var sp = HELIS[id], price = prices[id] || sp.price, st = own[id], rep = Math.max(1, Math.floor(price * (G.inv.hrep || HELI_REPAIR))), img = heliThumb(id);
      var btn1 = st === 1 ? '<button class="win-btn" data-h="launch" data-id="' + id + '"' + (G.heli ? ' disabled' : '') + '>' + (G.heli && G.heli.m === id ? 'Flying' : 'Launch') + '</button>'
        : st === 2 ? '<button class="win-btn" data-h="repair" data-id="' + id + '">' + (G.hArm === 'repair_' + id ? 'Click again to confirm' : 'Repair ' + fmt(rep)) + '</button>'
        : '<button class="win-btn" data-h="buy" data-id="' + id + '">' + (G.hArm === id ? 'Click again to confirm' : 'Buy') + '</button>';
      html += '<div class="ew-hcard">' + (img ? '<img alt="" src="' + img + '">' : '<img alt="">') + '<div class="nfo"><b>' + esc(sp.name) + '</b><small>' + esc(sp.facts) + '</small><span>' + fmt(price) + ' PlaceBux' + (st === 1 ? ' · owned' : st === 2 ? ' · wrecked' : '') + ' · top speed ' + sp.vmax + ' b/s</span></div><div class="act">' + btn1 + '</div></div>';
    });
    if (box.dataset.sig !== html) { box.dataset.sig = html; box.innerHTML = html; }
  }
  function heliShopClick(e) {
    var t = e.target; if (!t || !t.getAttribute || !t.getAttribute('data-h')) return;
    var id = t.getAttribute('data-id'), kind = t.getAttribute('data-h'), prices = G.inv.hp || {}, price = prices[id] || HELIS[id].price, now = performance.now();
    if (kind === 'launch') { closeShop(); G.hsel = id; toggleHeli(); return; }
    var item = kind === 'repair' ? 'repair_' + id : id, cost = kind === 'repair' ? Math.max(1, Math.floor(price * (G.inv.hrep || HELI_REPAIR))) : price;
    if (G.hArm === item && now < G.hArmT) { G.hArm = ''; send({ t: 'buy', item: item }); renderInv(); return; }
    if (balance() < cost) { $('ew-shop-note').textContent = 'You need ' + fmt(cost) + ' PlaceBux (you have ' + fmt(balance()) + ').'; return; }
    $('ew-shop-note').textContent = ''; G.hArm = item; G.hArmT = now + 3000; renderInv();
  }

  // ---- flying the drone
  function toggleDrone() {
    if (G.heli) return;
    if (G.drone) { send({ t: 'drone', on: false }); return; }
    if (!G.inv.drone) { sys('You do not own an FPV drone. Buy one in the shop (B).'); return; }
    if (!G.me || !G.me.onGround) { sys('Stand on the ground to launch the drone.'); return; }
    send({ t: 'drone', on: true });
  }
  function startDrone(m) {
    G.drone = { x: m.x, y: m.y, z: m.z, vx: 0, vy: 0, vz: 0, yaw: G.me.yaw, pitch: 0, crashAt: 0 };
    G.inv.armed = false; G.dlast = ''; G.dsent = 0;
    G.view.classList.add('fpv'); $('ew-osd').style.display = 'flex'; $('ew-cross').style.display = 'block';
    renderInv();
  }
  function endDrone(m) {
    G.drone = null; G.bodyWait = true;                             // back in your body; wait for the ground under it to be loaded before it moves
    G.view.classList.remove('fpv'); $('ew-osd').style.display = 'none';
    if (m && m.crash) { sys('Signal lost: your drone crashed.'); G.shake = performance.now() + 250; }
    renderInv();
  }
  function removeDrone(id) { var d = G.drones.get(id); if (d && d.el) d.el.remove(); G.drones.delete(id); }
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
    var me = G.me, sp = Math.sqrt(d.vx * d.vx + d.vy * d.vy + d.vz * d.vz);
    $('ew-osd-spd').textContent = sp.toFixed(1); $('ew-osd-alt').textContent = String(Math.round(d.y));
    $('ew-osd-dist').textContent = String(Math.round(Math.hypot(d.x - me.x, d.y - me.y, d.z - me.z)));
  }
  function drawDrones(vp, focal, ex, ey, ez, dt, now) {
    var layer = $('ew-fx'), f1 = Math.floor(now / 90) % 2 === 1;
    G.drones.forEach(function (dr) {
      var k = Math.min(1, dt * 14);
      dr.x += (dr.tx - dr.x) * k; dr.y += (dr.ty - dr.y) * k; dr.z += (dr.tz - dr.z) * k;
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
      var hd = headingOf((G.drone || G.heli || G.me).yaw);
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
        if (G.heli) flyHeli(dt);
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
    draw(dt);
  }

  function draw(dt) {
    var gl = G.gl, me = G.me;
    gl.clearColor(SKY[0], SKY[1], SKY[2], 1); gl.clear(gl.COLOR_BUFFER_BIT | gl.DEPTH_BUFFER_BIT);
    if (!me) return;
    var cam = G.drone || G.hpose, fovNow = G.drone ? FOV + 0.45 : G.hpose ? FOV + 0.1 : FOV + (G.sprint && G.keys.KeyW ? 0.12 : 0);        // a wide lens while flying
    var ex = cam ? cam.x : me.x, ey = cam ? cam.y : me.y + EYE, ez = cam ? cam.z : me.z;
    var vp = mul(perspective(fovNow, G.cw / G.ch, 0.1, 600), viewMatrix(ex, ey, ez, cam ? cam.yaw : me.yaw, cam ? cam.pitch : me.pitch));
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
    drawHelis(vp, ex, ey, ez, dt, performance.now());
    // outline of what you are aiming at
    if (G.ready && !G.drone && !G.heli) {
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
  window.EzWorld._t = { stepHeli: stepHeli, heliHit: heliHit, HELIS: HELIS, heliModelMat: heliModelMat, stepDrone: stepDrone, droneHits: droneHits, escapeBlocks: escapeBlocks, headingOf: headingOf, windName: windName, drawCompass: drawCompass, getBlock: getBlock, setBlock: setBlock, loadChunkFrame: loadChunkFrame, meshChunk: meshChunk, raycast: raycast, stepPlayer: stepPlayer,
                        collidesAt: collidesAt, perspective: perspective, viewMatrix: viewMatrix, mul: mul, project: project, chunks: chunks, ckey: ckey, setPal: function (p) { PAL = p; },
                        newChunk: newChunk, overlapsPlayer: overlapsPlayer };
})();
