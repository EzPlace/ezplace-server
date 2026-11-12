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
  function stepPlayer(pl, inp, dt) {
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

  // ------------------------------------------------------------------ compass
  var WIND = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW'];
  function headingOf(yaw) { return (((-yaw * 180 / Math.PI) % 360) + 360) % 360; }   // 0 = north (-Z), 90 = east (+X), 180 = south, 270 = west
  function windName(h) { return WIND[Math.round(h / 45) % 8]; }
  // Draws the scrolling strip (about 120 degrees visible, faded at both ends). cv/off are canvases, colours come from the page theme.
  function drawCompass(cv, off, heading, accent, text) {
    var ctx = cv.getContext('2d'), W = cv.width, H = cv.height, k = W / 380, w = 380, h = 40, ppd = 3.4;
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
          compassCv: $('ew-compass-cv'), compassOff: null, lastHeading: -999, compassDirty: true, msgEls: new Map(), typers: new Map(), replyTo: null, myTyping: false, typeTimer: 0, mention: null, hintAt: 0 };
    PAL = opts.colors.map(hexRgb);
    view.style.display = 'flex';
    document.body.classList.add('ew-open');
    $('ew-leave').onclick = function () { close(false); };
    $('ew-typing').style.display = 'none'; $('ew-reply-bar').style.display = 'none'; $('ew-mention').style.display = 'none';
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
    on(canvas, 'webglcontextlost', function (e) { e.preventDefault(); if (G) { G.ready = false; $('ew-load').style.display = 'flex'; $('ew-load').textContent = 'Your graphics card reset EzWorld. Press Leave, then join again.'; } });
    on($('ew-hint'), 'click', function () { if (G.ready && !locked()) { try { canvas.requestPointerLock(); } catch (er) {} } });
    on(canvas, 'click', function () { if (!locked() && !chatFocused() && G.ready) canvas.requestPointerLock && canvas.requestPointerLock(); });
    on(document, 'pointerlockchange', function () { $('ew-hint').style.display = (G.ready && !locked() && !chatFocused() && !menuOpen()) ? 'flex' : 'none'; if (!locked()) { G.keys = {}; G.sprint = false; } });
    on(document, 'mousemove', function (e) {
      if (!locked() || !G.me) return;
      G.me.yaw -= (e.movementX || 0) * 0.0023;
      G.me.pitch = Math.max(-1.55, Math.min(1.55, G.me.pitch - (e.movementY || 0) * 0.0023));
    });
    on(canvas, 'mousedown', function (e) {            // one action per click: holding the button does nothing
      if (!locked() || !G.ready) return;
      e.preventDefault();
      if (e.button === 0) doBreak(); else if (e.button === 2) doPlace(); else if (e.button === 1) doPick();
    });
    on(canvas, 'wheel', function (e) { e.preventDefault(); selectColor(G.sel + (e.deltaY > 0 ? 1 : -1)); }, { passive: false });
    on(window, 'keydown', function (e) {
      if (!G || menuOpen() || typingElsewhere()) return;
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
      if (e.code === 'KeyQ') selectColor(G.sel - 1);
      if (e.code === 'KeyE') selectColor(G.sel + 1);
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
    ws.onclose = function () { if (G && G.ws === ws && !G.closed) { sys('Disconnected from EzWorld. Press Leave and join again.'); G.ready = false; if (document.exitPointerLock) document.exitPointerLock(); $('ew-load').style.display = 'flex'; $('ew-load').textContent = 'Disconnected.'; } };
  }
  function onMessage(d) {
    if (d instanceof ArrayBuffer) {
      var dv = new DataView(d), t = dv.getUint8(0);
      if (t === 0x10) { loadChunkFrame(dv); }
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
        G.me = { x: m.x, y: m.y, z: m.z, vx: 0, vy: 0, vz: 0, yaw: 0, pitch: 0, onGround: false };
        (m.players || []).forEach(function (p) { addOther(p[0], p[1], p[2], p[3], p[4]); });
        updateOnline();
        $('ew-load').textContent = 'Loading the world...';
        ensureChunks();
        break;
      case 'sys': sys(m.text); break;
      case 'chat': chatLine(m); break;
      case 'typing': { var k = String(m.name).toLowerCase(); if (m.typing) G.typers.set(k, { name: m.name, exp: Date.now() + 7000 }); else G.typers.delete(k); renderTyping(); break; }
      case 'reaction': { var rel = G.msgEls.get(m.mid); if (rel && window.EzChat) window.EzChat.apply(rel, m.emoji, m.user, m.on); break; }
      case 'join': addOther(m.id, m.name, m.x, m.y, m.z); sys(m.name + ' joined EzWorld.'); updateOnline(); break;
      case 'leave': { var o2 = G.others.get(m.id); if (o2) { sys(o2.name + ' left EzWorld.'); o2.el.remove(); G.others.delete(m.id); G.typers.delete(o2.name.toLowerCase()); renderTyping(); updateOnline(); } break; }
      case 'b': {
        var x = m.d[0], y = m.d[1], z = m.d[2], c = m.d[3];
        var ch = setBlock(x, y, z, c);
        if (ch) meshNow(ch, x & 15, z & 15);
        break;
      }
      case 'tp': if (G.me) { G.me.x = m.x; G.me.y = m.y; G.me.z = m.z; G.me.vx = G.me.vy = G.me.vz = 0; sendPos(true); } break;
      case 'error': sys(m.text || 'Error'); G.ready = false; $('ew-load').style.display = 'flex'; $('ew-load').textContent = m.text || 'Error'; break;
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
    if (!G.me || !G.ws || G.ws.readyState !== 1) return;
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
    var pcx = Math.floor(G.me.x / 16), pcz = Math.floor(G.me.z / 16), now = performance.now(), want = [], pending = 0;
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
    var room = Math.max(0, 48 - pending), list = [];
    for (var i = 0; i < want.length && list.length < Math.min(24, room); i++) {
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
    if (menuOpen() && locked() && document.exitPointerLock) document.exitPointerLock();
    if (G.me && G.ready) {
      var hd = headingOf(G.me.yaw);
      if (Math.abs(hd - G.lastHeading) > 0.2 || G.compassDirty) {
        G.lastHeading = hd; G.compassDirty = false;
        if (!G.compassOff) G.compassOff = document.createElement('canvas');
        var cs = getComputedStyle(G.view);
        drawCompass(G.compassCv, G.compassOff, hd, (cs.getPropertyValue('--accent') || '#ee5a52').trim(), (cs.getPropertyValue('--text-primary') || '#ece6d8').trim());
        $('ew-compass-text').textContent = windName(hd) + '  ' + Math.round(hd) % 360 + '°';
      }
    }
    if (now - G.hintAt > 250) { G.hintAt = now; if (G.ready) $('ew-hint').style.display = (!locked() && !chatFocused() && !menuOpen()) ? 'flex' : 'none'; $('ew-compass').style.display = G.ready ? 'block' : 'none'; }
    var me = G.me;
    if (me) {
      // wait until the ground under you has arrived before letting physics run
      var under = chunks.get(ckey(Math.floor(me.x) >> 4, Math.floor(me.z) >> 4));
      var near = 0, total = 0;
      chunks.forEach(function (c) { total++; if (c.loaded) near++; });
      if (!G.ready && under && under.loaded && near >= Math.min(25, total)) {
        G.ready = true; $('ew-load').style.display = 'none';
        if (!locked() && !chatFocused()) $('ew-hint').style.display = 'flex';
      }
      if (G.ready) {
        var k = G.keys, inp = { f: k.KeyW || k.ArrowUp, b: k.KeyS || k.ArrowDown, l: k.KeyA || k.ArrowLeft, r: k.KeyD || k.ArrowRight, jump: k.Space, sprint: G.sprint && (k.KeyW || k.ArrowUp) };
        var changed = stepPlayer(me, inp, dt);
        sendPos(changed);
        $('ew-coords').textContent = 'X ' + me.x.toFixed(1) + '  Y ' + me.y.toFixed(1) + '  Z ' + me.z.toFixed(1);
      }
    }
    // rebuild a few changed chunks per frame, nearest first
    var budget = 3, t0 = performance.now();
    if (me) {
      var pend = [];
      chunks.forEach(function (c) { if (c.dirty && c.loaded) pend.push(c); });
      pend.sort(function (a, b) { return Math.abs(a.cx * 16 + 8 - me.x) + Math.abs(a.cz * 16 + 8 - me.z) - Math.abs(b.cx * 16 + 8 - me.x) - Math.abs(b.cz * 16 + 8 - me.z); });
      for (var i = 0; i < pend.length && budget > 0 && performance.now() - t0 < 8; i++, budget--) upload(pend[i]);
    }
    draw(dt);
  }

  function draw(dt) {
    var gl = G.gl, me = G.me;
    gl.clearColor(SKY[0], SKY[1], SKY[2], 1); gl.clear(gl.COLOR_BUFFER_BIT | gl.DEPTH_BUFFER_BIT);
    if (!me) return;
    var fovNow = FOV + (G.sprint && G.keys.KeyW ? 0.12 : 0);
    var ex = me.x, ey = me.y + EYE, ez = me.z;
    var vp = mul(perspective(fovNow, G.cw / G.ch, 0.1, 600), viewMatrix(ex, ey, ez, me.yaw, me.pitch));
    G.vp = vp;
    gl.useProgram(G.prog);
    gl.uniformMatrix4fv(G.loc.vp, false, vp);
    gl.uniform3f(G.loc.cam, ex, ey, ez);
    gl.uniform3f(G.loc.fogCol, SKY[0], SKY[1], SKY[2]);
    gl.uniform2f(G.loc.fog, RDIST * 16 - 44, RDIST * 16 - 6);
    gl.enableVertexAttribArray(G.loc.pos); gl.enableVertexAttribArray(G.loc.col);
    var fx = -Math.sin(me.yaw), fz = -Math.cos(me.yaw);
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
    // outline of what you are aiming at
    if (G.ready) {
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
    G.others.forEach(function (p) {
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
    g.timers.forEach(clearInterval);
    g.handlers.forEach(function (h) { h[0].removeEventListener(h[1], h[2], h[3]); });
    try { if (document.pointerLockElement && document.exitPointerLock) document.exitPointerLock(); } catch (e) {}
    try { if (g.ws) { g.ws.onclose = null; g.ws.close(); } } catch (e) {}
    chunks.forEach(function (c) { if (c.mesh && c.mesh.buf && g.gl) g.gl.deleteBuffer(c.mesh.buf); });
    chunks.clear();
    try { var ext = g.gl && g.gl.getExtension('WEBGL_lose_context'); if (ext) ext.loseContext(); } catch (e) {}
    g.view.style.display = 'none';
    document.body.classList.remove('ew-open');
    $('ew-leave').onclick = null;
    $('ew-tags').innerHTML = '';
    if (!silent && g.opts.onClose) g.opts.onClose();
  }

  window.EzWorld = { open: open, close: function () { close(false); } };
  window.EzWorld._t = { headingOf: headingOf, windName: windName, drawCompass: drawCompass, getBlock: getBlock, setBlock: setBlock, loadChunkFrame: loadChunkFrame, meshChunk: meshChunk, raycast: raycast, stepPlayer: stepPlayer,
                        collidesAt: collidesAt, perspective: perspective, viewMatrix: viewMatrix, mul: mul, project: project, chunks: chunks, ckey: ckey, setPal: function (p) { PAL = p; },
                        newChunk: newChunk, overlapsPlayer: overlapsPlayer };
})();
