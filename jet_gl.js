/* JetGL - procedural fighter jet models + tiny WebGL1 renderer for EzWorld.
   Units: metres. Model space: +Y up, nose toward -Z, +X starboard. */
(function (root) {
'use strict';
var PI = Math.PI, TAU = PI * 2;

/* ---------------- small math ---------------- */
function sub(a, b) { return [a[0] - b[0], a[1] - b[1], a[2] - b[2]]; }
function add(a, b) { return [a[0] + b[0], a[1] + b[1], a[2] + b[2]]; }
function mul(a, s) { return [a[0] * s, a[1] * s, a[2] * s]; }
function dot(a, b) { return a[0] * b[0] + a[1] * b[1] + a[2] * b[2]; }
function cross(a, b) { return [a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]]; }
function len(a) { return Math.sqrt(dot(a, a)); }
function norm(a) { var l = len(a); return l < 1e-12 ? [0, 1, 0] : [a[0] / l, a[1] / l, a[2] / l]; }
function sgn(x) { return x < 0 ? -1 : 1; }
function lerp(a, b, t) { return a + (b - a) * t; }
function clamp(x, a, b) { return x < a ? a : (x > b ? b : x); }

function mIdent() { return [1,0,0,0, 0,1,0,0, 0,0,1,0, 0,0,0,1]; }
function mMul(a, b) { // a*b column-major
  var o = new Array(16);
  for (var c = 0; c < 4; c++) for (var r = 0; r < 4; r++) {
    o[c * 4 + r] = a[r] * b[c * 4] + a[4 + r] * b[c * 4 + 1] + a[8 + r] * b[c * 4 + 2] + a[12 + r] * b[c * 4 + 3];
  }
  return o;
}
function mT(x, y, z) { var m = mIdent(); m[12] = x; m[13] = y; m[14] = z; return m; }
function mRX(a) { var c = Math.cos(a), s = Math.sin(a); return [1,0,0,0, 0,c,s,0, 0,-s,c,0, 0,0,0,1]; }
function mRY(a) { var c = Math.cos(a), s = Math.sin(a); return [c,0,-s,0, 0,1,0,0, s,0,c,0, 0,0,0,1]; }
function mRZ(a) { var c = Math.cos(a), s = Math.sin(a); return [c,s,0,0, -s,c,0,0, 0,0,1,0, 0,0,0,1]; }
function mPt(m, p) {
  return [m[0] * p[0] + m[4] * p[1] + m[8] * p[2] + m[12], m[1] * p[0] + m[5] * p[1] + m[9] * p[2] + m[13], m[2] * p[0] + m[6] * p[1] + m[10] * p[2] + m[14]];
}
function mDir(m, p) {
  return [m[0] * p[0] + m[4] * p[1] + m[8] * p[2], m[1] * p[0] + m[5] * p[1] + m[9] * p[2], m[2] * p[0] + m[6] * p[1] + m[10] * p[2]];
}
function mPersp(fovy, asp, n, f) {
  var t = 1 / Math.tan(fovy / 2), m = new Array(16);
  for (var i = 0; i < 16; i++) m[i] = 0;
  m[0] = t / asp; m[5] = t; m[10] = (f + n) / (n - f); m[11] = -1; m[14] = 2 * f * n / (n - f);
  return m;
}
function mLookAt(e, c, up) {
  var f = norm(sub(c, e)), s = norm(cross(f, up)), u = cross(s, f);
  return [s[0], u[0], -f[0], 0, s[1], u[1], -f[1], 0, s[2], u[2], -f[2], 0, -dot(s, e), -dot(u, e), dot(f, e), 1];
}

/* ---------------- colours ---------------- */
function hex(h, s) {
  s = s === undefined ? 1 : s;
  return [clamp(((h >> 16) & 255) * s, 0, 255), clamp(((h >> 8) & 255) * s, 0, 255), clamp((h & 255) * s, 0, 255), 255];
}
function shade(c, s) { return [clamp(c[0] * s, 0, 255), clamp(c[1] * s, 0, 255), clamp(c[2] * s, 0, 255), c[3] === undefined ? 255 : c[3]]; }
function withA(c, a) { return [c[0], c[1], c[2], a]; }
var MAT_GLASS = 1, MAT_EMIT = 2, MAT_TWO = 4;
var GLASS = { glass: true };

/* ---------------- mesh ---------------- */
function Mesh() { this.p = []; this.n = []; this.c = []; this.m = []; }
Mesh.prototype.vcount = function () { return this.p.length / 3; };
Mesh.prototype.tcount = function () { return this.p.length / 9; };
Mesh.prototype.v = function (p, n, c, m) {
  this.p.push(p[0], p[1], p[2]); this.n.push(n[0], n[1], n[2]);
  this.c.push(c[0], c[1], c[2], c[3] === undefined ? 255 : c[3]); this.m.push(m || (c[4] || 0));
};
Mesh.prototype.tri = function (a, b, c, col, mat, n) {
  n = n || norm(cross(sub(b, a), sub(c, a)));
  this.v(a, n, col, mat); this.v(b, n, col, mat); this.v(c, n, col, mat);
};
// flat quad; 'from' = a point inside the solid (normal is made to point away from it)
Mesh.prototype.quad = function (a, b, c, d, col, mat, from) {
  var n = norm(cross(sub(b, a), sub(c, a)));
  if (from) { var ctr = mul(add(add(a, b), add(c, d)), 0.25); if (dot(n, sub(ctr, from)) < 0) n = mul(n, -1); }
  this.tri(a, b, c, col, mat, n); this.tri(a, c, d, col, mat, n);
};
Mesh.prototype.apply = function (start, m) {
  for (var i = start; i < this.p.length / 3; i++) {
    var p = mPt(m, [this.p[i * 3], this.p[i * 3 + 1], this.p[i * 3 + 2]]);
    var n = norm(mDir(m, [this.n[i * 3], this.n[i * 3 + 1], this.n[i * 3 + 2]]));
    this.p[i * 3] = p[0]; this.p[i * 3 + 1] = p[1]; this.p[i * 3 + 2] = p[2];
    this.n[i * 3] = n[0]; this.n[i * 3 + 1] = n[1]; this.n[i * 3 + 2] = n[2];
  }
};
// run fn() then transform everything it added by m
Mesh.prototype.xf = function (m, fn) { var s = this.vcount(); fn(); this.apply(s, m); };
// append mesh other (already built) with transform
Mesh.prototype.append = function (o, m) {
  var s = this.vcount();
  for (var i = 0; i < o.p.length; i++) { this.p.push(o.p[i]); this.n.push(o.n[i]); }
  for (i = 0; i < o.c.length; i++) this.c.push(o.c[i]);
  for (i = 0; i < o.m.length; i++) this.m.push(o.m[i]);
  if (m) this.apply(s, m);
};
Mesh.prototype.bbox = function () {
  var mn = [1e9, 1e9, 1e9], mx = [-1e9, -1e9, -1e9];
  for (var i = 0; i < this.p.length; i += 3) for (var k = 0; k < 3; k++) { mn[k] = Math.min(mn[k], this.p[i + k]); mx[k] = Math.max(mx[k], this.p[i + k]); }
  return { min: mn, max: mx };
};

/* ---------------- primitives ---------------- */
function box(M, cx, cy, cz, hx, hy, hz, col, mat) {
  var c = [cx, cy, cz], x0 = cx - hx, x1 = cx + hx, y0 = cy - hy, y1 = cy + hy, z0 = cz - hz, z1 = cz + hz;
  M.quad([x0,y0,z0],[x1,y0,z0],[x1,y1,z0],[x0,y1,z0], col, mat, c);
  M.quad([x0,y0,z1],[x1,y0,z1],[x1,y1,z1],[x0,y1,z1], col, mat, c);
  M.quad([x0,y0,z0],[x0,y0,z1],[x0,y1,z1],[x0,y1,z0], col, mat, c);
  M.quad([x1,y0,z0],[x1,y0,z1],[x1,y1,z1],[x1,y1,z0], col, mat, c);
  M.quad([x0,y0,z0],[x1,y0,z0],[x1,y0,z1],[x0,y0,z1], col, mat, c);
  M.quad([x0,y1,z0],[x1,y1,z0],[x1,y1,z1],[x0,y1,z1], col, mat, c);
}
// hexahedron from 8 corners: 0-3 bottom ring, 4-7 top ring (same order)
function hexa(M, q, col, mat) {
  var c = [0, 0, 0], i; for (i = 0; i < 8; i++) c = add(c, q[i]); c = mul(c, 1 / 8);
  M.quad(q[0], q[1], q[2], q[3], col, mat, c); M.quad(q[4], q[5], q[6], q[7], col, mat, c);
  M.quad(q[0], q[1], q[5], q[4], col, mat, c); M.quad(q[1], q[2], q[6], q[5], col, mat, c);
  M.quad(q[2], q[3], q[7], q[6], col, mat, c); M.quad(q[3], q[0], q[4], q[7], col, mat, c);
}
// box tapered along z: r = {x,y,hw,hh} at z0 and z1
function taper(M, z0, r0, z1, r1, col, mat) {
  function ring(z, r, yy) { return [[r.x - r.hw, r.y + yy * r.hh, z], [r.x + r.hw, r.y + yy * r.hh, z]]; }
  var a = ring(z0, r0, -1), b = ring(z0, r0, 1), c = ring(z1, r1, -1), d = ring(z1, r1, 1);
  hexa(M, [a[0], a[1], c[1], c[0], b[0], b[1], d[1], d[0]], col, mat);
}

function S(z, top, bot, w, n, xc) { return { z: z, y: (top + bot) / 2, t: (top - bot) / 2, b: (top - bot) / 2, w: w, n: n || 2, x: xc || 0 }; }
function ringPt(s, a) {
  var c = Math.cos(a), sn = Math.sin(a), ex = 2 / s.n;
  var cx = sgn(c) * Math.pow(Math.abs(c), ex), sy = sgn(sn) * Math.pow(Math.abs(sn), ex);
  return [s.x + s.w * cx, s.y + (sn >= 0 ? s.t : s.b) * sy, s.z];
}
function uniAngles(K) { var a = []; for (var i = 0; i < K; i++) a.push(i * TAU / K); return a; }
function degAngles(list) { return list.map(function (d) { return d * PI / 180; }); }
// full ring from half list (degrees, 0..180 inclusive of 0 and 180 handled): mirrors to 360
function ringDeg(list) { // list ascending within (0,180) exclusive plus implied 0 and 180
  var a = [0].concat(list, [180]), k;
  for (var i = list.length; i >= 1; i--) a.push(360 - list[i - 1]);
  // a currently: 0, list..., 180, mirrored 360-list reversed
  return degAngles(a);
}

/* loft along z through sections. opts: angles, col(q)->colour|GLASS, G (glass mesh), caps, mat */
function loft(M, secs, o) {
  o = o || {};
  var ang = o.angles || uniAngles(o.K || 20), K = ang.length, nS = secs.length, j, i;
  var P = [], N = [];
  for (j = 0; j < nS; j++) { P[j] = []; for (i = 0; i < K; i++) P[j][i] = ringPt(secs[j], ang[i]); }
  for (j = 0; j < nS; j++) {
    N[j] = [];
    var s = secs[j], e = { x: s.x, y: s.y, z: s.z, t: Math.max(s.t, 0.03), b: Math.max(s.b, 0.03), w: Math.max(s.w, 0.03), n: s.n };
    for (i = 0; i < K; i++) {
      var d = 0.02, tr = sub(ringPt(e, ang[i] + d), ringPt(e, ang[i] - d));
      var j0 = Math.max(j - 1, 0), j1 = Math.min(j + 1, nS - 1);
      var tz = sub(P[j1][i], P[j0][i]);
      if (len(tz) < 1e-6) tz = [0, 0, 1];
      var n = norm(cross(tr, tz));
      var out = sub(P[j][i], [s.x, s.y, s.z]);
      if (len(out) < 0.02) out = [0, 0, j === 0 ? -1 : (j === nS - 1 ? 1 : 0)];
      if (dot(n, out) < 0) n = mul(n, -1);
      N[j][i] = n;
    }
  }
  for (j = 0; j < nS - 1; j++) for (i = 0; i < K; i++) {
    var i2 = (i + 1) % K;
    var a = P[j][i], b = P[j + 1][i], c = P[j + 1][i2], d2 = P[j][i2];
    if (len(sub(a, d2)) < 1e-5 && len(sub(b, c)) < 1e-5) continue;
    var mid = mul(add(add(a, b), add(c, d2)), 0.25);
    var am = (ang[i] + (i2 === 0 ? TAU : ang[i2])) / 2 * 180 / PI;
    var col = o.col ? o.col({ x: mid[0], y: mid[1], z: mid[2], a: am, j: j, i: i }) : (o.color || [200, 200, 200, 255]);
    var Mt = M, mat = o.mat || 0;
    if (col === GLASS) { Mt = o.G || M; col = o.glassCol || [20, 30, 40, 120]; mat = MAT_GLASS; }
    else if (!col) continue;
    else if (col[4]) mat = col[4];
    var A = [a, b, c, d2], NN = [N[j][i], N[j + 1][i], N[j + 1][i2], N[j][i2]];
    var ord = [[0, 1, 2], [0, 2, 3]];
    for (var q = 0; q < 2; q++) for (var r = 0; r < 3; r++) { var id = ord[q][r]; Mt.v(A[id], NN[id], col, mat); }
  }
  if (o.caps) {
    [0, nS - 1].forEach(function (jj) {
      if (o.caps === 'rear' && jj === 0) return;
      var s = secs[jj], cc = [s.x, s.y, s.z], nz = jj === 0 ? [0, 0, -1] : [0, 0, 1], col = o.capCol || o.color || (o.col ? o.col({ x: s.x, y: s.y, z: s.z, a: 90, j: jj, i: 0 }) : [200, 200, 200, 255]);
      if (col === GLASS || !col) col = [60, 60, 60, 255];
      for (var i = 0; i < K; i++) M.tri(cc, P[jj][i], P[jj][(i + 1) % K], col, o.mat || 0, nz);
    });
  }
}
function lerpSec(a, b, t) {
  return { z: lerp(a.z, b.z, t), y: lerp(a.y, b.y, t), t: lerp(a.t, b.t, t), b: lerp(a.b, b.b, t), w: lerp(a.w, b.w, t), n: lerp(a.n, b.n, t), x: lerp(a.x, b.x, t) };
}
function secAt(secs, z) {
  if (z <= secs[0].z) return secs[0];
  for (var j = 0; j < secs.length - 1; j++) if (z <= secs[j + 1].z) return lerpSec(secs[j], secs[j + 1], (z - secs[j].z) / Math.max(secs[j + 1].z - secs[j].z, 1e-9));
  return secs[secs.length - 1];
}
// point on loft surface at (z, y) on side (+1 starboard / -1 port) + normal
function surf(secs, z, y, side) {
  function X(zz, yy) {
    var s = secAt(secs, zz), h = yy >= s.y ? s.t : s.b;
    var sy = clamp((yy - s.y) / Math.max(h, 1e-4), -0.985, 0.985);
    var cx = Math.pow(Math.max(1 - Math.pow(Math.abs(sy), s.n), 0), 1 / s.n);
    return s.x + s.w * cx;
  }
  var x = X(z, y), e = 0.02;
  var dxdz = (X(z + e, y) - X(z - e, y)) / (2 * e), dxdy = (X(z, y + e) - X(z, y - e)) / (2 * e);
  var n = norm([1, -dxdy, -dxdz]);
  return { p: [side * x, y, z], n: [side * n[0], n[1], n[2]] };
}
// decal on the side of a loft; corners [[z,y]*4] c00,c10,c11,c01
function patch(M, secs, side, cr, nu, nv, col, off, mat) {
  var g = [], i, j, u, v;
  for (i = 0; i <= nu; i++) { g[i] = []; for (j = 0; j <= nv; j++) {
    u = i / nu; v = j / nv;
    var zz = (1 - u) * (1 - v) * cr[0][0] + u * (1 - v) * cr[1][0] + u * v * cr[2][0] + (1 - u) * v * cr[3][0];
    var yy = (1 - u) * (1 - v) * cr[0][1] + u * (1 - v) * cr[1][1] + u * v * cr[2][1] + (1 - u) * v * cr[3][1];
    var s = surf(secs, zz, yy, side);
    g[i][j] = { p: add(s.p, mul(s.n, off)), n: s.n };
  } }
  for (i = 0; i < nu; i++) for (j = 0; j < nv; j++) {
    var q = [g[i][j], g[i + 1][j], g[i + 1][j + 1], g[i][j + 1]];
    [[0, 1, 2], [0, 2, 3]].forEach(function (t) { t.forEach(function (k) { M.v(q[k].p, q[k].n, col, mat); }); });
  }
}
// rectangle shorthand: z0..z1, y0..y1
function rectPatch(M, secs, side, z0, z1, y0, y1, nu, nv, col, off, mat) {
  patch(M, secs, side, [[z0, y0], [z1, y0], [z1, y1], [z0, y1]], nu, nv, col, off, mat);
}
// outlined panel (door line): thin dark frame around rectangle
function panelLines(M, secs, side, z0, z1, y0, y1, col, wd, off) {
  wd = wd || 0.025; off = off || 0.01;
  rectPatch(M, secs, side, z0, z1, y0, y0 + wd, Math.max(2, Math.ceil((z1 - z0) / 0.6)), 1, col, off);
  rectPatch(M, secs, side, z0, z1, y1 - wd, y1, Math.max(2, Math.ceil((z1 - z0) / 0.6)), 1, col, off);
  rectPatch(M, secs, side, z0, z0 + wd, y0, y1, 1, Math.max(2, Math.ceil((y1 - y0) / 0.4)), col, off);
  rectPatch(M, secs, side, z1 - wd, z1, y0, y1, 1, Math.max(2, Math.ceil((y1 - y0) / 0.4)), col, off);
}

function cyl(M, p0, p1, r0, r1, seg, col, mat, caps) {
  var d = sub(p1, p0), L = len(d); if (L < 1e-9) return;
  d = mul(d, 1 / L);
  var ref = Math.abs(d[1]) < 0.9 ? [0, 1, 0] : [1, 0, 0];
  var u = norm(cross(d, ref)), v = cross(d, u);
  var slope = (r0 - r1) / L, i;
  for (i = 0; i < seg; i++) {
    var a0 = i * TAU / seg, a1 = (i + 1) * TAU / seg;
    var c0 = add(mul(u, Math.cos(a0)), mul(v, Math.sin(a0))), c1 = add(mul(u, Math.cos(a1)), mul(v, Math.sin(a1)));
    var n0 = norm(add(c0, mul(d, slope))), n1 = norm(add(c1, mul(d, slope)));
    var A = add(p0, mul(c0, r0)), B = add(p1, mul(c0, r1)), C = add(p1, mul(c1, r1)), D = add(p0, mul(c1, r0));
    M.v(A, n0, col, mat); M.v(B, n0, col, mat); M.v(C, n1, col, mat);
    M.v(A, n0, col, mat); M.v(C, n1, col, mat); M.v(D, n1, col, mat);
    if (caps) {
      if (r0 > 0) M.tri(p0, D, A, col, mat, mul(d, -1));
      if (r1 > 0) M.tri(p1, B, C, col, mat, d);
    }
  }
}
function ell(M, cx, cy, cz, rx, ry, rz, seg, rings, col, mat) {
  var secs = [], k; seg = seg || 10; rings = rings || 6;
  for (k = 0; k <= rings; k++) {
    var ph = PI * k / rings;
    secs.push({ z: cz + rz * Math.cos(ph), y: cy, t: ry * Math.sin(ph), b: ry * Math.sin(ph), w: rx * Math.sin(ph), n: 2, x: cx });
  }
  loft(M, secs, { K: seg, color: col, mat: mat });
}
function path(M, pts, r, seg, col, joints) {
  for (var i = 0; i < pts.length - 1; i++) cyl(M, pts[i], pts[i + 1], r, r, seg, col, 0, i === 0 || i === pts.length - 2);
  if (joints !== false) for (i = 1; i < pts.length - 1; i++) ell(M, pts[i][0], pts[i][1], pts[i][2], r, r, r, seg, 3, col);
}
function light(M, x, y, z, col, r) { ell(M, x, y, z, r || 0.035, r || 0.035, r || 0.035, 6, 3, col, MAT_EMIT); }
var RED = [255, 40, 30, 255], GREEN = [40, 255, 90, 255], WHITE = [255, 255, 245, 255];

// wheel (axle along x) with tyre + hub
function wheel(M, x, y, z, r, wd, tyre, hub) {
  var a = [x - wd / 2, y, z], b = [x + wd / 2, y, z];
  cyl(M, a, b, r, r, 16, tyre, 0, true);
  cyl(M, add(a, [-0.002, 0, 0]), add(b, [0.002, 0, 0]), r * 0.55, r * 0.55, 12, hub, 0, true);
}



var MAT_HOT = 8;
var MODELS = {};   // id -> builder
var SPECS = {};

/* ---------------- helpers ---------------- */
function newModel() { return { body: new Mesh(), glass: new Mesh(), parts: [], noz: [], spec: { wheels: [], nozzles: [] } }; }
function elev(a) { if (a <= 90) return a; if (a <= 180) return 180 - a; if (a <= 270) return -(a - 180); return -(360 - a); }
function ringQ(q) {
  var s = {}, out = [];
  [0, 90].concat(q).forEach(function (a) { [a, 180 - a, 180 + a, 360 - a].forEach(function (b) { b = ((b % 360) + 360) % 360; s[b.toFixed(4)] = b; }); });
  Object.keys(s).forEach(function (k) { out.push(s[k]); });
  out.sort(function (a, b) { return a - b; });
  return degAngles(out);
}
function mirror2(fn) { fn(1); fn(-1); }
function rotM(ax, a) { return ax === 'x' ? mRX(a) : (ax === 'y' ? mRY(a) : mRZ(a)); }
function hash2(a, b) { var h = Math.sin(a * 127.1 + b * 311.7) * 43758.5453; return h - Math.floor(h); }
function smooth(x) { x = clamp(x, 0, 1); return x * x * (3 - 2 * x); }
function mid3(a, b) { return [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2, (a[2] + b[2]) / 2]; }
// sections from table rows [d, top, bot, halfWidth, n, xc]
function SS(rows) { return rows.map(function (r) { return S(r[0], r[1], r[2], r[3], r[4], r[5]); }); }
// camo blotch colour: palette list, cell size, position
function blotch(pal, cell, q, seed) {
  var cz = Math.floor(q.z / cell), cx = Math.floor(Math.abs(q.x) / (cell * 0.8));
  var h = hash2(cz * 1.7 + (seed || 0), cx * 2.3 + 1.1 + (seed || 0));
  return pal[Math.min(pal.length - 1, Math.floor(h * pal.length))];
}

/* ---------------- foils (wings, fins, canards, tailplanes) ---------------- */
// section: {le:[x,y,z], te:[x,y,z], th: half thickness, up:[x,y,z] optional}
var PROF = [[0, 0], [0.22, 1], [0.7, 0.78], [1, 0]];
function profH(f) {
  f = clamp(f, 0, 1);
  for (var i = 0; i < 3; i++) if (f <= PROF[i + 1][0]) { var a = PROF[i], b = PROF[i + 1]; return lerp(a[1], b[1], (f - a[0]) / (b[0] - a[0])); }
  return 0;
}
function sPt(s, f, k, off) {
  var up = s.up || [0, 1, 0], h = (profH(f) * s.th + (off || 0)) * k;
  if (k === 0) h = 0;
  return [lerp(s.le[0], s.te[0], f) + up[0] * h, lerp(s.le[1], s.te[1], f) + up[1] * h, lerp(s.le[2], s.te[2], f) + up[2] * h];
}
function secAtT(secs, t) {
  var j = Math.max(0, Math.min(Math.floor(t), secs.length - 2)), a = secs[j], b = secs[j + 1], u = t - j;
  var ua = a.up || [0, 1, 0], ub = b.up || [0, 1, 0];
  return { le: [lerp(a.le[0], b.le[0], u), lerp(a.le[1], b.le[1], u), lerp(a.le[2], b.le[2], u)],
    te: [lerp(a.te[0], b.te[0], u), lerp(a.te[1], b.te[1], u), lerp(a.te[2], b.te[2], u)],
    th: lerp(a.th, b.th, u), up: norm([lerp(ua[0], ub[0], u), lerp(ua[1], ub[1], u), lerp(ua[2], ub[2], u)]) };
}
function foilPt(secs, t, f, k, off) { return sPt(secAtT(secs, t), f, k, off); }
function mirrorSecs(secs) {
  return secs.map(function (s) {
    var u = s.up || [0, 1, 0];
    return { le: [-s.le[0], s.le[1], s.le[2]], te: [-s.te[0], s.te[1], s.te[2]], th: s.th, up: [-u[0], u[1], u[2]] };
  });
}
function foil(M, secs, o) {
  o = o || {};
  var n = secs.length, P = [], j, i, FRs = [0, 0.22, 0.7, 1];
  for (j = 0; j < n; j++) { var s = secs[j]; P[j] = [sPt(s, 0, 1), sPt(s, 0.22, 1), sPt(s, 0.7, 1), sPt(s, 1, 1), sPt(s, 0.7, -1), sPt(s, 0.22, -1)]; }
  for (j = 0; j < n - 1; j++) {
    var frm = mid3(sPt(secs[j], 0.5, 0), sPt(secs[j + 1], 0.5, 0));
    for (i = 0; i < 6; i++) {
      var i2 = (i + 1) % 6, a = P[j][i], b = P[j + 1][i], c = P[j + 1][i2], d = P[j][i2];
      if (len(sub(a, d)) < 1e-6 && len(sub(b, c)) < 1e-6) continue;
      var mid = mul(add(add(a, b), add(c, d)), 0.25);
      var col = o.col ? o.col({ x: mid[0], y: mid[1], z: mid[2], top: i < 3, e: i, j: j, t: (j + 0.5) / (n - 1) }) : (i < 3 ? o.top : o.bot);
      if (!col) continue;
      M.quad(a, b, c, d, col, o.mat || 0, frm);
    }
  }
  function cap(jj, dir) {
    var cc = sPt(secs[jj], 0.5, 0), nn = norm(sub(sPt(secs[jj], 0.5, 0), sPt(secs[jj - dir], 0.5, 0)));
    nn = mul(nn, dir);
    for (var i = 0; i < 6; i++) M.tri(cc, P[jj][i], P[jj][(i + 1) % 6], o.capCol || o.top || [150, 150, 150, 255], 0, nn);
  }
  if (o.tipCap !== false && n > 1) cap(n - 1, 1);
  if (o.rootCap && n > 1) cap(0, -1);
}
// decal strip on a foil surface between span params t0..t1 and chord fractions f0..f1
function foilStrip(M, secs, t0, t1, f0, f1, k, col, off) {
  off = off === undefined ? 0.02 : off;
  var ts = [t0], fs = [f0], x, i, j;
  for (x = Math.floor(t0) + 1; x < t1; x++) ts.push(x);
  ts.push(t1);
  [0.22, 0.7].forEach(function (b) { if (b > f0 + 1e-6 && b < f1 - 1e-6) fs.push(b); });
  fs.push(f1); fs.sort(function (a, b) { return a - b; });
  if (f0 > f1) return;
  for (i = 0; i < ts.length - 1; i++) for (j = 0; j < fs.length - 1; j++) {
    var A = foilPt(secs, ts[i], fs[j], k, off), B = foilPt(secs, ts[i + 1], fs[j], k, off),
      C = foilPt(secs, ts[i + 1], fs[j + 1], k, off), D = foilPt(secs, ts[i], fs[j + 1], k, off);
    var up = secAtT(secs, (ts[i] + ts[i + 1]) / 2).up;
    var nn = mul(up, k);
    M.tri(A, B, C, col, 0, nn); M.tri(A, C, D, col, 0, nn);
  }
}
function foilNormal(secs, t, k) { return mul(secAtT(secs, t).up, k); }

/* ---------------- flat decals ---------------- */
function basis(n, up) {
  up = up || [0, 1, 0];
  var v = sub(up, mul(n, dot(up, n)));
  if (len(v) < 1e-3) { up = [0, 0, -1]; v = sub(up, mul(n, dot(up, n))); }
  v = norm(v);
  return [cross(v, n), v];
}
function discD(M, c, n, r, col, seg, up) {
  var b = basis(n, up), u = b[0], v = b[1], i; seg = seg || 14;
  for (i = 0; i < seg; i++) {
    var a0 = i * TAU / seg, a1 = (i + 1) * TAU / seg;
    var p0 = add(c, add(mul(u, r * Math.cos(a0)), mul(v, r * Math.sin(a0)))), p1 = add(c, add(mul(u, r * Math.cos(a1)), mul(v, r * Math.sin(a1))));
    M.tri(c, p0, p1, col, 0, n);
  }
}
function ringD(M, c, n, r0, r1, col, seg, up) {
  var b = basis(n, up), u = b[0], v = b[1], i; seg = seg || 14;
  for (i = 0; i < seg; i++) {
    var a0 = i * TAU / seg, a1 = (i + 1) * TAU / seg;
    function P(r, a) { return add(c, add(mul(u, r * Math.cos(a)), mul(v, r * Math.sin(a)))); }
    M.tri(P(r0, a0), P(r1, a0), P(r1, a1), col, 0, n); M.tri(P(r0, a0), P(r1, a1), P(r0, a1), col, 0, n);
  }
}
function starD(M, c, n, r, col, up, rotDeg) {
  var b = basis(n, up), u = b[0], v = b[1], i, pts = [];
  var a0 = (90 + (rotDeg || 0)) * PI / 180;
  for (i = 0; i < 10; i++) {
    var rr = i % 2 ? r * 0.40 : r, a = a0 + i * PI / 5;
    pts.push(add(c, add(mul(u, rr * Math.cos(a)), mul(v, rr * Math.sin(a)))));
  }
  for (i = 0; i < 10; i++) M.tri(c, pts[i], pts[(i + 1) % 10], col, 0, n);
}
function rectD(M, c, n, w, h, col, up) {
  var b = basis(n, up), u = b[0], v = b[1];
  var p = [add(c, add(mul(u, -w), mul(v, -h))), add(c, add(mul(u, w), mul(v, -h))), add(c, add(mul(u, w), mul(v, h))), add(c, add(mul(u, -w), mul(v, h)))];
  M.tri(p[0], p[1], p[2], col, 0, n); M.tri(p[0], p[2], p[3], col, 0, n);
}

/* ---------------- canopy, cockpit, nozzle, gear ---------------- */
// canopy glass loft (upper half), frames at given section indices, frame bars along the spine
function canopy(B, G, secs0, o) {
  var n0 = secs0.length, secs = [secs0[0], lerpSec(secs0[0], secs0[1], 0.16)];
  for (var q0 = 1; q0 < n0 - 1; q0++) secs.push(secs0[q0]);
  secs.push(lerpSec(secs0[n0 - 2], secs0[n0 - 1], 0.84), secs0[n0 - 1]);
  o.frames = o.frames === 'mid' ? [0, 2 + Math.floor((n0 - 2) / 2) - 1, secs.length - 2] : [0, secs.length - 2];
  var rim = secs.map(function (q) { var base = q.y - q.b; return S(q.z, base + 0.1, base - 0.5, q.w + 0.06, 2.6, q.x); });
  loft(B, rim, { angles: ringQ([30, 60]), color: o.rim || [120, 126, 134, 255], caps: true });
  var ang = degAngles([0, 22, 45, 68, 90, 112, 135, 158, 180]);
  var fr = o.frame || [30, 32, 36, 255], frames = o.frames || [0];
  loft(B, secs, { angles: ang, G: G, glassCol: o.glassCol || [26, 40, 54, 120], col: function (q) {
    if (frames.indexOf(q.j) >= 0) return fr;
    if (o.spine && (q.i === 3 || q.i === 4) ) return fr;
    return GLASS;
  } });
}
// seat + pilot + panel inside cockpit. x,y,d: seat reference (cushion centre)
function cockpitKit(B, x, y, d, o) {
  o = o || {};
  var dark = [44, 46, 50, 255], seatC = [58, 60, 66, 255], helm = [210, 205, 190, 255], suit = [70, 82, 70, 255];
  box(B, x, y, d, 0.2, 0.05, 0.25, seatC);
  box(B, x, y + 0.34, d + 0.22, 0.2, 0.36, 0.05, seatC);
  box(B, x, y + 0.2, d + 0.02, 0.17, 0.2, 0.12, suit);
  ell(B, x, y + 0.62, d + 0.06, 0.12, 0.13, 0.12, 8, 4, helm);
  if (!o.noPanel) {
    var pz = d - 0.95;
    box(B, x, y + 0.2, pz, 0.34, 0.09, 0.2, dark);                       // coaming / instrument hood
    box(B, x, y + 0.1, pz - 0.05, 0.3, 0.06, 0.03, [30, 32, 36, 255]);
    gauges(B, x - 0.2, x + 0.2, y + 0.13, pz - 0.1, 5, 2);
    // HUD frame
    box(B, x, y + 0.42, pz - 0.05, 0.14, 0.003, 0.1, [90, 130, 120, 255], MAT_EMIT);
    box(B, x - 0.15, y + 0.36, pz - 0.05, 0.008, 0.06, 0.1, dark); box(B, x + 0.15, y + 0.36, pz - 0.05, 0.008, 0.06, 0.1, dark);
  }
  cyl(B, [x, y + 0.02, d - 0.35], [x, y + 0.3, d - 0.3], 0.012, 0.012, 5, dark, 0, true);
}
// engine nozzle: static shell added to body, dynamic petals + hot liner stored in m.noz
function addNozzle(m, cx, cy, zs, ze, zx, R, o) {
  o = o || {};
  var B = m.body, ti = o.col || [86, 80, 76, 255], ti2 = o.col2 || [112, 104, 96, 255], dk = [40, 38, 38, 255], i;
  // fixed shell (afterburner can)
  cyl(B, [cx, cy, zs], [cx, cy, ze], R * 1.02, R, 16, ti2, 0, false);
  cyl(B, [cx, cy, ze - 0.05], [cx, cy, ze], R, R * 1.0, 16, [60, 60, 62, 255], 0, false);
  // dynamic petals
  var N = new Mesh(), n = 12, rA = R * 0.97, rB = R * 0.80, za = ze - 0.12;
  for (i = 0; i < n; i++) {
    var a0 = i * TAU / n, a1 = (i + 1) * TAU / n, col = i % 2 ? ti : ti2;
    var p0 = [cx + rA * Math.cos(a0), cy + rA * Math.sin(a0), za], p1 = [cx + rA * Math.cos(a1), cy + rA * Math.sin(a1), za];
    var q0 = [cx + rB * Math.cos(a0), cy + rB * Math.sin(a0), zx], q1 = [cx + rB * Math.cos(a1), cy + rB * Math.sin(a1), zx];
    N.quad(p0, p1, q1, q0, col, 0, [cx, cy, (za + zx) / 2]);
    // lip
    N.quad(q0, q1, [cx + rB * 0.92 * Math.cos(a1), cy + rB * 0.92 * Math.sin(a1), zx], [cx + rB * 0.92 * Math.cos(a0), cy + rB * 0.92 * Math.sin(a0), zx], dk, 0, [cx, cy, zx - 0.3]);
  }
  // hot liner (inside), two-sided, emissive-glow material
  var z0 = zx - R * 1.7, r0 = rB * 0.62;
  for (i = 0; i < n; i++) {
    var a0 = i * TAU / n, a1 = (i + 1) * TAU / n, hc = [90, 60, 50, 255];
    var p0 = [cx + rB * 0.92 * Math.cos(a0), cy + rB * 0.92 * Math.sin(a0), zx - 0.02], p1 = [cx + rB * 0.92 * Math.cos(a1), cy + rB * 0.92 * Math.sin(a1), zx - 0.02];
    var q0 = [cx + r0 * Math.cos(a0), cy + r0 * Math.sin(a0), z0], q1 = [cx + r0 * Math.cos(a1), cy + r0 * Math.sin(a1), z0];
    N.tri(p0, p1, q1, hc, MAT_HOT, [0, 0, 1]); N.tri(p0, q1, q0, hc, MAT_HOT, [0, 0, 1]);
    N.tri([cx, cy, z0], q0, q1, [60, 40, 36, 255], MAT_HOT, [0, 0, 1]);
  }
  m.noz.push({ mesh: N, c: [cx, cy, zx], r: rB * 0.92 });
  m.spec.nozzles.push([cx, cy, zx - 0.02]);
  m.spec.nozzleRadius = R * 0.97;
}

// main gear: leg + wheel(s) + outer door. retracts inboard (roll about z)
function mainGear(m, side, x, py, d, wr, ww, o) {
  o = o || {};
  var L = new Mesh(), steel = [176, 178, 184, 255], dark = [48, 50, 54, 255], tyre = [26, 26, 28, 255], hub = [170, 172, 176, 255];
  var yTop = wr + 0.55;
  cyl(L, [x, py, d], [x, yTop, d], 0.085, 0.085, 10, dark, 0, true);
  cyl(L, [x, yTop, d], [x, wr, d], 0.06, 0.06, 8, steel, 0, true);
  cyl(L, [x - side * 0.02, py - 0.25, d + 0.0], [x - side * 0.2, yTop - 0.05, d - 0.55], 0.04, 0.04, 6, dark, 0, true); // drag brace
  cyl(L, [x, yTop - 0.1, d], [x, yTop - 0.35, d + 0.15], 0.025, 0.025, 5, steel, 0, true); // torque link
  cyl(L, [x - ww / 2 - 0.03, wr, d], [x + ww / 2 + 0.03, wr, d], 0.04, 0.04, 6, dark, 0, true);
  wheel(L, x, wr, d, wr, ww, tyre, hub);
  m.parts.push({ mesh: L, pivot: [x, py, d], axis: 'z', ang: side > 0 ? -PI / 2 : PI / 2, kind: 'leg' });
  var D = new Mesh(), dw = o.doorW || 0.55, dl = o.doorL || 1.2, dy = o.doorY, xh = x + side * (o.hingeOff === undefined ? 0.28 : o.hingeOff);
  var doorCol = o.doorCol || [150, 154, 160, 255];
  box(D, xh - side * dw / 2, dy, d, dw / 2, 0.015, dl / 2, doorCol);
  m.parts.push({ mesh: D, pivot: [xh, dy, d], axis: 'z', ang: side * PI * 0.48, kind: 'door', closeDown: !!o.closeDown });
  m.spec.wheels.push([x, 0, d]);
}
// nose gear: leg + twin wheel + two doors; retracts aft
function noseGear(m, py, d, wr, ww, o) {
  o = o || {};
  var L = new Mesh(), steel = [176, 178, 184, 255], dark = [48, 50, 54, 255], tyre = [26, 26, 28, 255], hub = [170, 172, 176, 255];
  var twin = o.twin !== false, yTop = wr + 0.5;
  cyl(L, [0, py, d], [0, yTop, d], 0.07, 0.07, 10, dark, 0, true);
  cyl(L, [0, yTop, d], [0, wr, d], 0.05, 0.05, 8, steel, 0, true);
  cyl(L, [0, py - 0.3, d], [0, py - 0.5, d - 0.5], 0.03, 0.03, 6, dark, 0, true);
  cyl(L, [0, yTop - 0.1, d], [0, yTop - 0.4, d - 0.18], 0.025, 0.025, 5, steel, 0, true);
  cyl(L, [-ww - 0.04, wr, d], [ww + 0.04, wr, d], 0.035, 0.035, 6, dark, 0, true);
  if (twin) { wheel(L, -ww * 0.55, wr, d, wr, ww * 0.85, tyre, hub); wheel(L, ww * 0.55, wr, d, wr, ww * 0.85, tyre, hub); }
  else wheel(L, 0, wr, d, wr, ww, tyre, hub);
  box(L, 0, py - 0.12, d + 0.08, 0.15, 0.02, 0.05, [220, 220, 200, 255], MAT_EMIT); // landing light
  m.parts.push({ mesh: L, pivot: [0, py, d], axis: 'x', ang: -PI / 2, kind: 'leg' });
  var dl = o.doorL || 1.0, dw = o.doorW || 0.3, doorCol = o.doorCol || [150, 154, 160, 255];
  mirror2(function (s) {
    var D = new Mesh(), xh = s * (dw + 0.04);
    box(D, xh - s * dw / 2, o.doorY, d, dw / 2, 0.015, dl / 2, doorCol);
    m.parts.push({ mesh: D, pivot: [xh, o.doorY, d], axis: 'z', ang: s * PI * 0.47, kind: 'door' });
  });
  m.spec.wheels.unshift([0, 0, d]);
}
// finish model: shift so that centre of body bbox (z) is 0, fill SPECS fields
function finishModel(id, m, sp) {
  var parts = [m.body, m.glass], i, k;
  var bb0 = m.body.bbox();
  var zmn = bb0.min[2], zmx = bb0.max[2];
  m.noz.forEach(function (n) { var nb = n.mesh.bbox(); zmn = Math.min(zmn, nb.min[2]); zmx = Math.max(zmx, nb.max[2]); });
  var dz = -(zmn + zmx) / 2;
  m.noz.forEach(function (n) { parts.push(n.mesh); });
  m.parts.forEach(function (p) { parts.push(p.mesh); });
  parts.forEach(function (M) { M.apply(0, mT(0, 0, dz)); });
  function sh(p) { return [p[0], p[1], p[2] + dz]; }
  m.parts.forEach(function (p) { p.pivot = sh(p.pivot); });
  m.noz.forEach(function (n) { n.c = sh(n.c); });
  var s = m.spec;
  var bb = m.body.bbox();
  ['wheels', 'nozzles'].forEach(function (kk) { s[kk] = s[kk].map(sh); });
  s.wingTips = [sh(sp.tipL), sh(sp.tipR)];
  s.finTop = sh(sp.finTop);
  s.eye = sh(sp.eye);
  var nzmn = bb.min[2], nzmx = bb.max[2];
  m.noz.forEach(function (n) { var nb = n.mesh.bbox(); nzmn = Math.min(nzmn, nb.min[2]); nzmx = Math.max(nzmx, nb.max[2]); });
  s.noseZ = nzmn; s.tailZ = nzmx;
  s.halfSpan = Math.max(bb.max[0], -bb.min[0]);
  s.height = bb.max[1];
  for (k in sp.fields) s[k] = sp.fields[k];
  // sort mains: left (x<0) then right
  var w = s.wheels, nose = w[0], mains = w.slice(1).sort(function (a, b) { return a[0] - b[0]; });
  s.wheels = [nose, mains[0], mains[1]];
  SPECS[id] = s;
  m.dz = dz;
}
function gauges(M, x0, x1, y, z, nx, ny) {
  for (var i = 0; i < nx; i++) for (var j = 0; j < ny; j++) {
    var x = lerp(x0, x1, nx > 1 ? i / (nx - 1) : 0.5), yy = y + j * 0.05;
    box(M, x, yy, z, 0.018, 0.018, 0.004, ((i + j) % 3 === 0) ? [200, 220, 200, 255] : [150, 160, 150, 255], MAT_EMIT);
  }
}
// concentric roundel; cols outer->inner, fr radius fractions
function roundelD(M, c, n, r, cols, fr, up) {
  for (var k = 0; k < cols.length; k++) discD(M, add(c, mul(n, 0.012 * k)), n, r * fr[k], cols[k], 18, up);
}

function sidePanels(B, secs, list, col) {
  list.forEach(function (r) { mirror2(function (s) { panelLines(B, secs, s, r[0], r[1], r[2], r[3], col || [58, 64, 72, 255], 0.018, 0.07); }); });
}


/* ================= General Dynamics F-16XL ================= */
MODELS.f16xl = function () {
  var m = newModel(), B = m.body, G = m.glass;
  var white = hex(0xe6e8ea), belly = hex(0xcfd3d8), nose = hex(0xbfc4ca), dark = hex(0x2b2d31), red = hex(0xc4202c), blue = hex(0x1d3c8a), burnt = hex(0x6f6a66), grey = hex(0x9ea3aa);
  var rows = [[0.35, 2.52, 2.46, 0.03, 2], [0.9, 2.84, 2.12, 0.27, 2.2], [1.7, 3.05, 1.92, 0.46, 2.2], [2.6, 3.2, 1.77, 0.6, 2.3], [3.6, 3.25, 1.63, 0.68, 2.4],
    [5.0, 3.2, 1.52, 0.82, 2.5], [7.0, 3.15, 1.42, 1.0, 2.7], [9.0, 3.12, 1.44, 1.05, 2.7], [11.0, 3.1, 1.57, 0.98, 2.6], [13.0, 3.0, 1.7, 0.86, 2.4],
    [14.6, 2.88, 1.8, 0.64, 2.2], [15.4, 2.84, 1.76, 0.58, 2.1]];
  var secs = SS(rows), K = ringQ([15, 30, 45, 60, 75]);
  loft(B, secs, { angles: K, caps: 'rear', col: function (q) {
    var e = elev(q.a);
    if (q.z < 2.3) return nose;
    if (q.z > 14.4) return burnt;
    return e < -8 ? belly : white;
  } });
  // dorsal spine
  loft(B, SS([[4.9, 3.28, 2.9, 0.3, 2.2], [6.5, 3.4, 2.9, 0.42, 2.2], [9.0, 3.38, 2.9, 0.45, 2.2], [11.5, 3.3, 2.9, 0.34, 2.2], [13.5, 3.12, 2.9, 0.2, 2]]), { K: 12, color: white, caps: 'rear' });
  // canopy
  var cs = SS([[2.7, 3.3, 3.0, 0.3, 2], [3.1, 3.65, 3.0, 0.5, 2], [3.9, 3.95, 3.0, 0.57, 2], [4.7, 3.9, 3.0, 0.55, 2], [5.4, 3.66, 3.0, 0.45, 2], [6.0, 3.3, 3.0, 0.3, 2]]);
  canopy(B, G, cs, { frames: 'ends' });
  // intake
  var ik = SS([[4.9, 2.02, 1.0, 0.64, 3.2], [5.6, 2.02, 0.96, 0.72, 3.2], [7.0, 1.96, 1.1, 0.84, 3], [8.4, 1.86, 1.35, 0.9, 2.8]]);
  loft(B, ik, { angles: ringQ([20, 40, 60, 75]), color: grey, caps: true, capCol: [24, 24, 26, 255] });
  loft(B, SS([[4.78, 2.06, 0.94, 0.7, 3.2], [4.95, 2.06, 0.94, 0.7, 3.2]]), { angles: ringQ([20, 40, 60, 75]), color: [214, 216, 220, 255] });
  sidePanels(B, secs, [[6.5, 8.3, 2.25, 2.95], [9.5, 11.4, 2.3, 2.95], [12.2, 13.8, 2.2, 2.85]]);
  // wings
  var wing = [{ le: [0.5, 1.95, 4.3], te: [0.5, 1.95, 15.7], th: 0.30 }, { le: [2.6, 1.95, 9.9], te: [2.6, 1.95, 15.0], th: 0.17 }, { le: [5.2, 1.95, 12.9], te: [5.2, 1.95, 14.35], th: 0.05 }];
  var tail = [{ le: [0.5, 1.85, 14.5], te: [0.5, 1.85, 16.3], th: 0.08 }, { le: [2.8, 1.85, 15.9], te: [2.8, 1.85, 16.55], th: 0.03 }];
  var fin = [{ le: [0, 2.9, 11.0], te: [0, 2.9, 15.6], th: 0.07, up: [1, 0, 0] }, { le: [0, 5.1, 13.4], te: [0, 5.1, 15.5], th: 0.035, up: [1, 0, 0] }];
  var vent = [{ le: [0.5, 1.9, 11.6], te: [0.5, 1.9, 13.6], th: 0.04, up: [0.35, -0.2, 0] }, { le: [0.9, 1.25, 12.6], te: [0.9, 1.25, 13.8], th: 0.025, up: [0.35, -0.2, 0] }];
  vent.forEach(function (s) { s.up = norm(s.up); });
  mirror2(function (s) {
    var W = s > 0 ? wing : mirrorSecs(wing), T = s > 0 ? tail : mirrorSecs(tail);
    foil(B, W, { top: white, bot: belly });
    foil(B, T, { top: grey, bot: belly });
    // panel lines: elevons, LE flap
    foilStrip(B, W, 0.05, 1.98, 0.74, 0.752, 1, dark, 0.012); foilStrip(B, W, 0.05, 1.98, 0.74, 0.752, -1, dark, 0.012);
    foilStrip(B, W, 1.0, 1.98, 0.12, 0.128, 1, dark, 0.012);
    foilStrip(B, W, 1.05, 1.1, 0.74, 0.96, 1, dark, 0.012); foilStrip(B, W, 1.6, 1.65, 0.74, 0.96, 1, dark, 0.012);
    // NASA-style stripes across the wing top
    foilStrip(B, W, 1.1, 1.98, 0.30, 0.38, 1, red, 0.016); foilStrip(B, W, 1.1, 1.98, 0.38, 0.46, 1, blue, 0.016);
    foil(B, s > 0 ? vent : mirrorSecs(vent), { top: grey, bot: grey });
    // fuselage stripes
    rectPatch(B, secs, s, 1.2, 14.2, 2.50, 2.62, 18, 1, red, 0.045);
    rectPatch(B, secs, s, 1.0, 14.0, 2.36, 2.48, 18, 1, blue, 0.045);
  });
  foil(B, fin, { top: white, bot: white, capCol: grey });
  foilStrip(B, fin, 0.3, 0.45, 0.04, 0.97, 1, red, 0.02); foilStrip(B, fin, 0.3, 0.45, 0.04, 0.97, -1, red, 0.02);
  foilStrip(B, fin, 0.45, 0.58, 0.04, 0.97, 1, blue, 0.02); foilStrip(B, fin, 0.45, 0.58, 0.04, 0.97, -1, blue, 0.02);
  foilStrip(B, fin, 0.02, 0.98, 0.76, 0.768, 1, dark, 0.02); foilStrip(B, fin, 0.02, 0.98, 0.76, 0.768, -1, dark, 0.02);
  // NASA "meatball" on fin and tail number block
  [1, -1].forEach(function (s) { var c = foilPt(fin, 0.18, 0.5, s, 0.03); discD(B, c, [s, 0, 0], 0.32, blue, 16); ringD(B, add(c, [s * 0.005, 0, 0]), [s, 0, 0], 0.22, 0.3, white, 16); starD(B, add(c, [s * 0.01, 0, 0]), [s, 0, 0], 0.16, red, [0, 1, 0]); });
  box(B, 0, 5.12, 14.3, 0.03, 0.04, 0.5, dark);       // fin tip fairing
  // probe, lights
  cyl(B, [0, 2.5, 0.0], [0, 2.5, 0.5], 0.015, 0.03, 6, dark, 0, true);
  light(B, 0, 5.1, 15.55, WHITE, 0.04); light(B, -5.2, 1.97, 13.6, RED, 0.04); light(B, 5.2, 1.97, 13.6, GREEN, 0.04);
  // cockpit
  cockpitKit(B, 0, 3.12, 4.0);
  // nozzle
  addNozzle(m, 0, 2.3, 15.3, 15.95, 16.5, 0.56);
  // landing gear
  noseGear(m, 1.75, 3.7, 0.29, 0.2, { doorY: 1.62, doorL: 1.0 });
  mirror2(function (s) { mainGear(m, s, s * 1.22, 1.78, 9.4, 0.4, 0.24, { doorY: 1.62, doorL: 1.1, doorW: 0.6 }); });
  finishModel('f16xl', m, {
    tipL: [-5.2, 1.95, 13.4], tipR: [5.2, 1.95, 13.4], finTop: [0, 5.1, 14.4], eye: [0, 3.74, 3.95],
    fields: { name: 'General Dynamics F-16XL', maker: 'General Dynamics (NASA research aircraft)', realLengthM: 16.5, realSpanM: 10.4, realHeightM: 5.1, chaseDist: 30, chaseUp: 6.5, seats: 1, engines: 1, topSpeedMach: 2.0 }
  });
  return m;
};


/* ================= McDonnell Douglas F-15C Eagle ================= */
function sideRows(rows, s) { return rows.map(function (r) { return [r[0], r[1], r[2], r[3], r[4], r[5] * s]; }); }
MODELS.f15 = function () {
  var m = newModel(), B = m.body, G = m.glass;
  var pal = [hex(0x6c7686), hex(0x768090), hex(0x626c7c)], under = hex(0x8b95a2), radome = hex(0x5d6168), dark = hex(0x2a2c30), grey = hex(0x77808c), burnt = hex(0x5c5855), steel = hex(0x9fa5ad);
  var blue = hex(0x203c7c), white = hex(0xe8e8e8), red = hex(0xb02830);
  var rows = [[0, 2.9, 2.8, 0.03, 2], [0.8, 3.2, 2.35, 0.3, 2.2], [2.0, 3.45, 2.1, 0.55, 2.2], [3.4, 3.6, 1.95, 0.75, 2.3], [5.2, 3.65, 1.8, 1.0, 2.5],
    [7.0, 3.6, 1.7, 1.3, 3], [9.0, 3.55, 1.65, 1.55, 3.5], [11.0, 3.5, 1.7, 1.6, 3.5], [13.0, 3.45, 1.85, 1.5, 3.5], [15.0, 3.3, 2.0, 1.2, 3], [16.8, 3.1, 2.1, 0.7, 2.4], [17.8, 3.0, 2.2, 0.35, 2.2]];
  var secs = SS(rows), K = ringQ([15, 30, 45, 60, 75]);
  loft(B, secs, { angles: K, caps: 'rear', col: function (q) {
    var e = elev(q.a);
    if (q.z < 2.1) return radome;
    if (e < -10) return under;
    return blotch(pal, 1.6, q, 3);
  } });
  // engine nacelles
  var nac = [[10.4, 3.4, 1.7, 0.85, 3.2, 1.15], [11.5, 3.38, 1.75, 0.92, 3.2, 1.15], [13.5, 3.25, 1.85, 0.9, 3.2, 1.15], [15.5, 3.15, 1.9, 0.82, 2.8, 1.15], [17.4, 3.1, 1.9, 0.68, 2.3, 1.15], [18.4, 3.1, 1.9, 0.62, 2.1, 1.15]];
  mirror2(function (s) {
    loft(B, SS(sideRows(nac, s)), { angles: K, caps: 'rear', col: function (q) { var e = elev(q.a); if (q.z > 16.2) return burnt; return e < -10 ? under : pal[1]; } });
    // rectangular intake
    var ik = [[6.1, 3.3, 1.95, 0.5, 7, 1.5], [6.6, 3.4, 1.85, 0.54, 7, 1.5], [8.5, 3.45, 1.7, 0.58, 7, 1.48], [10.5, 3.4, 1.65, 0.62, 6, 1.4], [12.0, 3.3, 1.7, 0.78, 4, 1.25]];
    loft(B, SS(sideRows(ik, s)), { angles: ringQ([10, 25, 40, 55, 70, 80]), caps: true, capCol: [22, 22, 24, 255], col: function (q) { return q.j === 0 && q.z < 6.5 ? steel : (elev(q.a) < -20 ? under : pal[2]); } });
    // intake lip ring
    loft(B, SS(sideRows([[5.95, 3.32, 1.93, 0.53, 7, 1.5], [6.15, 3.32, 1.93, 0.53, 7, 1.5]], s)), { angles: ringQ([10, 25, 40, 55, 70, 80]), color: [175, 182, 192, 255] });
  });
  sidePanels(B, secs, [[2.9, 4.4, 2.4, 3.2]]);
  // upper spine + airbrake region
  loft(B, SS([[7.2, 3.66, 3.2, 0.5, 2.5], [9.0, 3.74, 3.2, 0.66, 2.5], [11.5, 3.66, 3.2, 0.7, 2.5], [14.0, 3.5, 3.2, 0.55, 2.5]]), { K: 14, color: pal[0], caps: 'rear' });
  rectD(B, [0, 3.76, 10.1], [0, 1, 0], 0.5, 1.05, [110, 118, 130, 255]);   // speed brake panel
  // canopy
  var cs = SS([[2.9, 3.78, 3.45, 0.38, 2], [3.4, 4.1, 3.45, 0.56, 2], [4.4, 4.38, 3.45, 0.64, 2], [5.5, 4.36, 3.45, 0.64, 2], [6.5, 4.12, 3.45, 0.54, 2], [7.4, 3.82, 3.45, 0.36, 2], [8.1, 3.6, 3.45, 0.2, 2]]);
  canopy(B, G, cs, { frames: 'ends' });
  // wings, stabilators, fins
  var wing = [{ le: [1.0, 2.7, 8.4], te: [1.0, 2.7, 15.3], th: 0.24 }, { le: [3.8, 2.66, 11.0], te: [3.8, 2.66, 15.15], th: 0.13 }, { le: [6.52, 2.6, 13.6], te: [6.52, 2.6, 15.0], th: 0.05 }];
  var stab = [{ le: [1.5, 2.4, 16.4], te: [1.5, 2.4, 19.0], th: 0.09 }, { le: [4.35, 2.4, 18.15], te: [4.35, 2.4, 19.1], th: 0.035 }];
  var fin = [{ le: [1.55, 3.1, 13.0], te: [1.55, 3.1, 18.3], th: 0.08, up: [1, 0, 0] }, { le: [1.85, 5.63, 15.4], te: [1.85, 5.63, 17.9], th: 0.04, up: [1, 0, 0] }];
  var strk = [{ le: [0.9, 2.75, 6.4], te: [0.9, 2.75, 9.5], th: 0.1 }, { le: [1.2, 2.72, 8.2], te: [1.2, 2.72, 9.6], th: 0.08 }];
  mirror2(function (s) {
    var W = s > 0 ? wing : mirrorSecs(wing), T = s > 0 ? stab : mirrorSecs(stab), F = s > 0 ? fin : mirrorSecs(fin);
    foil(B, W, { col: function (q) { return q.top ? pal[0] : under; } });
    foilStrip(B, W, 0.05, 1.98, 0.72, 0.732, 1, dark, 0.014); foilStrip(B, W, 0.05, 1.98, 0.72, 0.732, -1, dark, 0.014);
    foilStrip(B, W, 1.2, 1.24, 0.72, 0.99, 1, dark, 0.014);
    foilStrip(B, W, 0.1, 1.98, 0.14, 0.145, 1, dark, 0.014);
    foil(B, T, { top: pal[1], bot: under });
    foilStrip(B, T, 0.05, 0.98, 0.58, 0.592, 1, dark, 0.014);
    foil(B, F, { top: pal[2], bot: pal[2], capCol: dark });
    foilStrip(B, F, 0.02, 0.98, 0.7, 0.712, 1, dark, 0.02); foilStrip(B, F, 0.02, 0.98, 0.7, 0.712, -1, dark, 0.02);
    // US national insignia on the fin (both faces) and on the wing
    [1, -1].forEach(function (f) {
      var c = foilPt(F, 0.5, 0.42, f, 0.045), n = [f * s, 0, 0];
      discD(B, c, n, 0.46, blue, 18); starD(B, add(c, mul(n, 0.012)), n, 0.34, white, [0, 1, 0]);
      [-1, 1].forEach(function (b) { rectD(B, add(c, add(mul(n, 0.012), [0, 0, b * 0.64])), n, 0.2, 0.075, white, [0, 1, 0]); });
    });
    var cw = foilPt(W, 1.5, 0.55, 1, 0.03);
    discD(B, cw, [0, 1, 0], 0.5, blue, 18, [0, 0, -1]); starD(B, add(cw, [0, 0.012, 0]), [0, 1, 0], 0.38, white, [0, 0, -1]);
    var cb = foilPt(W, 1.5, 0.55, -1, 0.03);
    discD(B, cb, [0, -1, 0], 0.5, blue, 18, [0, 0, -1]); starD(B, add(cb, [0, -0.012, 0]), [0, -1, 0], 0.38, white, [0, 0, -1]);
    // fin-tip ECM pod
    box(B, s * 1.87, 5.55, 17.5, 0.05, 0.1, 0.45, dark);
    light(B, s * 1.9, 5.62, 17.95, s > 0 ? GREEN : RED, 0.04);
    // wing tip lights/dischargers
    light(B, s * 6.52, 2.6, 14.2, s > 0 ? GREEN : RED, 0.05);
    // leading-edge strake near the cockpit
    cyl(B, [s * 0.5, 3.1, 1.0], [s * 0.5, 3.1, 1.0], 0.01, 0.01, 3, dark, 0, false);
  });
  // radome bands + pitot probe + hook fairing
  cyl(B, [0, 2.8, 0.0], [0, 2.8, 0.5], 0.02, 0.04, 6, dark, 0, true);
  box(B, 0, 2.15, 17.4, 0.05, 0.05, 1.25, grey);
  cyl(B, [0, 2.1, 17.5], [0, 2.05, 19.0], 0.04, 0.03, 6, dark, 0, true);
  // cockpit
  cockpitKit(B, 0, 3.56, 4.6);
  // nozzles
  mirror2(function (s) { addNozzle(m, s * 1.15, 2.5, 18.3, 18.95, 19.43, 0.6); });
  // landing gear
  noseGear(m, 1.95, 5.0, 0.34, 0.22, { doorY: 1.9, doorL: 1.1 });
  mirror2(function (s) { mainGear(m, s, s * 1.42, 2.0, 10.6, 0.46, 0.3, { doorY: 1.64, doorL: 1.3, doorW: 0.7, hingeOff: 0.34 }); });
  finishModel('f15', m, {
    tipL: [-6.52, 2.6, 14.3], tipR: [6.52, 2.6, 14.3], finTop: [1.85, 5.63, 16.7], eye: [0, 4.17, 4.5],
    fields: { name: 'McDonnell Douglas F-15C Eagle', maker: 'McDonnell Douglas', realLengthM: 19.43, realSpanM: 13.05, realHeightM: 5.63, chaseDist: 36, chaseUp: 8, seats: 1, engines: 2, topSpeedMach: 2.5 }
  });
  return m;
};


/* ================= Eurofighter Typhoon ================= */
MODELS.typhoon = function () {
  var m = newModel(), B = m.body, G = m.glass;
  var pal = [hex(0x767d86), hex(0x6d747d), hex(0x80878f)], under = hex(0x9ba1a9), radome = hex(0x5b5f66), dark = hex(0x25272b), grey = hex(0x6a717a), burnt = hex(0x5a5653), steel = hex(0x9ea4ac);
  var red = hex(0xc02030), blue = hex(0x1c3a80), white = hex(0xeeeeee);
  var rows = [[0, 2.3, 2.25, 0.03, 2], [0.8, 2.58, 1.98, 0.26, 2.2], [2.0, 2.85, 1.85, 0.48, 2.2], [3.2, 3.02, 1.75, 0.62, 2.3], [4.5, 3.06, 1.6, 0.74, 2.4],
    [6.0, 3.02, 1.45, 0.9, 2.7], [8.0, 3.0, 1.42, 0.98, 2.9], [10.0, 3.0, 1.5, 0.98, 2.9], [12.0, 2.95, 1.68, 0.88, 2.7], [13.5, 2.9, 1.78, 0.8, 2.5], [14.6, 2.85, 1.85, 0.72, 2.3]];
  var secs = SS(rows), K = ringQ([15, 30, 45, 60, 75]);
  loft(B, secs, { angles: K, caps: 'rear', col: function (q) {
    var e = elev(q.a);
    if (q.z < 1.2) return [58, 60, 64, 255];
    if (q.z < 2.2) return radome;
    if (e < -10) return under;
    return blotch(pal, 1.4, q, 7);
  } });
  var nac = [[12.6, 2.9, 1.78, 0.58, 2.4, 0.5], [13.9, 2.82, 1.8, 0.5, 2.2, 0.5], [15.0, 2.78, 1.82, 0.48, 2.1, 0.5]];
  mirror2(function (s) { loft(B, SS(sideRows(nac, s)), { angles: K, caps: 'rear', col: function (q) { return q.z > 13.8 ? burnt : (elev(q.a) < -10 ? under : pal[1]); } }); });
  loft(B, SS([[6.0, 3.12, 2.8, 0.36, 2.4], [8.0, 3.2, 2.8, 0.5, 2.4], [10.5, 3.2, 2.8, 0.46, 2.4], [13.0, 3.02, 2.8, 0.3, 2.2]]), { K: 12, color: pal[2], caps: 'rear' });
  sidePanels(B, secs, [[3.4, 4.9, 2.2, 2.9], [8.5, 10.2, 2.3, 2.9], [10.8, 12.2, 2.3, 2.85]]);
  // chin intake
  var ik = SS([[4.5, 2.02, 0.98, 0.72, 4, 0], [5.2, 2.02, 0.92, 0.8, 4, 0], [6.8, 1.96, 1.05, 0.9, 3.5, 0], [8.4, 1.86, 1.3, 0.92, 3, 0], [9.8, 1.78, 1.45, 0.8, 2.6, 0]]);
  loft(B, ik, { angles: ringQ([15, 30, 45, 60, 75]), caps: true, capCol: [22, 22, 24, 255], col: function (q) { return q.j === 0 ? steel : (elev(q.a) < -20 ? under : pal[2]); } });
  loft(B, SS([[4.35, 2.06, 0.95, 0.76, 4], [4.55, 2.06, 0.95, 0.76, 4]]), { angles: ringQ([15, 30, 45, 60, 75]), color: [150, 156, 164, 255] });
  // canopy
  var cs = SS([[2.7, 3.12, 2.95, 0.3, 2], [3.3, 3.6, 2.95, 0.48, 2], [4.3, 3.92, 2.95, 0.54, 2], [5.3, 3.84, 2.95, 0.52, 2], [6.2, 3.44, 2.95, 0.4, 2], [7.0, 3.14, 2.95, 0.25, 2]]);
  canopy(B, G, cs, { frames: 'ends' });
  // wing, canard, fin
  var wing = [{ le: [0.7, 1.95, 6.6], te: [0.7, 1.95, 14.7], th: 0.3 }, { le: [3.2, 1.92, 9.9], te: [3.2, 1.92, 14.5], th: 0.14 }, { le: [5.475, 1.88, 12.9], te: [5.475, 1.88, 14.3], th: 0.05 }];
  var can = [{ le: [0.5, 2.72, 5.5], te: [0.5, 2.72, 6.95], th: 0.06 }, { le: [1.95, 2.62, 6.75], te: [1.95, 2.62, 7.7], th: 0.025 }];
  var fin = [{ le: [0, 2.85, 10.2], te: [0, 2.85, 14.9], th: 0.07, up: [1, 0, 0] }, { le: [0, 5.28, 12.9], te: [0, 5.28, 14.85], th: 0.03, up: [1, 0, 0] }];
  mirror2(function (s) {
    var W = s > 0 ? wing : mirrorSecs(wing), C = s > 0 ? can : mirrorSecs(can);
    foil(B, W, { col: function (q) { return q.top ? pal[0] : under; } });
    foilStrip(B, W, 0.05, 1.98, 0.74, 0.752, 1, dark, 0.014); foilStrip(B, W, 0.05, 1.98, 0.74, 0.752, -1, dark, 0.014);
    foilStrip(B, W, 1.2, 1.24, 0.74, 0.99, 1, dark, 0.014);
    foilStrip(B, W, 0.3, 1.98, 0.12, 0.128, 1, dark, 0.014);
    foil(B, C, { top: pal[1], bot: under });
    var cw = foilPt(W, 1.45, 0.55, 1, 0.03);
    roundelD(B, cw, [0, 1, 0], 0.55, [blue, white, red], [1, 0.68, 0.34], [0, 0, -1]);
    var cb = foilPt(W, 1.45, 0.55, -1, 0.03);
    roundelD(B, cb, [0, -1, 0], 0.55, [blue, white, red], [1, 0.68, 0.34], [0, 0, -1]);
    // fuselage-side roundel
    var sf = surf(secs, 7.4, 2.45, s);
    roundelD(B, add(sf.p, mul(sf.n, 0.04)), sf.n, 0.42, [blue, white, red], [1, 0.68, 0.34], [0, 1, 0]);
    light(B, s * 5.45, 1.9, 13.2, s > 0 ? GREEN : RED, 0.05);
  });
  foil(B, fin, { top: pal[0], bot: pal[0], capCol: dark });
  [1, -1].forEach(function (f) {
    foilStrip(B, fin, 0.02, 0.98, 0.74, 0.752, f, dark, 0.02);
    foilStrip(B, fin, 0.45, 0.98, 0.55, 0.65, f, red, 0.02); foilStrip(B, fin, 0.45, 0.98, 0.65, 0.75, f, white, 0.02); foilStrip(B, fin, 0.45, 0.98, 0.45, 0.55, f, blue, 0.02);
  });
  box(B, 0, 5.3, 14.1, 0.035, 0.05, 0.55, dark);
  cyl(B, [0, 2.3, 0.0], [0, 2.3, 0.4], 0.015, 0.04, 6, dark, 0, true);
  box(B, 0, 2.0, 14.9, 0.06, 0.04, 0.9, grey);   // arrestor hook fairing
  cockpitKit(B, 0, 3.05, 4.4);
  mirror2(function (s) { addNozzle(m, s * 0.5, 2.3, 14.9, 15.45, 15.96, 0.48); });
  noseGear(m, 1.85, 3.6, 0.3, 0.2, { doorY: 1.72, doorL: 1.0 });
  mirror2(function (s) { mainGear(m, s, s * 1.38, 1.85, 9.0, 0.4, 0.24, { doorY: 1.55, doorL: 1.2, doorW: 0.7, hingeOff: 0.32 }); });
  finishModel('typhoon', m, {
    tipL: [-5.475, 1.88, 13.6], tipR: [5.475, 1.88, 13.6], finTop: [0, 5.28, 13.9], eye: [0, 3.67, 4.3],
    fields: { name: 'Eurofighter Typhoon', maker: 'Eurofighter GmbH', realLengthM: 15.96, realSpanM: 10.95, realHeightM: 5.28, chaseDist: 30, chaseUp: 7, seats: 1, engines: 2, topSpeedMach: 2.0 }
  });
  return m;
};


/* ================= Dassault Rafale ================= */
MODELS.rafale = function () {
  var m = newModel(), B = m.body, G = m.glass;
  var pal = [hex(0xb2b8c0), hex(0xa8aeb6), hex(0xbac0c7)], under = hex(0xcdd1d6), radome = hex(0x9a9fa6), dark = hex(0x2a2c30), grey = hex(0x8a9099), burnt = hex(0x6a6663), steel = hex(0xa4a9b0);
  var red = hex(0xc4202c), blue = hex(0x1d3c8a), white = hex(0xf0f0f0);
  var rows = [[0, 2.3, 2.25, 0.03, 2], [0.8, 2.65, 1.95, 0.3, 2.2], [2.2, 2.92, 1.75, 0.52, 2.2], [3.5, 3.06, 1.65, 0.7, 2.3], [5.0, 3.08, 1.55, 0.88, 2.4],
    [7.0, 3.02, 1.42, 1.0, 2.7], [9.0, 2.98, 1.42, 1.05, 2.8], [11.0, 2.92, 1.55, 1.0, 2.7], [12.5, 2.86, 1.65, 0.9, 2.5], [13.8, 2.78, 1.78, 0.75, 2.3], [14.4, 2.74, 1.82, 0.62, 2.2]];
  var secs = SS(rows), K = ringQ([15, 30, 45, 60, 75]);
  loft(B, secs, { angles: K, caps: 'rear', col: function (q) {
    var e = elev(q.a);
    if (q.z < 2.4) return radome;
    if (e < -10) return under;
    return blotch(pal, 1.5, q, 11);
  } });
  var nac = [[12.6, 2.8, 1.8, 0.5, 2.3, 0.42], [13.9, 2.74, 1.82, 0.46, 2.1, 0.42], [14.6, 2.72, 1.82, 0.45, 2.1, 0.42]];
  mirror2(function (s) {
    loft(B, SS(sideRows(nac, s)), { angles: K, caps: 'rear', col: function (q) { return q.z > 13.8 ? burnt : (elev(q.a) < -10 ? under : pal[1]); } });
    // side semi-circular intake
    var ik = [[3.9, 2.84, 1.7, 0.42, 2.4, 0.92], [4.5, 2.88, 1.66, 0.46, 2.4, 0.92], [6.0, 2.88, 1.68, 0.4, 2.3, 0.88], [7.6, 2.82, 1.8, 0.3, 2.2, 0.8]];
    loft(B, SS(sideRows(ik, s)), { angles: ringQ([15, 30, 45, 60, 75]), caps: true, capCol: [22, 22, 24, 255], col: function (q) { return q.j === 0 ? steel : (elev(q.a) < -20 ? under : pal[2]); } });
    loft(B, SS(sideRows([[3.75, 2.88, 1.66, 0.46, 2.4, 0.92], [3.95, 2.88, 1.66, 0.46, 2.4, 0.92]], s)), { angles: ringQ([15, 30, 45, 60, 75]), color: [168, 174, 182, 255] });
    // intake splitter plate
    box(B, s * 0.56, 2.28, 4.6, 0.018, 0.55, 0.75, grey);
  });
  loft(B, SS([[6.0, 3.12, 2.8, 0.36, 2.4], [8.0, 3.2, 2.8, 0.5, 2.4], [10.5, 3.18, 2.8, 0.46, 2.4], [12.5, 3.0, 2.8, 0.28, 2.2]]), { K: 12, color: pal[2], caps: 'rear' });
  sidePanels(B, secs, [[5.2, 6.4, 2.3, 2.95], [10.0, 11.4, 2.3, 2.85], [12.0, 13.2, 2.2, 2.75]]);
  var cs = SS([[2.7, 3.12, 2.95, 0.3, 2], [3.3, 3.62, 2.95, 0.48, 2], [4.3, 3.94, 2.95, 0.53, 2], [5.3, 3.84, 2.95, 0.5, 2], [6.2, 3.44, 2.95, 0.4, 2], [7.0, 3.14, 2.95, 0.25, 2]]);
  canopy(B, G, cs, { frames: 'ends' });
  var wing = [{ le: [0.8, 1.95, 6.2], te: [0.8, 1.95, 13.6], th: 0.28 }, { le: [3.1, 1.92, 8.6], te: [3.1, 1.92, 13.3], th: 0.14 }, { le: [5.42, 1.9, 11.2], te: [5.42, 1.9, 12.9], th: 0.05 }];
  var can = [{ le: [0.55, 2.8, 4.9], te: [0.55, 2.8, 6.3], th: 0.06 }, { le: [2.0, 2.95, 6.0], te: [2.0, 2.95, 6.95], th: 0.025 }];
  var fin = [{ le: [0, 2.85, 9.6], te: [0, 2.85, 14.3], th: 0.07, up: [1, 0, 0] }, { le: [0, 5.34, 12.4], te: [0, 5.34, 14.35], th: 0.03, up: [1, 0, 0] }];
  mirror2(function (s) {
    var W = s > 0 ? wing : mirrorSecs(wing), C = s > 0 ? can : mirrorSecs(can);
    foil(B, W, { col: function (q) { return q.top ? pal[0] : under; } });
    foilStrip(B, W, 0.05, 1.98, 0.74, 0.752, 1, dark, 0.014); foilStrip(B, W, 0.05, 1.98, 0.74, 0.752, -1, dark, 0.014);
    foilStrip(B, W, 1.2, 1.24, 0.74, 0.99, 1, dark, 0.014);
    foilStrip(B, W, 0.3, 1.98, 0.14, 0.148, 1, dark, 0.014);
    foil(B, C, { top: pal[1], bot: under });
    var cw = foilPt(W, 1.5, 0.5, 1, 0.03);
    roundelD(B, cw, [0, 1, 0], 0.55, [red, white, blue], [1, 0.68, 0.34], [0, 0, -1]);
    var cb = foilPt(W, 1.5, 0.5, -1, 0.03);
    roundelD(B, cb, [0, -1, 0], 0.55, [red, white, blue], [1, 0.68, 0.34], [0, 0, -1]);
    var sf = surf(secs, 9.4, 2.45, s);
    roundelD(B, add(sf.p, mul(sf.n, 0.04)), sf.n, 0.42, [red, white, blue], [1, 0.68, 0.34], [0, 1, 0]);
    // wing-tip missile rail
    box(B, s * 5.47, 1.9, 12.3, 0.05, 0.07, 1.5, grey);
    light(B, s * 5.47, 1.9, 13.9, s > 0 ? GREEN : RED, 0.045);
  });
  foil(B, fin, { top: pal[0], bot: pal[0], capCol: dark });
  [1, -1].forEach(function (f) {
    foilStrip(B, fin, 0.02, 0.98, 0.74, 0.752, f, dark, 0.02);
    foilStrip(B, fin, 0.4, 0.98, 0.42, 0.56, f, blue, 0.02); foilStrip(B, fin, 0.4, 0.98, 0.56, 0.7, f, white, 0.02); foilStrip(B, fin, 0.4, 0.98, 0.7, 0.84, f, red, 0.02);
  });
  box(B, 0, 5.36, 14.1, 0.035, 0.05, 0.55, dark);
  cyl(B, [0, 2.3, 0.0], [0, 2.3, 0.4], 0.015, 0.04, 6, dark, 0, true);
  box(B, 0, 2.0, 14.6, 0.06, 0.04, 0.6, grey);   // arrestor hook fairing
  cockpitKit(B, 0, 3.05, 4.4);
  mirror2(function (s) { addNozzle(m, s * 0.42, 2.27, 14.5, 14.95, 15.27, 0.45); });
  noseGear(m, 1.85, 3.3, 0.3, 0.2, { doorY: 1.72, doorL: 1.0 });
  mirror2(function (s) { mainGear(m, s, s * 1.5, 1.85, 9.0, 0.42, 0.25, { doorY: 1.55, doorL: 1.2, doorW: 0.7, hingeOff: 0.32 }); });
  finishModel('rafale', m, {
    tipL: [-5.42, 1.9, 12.3], tipR: [5.42, 1.9, 12.3], finTop: [0, 5.34, 13.4], eye: [0, 3.67, 4.3],
    fields: { name: 'Dassault Rafale', maker: 'Dassault Aviation', realLengthM: 15.27, realSpanM: 10.8, realHeightM: 5.34, chaseDist: 29, chaseUp: 7, seats: 1, engines: 2, topSpeedMach: 1.8 }
  });
  return m;
};


/* ================= Sukhoi Su-30SM2 ================= */
MODELS.su30 = function () {
  var m = newModel(), B = m.body, G = m.glass;
  var pal = [hex(0x4e6178), hex(0x62768d), hex(0x3d4f66), hex(0x566a82)], under = hex(0x7f8fa3), radome = hex(0x62666c), dark = hex(0x25272b), grey = hex(0x6a7480), burnt = hex(0x5c5855), steel = hex(0x9ea6b0);
  var red = hex(0xc8202a), blue = hex(0x1d3c8a), white = hex(0xf0f0f0);
  var rows = [[1.3, 3.05, 2.95, 0.03, 2], [2.0, 3.3, 2.7, 0.3, 2.2], [3.2, 3.58, 2.55, 0.55, 2.2], [4.6, 3.78, 2.4, 0.78, 2.3], [6.2, 3.9, 2.25, 1.0, 2.4], [8.0, 3.98, 2.0, 1.3, 2.6],
    [10.0, 3.95, 2.0, 1.6, 3], [12.0, 3.85, 2.0, 1.5, 3], [14.0, 3.7, 2.1, 1.3, 3], [16.0, 3.5, 2.2, 1.0, 2.6], [17.5, 3.3, 2.3, 0.6, 2.4], [18.3, 3.2, 2.4, 0.3, 2.2]];
  var secs = SS(rows), K = ringQ([15, 30, 45, 60, 75]);
  loft(B, secs, { angles: K, caps: 'rear', col: function (q) {
    var e = elev(q.a);
    if (q.z < 3.0) return radome;
    if (e < -10) return under;
    return blotch(pal, 1.7, q, 5);
  } });
  var nac = [[7.0, 3.2, 1.1, 0.72, 3.2, 1.45], [8.2, 3.45, 1.05, 0.82, 3.0, 1.5], [10.5, 3.55, 1.1, 0.95, 3.0, 1.55], [13.0, 3.5, 1.3, 1.0, 3.0, 1.55], [15.5, 3.35, 1.6, 0.95, 2.8, 1.55], [17.5, 3.15, 1.8, 0.85, 2.4, 1.5], [19.0, 3.0, 1.9, 0.72, 2.1, 1.45]];
  mirror2(function (s) {
    var ik = SS(sideRows(nac, s));
    loft(B, ik, { angles: ringQ([12, 25, 40, 55, 70, 80]), caps: true, capCol: [22, 22, 24, 255], col: function (q) {
      if (q.j === 0 && q.z < 7.6) return steel;
      if (q.z > 17.0) return burnt;
      return elev(q.a) < -15 ? under : blotch(pal, 1.7, q, 9);
    } });
    loft(B, SS(sideRows([[6.85, 3.22, 1.08, 0.75, 3.2, 1.45], [7.1, 3.22, 1.08, 0.75, 3.2, 1.45]], s)), { angles: ringQ([12, 25, 40, 55, 70, 80]), color: [150, 158, 168, 255] });
  });
  sidePanels(B, secs, [[3.4, 4.9, 2.7, 3.5], [5.4, 7.0, 2.4, 3.6], [8.2, 9.6, 2.3, 3.7]]);
  // tail boom / stinger
  loft(B, SS([[15.0, 3.5, 2.4, 0.5, 2.2], [17.5, 3.4, 2.45, 0.42, 2.2], [19.5, 3.25, 2.55, 0.3, 2.2], [21.0, 3.1, 2.65, 0.18, 2.2], [21.9, 2.95, 2.8, 0.04, 2]]), { K: 12, color: burnt, caps: 'rear' });
  // spine behind canopy
  loft(B, SS([[8.0, 4.02, 3.5, 0.6, 2.4], [10.0, 4.08, 3.5, 0.8, 2.4], [12.5, 3.98, 3.5, 0.8, 2.4], [15.0, 3.72, 3.4, 0.6, 2.4]]), { K: 14, color: pal[1], caps: 'rear' });
  // tandem canopy
  var cs = SS([[3.4, 3.92, 3.6, 0.35, 2], [4.0, 4.38, 3.65, 0.52, 2], [5.0, 4.65, 3.7, 0.62, 2], [6.4, 4.65, 3.8, 0.64, 2], [7.6, 4.42, 3.85, 0.6, 2], [8.6, 4.12, 3.9, 0.48, 2], [9.8, 4.0, 3.9, 0.3, 2]]);
  canopy(B, G, cs, { frames: 'ends' });
  box(B, 0, 4.64, 6.45, 0.62, 0.035, 0.04, grey);
  // wings, stabs, fins
  var wing = [{ le: [0.9, 2.5, 6.0], te: [0.9, 2.5, 16.4], th: 0.3 }, { le: [2.6, 2.35, 9.3], te: [2.6, 2.35, 16.9], th: 0.2 }, { le: [7.2, 2.25, 13.5], te: [7.2, 2.25, 15.4], th: 0.05 }];
  var stab = [{ le: [1.9, 2.45, 17.8], te: [1.9, 2.45, 20.3], th: 0.1 }, { le: [4.4, 2.5, 19.5], te: [4.4, 2.5, 20.7], th: 0.04 }];
  var fin = [{ le: [2.35, 3.4, 15.2], te: [2.35, 3.4, 20.3], th: 0.09, up: [1, 0, 0] }, { le: [2.5, 6.4, 18.4], te: [2.5, 6.4, 21.1], th: 0.04, up: [1, 0, 0] }];
  mirror2(function (s) {
    var W = s > 0 ? wing : mirrorSecs(wing), T = s > 0 ? stab : mirrorSecs(stab), F = s > 0 ? fin : mirrorSecs(fin);
    foil(B, W, { col: function (q) { return q.top ? blotch(pal, 1.8, q, 2) : under; } });
    foilStrip(B, W, 1.05, 1.98, 0.72, 0.732, 1, dark, 0.014); foilStrip(B, W, 1.05, 1.98, 0.72, 0.732, -1, dark, 0.014);
    foilStrip(B, W, 1.5, 1.54, 0.72, 0.99, 1, dark, 0.014);
    foilStrip(B, W, 1.05, 1.98, 0.15, 0.158, 1, dark, 0.014);
    foil(B, T, { top: pal[0], bot: under });
    foil(B, F, { top: pal[3], bot: pal[3], capCol: dark });
    foilStrip(B, F, 0.02, 0.98, 0.72, 0.732, 1, dark, 0.02); foilStrip(B, F, 0.02, 0.98, 0.72, 0.732, -1, dark, 0.02);
    // red stars: fins (both faces), wing top / bottom
    [1, -1].forEach(function (f) {
      var c = foilPt(F, 0.62, 0.42, f, 0.045), n = [f * s, 0, 0];
      starD(B, c, n, 0.74, blue, [0, 1, 0]); starD(B, add(c, mul(n, 0.012)), n, 0.62, white, [0, 1, 0]); starD(B, add(c, mul(n, 0.024)), n, 0.54, red, [0, 1, 0]);
    });
    var cw = foilPt(W, 1.6, 0.55, 1, 0.03);
    starD(B, cw, [0, 1, 0], 0.62, white, [0, 0, -1]); starD(B, add(cw, [0, 0.012, 0]), [0, 1, 0], 0.54, red, [0, 0, -1]);
    var cb = foilPt(W, 1.6, 0.55, -1, 0.03);
    starD(B, cb, [0, -1, 0], 0.62, white, [0, 0, -1]); starD(B, add(cb, [0, -0.012, 0]), [0, -1, 0], 0.54, red, [0, 0, -1]);
    // wingtip pod
    cyl(B, [s * 7.28, 2.25, 12.6], [s * 7.28, 2.25, 15.7], 0.14, 0.14, 10, grey, 0, true);
    cyl(B, [s * 7.28, 2.25, 12.0], [s * 7.28, 2.25, 12.6], 0.015, 0.14, 10, grey, 0, true);
    light(B, s * 7.28, 2.25, 15.75, s > 0 ? GREEN : RED, 0.05);
    box(B, s * 2.6, 6.2, 20.0, 0.03, 0.2, 0.9, dark);
  });
  // probe, ventral details
  cyl(B, [0, 3.0, 0.0], [0, 3.0, 1.5], 0.025, 0.05, 6, dark, 0, true);
  box(B, 0, 2.18, 14.0, 0.1, 0.05, 1.8, grey);
  cockpitKit(B, 0, 3.86, 5.0);
  cockpitKit(B, 0, 3.98, 6.9, { noPanel: true });
  mirror2(function (s) { addNozzle(m, s * 1.45, 2.45, 18.95, 19.4, 19.75, 0.56); });
  noseGear(m, 2.68, 4.6, 0.36, 0.22, { doorY: 2.38, doorL: 1.2 });
  mirror2(function (s) { mainGear(m, s, s * 1.7, 2.5, 11.4, 0.5, 0.34, { doorY: 1.0, doorL: 1.6, doorW: 0.8, hingeOff: 0.4 }); });
  finishModel('su30', m, {
    tipL: [-7.4, 2.25, 14.2], tipR: [7.4, 2.25, 14.2], finTop: [2.5, 6.4, 19.7], eye: [0, 4.47, 4.9],
    fields: { name: 'Sukhoi Su-30SM2', maker: 'Sukhoi (UAC)', realLengthM: 21.9, realSpanM: 14.7, realHeightM: 6.4, chaseDist: 40, chaseUp: 9, seats: 2, engines: 2, topSpeedMach: 2.0 }
  });
  return m;
};


/* ---------------- model assembly + renderer ---------------- */
var ALL_IDS = ['su30', 'typhoon', 'rafale', 'f15', 'f16xl'];
var BUILT = {};
function buildModel(id) {
  if (BUILT[id]) return BUILT[id];
  var m = MODELS[id]();
  BUILT[id] = m;
  return m;
}

var VS = [
'attribute vec3 aPos; attribute vec3 aNrm; attribute vec4 aCol; attribute vec4 aMat;',
'uniform mat4 uVP; uniform mat4 uModel; uniform vec3 uCam;',
'varying vec3 vN; varying vec4 vC; varying float vM; varying vec3 vRel;',
'void main(){ vec4 w = uModel * vec4(aPos,1.0); vRel = w.xyz - uCam; vN = (uModel * vec4(aNrm,0.0)).xyz;',
' vC = aCol; vM = floor(aMat.x*255.0+0.5); gl_Position = uVP * w; }'
].join('\n');
var FS = [
'#ifdef GL_FRAGMENT_PRECISION_HIGH', 'precision highp float;', '#else', 'precision mediump float;', '#endif',
'varying vec3 vN; varying vec4 vC; varying float vM; varying vec3 vRel;',
'uniform vec2 uFog; uniform vec3 uSky; uniform float uTint; uniform float uAlpha; uniform vec2 uHot;',
'void main(){',
' float dist = length(vRel); vec3 v = -vRel / max(dist,1e-4);',
' vec3 n = normalize(vN);',
' if (vM > 3.5 && vM < 7.5) { if (dot(n, v) < 0.0) n = -n; }',
' vec3 sun = normalize(vec3(0.45,0.85,0.35));',
' float nd = max(dot(n,sun),0.0);',
' vec3 amb = mix(vec3(0.42,0.38,0.34), vec3(0.60,0.70,0.86), n.y*0.5+0.5);',
' vec3 base = vC.rgb;',
' base = mix(base, vec3(0.035,0.032,0.03) + base*0.10, uTint);',
' vec3 col = base * (amb*0.92 + nd*vec3(1.0,0.96,0.88)*0.78);',
' vec3 h = normalize(sun+v); float sp = pow(max(dot(n,h),0.0), 28.0) * 0.10 * (1.0-uTint);',
' col += sp;',
' if (vM > 0.5 && vM < 1.5) {',
'   float fr = pow(1.0 - max(dot(n,v),0.0), 3.0);',
'   float s2 = pow(max(dot(n,h),0.0), 70.0);',
'   col = col*0.8 + vec3(0.50,0.64,0.80)*(0.10+0.55*fr) + vec3(1.0,0.97,0.9)*s2*1.3;',
' } else if (vM > 1.5 && vM < 2.5) { col = base*1.15; }',
' else if (vM > 7.5) {',
'   vec3 hot = mix(vec3(1.0,0.42,0.10), vec3(1.0,0.86,0.60), uHot.y);',
'   col = mix(col*0.6, hot*(0.55+0.45*uHot.x), uHot.x);',
' }',
' float f = clamp((dist - uFog.x) / max(uFog.y - uFog.x, 1e-3), 0.0, 1.0);',
' col = mix(col, uSky, f);',
' gl_FragColor = vec4(col, vC.a * uAlpha);',
'}'
].join('\n');

var VSF = [
'attribute vec3 aF;',
'uniform mat4 uVP; uniform mat4 uM; uniform vec3 uCam;',
'uniform float uR; uniform float uL; uniform float uT; uniform float uPh;',
'varying vec3 vN; varying float vU; varying vec3 vRel;',
'void main(){',
' float u = aF.z;',
' float fl = 1.0 + 0.05*sin(uT*47.0+uPh) + 0.04*sin(uT*91.0+uPh*2.3);',
' float r = uR * (1.0 + 0.12*sin(u*3.14159)) * pow(max(1.0-u,0.0), 0.42);',
' r *= 1.0 + 0.06*sin(u*24.0 - uT*38.0 + uPh + aF.x*2.0);',
' vec3 p = vec3(aF.x*r, aF.y*r, u*uL*fl);',
' vec4 w = uM * vec4(p,1.0);',
' vRel = w.xyz - uCam; vN = (uM * vec4(aF.x, aF.y, 0.35, 0.0)).xyz; vU = u;',
' gl_Position = uVP * w; }'
].join('\n');
var FSF = [
'#ifdef GL_FRAGMENT_PRECISION_HIGH', 'precision highp float;', '#else', 'precision mediump float;', '#endif',
'varying vec3 vN; varying float vU; varying vec3 vRel;',
'uniform float uT; uniform float uAB; uniform float uMP; uniform float uA; uniform float uCore; uniform vec2 uFog; uniform float uPh;',
'void main(){',
' float dist = length(vRel); vec3 v = -vRel / max(dist,1e-4); vec3 n = normalize(vN);',
' float f = abs(dot(n, v));',
' float u = vU;',
' float dia = pow(0.5+0.5*cos(u*6.2831*5.0 - 0.4), 4.0) * (1.0-u);',
' float fk = 0.88 + 0.12*sin(uT*70.0 + u*18.0 + n.x*3.0 + uPh);',
' vec3 c; float a_dia;',
' if (uCore > 0.5) { c = mix(vec3(1.0,1.0,0.92), vec3(1.0,0.72,0.28), smoothstep(0.0,0.9,u)); c += vec3(0.5,0.5,0.4)*dia*uAB; }',
' else {',
'   vec3 orange = mix(vec3(1.0,0.55,0.14), vec3(1.0,0.20,0.04), smoothstep(0.25,1.0,u));',
'   vec3 blue = vec3(0.50,0.62,1.0);',
'   c = mix(blue, orange, smoothstep(0.0, mix(0.35,0.18,uAB), u));',
'   c = mix(c, vec3(0.65,0.78,1.0), (1.0-uAB)*0.45*(1.0-u));',
'   c += vec3(0.6,0.5,0.3)*dia*uAB*1.1;',
' }',
' a_dia = dia; float a = uA * (1.0 + 0.5*dia*uAB) * pow(max(1.0-u,0.0), mix(0.8,1.0,uAB)) * (0.40+0.60*pow(f,0.7)) * fk;',
' float fg = clamp((dist - uFog.x) / max(uFog.y - uFog.x, 1e-3), 0.0, 1.0);',
' a *= (1.0-fg);',
' gl_FragColor = vec4(c, a);',
'}'
].join('\n');

var ATT = { pos: 2, nrm: 3, col: 4, mat: 5 };
function compile(gl, type, src) {
  var s = gl.createShader(type); gl.shaderSource(s, src); gl.compileShader(s);
  if (!gl.getShaderParameter(s, gl.COMPILE_STATUS)) throw new Error('JetGL shader: ' + gl.getShaderInfoLog(s));
  return s;
}
function mkProg(gl, vs, fs, binds) {
  var p = gl.createProgram();
  gl.attachShader(p, compile(gl, gl.VERTEX_SHADER, vs)); gl.attachShader(p, compile(gl, gl.FRAGMENT_SHADER, fs));
  for (var k in binds) gl.bindAttribLocation(p, binds[k], k);
  gl.linkProgram(p);
  if (!gl.getProgramParameter(p, gl.LINK_STATUS)) throw new Error('JetGL link: ' + gl.getProgramInfoLog(p));
  return p;
}
function nozScale(b) { return 0.74 + 0.26 * smooth(b / 0.7) + 0.09 * smooth((b - 0.75) / 0.25); }

function create(gl) {
  var prog = mkProg(gl, VS, FS, { aPos: ATT.pos, aNrm: ATT.nrm, aCol: ATT.col, aMat: ATT.mat });
  var fprog = mkProg(gl, VSF, FSF, { aF: 2 });
  var U = {}, UF = {};
  ['uVP', 'uModel', 'uCam', 'uFog', 'uSky', 'uTint', 'uAlpha', 'uHot'].forEach(function (n) { U[n] = gl.getUniformLocation(prog, n); });
  ['uVP', 'uM', 'uCam', 'uR', 'uL', 'uT', 'uPh', 'uAB', 'uMP', 'uA', 'uCore', 'uFog'].forEach(function (n) { UF[n] = gl.getUniformLocation(fprog, n); });
  var cache = {}, thumbState = null, sky = [0.56, 0.75, 0.94];
  // flame mesh: (cos, sin, u)
  var fl = [], SEG = 16, ST = 12, i, j;
  for (j = 0; j < ST; j++) for (i = 0; i < SEG; i++) {
    var a0 = i * TAU / SEG, a1 = (i + 1) * TAU / SEG, u0 = j / ST, u1 = (j + 1) / ST;
    var A = [Math.cos(a0), Math.sin(a0), u0], B = [Math.cos(a1), Math.sin(a1), u0], C = [Math.cos(a1), Math.sin(a1), u1], D = [Math.cos(a0), Math.sin(a0), u1];
    fl.push.apply(fl, A.concat(B, C, A, C, D));
  }
  var flameBuf = gl.createBuffer(), flameN = fl.length / 3;
  (function () { var pa = gl.getParameter(gl.ARRAY_BUFFER_BINDING); gl.bindBuffer(gl.ARRAY_BUFFER, flameBuf); gl.bufferData(gl.ARRAY_BUFFER, new Float32Array(fl), gl.STATIC_DRAW); gl.bindBuffer(gl.ARRAY_BUFFER, pa); })();

  function pack(meshes) {
    var total = 0, i; meshes.forEach(function (m) { total += m.vcount(); });
    var ab = new ArrayBuffer(total * 32), f = new Float32Array(ab), u = new Uint8Array(ab);
    var off = 0, ranges = [];
    meshes.forEach(function (m) {
      var n = m.vcount();
      for (i = 0; i < n; i++) {
        var o = (off + i) * 8;
        f[o] = m.p[i * 3]; f[o + 1] = m.p[i * 3 + 1]; f[o + 2] = m.p[i * 3 + 2];
        f[o + 3] = m.n[i * 3]; f[o + 4] = m.n[i * 3 + 1]; f[o + 5] = m.n[i * 3 + 2];
        var b = (off + i) * 32 + 24;
        u[b] = m.c[i * 4]; u[b + 1] = m.c[i * 4 + 1]; u[b + 2] = m.c[i * 4 + 2]; u[b + 3] = m.c[i * 4 + 3];
        u[b + 4] = m.m[i];
      }
      ranges.push([off, n]); off += n;
    });
    var buf = gl.createBuffer();
    var prevAB = gl.getParameter(gl.ARRAY_BUFFER_BINDING);
    gl.bindBuffer(gl.ARRAY_BUFFER, buf); gl.bufferData(gl.ARRAY_BUFFER, ab, gl.STATIC_DRAW);
    gl.bindBuffer(gl.ARRAY_BUFFER, prevAB);
    return { buf: buf, ranges: ranges };
  }
  function getModel(id) {
    if (cache[id]) return cache[id];
    var m = buildModel(id), list = [m.body, m.glass];
    m.parts.forEach(function (p, k) { p._i = list.length; list.push(p.mesh); });
    m.noz.forEach(function (p, k) { p._i = list.length; list.push(p.mesh); });
    var pk = pack(list);
    cache[id] = { m: m, buf: pk.buf, ranges: pk.ranges };
    return cache[id];
  }

  function draw(id, model, vp, cam, fogNear, fogFar, burner, tint, gear) {
    var G = getModel(id), m = G.m, R = G.ranges;
    burner = clamp(burner || 0, 0, 1); tint = clamp(tint || 0, 0, 1); gear = clamp(gear || 0, 0, 1);
    if (!(fogFar > fogNear)) { fogNear = 1e8; fogFar = 2e8; }
    var T = (typeof JetGL !== 'undefined' && JetGL.time !== null && JetGL.time !== undefined) ? JetGL.time : (Date.now() / 1000) % 1000;
    var st = {
      prog: gl.getParameter(gl.CURRENT_PROGRAM), ab: gl.getParameter(gl.ARRAY_BUFFER_BINDING),
      blend: gl.isEnabled(gl.BLEND), depth: gl.isEnabled(gl.DEPTH_TEST), cull: gl.isEnabled(gl.CULL_FACE),
      dmask: gl.getParameter(gl.DEPTH_WRITEMASK),
      bsr: gl.getParameter(gl.BLEND_SRC_RGB), bdr: gl.getParameter(gl.BLEND_DST_RGB),
      bsa: gl.getParameter(gl.BLEND_SRC_ALPHA), bda: gl.getParameter(gl.BLEND_DST_ALPHA),
      en: [0, 1, 2, 3, 4, 5].map(function (i) { return gl.getVertexAttrib(i, gl.VERTEX_ATTRIB_ARRAY_ENABLED); })
    };
    gl.useProgram(prog);
    gl.uniformMatrix4fv(U.uVP, false, vp);
    gl.uniform3f(U.uCam, cam[0], cam[1], cam[2]);
    gl.uniform2f(U.uFog, fogNear, fogFar);
    gl.uniform3f(U.uSky, sky[0], sky[1], sky[2]);
    gl.uniform1f(U.uTint, tint);
    var hotA = smooth((burner - 0.05) / 0.5), hotB = smooth((burner - 0.7) / 0.3);
    gl.uniform2f(U.uHot, hotA, hotB);
    gl.bindBuffer(gl.ARRAY_BUFFER, G.buf);
    gl.enableVertexAttribArray(ATT.pos); gl.enableVertexAttribArray(ATT.nrm);
    gl.enableVertexAttribArray(ATT.col); gl.enableVertexAttribArray(ATT.mat);
    gl.vertexAttribPointer(ATT.pos, 3, gl.FLOAT, false, 32, 0);
    gl.vertexAttribPointer(ATT.nrm, 3, gl.FLOAT, false, 32, 12);
    gl.vertexAttribPointer(ATT.col, 4, gl.UNSIGNED_BYTE, true, 32, 24);
    gl.vertexAttribPointer(ATT.mat, 4, gl.UNSIGNED_BYTE, true, 32, 28);
    gl.enable(gl.DEPTH_TEST); gl.disable(gl.CULL_FACE); gl.disable(gl.BLEND); gl.depthMask(true);
    gl.blendFuncSeparate(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA, gl.ONE, gl.ONE_MINUS_SRC_ALPHA);
    var mf = new Float32Array(16);
    function setM(mat) { var a = mMul(model, mat); for (var i = 0; i < 16; i++) mf[i] = a[i]; gl.uniformMatrix4fv(U.uModel, false, mf); }
    function drawRange(k, alpha) { if (!R[k][1]) return; gl.uniform1f(U.uAlpha, alpha); gl.drawArrays(gl.TRIANGLES, R[k][0], R[k][1]); }
    function partMat(p) {
      var a;
      if (p.kind === 'leg') a = p.ang * (1 - smooth((gear - 0.12) / 0.7));
      else {
        a = p.ang * smooth(gear * 6);
        if (p.closeDown) a *= 1 - smooth((gear - 0.85) / 0.15);
      }
      return mMul(mT(p.pivot[0], p.pivot[1], p.pivot[2]), mMul(rotM(p.axis, a), mT(-p.pivot[0], -p.pivot[1], -p.pivot[2])));
    }
    setM(mIdent()); drawRange(0, 1);
    m.parts.forEach(function (p) {
      if (p.kind === 'leg' && gear < 0.01) return;
      setM(partMat(p)); drawRange(p._i, 1);
    });
    var sc = nozScale(burner);
    gl.uniform2f(U.uHot, hotA, hotB);
    m.noz.forEach(function (n) {
      setM(mMul(mT(n.c[0], n.c[1], 0), mMul([sc, 0, 0, 0, 0, sc, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1], mT(-n.c[0], -n.c[1], 0))));
      drawRange(n._i, 1);
    });
    gl.uniform2f(U.uHot, 0, 0);
    gl.enable(gl.BLEND); gl.depthMask(false);
    setM(mIdent()); drawRange(1, 1);
    // ---- afterburner / exhaust plumes
    if (burner > 0.08) {
      var mp = smooth((burner - 0.08) / 0.62), ab = smooth((burner - 0.72) / 0.26);
      gl.useProgram(fprog);
      gl.disableVertexAttribArray(ATT.nrm); gl.disableVertexAttribArray(ATT.col); gl.disableVertexAttribArray(ATT.mat);
      gl.bindBuffer(gl.ARRAY_BUFFER, flameBuf);
      gl.vertexAttribPointer(2, 3, gl.FLOAT, false, 12, 0);
      gl.blendFunc(gl.SRC_ALPHA, gl.ONE);
      gl.uniformMatrix4fv(UF.uVP, false, vp);
      gl.uniform3f(UF.uCam, cam[0], cam[1], cam[2]);
      gl.uniform2f(UF.uFog, fogNear, fogFar);
      gl.uniform1f(UF.uT, T); gl.uniform1f(UF.uAB, ab); gl.uniform1f(UF.uMP, mp);
      m.noz.forEach(function (n, k) {
        var Rn = n.r * sc, Lf = n.r * (0.6 + 2.6 * mp + 12 * ab);
        var mm = mMul(model, mT(n.c[0], n.c[1], n.c[2]));
        for (var i = 0; i < 16; i++) mf[i] = mm[i];
        gl.uniformMatrix4fv(UF.uM, false, mf);
        gl.uniform1f(UF.uPh, k * 1.7);
        gl.blendFunc(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA); gl.uniform1f(UF.uCore, 0); gl.uniform1f(UF.uR, Rn * 1.02); gl.uniform1f(UF.uL, Lf); gl.uniform1f(UF.uA, 0.42 + 0.30 * mp * (1 - ab) + 0.28 * ab);
        gl.drawArrays(gl.TRIANGLES, 0, flameN);
        gl.blendFunc(gl.SRC_ALPHA, gl.ONE); gl.uniform1f(UF.uCore, 1); gl.uniform1f(UF.uR, Rn * 0.52); gl.uniform1f(UF.uL, Lf * 0.62); gl.uniform1f(UF.uA, 0.35 * mp + 0.65 * ab);
        gl.drawArrays(gl.TRIANGLES, 0, flameN);
      });
      gl.disableVertexAttribArray(2);
      if (st.en[2]) gl.enableVertexAttribArray(2);
    }
    // ---- restore
    [ATT.pos, ATT.nrm, ATT.col, ATT.mat].forEach(function (i) { if (!st.en[i]) gl.disableVertexAttribArray(i); else gl.enableVertexAttribArray(i); });
    gl.bindBuffer(gl.ARRAY_BUFFER, st.ab);
    gl.blendFuncSeparate(st.bsr, st.bdr, st.bsa, st.bda);
    if (st.blend) gl.enable(gl.BLEND); else gl.disable(gl.BLEND);
    if (st.depth) gl.enable(gl.DEPTH_TEST); else gl.disable(gl.DEPTH_TEST);
    if (st.cull) gl.enable(gl.CULL_FACE); else gl.disable(gl.CULL_FACE);
    gl.depthMask(st.dmask);
    gl.useProgram(st.prog);
  }

  function thumbnail(id, w, h) {
    if (typeof document === 'undefined') return '';
    var S2 = 2;
    if (!thumbState) {
      var cv = document.createElement('canvas');
      var g2 = cv.getContext('webgl', { alpha: true, premultipliedAlpha: false, preserveDrawingBuffer: true, antialias: true, depth: true });
      if (!g2) return '';
      thumbState = { cv: cv, gl: g2, r: create(g2) };
    }
    var cv2 = thumbState.cv, g = thumbState.gl;
    cv2.width = w * S2; cv2.height = h * S2;
    g.viewport(0, 0, cv2.width, cv2.height);
    g.clearColor(0, 0, 0, 0); g.clearDepth(1); g.enable(g.DEPTH_TEST); g.depthMask(true);
    g.clear(g.COLOR_BUFFER_BIT | g.DEPTH_BUFFER_BIT);
    var mdl = buildModel(id), pts = [], bb = mdl.body.bbox(), i, k;
    for (i = 0; i < 8; i++) pts.push([i & 1 ? bb.max[0] : bb.min[0], i & 2 ? bb.max[1] : bb.min[1], i & 4 ? bb.max[2] : bb.min[2]]);
    var ctr = [0, 0, 0]; pts.forEach(function (p) { ctr = add(ctr, p); }); ctr = mul(ctr, 1 / pts.length);
    var fov = 28 * PI / 180, th = Math.tan(fov / 2), asp = w / h, dir = norm([-0.85, 0.42, -1.0]), dist = 0, eye, vpm;
    pts.forEach(function (p) { dist = Math.max(dist, len(sub(p, ctr)) * 3); });
    for (k = 0; k < 10; k++) {
      eye = add(ctr, mul(dir, dist));
      var view = mLookAt(eye, ctr, [0, 1, 0]);
      vpm = mMul(mPersp(fov, asp, 0.5, 2000), view);
      var mnx = 9, mxx = -9, mny = 9, mxy = -9;
      pts.forEach(function (p) {
        var c = [vpm[0] * p[0] + vpm[4] * p[1] + vpm[8] * p[2] + vpm[12], vpm[1] * p[0] + vpm[5] * p[1] + vpm[9] * p[2] + vpm[13], vpm[3] * p[0] + vpm[7] * p[1] + vpm[11] * p[2] + vpm[15]];
        var iw = 1 / Math.max(c[2], 0.05), x = c[0] * iw, y = c[1] * iw;
        mnx = Math.min(mnx, x); mxx = Math.max(mxx, x); mny = Math.min(mny, y); mxy = Math.max(mxy, y);
      });
      var cx = (mnx + mxx) / 2, cy = (mny + mxy) / 2, ext = Math.max((mxx - mnx) / 2, (mxy - mny) / 2) / 0.9;
      var right = [view[0], view[4], view[8]], up = [view[1], view[5], view[9]];
      ctr = add(ctr, add(mul(right, cx * th * asp * dist), mul(up, cy * th * dist)));
      dist = dist * clamp(ext, 0.6, 1.6);
    }
    eye = add(ctr, mul(dir, dist));
    vpm = mMul(mPersp(fov, asp, dist * 0.05, dist * 6), mLookAt(eye, ctr, [0, 1, 0]));
    var vp = new Float32Array(vpm), md = new Float32Array(mIdent());
    thumbState.r.draw(id, md, vp, eye, 1e8, 2e8, 0, 0, 0);
    var out = document.createElement('canvas'); out.width = w; out.height = h;
    var c2 = out.getContext('2d'); c2.imageSmoothingEnabled = true; c2.imageSmoothingQuality = 'high';
    c2.drawImage(cv2, 0, 0, w, h);
    return out.toDataURL('image/png');
  }

  function dispose() {
    Object.keys(cache).forEach(function (k) { gl.deleteBuffer(cache[k].buf); });
    cache = {};
    gl.deleteBuffer(flameBuf);
    gl.deleteProgram(prog); gl.deleteProgram(fprog);
    if (thumbState) { thumbState.r.dispose(); thumbState = null; }
  }
  return { draw: draw, thumbnail: thumbnail, dispose: dispose };
}

var JetGL = {
  IDS: ALL_IDS, SPECS: SPECS, create: create, time: null,
  _buildForTest: function (id) {
    var m = buildModel(id);
    var tris = m.body.tcount() + m.glass.tcount();
    m.parts.forEach(function (p) { tris += p.mesh.tcount(); });
    m.noz.forEach(function (p) { tris += p.mesh.tcount(); });
    return { body: m.body, glass: m.glass, parts: m.parts, noz: m.noz, triangles: tris };
  }
};
ALL_IDS.forEach(function (id) { if (MODELS[id]) buildModel(id); });
if (typeof window !== 'undefined') window.JetGL = JetGL;
if (typeof module !== 'undefined' && module.exports) module.exports = JetGL;
if (typeof globalThis !== 'undefined' && typeof window === 'undefined') globalThis.JetGL = JetGL;
})(typeof window !== 'undefined' ? window : this);
