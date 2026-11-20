/* CarGL - procedural low-poly road car models + tiny WebGL1 renderer for EzWorld.
   Units: metres. Model space: +Y up, nose toward -Z, +X right (left-hand drive: driver at x<0). */
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
  var c = Math.cos(a), sn = Math.sin(a), ex = 2 / (sn >= 0 ? s.n : (s.nb || s.n));
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
    var s = secs[j], e = { x: s.x, y: s.y, z: s.z, t: Math.max(s.t, 0.03), b: Math.max(s.b, 0.03), w: Math.max(s.w, 0.03), n: s.n, nb: s.nb };
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
  return { z: lerp(a.z, b.z, t), y: lerp(a.y, b.y, t), t: lerp(a.t, b.t, t), b: lerp(a.b, b.b, t), w: lerp(a.w, b.w, t), n: lerp(a.n, b.n, t), nb: lerp(a.nb || a.n, b.nb || b.n, t), x: lerp(a.x, b.x, t) };
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
    var nn = yy >= s.y ? s.n : (s.nb || s.n);
    var cx = Math.pow(Math.max(1 - Math.pow(Math.abs(sy), nn), 0), 1 / nn);
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



/* ---------------- car builder ---------------- */
var ALL_IDS = ['yaris', 'civic', 'f150', 'c5', 'm3', '911', 'cls63', 'aventador'];
var SPECS = {}, CFG = {}, BUILT = {};
var MT = { MATTE: 0, GLASS: 1, EMIT: 2, HEAD: 3, TAIL: 4, CHROME: 5, PAINT: 6, GLOW: 7, SCREEN: 9 };
var FL_ND = 256, FL_RIG = 512, FL_LOOSE = 1024;
function smooth(x) { x = clamp(x, 0, 1); return x * x * (3 - 2 * x); }
function flagMesh(M, from, f) { for (var i = from; i < M.m.length; i++) M.m[i] = (M.m[i] | f); }
function col3(h, mat, a) { var c = hex(h); c[3] = a === undefined ? 255 : a; c[4] = mat || 0; return c; }
function rotAxis(ax, a) {
  var n = norm(ax), x = n[0], y = n[1], z = n[2], c = Math.cos(a), s = Math.sin(a), t = 1 - c;
  return [t * x * x + c, t * x * y + s * z, t * x * z - s * y, 0, t * x * y - s * z, t * y * y + c, t * y * z + s * x, 0, t * x * z + s * y, t * y * z - s * x, t * z * z + c, 0, 0, 0, 0, 1];
}
// monotone cubic interpolation through [x,y] points
function pchip(P) {
  var n = P.length, x = [], y = [], h = [], d = [], m = [], i;
  for (i = 0; i < n; i++) { x[i] = P[i][0]; y[i] = P[i][1]; }
  if (n === 1) return function () { return y[0]; };
  for (i = 0; i < n - 1; i++) { h[i] = x[i + 1] - x[i]; d[i] = (y[i + 1] - y[i]) / h[i]; }
  m[0] = d[0]; m[n - 1] = d[n - 2];
  for (i = 1; i < n - 1; i++) {
    if (d[i - 1] * d[i] <= 0) m[i] = 0;
    else { var w1 = 2 * h[i] + h[i - 1], w2 = h[i] + 2 * h[i - 1]; m[i] = (w1 + w2) / (w1 / d[i - 1] + w2 / d[i]); }
  }
  return function (X) {
    if (X <= x[0]) return y[0];
    if (X >= x[n - 1]) return y[n - 1];
    var k = 0; while (k < n - 2 && X > x[k + 1]) k++;
    var t = (X - x[k]) / h[k], t2 = t * t, t3 = t2 * t;
    return (2 * t3 - 3 * t2 + 1) * y[k] + (t3 - 2 * t2 + t) * h[k] * m[k] + (-2 * t3 + 3 * t2) * y[k + 1] + (t3 - t2) * h[k] * m[k + 1];
  };
}
function lathe(M, prof, seg, colFn, mat) {
  for (var k = 0; k < prof.length - 1; k++) {
    var p0 = prof[k], p1 = prof[k + 1], dx = p1[0] - p0[0], dr = p1[1] - p0[1], l = Math.sqrt(dx * dx + dr * dr) || 1;
    var nx = dr / l, nr = -dx / l, col = colFn(k);
    for (var i = 0; i < seg; i++) {
      var a0 = i * TAU / seg, a1 = (i + 1) * TAU / seg, c0 = Math.cos(a0), s0 = Math.sin(a0), c1 = Math.cos(a1), s1 = Math.sin(a1);
      var A = [p0[0], p0[1] * c0, p0[1] * s0], B2 = [p1[0], p1[1] * c0, p1[1] * s0], C = [p1[0], p1[1] * c1, p1[1] * s1], D = [p0[0], p0[1] * c1, p0[1] * s1];
      var n0 = [nx, nr * c0, nr * s0], n1 = [nx, nr * c1, nr * s1];
      if (p1[1] > 1e-9) { M.v(A, n0, col, mat); M.v(B2, n0, col, mat); M.v(C, n1, col, mat); }
      if (p0[1] > 1e-9) { M.v(A, n0, col, mat); M.v(C, n1, col, mat); M.v(D, n1, col, mat); }
    }
  }
}
var TYRE = [20, 20, 22, 255, 0], SIDEW = [34, 34, 37, 255, 0], DARK = [18, 18, 20, 255, 0], UNDER = [26, 26, 29, 255, 0];
var GLASSCOL = [18, 28, 36, 150, MT.GLASS];

function makeWheel(cfg, side, twv) {
  var R = cfg.R, hw = (twv || cfg.tw) / 2, rr = R * (cfg.rimF || 0.66), M = new Mesh(), seg = 16, k;
  var rc = cfg.rimCol || [190, 194, 202, 255, MT.CHROME];
  lathe(M, [[-hw, rr + 0.004], [-hw * 0.96, R * 0.9], [-hw * 0.78, R], [hw * 0.78, R], [hw * 0.96, R * 0.9], [hw, rr + 0.004]], seg, function (j) { return j === 2 ? TYRE : SIDEW; }, 0);
  lathe(M, [[hw * 0.45, 0], [hw * 0.45, rr * 0.97]], seg, function () { return DARK; }, 0);
  var K = new Mesh();
  lathe(K, [[hw * 0.6, rr * 0.3], [hw * 0.6, rr * 0.86]], seg, function () { return [118, 120, 126, 255, 0]; }, 0);
  lathe(M, [[hw * 0.985, rr * 0.92], [hw * 0.985, rr + 0.003]], seg, function () { return rc; }, 0);
  var th = 0.012 + 0.012 * R / 0.33, xc = hw * 0.8;
  for (k = 0; k < 5; k++) {
    var ph = k * TAU / 5 + 0.31, cs = Math.cos(ph), sn = Math.sin(ph), r0 = rr * 0.15, r1 = rr * 0.94, w0 = rr * 0.075, w1 = rr * 0.14;
    var P = function (x, r, t) { return [x, r * cs - t * sn, r * sn + t * cs]; };
    hexa(M, [P(xc - th / 2, r0, -w0), P(xc - th / 2, r0, w0), P(xc - th / 2, r1, w1), P(xc - th / 2, r1, -w1), P(xc + th / 2, r0, -w0), P(xc + th / 2, r0, w0), P(xc + th / 2, r1, w1), P(xc + th / 2, r1, -w1)], rc, 0);
  }
  cyl(K, [hw * 0.55, 0, 0], [hw * 0.66, 0, 0], rr * 0.2, rr * 0.2, 14, rc, 0, true);
  cyl(M, [hw * 0.78, 0, 0], [hw * 0.97, 0, 0], rr * 0.2, rr * 0.17, 14, rc, 0, true);
  if (cfg.caliper) { var cc = cfg.caliper; box(K, hw * 0.55, -rr * 0.55, rr * 0.0, 0.03, 0.09 * rr / 0.2, 0.05, cc, 0); }
  if (side < 0) { M.apply(0, mRY(PI)); K.apply(0, mRY(PI)); }
  flagMesh(M, 0, FL_RIG); flagMesh(K, 0, FL_RIG);
  return { wheel: M, core: K };
}

function pruneMesh(M) {
  var P = [], N = [], Cc = [], Mm = [], i, k, keep = 0;
  for (i = 0; i < M.p.length; i += 9) {
    var ax = M.p[i + 3] - M.p[i], ay = M.p[i + 4] - M.p[i + 1], az = M.p[i + 5] - M.p[i + 2], bx = M.p[i + 6] - M.p[i], by = M.p[i + 7] - M.p[i + 1], bz = M.p[i + 8] - M.p[i + 2];
    var cx = ay * bz - az * by, cy = az * bx - ax * bz, cz = ax * by - ay * bx;
    if (cx * cx + cy * cy + cz * cz < 1e-14) continue;
    for (k = 0; k < 9; k++) { P.push(M.p[i + k]); N.push(M.n[i + k]); }
    for (k = 0; k < 3; k++) { var v = i / 3 + k; Cc.push(M.c[v * 4], M.c[v * 4 + 1], M.c[v * 4 + 2], M.c[v * 4 + 3]); Mm.push(M.m[v]); }
  }
  M.p = P; M.n = N; M.c = Cc; M.m = Mm;
}
function ringPtAt(sec, deg) { return ringPt(sec, deg * PI / 180); }

function buildCar(cfg) {
  var L = cfg.L, zOff = cfg.wb / 2 + cfg.fo;
  function Z(d) { return d - zOff; }
  var B = new Mesh(), G = new Mesh(), I = new Mesh(), CK = new Mesh(), GL = new Mesh(), i, k;
  var T = pchip(cfg.top), Bt = pchip(cfg.bot), Wd0 = pchip(cfg.wid), Nn = pchip(cfg.n);
  function Wd(d) { return Wd0(d) + 0.02; }
  var cab = cfg.cab, nb = cfg.nb || 7.5, holeA = cab.holeA || 40;
  var belt = typeof cab.belt === 'number' ? function () { return cab.belt; } : pchip(cab.belt);
  var cw = cab.cw || 1;
  var dWs = cab.dWs, dHdr = cab.dHdr, dEnd = cab.dEnd, dRoofR = cab.dRoofR, dGE = cab.dGlassEnd;
  var roofF = pchip(cab.roof), yWs = cab.yWs, yH = cab.roof[0][1];
  function ctop(d) { return d <= dHdr ? yWs + (yH - yWs) * (d - dWs) / (dHdr - dWs) : roofF(d); }
  var cosA = Math.pow(Math.cos(holeA * PI / 180), 1);
  function bodyEdge(d) { return Wd(d) * Math.pow(cosA, 2 / Nn(d)); }
  function wcAt(d) { return bodyEdge(d) * cw + 0.004; }
  var PAINT = col3(cfg.color, MT.PAINT);
  var TRIM = [14, 14, 16, 255, MT.PAINT], PILL = cfg.pillCol || [16, 16, 18, 255, MT.PAINT];
  var holes = cfg.holes || [[dWs, dGE]];

  /* ---- body hull ---- */
  var ds = [0, 0.05, 0.12, 0.22, 0.35], dd;
  for (dd = 0.55; dd < L - 0.5; dd += 0.3) ds.push(dd);
  [cfg.fo, cfg.fo + cfg.wb].forEach(function (w) { var r0 = cfg.R + 0.05; for (var kk = -5; kk <= 5; kk++) ds.push(w + kk * r0 / 4); });
  ds.push(L - 0.35, L - 0.22, L - 0.12, L - 0.05, L);
  ds.push(dWs, dGE); holes.forEach(function (h) { ds.push(h[0], h[1]); });
  (cfg.wheelD || []).forEach(function (d) { ds.push(d); });
  ds.sort(function (a, b) { return a - b; });
  var du = [];
  ds.forEach(function (d) { if (d > L) d = L; if (!du.length || d - du[du.length - 1] > 0.03) du.push(d); else if (d === L) du[du.length - 1] = L; });
  var bsecs = du.map(function (d) {
    var top = T(d), bot = Bt(d);
    return { z: Z(d), y: (top + bot) / 2, t: (top - bot) / 2, b: (top - bot) / 2, w: Wd(d), n: Nn(d), nb: nb, x: 0 };
  });
  var angSet = {}, aa;
  function addA(a) { a = ((a % 360) + 360) % 360; angSet[a] = 1; }
  for (aa = 0; aa <= 40; aa += 5) { addA(aa); addA(180 - aa); }
  for (aa = 300; aa < 360; aa += 5) { addA(aa); addA(180 - aa); }
  for (aa = 50; aa <= 130; aa += 10) addA(aa);
  for (aa = 250; aa <= 290; aa += 10) addA(aa);
  var bang = degAngles(Object.keys(angSet).map(Number).sort(function (x, y) { return x - y; }));
  var archR = cfg.R + (cfg.archGap || 0.05), wD = [cfg.fo, cfg.fo + cfg.wb];
  var s0 = B.vcount();
  loft(B, bsecs, {
    angles: bang, caps: true, capCol: PAINT,
    col: function (q) {
      var d = q.z + zOff, a = q.a, h;
      for (h = 0; h < holes.length; h++) if (d > holes[h][0] - 1e-4 && d < holes[h][1] + 1e-4 && a > holeA && a < 180 - holeA) return null;
      if (!(a >= 65 && a <= 115)) for (h = 0; h < 2; h++) { var ddx = d - wD[h], ddy = q.y - cfg.R; if (ddx * ddx + ddy * ddy < archR * archR) return null; }
      if (a > 195 && a < 345) return UNDER;
      if (cfg.trim) { var tc = cfg.trim(d, q.y, a); if (tc) return tc; }
      return PAINT;
    }
  });
  function secBody(z) { return secAt(bsecs, z); }

  /* ---- cabin (greenhouse) ---- */
  var cds = [], nWs = 4, nR;
  for (k = 0; k <= nWs; k++) cds.push(dWs + (dHdr - dWs) * k / nWs);
  nR = Math.max(4, Math.ceil((dEnd - dHdr) / 0.22));
  for (k = 1; k <= nR; k++) cds.push(dHdr + (dEnd - dHdr) * k / nR);
  cds.push(dRoofR, dGE); (cab.pillars || []).forEach(function (p) { cds.push(p[0], p[1]); });
  (cab.extraD || []).forEach(function (d) { cds.push(d); });
  cds.sort(function (a, b) { return a - b; });
  var cu = []; cds.forEach(function (d) { if (d > dEnd) d = dEnd; if (!cu.length || d - cu[cu.length - 1] > 0.02) cu.push(d); else if (d === dEnd) cu[cu.length - 1] = dEnd; });
  function csec(d, inset) {
    inset = inset || 0;
    var tp = ctop(d), bl = belt(d), t = Math.max(tp - bl, 0.03);
    return { z: Z(d), y: bl, t: Math.max(t - inset, 0.01), b: cab.depth || 0.45, w: wcAt(d) - inset, n: cab.n || 2.5, nb: cab.n || 2.5, x: 0 };
  }
  var csecs = cu.map(function (d) { return csec(d, 0); });
  var cang = degAngles([0, 5, 18, 32, 45, 60, 75, 90, 105, 120, 135, 148, 162, 175, 180, 200, 240, 270, 300, 340]);
  function inPill(d) { var p = cab.pillars || []; for (var j = 0; j < p.length; j++) if (d > p[j][0] && d < p[j][1]) return true; return false; }
  var TARGA = [14, 16, 20, 255, MT.PAINT];
  function cabKind(q, glassRet, paintRet, inner) {
    var d = q.z + zOff, a = q.a;
    if (a >= 180) return paintRet;
    var side = (a < 32 || a > 148), band = (a < 5 || a > 175);
    if (d < dHdr) return (a > 20 && a < 160) ? glassRet : paintRet;
    if (d < dRoofR) {
      if (band) return inner ? paintRet : TRIM;
      if (side) return inPill(d) ? (inner ? paintRet : PILL) : glassRet;
      if (cab.targa && d > cab.targa[0] && d < cab.targa[1] && a > 50 && a < 130 && !inner) return TARGA;
      return paintRet;
    }
    if (d < dGE) {
      if (band && !inner) return TRIM;
      return ((a > 20 && a < 160) || (cab.quarter && side)) ? glassRet : paintRet;
    }
    return paintRet;
  }
  loft(B, csecs, { angles: cang, caps: true, capCol: PAINT, G: G, glassCol: GLASSCOL, col: function (q) { return cabKind(q, GLASS, PAINT, false); } });
  // inner lining (cockpit only)
  var LIN = col3(cfg.lining || 0x6c6a66, MT.EMIT);
  var isecs = cu.filter(function (d, ix) { return ix % 2 === 0 || ix === cu.length - 1; }).map(function (d) { return csec(d, 0.03); });
  loft(CK, isecs, { angles: cang, col: function (q) { var r = cabKind(q, null, LIN, true); return r === GLASS ? null : r; } });
  CK.p.length; // (glass zones return null above)

  /* ---- decals on the end caps ---- */
  var zF = Z(0), zR = Z(L);
  function capHalf(sec, y) {
    var h = y >= sec.y ? sec.t : sec.b, nn = y >= sec.y ? sec.n : (sec.nb || sec.n);
    var sy = clamp((y - sec.y) / Math.max(h, 1e-4), -0.985, 0.985);
    return sec.w * Math.pow(Math.max(1 - Math.pow(Math.abs(sy), nn), 0), 1 / nn);
  }
  function capQ(front, x0, x1, y0, y1, col, layer) {
    var z = front ? zF - 0.003 * (1 + layer) : zR + 0.003 * (1 + layer);
    B.quad([x0, y0, z], [x1, y0, z], [x1, y1, z], [x0, y1, z], col, 0, [0, (y0 + y1) / 2, front ? z + 1 : z - 1]);
  }
  function decals(front, list) {
    var sec = front ? bsecs[0] : bsecs[bsecs.length - 1];
    list.forEach(function (it, li) {
      var x0 = it[0], x1 = it[1], y0 = it[2], y1 = it[3], col = col3(it[4], it[5] || 0), sym = it[6], wrap = it[7] || 0;
      var lim = Math.min(capHalf(sec, y0), capHalf(sec, y1)) - 0.004;
      if (wrap) x1 = Math.min(x1, lim);
      x1 = Math.min(x1, lim); if (x0 > 0) x0 = Math.min(x0, x1 - 0.02); if (x0 < -lim) x0 = -lim;
      if (x1 <= x0) return;
      capQ(front, x0, x1, y0, y1, col, li);
      if (sym) { capQ(front, -x1, -x0, y0, y1, col, li); }
      if (wrap) {
        var zA = front ? zF : zR, zB = front ? Z(wrap) : Z(L - wrap);
        var za = Math.min(zA, zB), zb = Math.max(zA, zB);
        [1, -1].forEach(function (sd) {
          var NU = 3, NV = 1, g = [], gi, gj;
          for (gi = 0; gi <= NU; gi++) { g[gi] = []; for (gj = 0; gj <= NV; gj++) { var zz = za + 0.012 + (zb - za - 0.012) * gi / NU, yy = y0 + (y1 - y0) * gj / NV; g[gi][gj] = [sd * (Math.abs(surf(bsecs, zz, yy, 1).p[0]) + 0.010 + 0.003 * li), yy, zz]; } }
          for (gi = 0; gi < NU; gi++) { var qq = [g[gi][0], g[gi + 1][0], g[gi + 1][1], g[gi][1]]; B.quad(qq[0], qq[1], qq[2], qq[3], col, it[5] || 0, [0, (y0 + y1) / 2, qq[0][2]]); }
        });
      }
    });
  }
  decals(true, cfg.decF || []);
  decals(false, cfg.decR || []);

  /* ---- wheel arches, door lines, handles, mirrors ---- */
  var archCol = [10, 10, 12, 255, 0];
  // rocker panel (dark sill strip between the arches)
  [1, -1].forEach(function (sd) {
    var zA = Z(cfg.fo + archR + 0.04), zB = Z(cfg.fo + cfg.wb - archR - 0.04), sB = secBody((zA + zB) / 2);
    rectPatch(B, bsecs, sd, zA, zB, sB.y - sB.b + 0.035, sB.y - sB.b + 0.13, 6, 1, [22, 22, 25, 255, 0], 0.004, 0);
  });
  var doorLow = cfg.doorLow || 0.3;
  (cfg.doors || []).forEach(function (dr) {
    var dm = (dr[0] + dr[1]) / 2, sc = secBody(Z(dm));
    var yHi = sc.y + sc.t * Math.pow(Math.sin(holeA * PI / 180), 2 / sc.n) - 0.035;
    [1, -1].forEach(function (sd) {
      panelLines(B, bsecs, sd, Z(dr[0]), Z(dr[1]), doorLow, yHi, [20, 20, 22, 255, 0], 0.012, 0.003);
      var zh = Z(dr[1]) - 0.2, yh = yHi - 0.05, sf = surf(bsecs, zh, yh, sd);
      box(B, sf.p[0] + sd * 0.004, yh, zh, 0.014, 0.014, 0.075, [34, 34, 38, 255, MT.CHROME], 0);
    });
  });
  // mirrors (small, aerodynamic)
  var dm0 = cab.mirD || (dWs + 0.25), wm = wcAt(dm0), ym = belt(dm0) + 0.035, mx = wm + (cab.mirOff || 0.07);
  var MC = cfg.mirBlack ? [12, 12, 14, 255, MT.PAINT] : PAINT;
  var MIRM = {};
  [1, -1].forEach(function (sd) {
    var MM = MIRM[sd] = new Mesh(), f0 = 0, zm = Z(dm0);
    ell(MM, sd * mx, ym, zm, 0.032, 0.036, 0.068, 8, 5, MC, MT.PAINT);
    cyl(MM, [sd * (mx - 0.03), ym - 0.03, zm + 0.02], [sd * (wm - 0.01), ym - 0.05, zm + 0.05], 0.008, 0.01, 5, TRIM, 0, true);
    box(MM, sd * mx, ym, zm + 0.066, 0.026, 0.028, 0.003, [10, 12, 16, 255, MT.CHROME], 0);
    flagMesh(MM, f0, FL_LOOSE);
  });

  /* ---- lamps ---- */
  var hlSpec = [], tlSpec = [];
  (cfg.hl || []).forEach(function (h) { hlSpec.push([h[0] / 2 + h[1] / 2, (h[2] + h[3]) / 2, zF]); hlSpec.push([-(h[0] / 2 + h[1] / 2), (h[2] + h[3]) / 2, zF]); });
  (cfg.tl || []).forEach(function (h) { tlSpec.push([h[0] / 2 + h[1] / 2, (h[2] + h[3]) / 2, zR]); tlSpec.push([-(h[0] / 2 + h[1] / 2), (h[2] + h[3]) / 2, zR]); });

  if (cfg.tlPos) { tlSpec = []; cfg.tlPos.forEach(function (h) { tlSpec.push([h[0], h[1], Z(h[2])]); tlSpec.push([-h[0], h[1], Z(h[2])]); }); }
  if (cfg.hlPos) { hlSpec = []; cfg.hlPos.forEach(function (h) { hlSpec.push([h[0], h[1], Z(h[2])]); hlSpec.push([-h[0], h[1], Z(h[2])]); }); }
  /* ---- per car extras ---- */
  var ctx = {
    B: B, G: G, I: I, CK: CK, cfg: cfg, Z: Z, T: T, Bt: Bt, Wd: Wd, bsecs: bsecs, zOff: zOff, PAINT: PAINT, TRIM: TRIM, wcAt: wcAt, belt: belt, ctop: ctop,
    capQ: capQ, surf: surf, secBody: secBody, MT: MT, zF: zF, zR: zR
  };
  if (cfg.extra) cfg.extra(ctx);
  // front lamp lenses: add (symmetric) from cfg.hl as HEAD, rear from cfg.tl as TAIL
  decals(true, (cfg.hl || []).map(function (h) { return [h[0], h[1], h[2], h[3], h[4] || 0xdfe6ee, MT.HEAD, 1, h[5] || 0.18]; }));
  decals(false, (cfg.tl || []).map(function (h) { return [h[0], h[1], h[2], h[3], h[4] || 0x6a0c10, MT.TAIL, 1, h[5] || 0.15]; }));

  /* ---- interior (visible through the glass) ---- */
  var xd = -0.36, seatY = cab.seatY, eyeD = cab.eyeD, hubD = cab.hubD, eyeY = cab.eyeY;
  var ICOL = shade(col3(cfg.intCol || 0x25262a, 0), 1.7), SCOL = shade(col3(cfg.seatCol || 0x3a3b40, 0), 1.6), ACC = shade(col3(cfg.accent || 0x55565c, 0), 1.5);
  var i0 = I.vcount();
  var wcm = wcAt((dWs + dEnd) / 2 * 0.5 + dHdr * 0.5);
  var floorY = seatY - 0.17, dRear = cab.rearD || null;
  var zEnd = Z(Math.min(dEnd - 0.1, (dRear || eyeD) + 0.5));
  box(I, 0, floorY, (Z(dWs + 0.1) + zEnd) / 2, wcm * 0.92, 0.012, (zEnd - Z(dWs + 0.1)) / 2, col3(0x1a1a1c, 0), 0);
  // dash
  var dashZ0 = Z(dWs + 0.03), dashZ1 = Z(Math.max(hubD - 0.1, dWs + 0.22));
  var hubY = eyeY - (cab.hubDy || 0.33), dTop = Math.min(belt(dWs) - 0.015, hubY + 0.06);
  box(I, 0, dTop - 0.145, (dashZ0 + dashZ1) / 2, wcAt(dWs + 0.2) - 0.05, 0.14, (dashZ1 - dashZ0) / 2, ICOL, 0);
  box(I, 0, dTop, (dashZ0 + dashZ1) / 2 + 0.03, wcAt(dWs + 0.2) - 0.08, 0.012, (dashZ1 - dashZ0) / 2 - 0.04, ACC, 0);
  var hubZ = Z(hubD);
  // instrument binnacle + gauges
  box(I, xd, hubY + 0.1, hubZ - 0.14, 0.15, 0.05, 0.05, DARK, 0);
  [-0.06, 0.06].forEach(function (dx) {
    cyl(I, [xd + dx, hubY + 0.1, hubZ - 0.075], [xd + dx, hubY + 0.1, hubZ - 0.068], 0.04, 0.04, 12, col3(0x0c0f14, 0), 0, true);
    cyl(I, [xd + dx, hubY + 0.1, hubZ - 0.068], [xd + dx, hubY + 0.1, hubZ - 0.066], 0.032, 0.032, 12, col3(0x35c6ff, MT.SCREEN), 0, true);
    box(I, xd + dx, hubY + 0.105, hubZ - 0.064, 0.003, 0.025, 0.001, col3(0xffffff, MT.SCREEN), 0);
  });
  // centre stack / console
  box(I, 0, dTop - 0.1, hubZ - 0.05, 0.14, 0.09, 0.05, DARK, 0);
  box(I, 0, dTop - 0.09, hubZ - 0.0, 0.12, 0.06, 0.003, col3(0x2a6a8a, MT.SCREEN), 0);
  var consZ0 = hubZ + 0.02, consZ1 = Z(eyeD + 0.3);
  box(I, 0, seatY + 0.02, (consZ0 + consZ1) / 2, 0.095, 0.1, (consZ1 - consZ0) / 2, ICOL, 0);
  ell(I, 0, seatY + 0.16, Z(eyeD - 0.15), 0.02, 0.05, 0.02, 6, 3, DARK, 0);
  // steering column
  var tilt = 0.38, nrm = [0, Math.sin(tilt), Math.cos(tilt)], hub = [xd, hubY, hubZ];
  cyl(I, add(hub, mul(nrm, -0.3)), add(hub, mul(nrm, -0.02)), 0.03, 0.03, 8, DARK, 0, true);
  // seats
  function seat(x, d, rear) {
    var z = Z(d), y0 = seatY;
    box(I, x, y0 - 0.04, z, 0.23, 0.07, 0.25, SCOL, 0);
    var bk = new Mesh(); box(bk, 0, 0, 0, 0.23, 0.31, 0.05, SCOL, 0);
    box(bk, 0, 0.4, 0, 0.1, 0.08, 0.045, SCOL, 0);
    bk.apply(0, mMul(mT(x, y0 + 0.28, z + 0.25), mRX(0.2)));
    I.append(bk);
    if (cfg.bolster) { box(I, x - 0.22, y0 + 0.0, z, 0.03, 0.06, 0.24, SCOL, 0); box(I, x + 0.22, y0 + 0.0, z, 0.03, 0.06, 0.24, SCOL, 0); }
  }
  seat(-0.36, eyeD - 0.1); seat(0.36, eyeD - 0.1);
  if (dRear) { seat(-0.36, dRear); seat(0.36, dRear); if ((cfg.seats || 4) >= 5) seat(0, dRear); }
  // rear view mirror
  var tw = 0.82, dmr = dWs + (dHdr - dWs) * tw, ymr = yWs + (yH - yWs) * tw - 0.07;
  box(I, 0, ymr, Z(dmr) + 0.03, 0.06, 0.017, 0.008, DARK, 0);
  cyl(I, [0, ymr + 0.03, Z(dmr) + 0.012], [0, ymr + 0.005, Z(dmr) + 0.03], 0.006, 0.006, 5, DARK, 0, true);
  flagMesh(I, i0, FL_ND);

  /* ---- cockpit-only extras ---- */
  var k0 = CK.vcount();
  var dcA = dWs + 0.15, dcB = Math.min(dEnd - 0.2, (dRear || eyeD) + 0.45);
  [1, -1].forEach(function (sd) {
    var dmid = (dcA + dcB) / 2, xw = wcAt(dmid) - 0.035 - 0.02, yLo = floorY + 0.05, yHi = belt(dmid) - 0.03;
    box(CK, sd * xw, (yLo + yHi) / 2, (Z(dcA) + Z(dcB)) / 2, 0.022, (yHi - yLo) / 2, (Z(dcB) - Z(dcA)) / 2, ICOL, 0);
    box(CK, sd * (xw - 0.04), belt(dmid) - 0.12, (Z(dcA) + Z(dcB)) / 2 + 0.1, 0.04, 0.03, (Z(dcB) - Z(dcA)) / 2 - 0.25, ACC, 0);
    box(CK, sd * (xw - 0.02), belt(dmid) - 0.04, (Z(dcA) + Z(dcB)) / 2, 0.012, 0.012, (Z(dcB) - Z(dcA)) / 2, ACC, 0);
  });
  box(CK, xd - 0.01, hubY - 0.06, hubZ - 0.2, 0.15, 0.08, 0.12, ICOL, 0);
  flagMesh(CK, k0, FL_ND);
  // the lining already flagged by loft: set no-dent on all
  flagMesh(CK, 0, FL_ND);

  /* ---- steering wheel (separate rotating part) ---- */
  var SW = new Mesh(), swn = 16, ur = [1, 0, 0], vr = [0, Math.cos(tilt), -Math.sin(tilt)], rr0 = cfg.swR || 0.18;
  function swp(a, r) { return add(hub, add(mul(ur, Math.cos(a) * r), mul(vr, Math.sin(a) * r))); }
  var SWC = col3(0x16161a, 0);
  for (i = 0; i < swn; i++) cyl(SW, swp(i * TAU / swn, rr0), swp((i + 1) * TAU / swn, rr0), 0.017, 0.017, 6, SWC, 0, false);
  [PI, 0, 1.5 * PI].forEach(function (a) { cyl(SW, hub, swp(a, rr0 - 0.01), 0.012, 0.012, 5, SWC, 0, true); });
  cyl(SW, add(hub, mul(nrm, -0.015)), add(hub, mul(nrm, 0.035)), 0.05, 0.045, 10, SWC, 0, true);
  cyl(SW, swp(PI / 2, rr0 - 0.02), swp(PI / 2, rr0 + 0.02), 0.012, 0.012, 5, col3(0xd8d8dc, MT.CHROME), 0, true);
  flagMesh(SW, 0, FL_ND);

  /* ---- headlight glow cones ---- */
  var GC = [255, 244, 205, 0];
  hlSpec.forEach(function (h) {
    var ax = [0, -0.06, -1], n = 10, len = 9, r1 = 1.5, apex = [h[0], h[1], h[2] - 0.02], ring = [], j;
    var u = [1, 0, 0], v = [0, 1, 0];
    for (j = 0; j < n; j++) { var a = j * TAU / n; ring.push(add(add(apex, mul(ax, len)), add(mul(u, Math.cos(a) * r1), mul(v, Math.sin(a) * r1 * 0.7)))); }
    for (j = 0; j < n; j++) {
      var p1 = ring[j], p2 = ring[(j + 1) % n];
      var nn = norm(cross(sub(p1, apex), sub(p2, apex)));
      GL.v(apex, nn, [255, 244, 205, 120, MT.GLOW], MT.GLOW); GL.v(p1, nn, [255, 240, 190, 0, MT.GLOW], MT.GLOW); GL.v(p2, nn, [255, 240, 190, 0, MT.GLOW], MT.GLOW);
    }
  });
  flagMesh(GL, 0, FL_ND);

  /* ---- wheels ---- */
  var wheels = [];
  [[0, 1], [0, -1], [1, 1], [1, -1]].forEach(function (w) {
    // FL, FR, RL, RR ordering below
  });
  var order = [[0, -1], [0, 1], [1, -1], [1, 1]]; // front/rear, side(-1 left)
  var track = [0, 0];
  order.forEach(function (o) {
    var dwh = o[0] ? cfg.fo + cfg.wb : cfg.fo, hwB = Wd(dwh), outer = hwB - 0.03, tw = o[0] && cfg.twR ? cfg.twR : cfg.tw;
    var cx = outer - tw / 2, c = [o[1] * cx, cfg.R, Z(dwh)];
    var wr = makeWheel(cfg, o[1], tw), wm = wr.wheel;
    // wheel house liner (dark half-shell behind the arch opening)
    (function () { var ra = archR - 0.012, xin = Math.abs(c[0]) - tw / 2 - 0.03, xout = hwB, N = 14, j, sdn = o[1]; for (j = 0; j < N; j++) { var a0 = j * PI / N, a1 = (j + 1) * PI / N; var P = function (x, a) { return [sdn * x, c[1] + ra * Math.sin(a), c[2] + ra * Math.cos(a)]; }; B.quad(P(xin, a0), P(xout, a0), P(xout, a1), P(xin, a1), archCol, 0, [sdn * xin, c[1], c[2]]); B.tri([sdn * xin, c[1], c[2]], P(xin, a0), P(xin, a1), archCol, 0, [sdn, 0, 0]); } })();
    wm.apply(0, mT(c[0], c[1], c[2])); wr.core.apply(0, mT(c[0], c[1], c[2]));
    wheels.push({ c: c, front: !o[0], mesh: wm, core: wr.core });
    track[o[0]] = 2 * cx;
  });
  /* ---- spec ---- */
  var allm = [B, G, I, CK, GL, SW].concat(wheels.map(function (w) { return w.mesh; }));
  allm.forEach(pruneMesh);
  /* ---- detachable parts ---- */
  function extractTris(M, pred) {
    var out = new Mesh(), kp = [], kn = [], kc = [], km = [], i, k, nt = M.p.length / 9;
    for (var t = 0; t < nt; t += 2) {
      var cnt = (t + 1 < nt) ? 2 : 1, i0 = t * 9, cx = 0, cy = 0, cz = 0, nx = 0, ny = 0, nz = 0, vv = cnt * 3;
      for (k = 0; k < vv; k++) { cx += M.p[i0 + k * 3]; cy += M.p[i0 + k * 3 + 1]; cz += M.p[i0 + k * 3 + 2]; nx += M.n[i0 + k * 3]; ny += M.n[i0 + k * 3 + 1]; nz += M.n[i0 + k * 3 + 2]; }
      cx /= vv; cy /= vv; cz /= vv; nx /= vv; ny /= vv; nz /= vv;
      var take = pred(cx, cy, cz + zOff, nx, ny, nz), P = take ? out.p : kp, N = take ? out.n : kn, Cc = take ? out.c : kc, Mm = take ? out.m : km;
      for (k = 0; k < vv * 3; k++) { P.push(M.p[i0 + k]); N.push(M.n[i0 + k]); }
      for (k = 0; k < vv; k++) { var v = i0 / 3 + k; Cc.push(M.c[v * 4], M.c[v * 4 + 1], M.c[v * 4 + 2], M.c[v * 4 + 3]); Mm.push(M.m[v]); }
    }
    M.p = kp; M.n = kn; M.c = kc; M.m = km;
    return out;
  }
  // back skin: copy of a panel pushed along -normal in a dark colour, so a detached panel has thickness
  function addBack(M, off, col) {
    var n0 = M.p.length / 3, i, k;
    for (i = 0; i < n0; i++) { var sg = (M.n[i * 3] * M.p[i * 3] + M.n[i * 3 + 1] * (M.p[i * 3 + 1] - 0.3)) > 0 ? 1 : -1; M.p.push(M.p[i * 3] - sg * M.n[i * 3] * off, M.p[i * 3 + 1] - sg * M.n[i * 3 + 1] * off, M.p[i * 3 + 2] - sg * M.n[i * 3 + 2] * off); M.n.push(-M.n[i * 3], -M.n[i * 3 + 1], -M.n[i * 3 + 2]); M.c.push(col[0], col[1], col[2], 255); M.m.push(M.m[i] & 0xFF00); }
  }
  var parts = {}, PN = ['hood', 'trunk', 'bumperF', 'bumperB', 'doorL', 'doorR', 'mirrorL', 'mirrorR'];
  PN.forEach(function (n) { parts[n] = { mesh: new Mesh(), glass: new Mesh(), filler: new Mesh() }; });
  parts.mirrorL.mesh = MIRM[-1]; parts.mirrorR.mesh = MIRM[1];
  var dr0 = (cfg.doors && cfg.doors[0]) ? cfg.doors[0] : [dWs + 0.2, dWs + 1.0];
  [[-1, 'doorL'], [1, 'doorR']].forEach(function (e) {
    var sd = e[0];
    parts[e[1]].mesh = extractTris(B, function (x, y, d, nx, ny, nz) { return x * sd > 0 && Math.abs(nx) > 0.55 && d >= dr0[0] && d <= dr0[1] && y >= doorLow - 0.02; });
    parts[e[1]].glass = extractTris(G, function (x, y, d, nx, ny, nz) { return x * sd > 0 && Math.abs(nx) > 0.35 && d >= dr0[0] && d <= dr0[1] && y >= doorLow - 0.02; });
  });
  var t0 = cfg.hatchD ? cfg.hatchD : Math.max(dGE, L - 1.0);
  function upper(x, y, d, ny) { return ny > 0.3 && y > T(d) - 0.14 && Math.abs(x) < 0.7 * Wd(d); }
  parts.hood.mesh = extractTris(B, function (x, y, d, nx, ny) { return d < dWs + 0.02 && upper(x, y, d, ny); });
  if (cfg.hatchD) {
    parts.trunk.mesh = extractTris(B, function (x, y, d) { return d >= cfg.hatchD && y > 0.62 && Math.abs(x) < 0.6 * Wd(d); });
    parts.trunk.glass = extractTris(G, function (x, y, d) { return d >= cfg.hatchD && Math.abs(x) < 0.65 * Wd(d); });
  } else if (cfg.tailgate) parts.trunk.mesh = extractTris(B, function (x, y, d) { return d > L - 0.16 && y > cfg.tailgate; });
  else parts.trunk.mesh = extractTris(B, function (x, y, d, nx, ny) { return d >= t0 && upper(x, y, d, ny); });
  var bumpEnd = cfg.bumperTop || 99;
  parts.bumperF.mesh = extractTris(B, function (x, y, d) { return d < 0.42 && y < bumpEnd; });
  parts.bumperB.mesh = extractTris(B, function (x, y, d) { return d > L - 0.42 && y < (cfg.tailgate ? cfg.tailgate : bumpEnd); });
  // fillers (what is left visible when a part has come off)
  var DK = [14, 14, 16, 255, 0], MET = [52, 54, 60, 255, MT.CHROME], STEEL = [110, 114, 122, 255, MT.CHROME], WALL = [26, 26, 30, 255, 0];
  (function () {
    var F = parts.hood.filler, dA = 0.46, dB = dWs - 0.12, zc = Z((dA + dB) / 2), hz = Z(dB) - Z(dA), fy = Bt(dB) + 0.16, ty = T(dB) - 0.12, wx = 0.72 * Wd(1.0);
    box(F, 0, fy, zc, wx, 0.012, hz / 2, DK, 0);
    [1, -1].forEach(function (sd) { box(F, sd * wx, (fy + ty) / 2, zc, 0.015, (ty - fy) / 2, hz / 2, WALL, 0); });
    box(F, 0, (fy + ty) / 2, Z(dA), wx, (ty - fy) / 2, 0.012, WALL, 0);
    if (cfg.engineFront === false) { box(F, 0, fy + 0.1, zc, 0.35, 0.08, hz * 0.3, [36, 38, 44, 255, 0], 0); cyl(F, [0.2, fy + 0.2, zc], [0.2, fy + 0.2, zc + 0.01], 0.12, 0.12, 10, [24, 24, 26, 255, 0], 0, true); }
    else {
      box(F, 0, fy + 0.2, zc + 0.04, 0.27, 0.17, hz * 0.34, [40, 42, 48, 255, 0], 0);
      box(F, 0, fy + 0.4, zc + 0.04, 0.2, 0.025, hz * 0.28, STEEL, 0);
      [-1, 1].forEach(function (sd) { box(F, sd * 0.15, fy + 0.38, zc + 0.04, 0.07, 0.03, hz * 0.3, [150, 30, 24, 255, 0], 0); });
      box(F, 0, fy + 0.14, Z(dA) + 0.1, 0.4, 0.1, 0.05, MET, 0);
    }
  })();
  (function () {
    if (cfg.tailgate) return;
    var F = parts.trunk.filler, dA = t0 + 0.02, dB = L - 0.5, zc = Z((dA + dB) / 2), hz = Z(dB) - Z(dA), fy = Math.max(Bt(dB) + 0.3, cfg.trunkFloor || 0), ty = T(dB) - 0.06, wx = 0.7 * Wd(dB);
    if (hz < 0.1) hz = 0.1;
    box(F, 0, fy, zc, wx, 0.012, hz / 2, DK, 0);
    [1, -1].forEach(function (sd) { box(F, sd * wx, (fy + ty) / 2, zc, 0.015, Math.max((ty - fy) / 2, 0.02), hz / 2, WALL, 0); });
    box(F, 0, (fy + ty) / 2, Z(dB), wx, Math.max((ty - fy) / 2, 0.02), 0.012, WALL, 0);
    if (cfg.engineRear) { box(F, 0, fy + 0.16, zc, 0.3, 0.15, hz * 0.4, [40, 42, 48, 255, 0], 0); box(F, 0, fy + 0.33, zc, 0.22, 0.03, hz * 0.34, STEEL, 0); [-1, 1].forEach(function (sd) { box(F, sd * 0.2, fy + 0.2, zc, 0.06, 0.04, hz * 0.4, [150, 30, 24, 255, 0], 0); }); }
    else cyl(F, [0, fy + 0.012, zc], [0, fy + 0.03, zc], 0.3, 0.3, 14, [30, 30, 33, 255, 0], 0, true);
  })();
  [[true, 'bumperF'], [false, 'bumperB']].forEach(function (e) {
    var F = parts[e[1]].filler, fr = e[0], d0 = fr ? 0.42 : L - 0.42, z0 = Z(d0), wx = 0.8 * Wd(d0), yb = Bt(d0) + 0.06, yt = (cfg.bumperTop && cfg.bumperTop < 90 ? cfg.bumperTop : T(d0) - 0.2);
    box(F, 0, (yb + yt) / 2, z0, wx, Math.max((yt - yb) / 2, 0.03), 0.01, DK, 0);
    var zb = z0 + (fr ? -0.07 : 0.07);
    box(F, 0, yb + (yt - yb) * 0.55, zb, wx * 0.92, 0.045, 0.035, STEEL, 0);
    [1, -1].forEach(function (sd) { box(F, sd * wx * 0.7, yb + (yt - yb) * 0.55, (zb + z0) / 2, 0.04, 0.04, 0.05, MET, 0); });
  });
  [[-1, 'doorL'], [1, 'doorR']].forEach(function (e) {
    var sd = e[0], F = parts[e[1]].filler, dm = (dr0[0] + dr0[1]) / 2, sc = secBody(Z(dm)), yHi = sc.y + sc.t * Math.pow(Math.sin(holeA * PI / 180), 2 / sc.n) - 0.035, xw = Wd(dm) - 0.1;
    box(F, sd * xw, (doorLow + yHi) / 2, Z(dm), 0.02, (yHi - doorLow) / 2, (Z(dr0[1]) - Z(dr0[0])) / 2, [44, 44, 50, 255, 0], 0);
    box(F, sd * (xw + 0.04), doorLow + 0.02, Z(dm), 0.06, 0.02, (Z(dr0[1]) - Z(dr0[0])) / 2, [20, 20, 22, 255, 0], 0);
    box(F, sd * xw, yHi - 0.03, Z(dm), 0.03, 0.025, (Z(dr0[1]) - Z(dr0[0])) / 2, [60, 60, 66, 255, 0], 0);
  });
  addBack(parts.hood.mesh, 0.03, [34, 34, 38, 255]); addBack(parts.trunk.mesh, 0.03, [34, 34, 38, 255]); addBack(parts.doorL.mesh, 0.07, [46, 46, 52, 255]); addBack(parts.doorR.mesh, 0.07, [46, 46, 52, 255]);
  var pc = {};
  PN.forEach(function (n) { [parts[n].mesh, parts[n].glass, parts[n].filler].forEach(pruneMesh); allm.push(parts[n].mesh, parts[n].glass, parts[n].filler); });
  wheels.forEach(function (w) { allm.push(w.core); });
  PN.forEach(function (n) { var b = parts[n].mesh.bbox(); pc[n] = parts[n].mesh.vcount() ? [(b.min[0] + b.max[0]) / 2, (b.min[1] + b.max[1]) / 2, (b.min[2] + b.max[2]) / 2] : [0, 0, 0]; });
  ['wheelFL', 'wheelFR', 'wheelRL', 'wheelRR'].forEach(function (n, i) { pc[n] = wheels[i].c.slice(); });
  var mn = [1e9, 1e9, 1e9], mx = [-1e9, -1e9, -1e9];
  [B, G].concat(wheels.map(function (w) { return w.mesh; }), PN.map(function (n) { return parts[n].mesh; }), PN.map(function (n) { return parts[n].glass; })).forEach(function (M) { var b = M.bbox(); for (var j = 0; j < 3; j++) { mn[j] = Math.min(mn[j], b.min[j]); mx[j] = Math.max(mx[j], b.max[j]); } });
  var eye = [xd, eyeY, Z(eyeD)];
  var hoodD = cab.hoodD || dWs * 0.55;
  var sp = {
    name: cfg.name, maker: cfg.maker, year: cfg.year, realLengthM: cfg.L, realWidthM: cfg.W, realHeightM: cfg.H,
    wheelbase: cfg.wb, trackFront: track[0], trackRear: track[1], wheelRadius: cfg.R,
    wheels: wheels.map(function (w) { return w.c.slice(); }),
    noseZ: mn[2], tailZ: mx[2], halfWidth: Math.max(mx[0], -mn[0]), height: mx[1],
    eye: eye, chaseDist: Math.round((cfg.L * 1.7) * 10) / 10, chaseUp: Math.round((cfg.H * 1.2 + 0.6) * 10) / 10,
    hood: [0, T(hoodD) + 0.01, Z(hoodD)],
    lights: { front: hlSpec, tail: tlSpec }, parts: pc, partRadius: cfg.R,
    seats: cfg.seats, drive: cfg.drive, engine: cfg.engine, topSpeedKph: cfg.topSpeedKph
  };
  return { body: B, glass: G, interior: I, cockpit: CK, glow: GL, steer: { mesh: SW, pivot: hub, axis: nrm }, wheels: wheels, spec: sp, all: allm, parts: parts, partNames: PN };
}
function buildModel(id) {
  if (BUILT[id]) return BUILT[id];
  var m = buildCar(CFG[id]);
  BUILT[id] = m; SPECS[id] = m.spec;
  return m;
}


/* ---------------- the seven cars ---------------- */
var CHR = [200, 204, 210, 255, MT.CHROME], BLK = [14, 14, 16, 255, 0];
function topAt(c, d, x) {
  var top = c.T(d), bot = c.Bt(d), n = c.cfg.nIdx ? 3 : 3, w = c.Wd(d), mid = (top + bot) / 2, t = (top - bot) / 2;
  var nn = c.cfg.nHood || 3.2;
  return mid + t * Math.pow(Math.max(1 - Math.pow(Math.abs(x) / w, nn), 0), 1 / nn);
}
function ribbon(c, xc, hwd, d0, d1, lift, col) {
  var N = 8, j, B = c.B;
  for (j = 0; j < N; j++) {
    var da = d0 + (d1 - d0) * j / N, db = d0 + (d1 - d0) * (j + 1) / N;
    var P = function (d, dx, up) { return [xc + dx, topAt(c, d, xc + dx) + up, c.Z(d)]; };
    var env = function (d) { return Math.sin(Math.PI * Math.min(1, Math.max(0, (d - d0) / (d1 - d0)))) ; };
    var la = lift * Math.min(1, env(da) * 2.5 + 0.1), lb = lift * Math.min(1, env(db) * 2.5 + 0.1);
    var a0 = P(da, -hwd, 0.002), a1 = P(da, 0, la), a2 = P(da, hwd, 0.002), b0 = P(db, -hwd, 0.002), b1 = P(db, 0, lb), b2 = P(db, hwd, 0.002);
    B.quad(a0, b0, b1, a1, col, 0, [xc, topAt(c, da, xc) - 0.2, c.Z(da)]);
    B.quad(a1, b1, b2, a2, col, 0, [xc, topAt(c, da, xc) - 0.2, c.Z(da)]);
  }
}
function pipe(c, x, y, r, len, col) {
  cyl(c.B, [x, y, c.zR - 0.04], [x, y, c.zR + (len || 0.08)], r, r, 10, col || [60, 62, 68, 255, MT.CHROME], 0, true);
  cyl(c.B, [x, y, c.zR + (len || 0.08) - 0.002], [x, y, c.zR + (len || 0.08) + 0.001], r * 0.72, r * 0.72, 10, [8, 8, 8, 255, 0], 0, true);
}
function discZ(c, x, y, z, r, col, mat, front, th) {
  th = th || 0.012;
  cyl(c.B, [x, y, front ? z : z], [x, y, front ? z - th : z + th], r, r, 14, [col[0], col[1], col[2], 255, mat || 0], 0, true);
}
function trimBumpers(c0, yMax) { return function (d, y, a) { return (d < 0.3 || d > this.L - 0.3) && y < yMax ? [28, 28, 31, 255, 0] : null; }; }
function lowerTrim(cfg, yF, yR) {
  return function (d, y, a) { if (d < 0.32 && y < yF) return [28, 28, 31, 255, 0]; if (d > cfg.L - 0.34 && y < yR) return [28, 28, 31, 255, 0]; return null; };
}

CFG.yaris = {
  hatchD: 3.3,
  id: 'yaris', name: 'Toyota Yaris', maker: 'Toyota', year: 2020, L: 3.95, W: 1.70, H: 1.50, wb: 2.55, fo: 0.82, R: 0.30, tw: 0.185, rimF: 0.70,
  color: 0xc61a2b, seats: 5, drive: 'FWD', engine: '1.5 L 3-cyl hybrid, 85 kW', topSpeedKph: 175,
  intCol: 0x2a2b2f, seatCol: 0x45464c,
  top: [[0, 0.70], [0.12, 0.77], [0.5, 0.83], [0.95, 0.88], [1.4, 0.95], [2.0, 0.96], [3.0, 0.965], [3.7, 0.97], [3.88, 0.96], [3.95, 0.92]],
  bot: [[0, 0.33], [0.15, 0.26], [0.5, 0.19], [1.0, 0.16], [3.0, 0.16], [3.6, 0.2], [3.95, 0.34]],
  wid: [[0, 0.70], [0.1, 0.77], [0.4, 0.815], [1.0, 0.83], [3.2, 0.83], [3.7, 0.80], [3.95, 0.72]],
  n: [[0,5],[1,5.2],[1.6,5.8],[3.2,5.8],[3.95,5.2]],
  cab: { dWs: 1.4, yWs: 0.995, dHdr: 2.15, roof: [[2.15, 1.455], [2.4, 1.488], [2.9, 1.495], [3.3, 1.45], [3.6, 1.30], [3.88, 1.0]], dRoofR: 3.3, dGlassEnd: 3.88, dEnd: 3.88,
    belt: 0.97, depth: 0.45, n: 2.5, pillars: [[2.62, 2.74]], seatY: 0.5, eyeD: 2.3, eyeY: 1.18, hubD: 1.78, rearD: 3.15, mirD: 1.7 },
  doors: [[1.6, 2.68], [2.68, 3.4]],
  decF: [[-0.30, 0.30, 0.57, 0.66, 0x101012, 0], [-0.52, 0.52, 0.34, 0.50, 0x0c0c0e, 0], [-0.46, 0.46, 0.50, 0.53, 0x1a1a1c, 0]],
  hl: [[0.34, 0.72, 0.55, 0.68, 0xdfe6ee, 0.2]],
  decR: [[-0.26, 0.26, 0.5, 0.62, 0xdddddd, 0], [-0.62, 0.62, 0.34, 0.42, 0x1a1a1c, 0]],
  tl: [[0.46, 0.80, 0.64, 0.86, 0x6a0c10, 0.16]],
  trim: function (d, y, a) { return lowerTrim(CFG.yaris, 0.4, 0.4)(d, y, a); },
  extra: function (c) {
    pipe(c, 0.5, 0.24, 0.035, 0.05);
  }
};

CFG.civic = {
  id: 'civic', name: 'Honda Civic', maker: 'Honda', year: 2020, L: 4.67, W: 1.80, H: 1.41, wb: 2.74, fo: 0.90, R: 0.325, tw: 0.215, rimF: 0.68,
  color: 0x1f4a9c, seats: 5, drive: 'FWD', engine: '1.5 L turbo I4, 150 kW (Si)', topSpeedKph: 200,
  intCol: 0x1c1c20, seatCol: 0x2b2b30, accent: 0x6a1a1a,
  top: [[0, 0.64], [0.15, 0.71], [0.55, 0.78], [1.1, 0.84], [1.7, 0.935], [2.5, 0.945], [3.8, 0.955], [4.3, 0.965], [4.5, 0.97], [4.67, 0.92]],
  bot: [[0, 0.32], [0.15, 0.25], [0.5, 0.18], [1.0, 0.15], [3.6, 0.15], [4.2, 0.2], [4.67, 0.33]],
  wid: [[0, 0.72], [0.1, 0.80], [0.4, 0.86], [1.0, 0.88], [3.8, 0.88], [4.3, 0.85], [4.67, 0.78]],
  n: [[0,5],[1,5.2],[1.8,5.8],[4.67,5.4]],
  cab: { dWs: 1.7, yWs: 0.985, dHdr: 2.45, roof: [[2.45, 1.385], [2.7, 1.408], [3.3, 1.41], [3.65, 1.37], [3.98, 1.24], [4.3, 0.99]], dRoofR: 3.65, dGlassEnd: 4.3, dEnd: 4.3,
    belt: 0.95, depth: 0.45, n: 2.5, pillars: [[3.02, 3.14]], seatY: 0.46, eyeD: 2.65, eyeY: 1.14, hubD: 2.1, rearD: 3.5, mirD: 1.95 },
  doors: [[1.9, 3.08], [3.08, 4.0]],
  decF: [[-0.44, 0.44, 0.52, 0.62, 0x101012, 0], [-0.48, 0.48, 0.62, 0.645, 0xc8ccd2, MT.CHROME], [-0.58, 0.58, 0.33, 0.49, 0x0c0c0e, 0]],
  hl: [[0.36, 0.76, 0.50, 0.62, 0xdfe6ee, 0.22]],
  decR: [[-0.26, 0.26, 0.52, 0.64, 0xdddddd, 0], [-0.7, 0.7, 0.34, 0.44, 0x1a1a1c, 0]],
  tl: [[0.5, 0.86, 0.66, 0.86, 0x6a0c10, 0.2]],
  trim: function (d, y, a) { return lowerTrim(CFG.civic, 0.4, 0.4)(d, y, a); },
  extra: function (c) {
    box(c.B, 0, 0.99, c.Z(4.52), 0.56, 0.012, 0.07, c.PAINT, 0);
    pipe(c, 0.5, 0.25, 0.04, 0.06); pipe(c, -0.5, 0.25, 0.04, 0.06);
  }
};

CFG.f150 = {
  tailgate: 0.66,
  id: 'f150', name: 'Ford F-150', maker: 'Ford', year: 2021, L: 5.89, W: 2.03, H: 1.97, wb: 3.68, fo: 0.98, R: 0.375, tw: 0.27, rimF: 0.64, archGap: 0.06,
  color: 0xe8e9ec, seats: 5, drive: 'RWD', engine: '3.5 L EcoBoost twin-turbo V6, 298 kW', topSpeedKph: 180,
  intCol: 0x232427, seatCol: 0x3a3b3f, nb: 9,
  top: [[0, 1.14], [0.12, 1.22], [0.5, 1.29], [1.0, 1.33], [1.8, 1.37], [2.0, 1.40], [3.9, 1.41], [4.0, 1.41], [5.7, 1.41], [5.89, 1.38]],
  bot: [[0, 0.40], [0.2, 0.32], [0.6, 0.27], [1.2, 0.26], [4.7, 0.26], [5.5, 0.30], [5.89, 0.40]],
  wid: [[0, 0.90], [0.1, 0.96], [0.4, 0.99], [1.0, 0.995], [5.5, 0.995], [5.89, 0.94]],
  n: [[0,6],[1,6.5],[2,7],[5.89,7]],
  holes: [[2.0, 3.96], [4.08, 5.76]],
  cab: { dWs: 2.0, yWs: 1.45, dHdr: 2.75, roof: [[2.75, 1.93], [2.95, 1.965], [3.7, 1.97], [3.88, 1.96], [3.92, 1.52], [3.96, 1.43]], dRoofR: 3.88, dGlassEnd: 3.92, dEnd: 3.96, holeA: 40,
    belt: 1.42, depth: 0.5, n: 3.0, cw: 1.0, pillars: [[2.98, 3.12]], seatY: 0.74, eyeD: 3.1, eyeY: 1.52, hubD: 2.55, rearD: 3.75, mirD: 2.2, mirOff: 0.1 },
  doors: [[2.1, 3.05], [3.05, 3.92]], doorLow: 0.42,
  decF: [[-0.62, 0.62, 0.76, 1.13, 0xc4c8ce, MT.CHROME], [-0.57, 0.57, 0.80, 1.09, 0x0e0e10, 0], [-0.57, 0.57, 0.94, 0.97, 0xc4c8ce, MT.CHROME], [-0.96, 0.96, 0.52, 0.66, 0xc4c8ce, MT.CHROME], [-0.5, 0.5, 0.42, 0.52, 0x0c0c0e, 0]],
  hl: [[0.66, 0.97, 1.0, 1.15, 0xdfe6ee, 0.25]],
  decR: [[-0.96, 0.96, 0.5, 0.64, 0xc4c8ce, MT.CHROME], [-0.3, 0.3, 1.15, 1.25, 0x8a8e94, MT.CHROME]],
  tl: [[0.78, 0.97, 0.98, 1.3, 0x6a0c10, 0.1]],
  trim: function (d, y, a) { return lowerTrim(CFG.f150, 0.52, 0.52)(d, y, a); },
  extra: function (c) {
    box(c.B, 0, 1.0, (c.Z(4.08) + c.Z(5.76)) / 2, 0.89, 0.012, (c.Z(5.76) - c.Z(4.08)) / 2, [34, 34, 36, 255, 0], 0);
    [1, -1].forEach(function (s) { box(c.B, s * 0.76, 1.08, c.Z(4.66), 0.14, 0.08, 0.36, [34, 34, 36, 255, 0], 0); });
    pipe(c, 0.7, 0.4, 0.04, 0.06); pipe(c, -0.7, 0.4, 0.04, 0.06);
  }
};

CFG.c5 = {
  id: 'c5', name: 'Chevrolet Corvette C5', maker: 'Chevrolet', year: 2001, L: 4.56, W: 1.87, H: 1.21, wb: 2.656, fo: 0.90, R: 0.335, tw: 0.255, rimF: 0.68,
  color: 0xe8b81c, seats: 2, drive: 'RWD', engine: '5.7 L LS1 V8, 257 kW', topSpeedKph: 270, intCol: 0x1e1d1b, seatCol: 0x3a352c,
  top: [[0, 0.50], [0.15, 0.57], [0.5, 0.66], [1.0, 0.75], [1.5, 0.83], [1.75, 0.865], [2.5, 0.89], [3.2, 0.91], [3.9, 0.925], [4.3, 0.935], [4.56, 0.88]],
  bot: [[0, 0.27], [0.15, 0.2], [0.5, 0.14], [1.0, 0.12], [3.6, 0.12], [4.1, 0.15], [4.56, 0.28]],
  wid: [[0, 0.76], [0.1, 0.83], [0.4, 0.88], [1.0, 0.90], [2.0, 0.905], [2.7, 0.915], [3.4, 0.915], [4.0, 0.9], [4.4, 0.85], [4.56, 0.8]],
  n: [[0,5],[1.5,5.2],[2.5,5.6],[4.56,5.2]],
  cab: { dWs: 1.78, yWs: 0.885, dHdr: 2.78, roof: [[2.78, 1.185], [2.95, 1.205], [3.4, 1.2], [3.75, 1.15], [4.1, 1.03], [4.35, 0.945]], dRoofR: 3.55, dGlassEnd: 4.35, dEnd: 4.35, targa: [2.95, 3.55],
    belt: 0.85, depth: 0.4, n: 2.6, pillars: [], seatY: 0.34, eyeD: 2.62, eyeY: 1.0, hubD: 2.05, mirD: 2.0, cw: 0.99 },
  doors: [[2.0, 3.15]], doorLow: 0.26,
  decF: [[-0.5, 0.5, 0.28, 0.39, 0x0c0c0e, 0], [-0.3, 0.3, 0.42, 0.48, 0x101012, 0]],
  hl: [[0.46, 0.84, 0.38, 0.5, 0xdfe6ee, 0.26]],
  decR: [[-0.26, 0.26, 0.5, 0.6, 0xdddddd, 0], [-0.7, 0.7, 0.3, 0.4, 0x1a1a1c, 0]],
  tl: [], tlPos: [[0.64, 0.62, 4.56]],
  trim: function (d, y, a) { return lowerTrim(CFG.c5, 0.3, 0.32)(d, y, a); },
  extra: function (c) {
    [0.52, 0.76, -0.52, -0.76].forEach(function (x) {
      cyl(c.B, [x, 0.62, c.zR + 0.004], [x, 0.62, c.zR + 0.03], 0.075, 0.075, 14, [150, 12, 16, 255, MT.TAIL], 0, true);
      cyl(c.B, [x, 0.62, c.zR + 0.03], [x, 0.62, c.zR + 0.034], 0.05, 0.05, 14, [90, 8, 10, 255, MT.TAIL], 0, true);
    });
    pipe(c, 0.38, 0.3, 0.04, 0.06); pipe(c, -0.38, 0.3, 0.04, 0.06);
  }
};

CFG.m3 = {
  id: 'm3', name: 'BMW M3 Competition', maker: 'BMW', year: 2021, L: 4.80, W: 1.90, H: 1.43, wb: 2.86, fo: 0.93, R: 0.34, tw: 0.25, twR: 0.285, rimF: 0.68, mirBlack: true,
  color: 0x1c7048, seats: 5, drive: 'RWD', engine: '3.0 L twin-turbo straight-six, 375 kW (510 hp)', topSpeedKph: 290, intCol: 0x1d1d20, seatCol: 0x2f3036, accent: 0x4a1a12, caliper: [200, 30, 30, 255, 0],
  top: [[0, 0.68], [0.15, 0.78], [0.5, 0.84], [1.0, 0.88], [1.7, 0.925], [1.9, 0.945], [3.0, 0.975], [4.3, 1.0], [4.5, 1.01], [4.7, 1.02], [4.8, 0.97]],
  bot: [[0, 0.30], [0.15, 0.22], [0.5, 0.15], [1.0, 0.13], [3.7, 0.13], [4.3, 0.17], [4.8, 0.30]],
  wid: [[0, 0.76], [0.1, 0.84], [0.4, 0.90], [0.93, 0.93], [1.5, 0.915], [2.4, 0.90], [3.4, 0.92], [3.79, 0.935], [4.3, 0.9], [4.8, 0.80]],
  n: [[0, 5], [1, 5.2], [1.9, 5.8], [4.8, 5.4]],
  cab: { dWs: 1.85, yWs: 1.0, dHdr: 2.6, roof: [[2.6, 1.40], [2.85, 1.425], [3.4, 1.425], [3.8, 1.38], [4.15, 1.24], [4.5, 1.02]], dRoofR: 3.85, dGlassEnd: 4.5, dEnd: 4.5,
    belt: 0.97, depth: 0.45, n: 2.5, pillars: [[3.12, 3.26]], seatY: 0.46, eyeD: 2.72, eyeY: 1.15, hubD: 2.2, rearD: 3.65, mirD: 2.05 },
  doors: [[1.95, 3.2], [3.2, 4.1]],
  decF: [[0.04, 0.31, 0.37, 0.67, 0x151517, 0, 1], [0.065, 0.285, 0.395, 0.65, 0x040405, 0, 1],
    [0.07, 0.28, 0.44, 0.452, 0x3a3a3f, 0, 1], [0.07, 0.28, 0.49, 0.502, 0x3a3a3f, 0, 1], [0.07, 0.28, 0.54, 0.552, 0x3a3a3f, 0, 1], [0.07, 0.28, 0.59, 0.602, 0x3a3a3f, 0, 1],
    [-0.62, 0.62, 0.31, 0.43, 0x0c0c0e, 0], [0.66, 0.9, 0.34, 0.5, 0x0c0c0e, 0, 1]],
  hl: [[0.40, 0.93, 0.60, 0.675, 0xdfe6ee, 0.26]],
  decR: [[-0.26, 0.26, 0.6, 0.72, 0xdddddd, 0], [-0.8, 0.8, 0.3, 0.42, 0x1a1a1c, 0]],
  tl: [[0.5, 0.93, 0.74, 0.9, 0x6a0c10, 0.2]],
  trim: function (d, y, a) { return lowerTrim(CFG.m3, 0.38, 0.36)(d, y, a); },
  extra: function (c) {
    ribbon(c, 0.22, 0.07, 0.3, 1.75, 0.022, c.PAINT); ribbon(c, -0.22, 0.07, 0.3, 1.75, 0.022, c.PAINT);
    box(c.B, 0, 1.035, c.Z(4.68), 0.62, 0.012, 0.06, [12, 12, 14, 255, MT.PAINT], 0);
    [0.5, 0.64, -0.5, -0.64].forEach(function (x) { pipe(c, x, 0.25, 0.036, 0.05); });
    [0.58, -0.58].forEach(function (x) { discZ(c, x, 0.64, c.zF - 0.004, 0.03, [210, 220, 235], MT.CHROME, true, 0.006); });
  }
};

CFG['911'] = {
  engineFront: false, engineRear: true,
  id: '911', name: 'Porsche 911 Carrera (997)', maker: 'Porsche', year: 2006, L: 4.45, W: 1.80, H: 1.30, wb: 2.35, fo: 0.93, R: 0.33, tw: 0.245, rimF: 0.69,
  color: 0x16161a, seats: 4, drive: 'RWD', engine: '3.6 L flat-6, 239 kW (rear)', topSpeedKph: 285, intCol: 0x232326, seatCol: 0x34343a, rimCol: [200, 204, 210, 255, MT.CHROME],
  top: [[0, 0.58], [0.2, 0.655], [0.6, 0.715], [1.0, 0.75], [1.42, 0.79], [2.5, 0.85], [3.4, 0.86], [3.6, 0.80], [4.0, 0.78], [4.3, 0.76], [4.45, 0.72]],
  bot: [[0, 0.25], [0.15, 0.19], [0.5, 0.13], [1.0, 0.12], [3.4, 0.12], [4.0, 0.16], [4.45, 0.28]],
  wid: [[0, 0.70], [0.1, 0.77], [0.4, 0.82], [1.0, 0.83], [1.8, 0.84], [2.5, 0.87], [3.1, 0.88], [3.8, 0.875], [4.2, 0.84], [4.45, 0.76]],
  n: [[0,5],[1,5],[2,5.4],[4.45,5.2]],
  cab: { dWs: 1.42, yWs: 0.85, dHdr: 2.12, roof: [[2.12, 1.285], [2.4, 1.30], [2.85, 1.28], [3.2, 1.2], [3.6, 1.07], [4.0, 0.95], [4.4, 0.86]], dRoofR: 3.0, dGlassEnd: 3.55, dEnd: 4.4, extraD: [3.9],
    belt: 0.82, depth: 0.4, n: 2.6, pillars: [], seatY: 0.33, eyeD: 2.22, eyeY: 1.0, hubD: 1.72, rearD: 2.95, mirD: 1.62, cw: 0.97 },
  holes: [[1.42, 3.55]],
  doors: [[1.6, 2.95]], doorLow: 0.26,
  decF: [[-0.5, 0.5, 0.29, 0.42, 0x0c0c0e, 0], [-0.25, 0.25, 0.44, 0.5, 0x101012, 0]],
  hl: [],
  decR: [[-0.26, 0.26, 0.45, 0.56, 0xdddddd, 0], [-0.6, 0.6, 0.3, 0.38, 0x1a1a1c, 0]],
  tl: [[0.16, 0.76, 0.6, 0.69, 0x6a0c10, 0.2]],
  trim: function (d, y, a) { return lowerTrim(CFG['911'], 0.34, 0.34)(d, y, a); },
  extra: function (c) {
    [1, -1].forEach(function (s) {
      ell(c.B, s * 0.58, 0.69, c.Z(0.62), 0.25, 0.08, 0.42, 12, 6, c.PAINT, MT.PAINT);
      ell(c.B, s * 0.60, 0.72, c.Z(0.30), 0.085, 0.085, 0.05, 10, 4, [226, 232, 238, 255, MT.HEAD], MT.HEAD);
      ell(c.B, s * 0.60, 0.72, c.Z(0.335), 0.098, 0.098, 0.03, 10, 3, [30, 30, 34, 255, 0], 0);
    });
    pipe(c, 0.46, 0.24, 0.04, 0.06); pipe(c, -0.46, 0.24, 0.04, 0.06);
    // engine-lid vent
    box(c.B, 0, 0.96, c.Z(4.1), 0.34, 0.004, 0.1, [10, 10, 12, 255, 0], 0);
  }
};
CFG['911'].hlPos = [[0.6, 0.72, 0.26]];

CFG.cls63 = {
  id: 'cls63', name: 'Mercedes-Benz CLS 63 AMG', maker: 'Mercedes-Benz', year: 2014, L: 4.99, W: 1.88, H: 1.42, wb: 2.874, fo: 0.85, R: 0.335, tw: 0.255, rimF: 0.67,
  color: 0x6e737c, seats: 5, drive: 'RWD', engine: '5.5 L biturbo V8, 410 kW (AMG)', topSpeedKph: 250, intCol: 0x1c1c1f, seatCol: 0x4a3c30, accent: 0x6b5a48,
  top: [[0, 0.64], [0.15, 0.72], [0.6, 0.80], [1.2, 0.86], [1.7, 0.92], [1.85, 0.94], [3.0, 0.96], [4.4, 1.0], [4.7, 1.0], [4.99, 0.95]],
  bot: [[0, 0.32], [0.15, 0.24], [0.5, 0.16], [1.0, 0.14], [3.8, 0.14], [4.4, 0.18], [4.99, 0.32]],
  wid: [[0, 0.72], [0.1, 0.80], [0.4, 0.88], [1.0, 0.92], [4.0, 0.92], [4.6, 0.88], [4.99, 0.80]],
  n: [[0,5],[1,5.2],[1.8,5.8],[4.99,5.4]],
  cab: { dWs: 1.8, yWs: 0.975, dHdr: 2.55, roof: [[2.55, 1.395], [2.8, 1.418], [3.25, 1.415], [3.65, 1.345], [4.0, 1.2], [4.25, 1.07], [4.45, 1.005]], dRoofR: 3.7, dGlassEnd: 4.45, dEnd: 4.45,
    belt: [[1.8, 0.945], [3.0, 0.97], [4.4, 1.0]], depth: 0.45, n: 2.5, pillars: [[3.0, 3.12]], seatY: 0.45, eyeD: 2.62, eyeY: 1.13, hubD: 2.1, rearD: 3.45, mirD: 2.05 },
  doors: [[1.92, 3.04], [3.04, 3.95]],
  decF: [[-0.44, 0.44, 0.50, 0.81, 0xc4c8ce, MT.CHROME], [-0.40, 0.40, 0.53, 0.78, 0x0e0e10, 0], [-0.40, 0.40, 0.63, 0.66, 0xc4c8ce, MT.CHROME], [-0.56, 0.56, 0.33, 0.45, 0x0c0c0e, 0], [0.58, 0.8, 0.34, 0.45, 0x0c0c0e, 0, 1]],
  hl: [[0.5, 0.9, 0.62, 0.75, 0xdfe6ee, 0.24]],
  decR: [[-0.26, 0.26, 0.62, 0.74, 0xdddddd, 0], [-0.7, 0.7, 0.3, 0.42, 0x1a1a1c, 0]],
  tl: [[0.55, 0.9, 0.72, 0.87, 0x6a0c10, 0.2]],
  trim: function (d, y, a) { return lowerTrim(CFG.cls63, 0.4, 0.4)(d, y, a); },
  extra: function (c) {
    var z = c.zF - 0.016, y = 0.655;
    cyl(c.B, [0, y, z], [0, y, z - 0.008], 0.075, 0.075, 20, [200, 204, 210, 255, MT.CHROME], 0, true);
    cyl(c.B, [0, y, z - 0.008], [0, y, z - 0.011], 0.063, 0.063, 20, [12, 12, 14, 255, 0], 0, true);
    [90, 210, 330].forEach(function (dg) {
      var a = dg * PI / 180, ca = Math.cos(a), sa = Math.sin(a), zz = z - 0.014, w = 0.008;
      var p0 = [0, y, zz], p1 = [ca * 0.063 - sa * w, y + sa * 0.063 + ca * w, zz], p2 = [ca * 0.063 + sa * w, y + sa * 0.063 - ca * w, zz];
      c.B.tri(p0, p1, p2, [210, 214, 220, 255, MT.CHROME], 0, [0, 0, -1]);
    });
    [0.5, 0.68, -0.5, -0.68].forEach(function (x) { pipe(c, x, 0.25, 0.04, 0.06); });
  }
};

CFG.aventador = {
  engineFront: false, engineRear: true,
  id: 'aventador', name: 'Lamborghini Aventador', maker: 'Lamborghini', year: 2011, L: 4.84, W: 2.03, H: 1.14, wb: 2.70, fo: 1.0, R: 0.35, tw: 0.255, twR: 0.35, rimF: 0.66, mirBlack: true,
  color: 0xf26a0f, seats: 2, drive: 'AWD', engine: '6.5 L V12, 544 kW (740 hp)', topSpeedKph: 350, intCol: 0x18181a, seatCol: 0x2a2a2e, accent: 0x55555a, caliper: [230, 200, 30, 255, 0],
  top: [[0, 0.40], [0.2, 0.50], [0.6, 0.62], [1.2, 0.72], [1.55, 0.78], [3.3, 0.80], [4.0, 0.78], [4.5, 0.78], [4.84, 0.76]],
  bot: [[0, 0.17], [0.2, 0.12], [0.6, 0.10], [1.0, 0.10], [4.0, 0.10], [4.5, 0.13], [4.84, 0.24]],
  wid: [[0, 0.78], [0.15, 0.88], [0.5, 0.95], [1.0, 0.99], [2.0, 0.985], [2.9, 0.995], [3.6, 0.99], [4.2, 0.95], [4.84, 0.80]],
  n: [[0, 5.5], [1, 6], [4.84, 6]], nb: 9,
  cab: { dWs: 1.5, yWs: 0.80, dHdr: 2.25, roof: [[2.25, 1.115], [2.5, 1.14], [2.9, 1.13], [3.3, 1.07], [3.8, 0.97], [4.5, 0.88]], dRoofR: 3.05, dGlassEnd: 3.3, dEnd: 4.5,
    belt: 0.76, depth: 0.4, n: 2.6, cw: 0.98, pillars: [], seatY: 0.30, eyeD: 2.2, eyeY: 0.93, hubD: 1.72, mirD: 1.75, mirOff: 0.05, hubDy: 0.2 },
  holes: [[1.5, 3.3]],
  doors: [[1.7, 2.85]], doorLow: 0.22,
  decF: [[-0.75, 0.75, 0.19, 0.27, 0x0c0c0e, 0], [-0.3, 0.3, 0.3, 0.34, 0x101012, 0]],
  hl: [[0.40, 0.96, 0.345, 0.385, 0xdfe6ee, 0.28], [0.40, 0.56, 0.30, 0.345, 0xdfe6ee, 0.18]],
  decR: [[-0.26, 0.26, 0.32, 0.4, 0xdddddd, 0], [-0.9, 0.9, 0.25, 0.3, 0x141416, 0]],
  tl: [[0.55, 0.95, 0.62, 0.68, 0x6a0c10, 0.2], [0.55, 0.7, 0.56, 0.62, 0x6a0c10, 0.12]],
  trim: function (d, y, a) { return lowerTrim(CFG.aventador, 0.3, 0.3)(d, y, a); },
  extra: function (c) {
    var Z = c.Z;
    // hexagonal side intakes behind the doors
    [1, -1].forEach(function (sd) {
      var K = [10, 10, 12, 255, 0];
      patch(c.B, c.bsecs, sd, [[Z(2.9), 0.36], [Z(3.35), 0.30], [Z(3.35), 0.5], [Z(2.9), 0.56]], 3, 2, K, 0.006, 0);
      patch(c.B, c.bsecs, sd, [[Z(2.9), 0.56], [Z(3.35), 0.5], [Z(3.5), 0.52], [Z(3.0), 0.66]], 3, 2, K, 0.006, 0);
    });
    // engine-cover vents
    [0.2, -0.2].forEach(function (x) { box(c.B, x, 0.95, Z(3.85), 0.11, 0.004, 0.45, [8, 8, 10, 255, 0], 0); });
    // huge hexagonal exhaust, top centre rear
    cyl(c.B, [0, 0.62, c.zR - 0.02], [0, 0.62, c.zR + 0.04], 0.12, 0.12, 6, [60, 62, 68, 255, MT.CHROME], 0, true);
    cyl(c.B, [0, 0.62, c.zR + 0.039], [0, 0.62, c.zR + 0.042], 0.085, 0.085, 6, [6, 6, 6, 255, 0], 0, true);
    // rear wing
    box(c.B, 0, 1.06, Z(4.76), 0.82, 0.012, 0.13, [14, 14, 16, 255, MT.PAINT], 0);
    [0.32, -0.32].forEach(function (x) { box(c.B, x, 0.97, Z(4.7), 0.02, 0.1, 0.035, [14, 14, 16, 255, MT.PAINT], 0); });
    [0.84, -0.84].forEach(function (x) { box(c.B, x, 1.06, Z(4.76), 0.008, 0.045, 0.14, [14, 14, 16, 255, MT.PAINT], 0); });
    // splitter
    box(c.B, 0, 0.115, c.zF + 0.1, 0.82, 0.008, 0.12, [10, 10, 12, 255, 0], 0);
  }
};


/* ---------------- renderer ---------------- */
var VS = [
'attribute vec3 aPos; attribute vec3 aNrm; attribute vec4 aCol; attribute vec4 aMat;',
'uniform mat4 uVP; uniform mat4 uCar; uniform mat4 uPart; uniform vec3 uCam;',
'uniform vec4 uDent; uniform vec4 uBox; uniform float uDam; uniform vec3 uWC;',
'varying vec3 vN; varying vec4 vC; varying float vM; varying vec3 vRel; varying vec3 vObj; varying float vDent;',
'float h3(vec3 p){ return fract(sin(dot(p,vec3(12.9898,78.233,37.719)))*43758.5453); }',
'float vn(vec3 p){ vec3 i=floor(p); vec3 f=fract(p); f=f*f*(3.0-2.0*f);',
' return mix(mix(mix(h3(i),h3(i+vec3(1.0,0.0,0.0)),f.x),mix(h3(i+vec3(0.0,1.0,0.0)),h3(i+vec3(1.0,1.0,0.0)),f.x),f.y),',
'            mix(mix(h3(i+vec3(0.0,0.0,1.0)),h3(i+vec3(1.0,0.0,1.0)),f.x),mix(h3(i+vec3(0.0,1.0,1.0)),h3(i+vec3(1.0,1.0,1.0)),f.x),f.y),f.z); }',
'void main(){',
' float fl = floor(aMat.y*255.0+0.5);',
' float nod = mod(fl,2.0); float rig = mod(floor(fl/2.0),2.0); float loo = mod(floor(fl/4.0),2.0);',
' vec3 pc = (uPart*vec4(aPos,1.0)).xyz; vec3 p = pc; float amt = 0.0;',
' float dmax = max(max(uDent.x,uDent.y),max(uDent.z,uDent.w));',
' if (dmax > 0.001 || uDam > 0.001) {',
'  float stat = max(rig, nod); vec3 q = rig > 0.5 ? uWC : pc; float nr = 1.0 - stat;',
'  float Lb = uBox.y - uBox.x; float Lc = 0.27*Lb; float hy = clamp((q.y-0.4)/max(uBox.w-0.4,0.3),0.0,1.0);',
'  float nz = stat > 0.5 ? 0.5 : vn(q*4.3+vec3(1.7,3.1,5.9));',
'  float nz2 = stat > 0.5 ? 0.5 : vn(q*9.0+vec3(7.0,2.0,1.0));',
'  float zm = 0.5*(uBox.x+uBox.y); float hl = 0.5*Lb;',
'  if (uDent.x > 0.001) { float s=(q.z-uBox.x)/Lc; float f=pow(1.0-clamp(s,0.0,1.0),1.4); float k=uDent.x*f;',
'   p.z += k*Lc*0.55*(0.5+0.8*nz);',
'   p.x *= 1.0-0.18*k*(0.5+nz2)*nr;',
'   p.y += uDent.x*sin(clamp((s-0.08)/0.5,0.0,1.0)*3.14159)*0.18*hy*(0.3+nz)*nr;',
'   p.y -= k*(0.06+0.16*nz2)*hy*nr;',
'   p.z += k*0.22*(1.0-hy)*nz2*nr;',
'   amt = max(amt,k*nr); }',
'  if (uDent.y > 0.001) { float s=(uBox.y-q.z)/Lc; float f=pow(1.0-clamp(s,0.0,1.0),1.4); float k=uDent.y*f;',
'   p.z -= k*Lc*0.55*(0.5+0.8*nz);',
'   p.x *= 1.0-0.18*k*(0.5+nz2)*nr;',
'   p.y += uDent.y*sin(clamp((s-0.08)/0.5,0.0,1.0)*3.14159)*0.18*hy*(0.3+nz)*nr;',
'   p.y -= k*(0.06+0.16*nz2)*hy*nr;',
'   p.z -= k*0.22*(1.0-hy)*nz2*nr;',
'   amt = max(amt,k*nr); }',
'  float gz = 1.0 - smoothstep(0.2,0.9,abs(q.z-zm)/hl);',
'  if (uDent.z > 0.001) { float s=(q.x+uBox.z)/0.55; float f=pow(1.0-clamp(s,0.0,1.0),1.2); float k=uDent.z*f*gz*(0.7+0.6*nz);',
'   p.x += k*0.34*(0.5+nz)+k*0.12*hy*nr; p.y -= k*(0.05+0.14*nz2)*hy*nr; p.z += (nz2-0.5)*k*0.2*nr;',
'   amt = max(amt,k*nr); }',
'  if (uDent.w > 0.001) { float s=(uBox.z-q.x)/0.55; float f=pow(1.0-clamp(s,0.0,1.0),1.2); float k=uDent.w*f*gz*(0.7+0.6*nz);',
'   p.x -= k*0.34*(0.5+nz)+k*0.12*hy*nr; p.y -= k*(0.05+0.14*nz2)*hy*nr; p.z += (nz2-0.5)*k*0.2*nr;',
'   amt = max(amt,k*nr); }',
'  if (uDam > 0.001 && stat < 0.5) { vec3 j = vec3(vn(q*6.0),vn(q*6.0+vec3(5.0,1.0,3.0)),vn(q*6.0+vec3(9.0,4.0,2.0)))-0.5;',
'   p += j*0.008*uDam; }',
' }',
' if (loo > 0.5) { p.y -= uDam*0.13*(0.35+0.65*step(0.0,aPos.x)); p.x += uDam*0.035*(aPos.x < 0.0 ? -1.0 : 1.0); }',
' vec4 w = uCar * vec4(p,1.0); vRel = w.xyz - uCam;',
' vN = (uCar * vec4((uPart*vec4(aNrm,0.0)).xyz,0.0)).xyz;',
' vC = aCol; vM = floor(aMat.x*255.0+0.5); vObj = aPos; vDent = amt; gl_Position = uVP * w; }'
].join('\n');

var FS = [
'#ifdef GL_FRAGMENT_PRECISION_HIGH', 'precision highp float;', '#else', 'precision mediump float;', '#endif',
'varying vec3 vN; varying vec4 vC; varying float vM; varying vec3 vRel; varying vec3 vObj; varying float vDent;',
'uniform vec2 uFog; uniform vec3 uSky; uniform float uTint; uniform float uAlpha; uniform float uDam; uniform float uHead; uniform float uBrake; uniform float uTime;',
'float h3(vec3 p){ return fract(sin(dot(p,vec3(12.9898,78.233,37.719)))*43758.5453); }',
'float vn(vec3 p){ vec3 i=floor(p); vec3 f=fract(p); f=f*f*(3.0-2.0*f);',
' return mix(mix(mix(h3(i),h3(i+vec3(1.0,0.0,0.0)),f.x),mix(h3(i+vec3(0.0,1.0,0.0)),h3(i+vec3(1.0,1.0,0.0)),f.x),f.y),',
'            mix(mix(h3(i+vec3(0.0,0.0,1.0)),h3(i+vec3(1.0,0.0,1.0)),f.x),mix(h3(i+vec3(0.0,1.0,1.0)),h3(i+vec3(1.0,1.0,1.0)),f.x),f.y),f.z); }',
'vec3 envc(vec3 r){ float t = smoothstep(-0.15,0.35,r.y); vec3 g = vec3(0.30,0.29,0.27); vec3 s = mix(vec3(0.62,0.72,0.88),vec3(0.38,0.55,0.86),clamp(r.y,0.0,1.0)); return mix(g,s,t); }',
'void main(){',
' float dist = length(vRel); vec3 v = -vRel / max(dist,1e-4);',
' vec3 n = normalize(vN); if (dot(n,v) < 0.0) n = -n;',
' float dm = max(vDent, 0.0);',
' if (dm > 0.01) { vec3 g = vec3(vn(vObj*8.0+vec3(1.0)),vn(vObj*8.0+vec3(5.0)),vn(vObj*8.0+vec3(9.0)))-0.5; n = normalize(n + g*1.3*min(dm*1.5,1.0)); }',
' vec3 sun = normalize(vec3(0.45,0.85,0.35));',
' float nd = max(dot(n,sun),0.0);',
' vec3 amb = mix(vec3(0.40,0.37,0.33), vec3(0.60,0.70,0.86), n.y*0.5+0.5);',
' vec3 base = vC.rgb; float mt = vM;',
' float scr = smoothstep(0.0,0.5,uDam); float streak = smoothstep(0.74,0.78, vn(vObj*vec3(44.0,4.0,11.0)+vec3(2.0))) * max(dm, scr*0.8);',
' if (mt < 7.5 || mt > 8.5) {',
'  base = mix(base, vec3(0.62,0.6,0.57), streak*0.55);',
'  base *= 1.0 - 0.45*dm*(0.5+0.5*vn(vObj*5.0));',
' }',
' base = mix(base, vec3(0.035,0.032,0.03) + base*0.10, uTint);',
' vec3 col = base * (amb*0.92 + nd*vec3(1.0,0.96,0.88)*0.78);',
' vec3 h = normalize(sun+v); float ndh = max(dot(n,h),0.0);',
' float dull = (1.0-uTint)*(1.0-0.6*dm);',
' vec3 r = reflect(-v,n); float fr = pow(1.0-max(dot(n,v),0.0),4.0);',
' float a = vC.a * uAlpha;',
' if (mt > 5.5 && mt < 6.5) { col += pow(ndh,60.0)*0.55*dull + envc(r)*(0.05+0.35*fr)*dull; }',
' else if (mt > 4.5 && mt < 5.5) { col = envc(r)*(0.25+0.65*base)*(1.0-0.5*uTint) + pow(ndh,40.0)*0.6*(1.0-uTint) + base*nd*0.3; }',
' else if (mt > 0.5 && mt < 1.5) {',
'  float cr = max(smoothstep(0.975,0.99,sin(vObj.x*23.0+vObj.z*17.0+3.0*vn(vObj*4.0))), smoothstep(0.975,0.99,sin(vObj.y*29.0-vObj.z*13.0+2.5*vn(vObj*5.0+vec3(2.0)))));',
'  col = base*0.55 + envc(r)*(0.10+0.55*fr) + vec3(1.0,0.97,0.9)*pow(ndh,70.0)*1.2*(1.0-uTint);',
'  float ck = smoothstep(0.35,0.9,uDam); col = mix(col, vec3(0.7,0.74,0.78), cr*ck*0.6); a = clamp(a + cr*ck*0.3 + uTint*0.25, 0.0, 1.0);',
' }',
' else if (mt > 1.5 && mt < 2.5) { col = base*1.1; }',
' else if (mt > 2.5 && mt < 3.5) {',
'  float fk = 0.97 + 0.03*sin(uTime*53.0);',
'  col = col*0.55 + envc(r)*(0.2+0.5*fr)*dull + pow(ndh,60.0)*0.5;',
'  col = mix(col, vec3(1.0,0.97,0.82)*1.7*fk, uHead*(1.0-uTint));',
' }',
' else if (mt > 3.5 && mt < 4.5) {',
'  col = col*0.7 + envc(r)*0.12*dull;',
'  vec3 gl = vec3(1.0,0.10,0.05);',
'  col = mix(col, gl*0.75, uHead*0.55*(1.0-uBrake)*(1.0-uTint));',
'  col = mix(col, gl*1.9, uBrake*(1.0-uTint));',
' }',
' else if (mt > 8.5) { col = base*(1.0-uTint)*(0.55+0.7*uHead) + base*0.25; }',
' else if (mt > 6.5 && mt < 7.5) { col = base; a = vC.a * uAlpha * uHead * (1.0-uTint) * pow(max(dot(n,v),0.0),1.1) * 0.9; }',
' if (!(mt > 6.5 && mt < 7.5) && !(mt > 0.5 && mt < 1.5)) col += pow(ndh,28.0)*0.05*dull;',
' float f = clamp((dist - uFog.x) / max(uFog.y - uFog.x, 1e-3), 0.0, 1.0);',
' col = mix(col, uSky, f);',
' gl_FragColor = vec4(col, a);',
'}'
].join('\n');

var ATT = { pos: 2, nrm: 3, col: 4, mat: 5 };
function compile(gl, type, src) {
  var s = gl.createShader(type); gl.shaderSource(s, src); gl.compileShader(s);
  if (!gl.getShaderParameter(s, gl.COMPILE_STATUS)) throw new Error('CarGL shader: ' + gl.getShaderInfoLog(s));
  return s;
}
function mkProg(gl, vs, fs, binds) {
  var p = gl.createProgram();
  gl.attachShader(p, compile(gl, gl.VERTEX_SHADER, vs)); gl.attachShader(p, compile(gl, gl.FRAGMENT_SHADER, fs));
  for (var k in binds) gl.bindAttribLocation(p, binds[k], k);
  gl.linkProgram(p);
  if (!gl.getProgramParameter(p, gl.LINK_STATUS)) throw new Error('CarGL link: ' + gl.getProgramInfoLog(p));
  return p;
}

function create(gl) {
  var prog = mkProg(gl, VS, FS, { aPos: ATT.pos, aNrm: ATT.nrm, aCol: ATT.col, aMat: ATT.mat });
  var U = {}, cache = {}, thumbState = null, sky = [0.56, 0.75, 0.94];
  ['uVP', 'uCar', 'uPart', 'uCam', 'uFog', 'uSky', 'uTint', 'uAlpha', 'uDent', 'uBox', 'uDam', 'uWC', 'uHead', 'uBrake', 'uTime'].forEach(function (n) { U[n] = gl.getUniformLocation(prog, n); });

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
        u[b + 4] = m.m[i] & 255; u[b + 5] = (m.m[i] >> 8) & 255;
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
    var m = buildModel(id);
    var list = [m.body, m.interior, m.cockpit, m.glass, m.glow, m.steer.mesh].concat(m.wheels.map(function (w) { return w.mesh; }), m.wheels.map(function (w) { return w.core; }));
    m.partNames.forEach(function (n) { list.push(m.parts[n].mesh, m.parts[n].glass, m.parts[n].filler); });
    var pk = pack(list);
    cache[id] = { m: m, buf: pk.buf, ranges: pk.ranges };
    return cache[id];
  }

  var IDM = new Float32Array(mIdent());
  function draw(id, model, vp, cam, fogNear, fogFar, p) {
    p = p || {};
    var G = getModel(id), m = G.m, R = G.ranges, sp = m.spec;
    var steer = +p.steer || 0, spin = +p.wheelSpin || 0, dam = clamp(+p.damage || 0, 0, 1);
    var dF = clamp(+p.dentF || 0, 0, 1), dB = clamp(+p.dentB || 0, 0, 1), dL = clamp(+p.dentL || 0, 0, 1), dR = clamp(+p.dentR || 0, 0, 1);
    var tint = clamp(+p.tint || 0, 0, 1), brake = p.brake ? clamp(+p.brake, 0, 1) : 0, head = p.head ? clamp(+p.head, 0, 1) : 0;
    var cock = !!p.cockpit, lost = (+p.lost) | 0, only = (typeof p.only === 'string' && p.only) ? p.only : null;
    if (only) { dF = dB = dL = dR = 0; dam = 0; lost = 0; }
    if (!(fogFar > fogNear)) { fogNear = 1e8; fogFar = 2e8; }
    var T = (typeof CarGL !== 'undefined' && CarGL.time !== null && CarGL.time !== undefined) ? CarGL.time : (Date.now() / 1000) % 1000;
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
    gl.uniformMatrix4fv(U.uCar, false, new Float32Array(model));
    gl.uniform3f(U.uCam, cam[0], cam[1], cam[2]);
    gl.uniform2f(U.uFog, fogNear, fogFar);
    gl.uniform3f(U.uSky, sky[0], sky[1], sky[2]);
    gl.uniform1f(U.uTint, tint);
    gl.uniform1f(U.uDam, dam);
    gl.uniform4f(U.uDent, dF, dB, dL, dR);
    gl.uniform4f(U.uBox, sp.noseZ, sp.tailZ, sp.halfWidth, sp.height);
    gl.uniform1f(U.uHead, head); gl.uniform1f(U.uBrake, brake); gl.uniform1f(U.uTime, T);
    gl.uniform3f(U.uWC, 0, 0, 0);
    gl.bindBuffer(gl.ARRAY_BUFFER, G.buf);
    gl.enableVertexAttribArray(ATT.pos); gl.enableVertexAttribArray(ATT.nrm);
    gl.enableVertexAttribArray(ATT.col); gl.enableVertexAttribArray(ATT.mat);
    gl.vertexAttribPointer(ATT.pos, 3, gl.FLOAT, false, 32, 0);
    gl.vertexAttribPointer(ATT.nrm, 3, gl.FLOAT, false, 32, 12);
    gl.vertexAttribPointer(ATT.col, 4, gl.UNSIGNED_BYTE, true, 32, 24);
    gl.vertexAttribPointer(ATT.mat, 4, gl.UNSIGNED_BYTE, true, 32, 28);
    gl.enable(gl.DEPTH_TEST); gl.disable(gl.CULL_FACE); gl.disable(gl.BLEND); gl.depthMask(true);
    gl.blendFuncSeparate(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA, gl.ONE, gl.ONE_MINUS_SRC_ALPHA);
    var pf = new Float32Array(16);
    function setPart(mat) { for (var i = 0; i < 16; i++) pf[i] = mat[i]; gl.uniformMatrix4fv(U.uPart, false, pf); }
    function drawRange(k, alpha) { if (!R[k][1]) return; gl.uniform1f(U.uAlpha, alpha); gl.drawArrays(gl.TRIANGLES, R[k][0], R[k][1]); }
    gl.uniformMatrix4fv(U.uPart, false, IDM);
    var PB = { hood: 16, trunk: 32, bumperF: 64, bumperB: 128, doorL: 256, doorR: 512, mirrorL: 1024, mirrorR: 2048 };
    var WN = ['wheelFL', 'wheelFR', 'wheelRL', 'wheelRR'];
    function partIdx(n) { return 14 + 3 * m.partNames.indexOf(n); }
    if (only) {
      var wi = WN.indexOf(only);
      if (wi >= 0) { gl.uniform3f(U.uWC, 0, 0, 0); drawRange(6 + wi, 1); }
      else if (m.parts[only]) {
        var pi0 = partIdx(only); drawRange(pi0, 1);
        gl.enable(gl.BLEND); gl.depthMask(false); drawRange(pi0 + 1, 1);
      }
    } else {
      drawRange(0, 1);       // body
      drawRange(1, 1);       // interior
      if (cock) drawRange(2, 1);
      var sw = m.steer, pv = sw.pivot;
      setPart(mMul(mT(pv[0], pv[1], pv[2]), mMul(rotAxis(sw.axis, -steer * 12), mT(-pv[0], -pv[1], -pv[2]))));
      drawRange(5, 1);
      m.wheels.forEach(function (w, k) {
        var c = w.c, pm = mMul(w.front ? mRY(-steer) : mIdent(), mRX(-spin));
        setPart(mMul(mT(c[0], c[1], c[2]), mMul(pm, mT(-c[0], -c[1], -c[2]))));
        gl.uniform3f(U.uWC, c[0], c[1], c[2]);
        if (!(lost & (1 << k))) drawRange(6 + k, 1);
        drawRange(10 + k, 1);
      });
      gl.uniform3f(U.uWC, 0, 0, 0);
      gl.uniformMatrix4fv(U.uPart, false, IDM);
      m.partNames.forEach(function (n) { var i0 = partIdx(n); if (lost & PB[n]) drawRange(i0 + 2, 1); else drawRange(i0, 1); });
      gl.enable(gl.BLEND); gl.depthMask(false);
      drawRange(3, cock ? 0.55 : 1);
      m.partNames.forEach(function (n) { if (!(lost & PB[n])) drawRange(partIdx(n) + 1, cock ? 0.55 : 1); });
      if (head * (1 - tint) > 0.02) { gl.blendFunc(gl.SRC_ALPHA, gl.ONE); drawRange(4, 1); }
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
    pts.push([0, 0, bb.min[2]], [0, 0, bb.max[2]]);
    var ctr = [0, 0, 0]; pts.forEach(function (p) { ctr = add(ctr, p); }); ctr = mul(ctr, 1 / pts.length);
    var fov = 26 * PI / 180, th = Math.tan(fov / 2), asp = w / h, dir = norm([-0.85, 0.36, -1.0]), dist = 0, eye, vpm;
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
    thumbState.r.draw(id, md, vp, eye, 1e8, 2e8, {});
    var out = document.createElement('canvas'); out.width = w; out.height = h;
    var c2 = out.getContext('2d'); c2.imageSmoothingEnabled = true; c2.imageSmoothingQuality = 'high';
    c2.drawImage(cv2, 0, 0, w, h);
    return out.toDataURL('image/png');
  }

  function dispose() {
    Object.keys(cache).forEach(function (k) { gl.deleteBuffer(cache[k].buf); });
    cache = {};
    gl.deleteProgram(prog);
    if (thumbState) { thumbState.r.dispose(); thumbState = null; }
  }
  return { draw: draw, thumbnail: thumbnail, dispose: dispose };
}

ALL_IDS.forEach(function (id) { buildModel(id); });
var CarGL = {
  IDS: ALL_IDS, SPECS: SPECS, create: create, time: null,
  _buildForTest: function (id) {
    var m = buildModel(id), tris = 0;
    m.all.forEach(function (M) { tris += M.tcount(); });
    return { body: m.body, glass: m.glass, interior: m.interior, cockpit: m.cockpit, glow: m.glow, steer: m.steer, wheels: m.wheels, triangles: tris, all: m.all, parts: m.parts, partNames: m.partNames };
  }
};
if (typeof window !== 'undefined') window.CarGL = CarGL;
if (typeof module !== 'undefined' && module.exports) module.exports = CarGL;
if (typeof globalThis !== 'undefined' && typeof window === 'undefined') globalThis.CarGL = CarGL;
})(typeof window !== 'undefined' ? window : this);

/* Real car models (CC BY, from Sketchfab, converted offline to a small binary): loading and drawing.
   CarReal.create(gl) -> { draw, thumbnail, dispose, has(id), want(id) }.  Same draw options as the hand-built models.  */
(function (root) {
  'use strict';
  var BASE = (typeof document !== 'undefined' && document.currentScript && document.currentScript.src || '').replace(/car_gl\.js.*$/, 'car_snd/');
  var cache = {};          // id -> { meta, data (ArrayBuffer), state }
  var listeners = [];
  var VS = 'attribute vec3 aPos;attribute vec3 aNrm;attribute float aPart;attribute vec4 aCol;' +
    'uniform mat4 uVP;uniform mat4 uModel;uniform vec3 uMin;uniform vec3 uExt;uniform vec3 uWC0;uniform vec3 uWC1;uniform vec3 uWC2;uniform vec3 uWC3;uniform float uSteer;uniform float uSpin;uniform float uLost;uniform float uOnly;uniform vec4 uDent;uniform vec2 uHalf;' +
    'varying vec3 vN;varying vec3 vW;varying vec4 vC;varying float vDent;varying float vCls;' +
    'float hash(vec3 p){return fract(sin(dot(p,vec3(12.9898,78.233,37.719)))*43758.5453);}' +
    'void main(){' +
    ' vec3 p=uMin+aPos/65535.0*uExt; vec3 n=aNrm; float part=aPart; float dent=0.0;' +
    ' float bit=floor(uLost/pow(2.0,part-1.0));float gone=(part>0.5&&mod(bit,2.0)>0.5)?1.0:0.0;' +
    ' if(uOnly>0.5&&abs(part-uOnly)>0.5)gone=1.0;' +
    ' if(uOnly<-0.5&&false)gone=0.0;' +
    ' if(part>0.5&&part<4.5){vec3 c=part<1.5?uWC0:part<2.5?uWC1:part<3.5?uWC2:uWC3;vec3 l=p-c;vec3 ln=n;' +
    '  if(part<2.5&&uOnly<0.5){float cs=cos(uSteer),sn=sin(uSteer);l=vec3(cs*l.x+sn*l.z,l.y,-sn*l.x+cs*l.z);ln=vec3(cs*ln.x+sn*ln.z,ln.y,-sn*ln.x+cs*ln.z);}' +
    '  float cr=cos(uSpin),sr=sin(uSpin);l=vec3(l.x,cr*l.y-sr*l.z,sr*l.y+cr*l.z);ln=vec3(ln.x,cr*ln.y-sr*ln.z,sr*ln.y+cr*ln.z);' +
    '  p=c+l;n=ln;' +
    ' } else if(uOnly<0.5){' +
    '  float hl=uHalf.x,hw=uHalf.y;float nz=hash(floor(p*4.0));' +
    '  float ff=smoothstep(hl*0.30,hl*1.0,-p.z)*uDent.x;float fb=smoothstep(hl*0.30,hl*1.0,p.z)*uDent.y;' +
    '  float fl=smoothstep(hw*0.55,hw,-p.x)*uDent.z;float fr=smoothstep(hw*0.55,hw,p.x)*uDent.w;' +
    '  p.z+=ff*(1.1+0.6*nz);p.y-=ff*(0.18+0.12*nz)*smoothstep(0.4,1.0,p.y/1.4);p.x*=1.0-ff*0.10*nz;' +
    '  p.z-=fb*(1.0+0.6*nz);p.y-=fb*(0.14+0.1*nz);p.x*=1.0-fb*0.08*nz;' +
    '  p.x+=fl*(0.42+0.25*nz);p.y-=fl*0.10*nz;p.x-=fr*(0.42+0.25*nz);p.y-=fr*0.10*nz;' +
    '  dent=max(max(ff,fb),max(fl,fr));' +
    ' }' +
    ' vec4 w=uModel*vec4(p,1.0);vW=w.xyz;vN=normalize(mat3(uModel)*n);vC=aCol;vDent=dent;vCls=aCol.w;' +
    ' gl_Position=gone>0.5?vec4(2.0,2.0,2.0,1.0):uVP*w;}';
  var FS = 'precision mediump float;varying vec3 vN;varying vec3 vW;varying vec4 vC;varying float vDent;varying float vCls;' +
    'uniform vec3 uCam;uniform vec2 uFog;uniform vec3 uSky;uniform float uHead;uniform float uBrake;uniform float uTint;uniform float uCockpit;uniform float uGlassPass;uniform float uDamage;' +
    'void main(){' +
    ' float cls=floor(vCls+0.5);bool glass=cls>0.5&&cls<1.5;' +
    ' if(glass!=(uGlassPass>0.5))discard;' +
    ' vec3 N=normalize(vN);float back=gl_FrontFacing?1.0:0.0;if(back<0.5)N=-N;' +
    ' vec3 col=vC.rgb/255.0;' +
    ' vec3 sun=normalize(vec3(0.5,0.85,0.35));float d=max(dot(N,sun),0.0);float hemi=0.5+0.5*N.y;' +
    ' vec3 V=normalize(uCam-vW);vec3 H=normalize(sun+V);' +
    ' float lit=0.45+0.5*hemi*0.6+0.62*d;vec3 c=col*lit;' +
    ' float spec=0.0;' +
    ' if(cls>4.5&&cls<5.5)spec=pow(max(dot(N,H),0.0),24.0)*0.7;else if(cls<0.5)spec=pow(max(dot(N,H),0.0),48.0)*0.28;else if(glass)spec=pow(max(dot(N,H),0.0),64.0)*0.9;' +
    ' c+=vec3(spec);' +
    ' if(cls>1.5&&cls<2.5&&uHead>0.5)c=mix(c,vec3(1.0,0.97,0.82),0.85);' +
    ' if(cls>2.5&&cls<3.5)c=mix(c*0.8,vec3(0.45,0.02,0.02),0.55)+vec3(uBrake*0.7,uBrake*0.05,uBrake*0.05);' +
    ' if(cls>5.5&&cls<6.5)c*=0.8;' +
    ' if(back<0.5)c=mix(c,vec3(0.07,0.07,0.08),0.7);' +
    ' c=mix(c,vec3(0.08,0.07,0.07),clamp(vDent*0.55,0.0,0.7));' +
    ' c=mix(c,vec3(0.1,0.09,0.08)*(0.5+0.5*lit),uTint);' +
    ' float dist=distance(uCam,vW);float f=smoothstep(uFog.x,uFog.y,dist);c=mix(c,uSky,f);' +
    ' float a=1.0;if(glass){a=0.30+0.25*(1.0-abs(dot(N,V)));c=mix(c*0.35,c,0.4);}' +
    ' gl_FragColor=vec4(c,a*(1.0-f*0.0));}';

  function parse(buf) {
    var dv = new DataView(buf); if (dv.getUint32(0, true) !== 0x4D444C31) throw new Error('bad model');
    var jl = dv.getUint32(4, true), meta = JSON.parse(typeof TextDecoder !== 'undefined' ? new TextDecoder().decode(new Uint8Array(buf, 8, jl)) : String.fromCharCode.apply(null, new Uint8Array(buf, 8, jl)));
    var off = 8 + jl; off += (4 - off % 4) % 4;
    return { meta: meta, vbytes: new Uint8Array(buf, off, meta.verts * 16), idx: new Uint16Array(buf.slice(off + meta.verts * 16, off + meta.verts * 16 + meta.tris * 6)) };
  }
  function want(id) {
    var e = cache[id]; if (e) return e;
    e = cache[id] = { state: 'loading', meta: null };
    if (typeof fetch !== 'function') { e.state = 'failed'; return e; }
    fetch(BASE + id + '.bin').then(function (r) { if (!r.ok) throw 0; return r.arrayBuffer(); }).then(function (ab) {
      var m = parse(ab); e.meta = m.meta; e.parsed = m; e.state = 'ready'; listeners.forEach(function (f) { try { f(id); } catch (x) {} });
    }).catch(function () { e.state = 'failed'; });
    return e;
  }
  function create(gl) {
    var prog = null, U = {}, A = {}, gpu = {};
    function sh(t, s) { var o = gl.createShader(t); gl.shaderSource(o, s); gl.compileShader(o); return o; }
    function init() {
      if (prog) return;
      prog = gl.createProgram(); gl.attachShader(prog, sh(gl.VERTEX_SHADER, VS)); gl.attachShader(prog, sh(gl.FRAGMENT_SHADER, FS));
      gl.bindAttribLocation(prog, 2, 'aPos'); gl.bindAttribLocation(prog, 3, 'aNrm'); gl.bindAttribLocation(prog, 4, 'aPart'); gl.bindAttribLocation(prog, 5, 'aCol'); gl.linkProgram(prog);
      ['uVP', 'uModel', 'uMin', 'uExt', 'uWC0', 'uWC1', 'uWC2', 'uWC3', 'uSteer', 'uSpin', 'uLost', 'uOnly', 'uDent', 'uHalf', 'uCam', 'uFog', 'uSky', 'uHead', 'uBrake', 'uTint', 'uCockpit', 'uGlassPass', 'uDamage'].forEach(function (n) { U[n] = gl.getUniformLocation(prog, n); });
    }
    function upload(id) {
      var e = cache[id]; if (!e || e.state !== 'ready') return null;
      var g = gpu[id]; if (g) return g;
      var vb = gl.createBuffer(), ib = gl.createBuffer(), m = e.parsed;
      gl.bindBuffer(gl.ARRAY_BUFFER, vb); gl.bufferData(gl.ARRAY_BUFFER, m.vbytes, gl.STATIC_DRAW);
      gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, ib); gl.bufferData(gl.ELEMENT_ARRAY_BUFFER, m.idx, gl.STATIC_DRAW);
      var firstGlass = m.meta.tris * 3, ranges = m.meta.ranges;
      for (var i = 0; i < ranges.length; i++) if (ranges[i].cls === 1) { firstGlass = ranges[i].first; break; }
      return (gpu[id] = { vb: vb, ib: ib, firstGlass: firstGlass, total: m.meta.tris * 3 });
    }
    function bindAttrs(g) {
      gl.bindBuffer(gl.ARRAY_BUFFER, g.vb); gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, g.ib);
      gl.enableVertexAttribArray(2); gl.vertexAttribPointer(2, 3, gl.UNSIGNED_SHORT, false, 16, 0);
      gl.enableVertexAttribArray(3); gl.vertexAttribPointer(3, 3, gl.BYTE, true, 16, 6);
      gl.enableVertexAttribArray(4); gl.vertexAttribPointer(4, 1, gl.UNSIGNED_BYTE, false, 16, 9);
      gl.enableVertexAttribArray(5); gl.vertexAttribPointer(5, 4, gl.UNSIGNED_BYTE, false, 16, 10);
    }
    function draw(id, model, vp, cam, fogN, fogF, p) {
      p = p || {}; var e = cache[id]; if (!e || e.state !== 'ready') { want(id); return false; }
      init(); var g = upload(id); if (!g) return false; var meta = e.meta;
      var st = { prog: gl.getParameter(gl.CURRENT_PROGRAM), ab: gl.getParameter(gl.ARRAY_BUFFER_BINDING), eb: gl.getParameter(gl.ELEMENT_ARRAY_BUFFER_BINDING), blend: gl.isEnabled(gl.BLEND), depth: gl.isEnabled(gl.DEPTH_TEST), cull: gl.isEnabled(gl.CULL_FACE), dm: gl.getParameter(gl.DEPTH_WRITEMASK),
        en: [0, 1, 2, 3, 4, 5].map(function (i) { return gl.getVertexAttrib(i, gl.VERTEX_ATTRIB_ARRAY_ENABLED); }) };
      gl.useProgram(prog); bindAttrs(g);
      var ext = [meta.max[0] - meta.min[0] + 1e-4, meta.max[1] - meta.min[1] + 1e-4, meta.max[2] - meta.min[2] + 1e-4], w = meta.wheels;
      gl.uniformMatrix4fv(U.uVP, false, vp); gl.uniformMatrix4fv(U.uModel, false, model);
      gl.uniform3f(U.uMin, meta.min[0], meta.min[1], meta.min[2]); gl.uniform3f(U.uExt, ext[0], ext[1], ext[2]);
      gl.uniform3fv(U.uWC0, w.FL); gl.uniform3fv(U.uWC1, w.FR); gl.uniform3fv(U.uWC2, w.RL); gl.uniform3fv(U.uWC3, w.RR);
      gl.uniform1f(U.uSteer, p.steer || 0); gl.uniform1f(U.uSpin, p.wheelSpin || 0); gl.uniform1f(U.uLost, p.lost || 0);
      var only = 0; if (p.only) only = ['', 'wheelFL', 'wheelFR', 'wheelRL', 'wheelRR', 'hood', 'trunk', 'bumperF', 'bumperB', 'doorL', 'doorR', 'mirrorL', 'mirrorR'].indexOf(p.only); if (only < 0) only = 0;
      gl.uniform1f(U.uOnly, only);
      gl.uniform4f(U.uDent, p.dentF || 0, p.dentB || 0, p.dentL || 0, p.dentR || 0); gl.uniform2f(U.uHalf, (meta.tailZ - meta.noseZ) / 2, meta.width / 2);
      gl.uniform3f(U.uCam, cam[0], cam[1], cam[2]); gl.uniform2f(U.uFog, fogN, fogF); gl.uniform3f(U.uSky, 0.56, 0.75, 0.94);
      gl.uniform1f(U.uHead, p.head ? 1 : 0); gl.uniform1f(U.uBrake, p.brake ? 1 : 0); gl.uniform1f(U.uTint, p.tint || 0); gl.uniform1f(U.uCockpit, p.cockpit ? 1 : 0); gl.uniform1f(U.uDamage, p.damage || 0);
      gl.enable(gl.DEPTH_TEST); gl.depthMask(true); gl.disable(gl.BLEND);
      if (p.cockpit) gl.disable(gl.CULL_FACE); else { gl.enable(gl.CULL_FACE); gl.cullFace(gl.BACK); }
      gl.uniform1f(U.uGlassPass, 0); gl.drawElements(gl.TRIANGLES, g.firstGlass, gl.UNSIGNED_SHORT, 0);
      if (g.total > g.firstGlass) {
        gl.enable(gl.BLEND); gl.blendFuncSeparate(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA, gl.ONE, gl.ONE_MINUS_SRC_ALPHA); gl.depthMask(false); gl.disable(gl.CULL_FACE);
        gl.uniform1f(U.uGlassPass, 1); gl.drawElements(gl.TRIANGLES, g.total - g.firstGlass, gl.UNSIGNED_SHORT, g.firstGlass * 2);
      }
      gl.depthMask(st.dm); if (!st.blend) gl.disable(gl.BLEND); else gl.enable(gl.BLEND); if (st.cull) gl.enable(gl.CULL_FACE); else gl.disable(gl.CULL_FACE); if (!st.depth) gl.disable(gl.DEPTH_TEST);
      for (var i = 0; i < 6; i++) { if (st.en[i]) gl.enableVertexAttribArray(i); else gl.disableVertexAttribArray(i); }
      gl.bindBuffer(gl.ARRAY_BUFFER, st.ab); gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, st.eb); gl.useProgram(st.prog);
      return true;
    }
    function dispose() { Object.keys(gpu).forEach(function (k) { gl.deleteBuffer(gpu[k].vb); gl.deleteBuffer(gpu[k].ib); }); gpu = {}; if (prog) gl.deleteProgram(prog); prog = null; }
    return { draw: draw, dispose: dispose, has: function (id) { return !!cache[id] && cache[id].state === 'ready'; }, want: want, meta: function (id) { return cache[id] && cache[id].meta; } };
  }
  root.CarReal = { setBase: function (b) { BASE = b; }, create: create, want: want, meta: function (id) { return cache[id] && cache[id].meta; }, onReady: function (f) { listeners.push(f); }, BASE: BASE };
})(typeof window !== 'undefined' ? window : this);
