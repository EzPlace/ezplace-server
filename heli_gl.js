/* HeliGL - procedural helicopter models + tiny WebGL1 renderer for EzWorld.
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
  cyl(M, a, b, r, r, 14, tyre, 0, true);
  cyl(M, add(a, [-0.002, 0, 0]), add(b, [0.002, 0, 0]), r * 0.55, r * 0.55, 12, hub, 0, true);
}

/* ---------------- rotors ---------------- */
// blades: built in local XZ plane around origin, about +Y axis
function makeBlades(o) {
  var M = new Mesh(), k, R = o.R, n = o.n;
  var tip1 = R - (o.tipLen || 0), tip2 = R - (o.tipLen2 || 0) - (o.tipLen || 0);
  var secs = [];
  var r0 = o.root, ch = o.chord / 2, th = (o.thick || o.chord * 0.09) / 2;
  var zs = [r0, tip2 - 0.001, tip2, tip1 - 0.001, tip1, R - 0.0005, R];
  zs = zs.filter(function (z, i) { return i === 0 || z > zs[i - 1] - 0.0004; });
  zs.forEach(function (z) { var f = (z - r0) / (R - r0); secs.push({ z: z, y: 0, t: th * (1 - 0.3 * f), b: th * (1 - 0.3 * f), w: ch * (z > R - 0.01 ? 0.85 : 1), n: 2.6, x: 0 }); });
  function colf(q) {
    if (o.tipLen && q.z > tip1 - 0.0001) return o.tipCol;
    if (o.tipLen2 && q.z > tip2 - 0.0001) return o.tipCol2;
    return o.col;
  }
  var one = new Mesh();
  loft(one, secs, { K: 8, col: colf, caps: true, capCol: o.col });
  var cone = (o.cone === undefined ? 2.5 : o.cone) * PI / 180, pitch = (o.pitch === undefined ? 4 : o.pitch) * PI / 180;
  for (k = 0; k < n; k++) {
    var m = mMul(mRY(k * TAU / n), mMul(mRZ(cone), mMul(mRX(pitch), mRY(PI / 2))));
    M.append(one, m);
  }
  return M;
}
// translucent disc + streaks (local XZ plane)
function makeDisc(o) {
  var M = new Mesh(), R = o.R, n = o.n, seg = 48, i, rin = Math.max(o.root, 0.1 * R);
  var dc = o.discCol || [215, 220, 225, 255];
  var aDisc = 44, aTip = 120, aStreak = 70;
  var cone = (o.cone === undefined ? 2.5 : o.cone) * PI / 180;
  function pt(r, a, y) { return [r * Math.cos(a), y === undefined ? r * Math.sin(cone) : y, -r * Math.sin(a)]; }
  var up = [0, 1, 0];
  for (i = 0; i < seg; i++) {
    var a0 = i * TAU / seg, a1 = (i + 1) * TAU / seg;
    var c = withA(dc, aDisc), tc = withA(o.tipCol || dc, aTip);
    var A = pt(rin, a0), B = pt(R * 0.96, a0), C = pt(R * 0.96, a1), D = pt(rin, a1);
    M.tri(A, B, C, c, MAT_TWO, up); M.tri(A, C, D, c, MAT_TWO, up);
    var E = pt(R, a0), F = pt(R, a1);
    var B2 = pt(R * 0.955, a0), C2 = pt(R * 0.955, a1);
    M.tri(B2, E, F, tc, MAT_TWO, up); M.tri(B2, F, C2, tc, MAT_TWO, up);
  }
  var sc = withA(o.col || [30, 30, 30, 255], aStreak);
  for (i = 0; i < n; i++) {
    var az = i * TAU / n;
    for (var k = 0; k < 2; k++) {
      var w0 = 0.0, w1 = 0.30 + 0.08 * k;
      var A1 = pt(rin * 1.2, az), B1 = pt(R * 0.97, az + w0 * 0.0), C1 = pt(R * 0.97, az - 0.16 - 0.0 * k), D1 = pt(rin * 1.2, az - 0.02);
      if (k === 1) { A1 = pt(R * 0.55, az); B1 = pt(R * 0.97, az); C1 = pt(R * 0.97, az - 0.30); D1 = pt(R * 0.55, az - 0.05); sc = withA(o.col || [30, 30, 30, 255], aStreak * 0.7); }
      M.tri(A1, B1, C1, sc, MAT_TWO, up); M.tri(A1, C1, D1, sc, MAT_TWO, up);
    }
  }
  return M;
}

var MODELS = {};   // id -> builder returning model parts
var SPECS = {};



/* ---------------- shared model helpers ---------------- */
function newModel() { return { body: new Mesh(), gear: new Mesh(), glass: new Mesh(), rotors: [], tail: null }; }
function elev(a) { // signed elevation angle (deg) of a ring angle, -90..90
  if (a <= 90) return a; if (a <= 180) return 180 - a; if (a <= 270) return -(a - 180); return -(360 - a);
}
function ringQ(q) { // symmetric ring from first-quadrant list (degrees, ascending, excl 0/90)
  var s = {}, out = [];
  [0, 90].concat(q).forEach(function (a) { [a, 180 - a, 180 + a, 360 - a].forEach(function (b) { b = ((b % 360) + 360) % 360; s[b.toFixed(4)] = b; }); });
  Object.keys(s).forEach(function (k) { out.push(s[k]); });
  out.sort(function (a, b) { return a - b; });
  return degAngles(out);
}
function mirror2(fn) { fn(1); fn(-1); }
function shrinkSecs(secs, d, z0, z1) {
  var o = [];
  secs.forEach(function (s) {
    if (s.z < z0 - 1e-6 || s.z > z1 + 1e-6) return;
    o.push({ z: s.z, y: s.y, t: Math.max(s.t - d, 0.01), b: Math.max(s.b - d, 0.01), w: Math.max(s.w - d, 0.01), n: s.n, x: s.x });
  });
  return o;
}
// dark interior shell seen through the glass
function interior(M, secs, d, z0, z1, angles, isGlass, col) {
  var sh = shrinkSecs(secs, d, z0, z1);
  if (sh.length < 2) return;
  col = col || [66, 68, 72, 255];
  loft(M, sh, { angles: angles, mat: MAT_TWO, caps: 'rear', capCol: col, col: function (q) { return isGlass && isGlass(q) ? null : col; } });
}
function seat(M, x, y, z, w, col, back) { // seat cushion + backrest, facing -z
  box(M, x, y, z, w / 2, 0.05, 0.22, col);
  box(M, x, y + (back || 0.28), z + 0.2, w / 2, (back || 0.28), 0.04, col);
}
function mastHub(M, y0, y1, r, col) { cyl(M, [0, y0, 0], [0, y1, 0], r, r * 0.9, 10, col, 0, true); }
function antenna(M, a, b, r, col) { cyl(M, a, b, r, r * 0.6, 5, col, 0, true); }
// instrument panel dots
function gauges(M, x0, x1, y, z, nx, ny) {
  for (var i = 0; i < nx; i++) for (var j = 0; j < ny; j++) {
    var x = lerp(x0, x1, nx > 1 ? i / (nx - 1) : 0.5), yy = y + j * 0.05;
    box(M, x, yy, z, 0.018, 0.018, 0.004, ((i + j) % 3 === 0) ? [200, 220, 200, 255] : [150, 160, 150, 255], MAT_EMIT);
  }
}


/* ================= Robinson R22 ================= */
SPECS.r22 = {
  name: 'Robinson R22', maker: 'Robinson Helicopter Company',
  realLengthM: 6.3, realWidthM: 1.1, realHeightM: 2.7, mainRotorDiameterM: 7.67, tailRotorDiameterM: 1.07, seats: 2,
  rotors: [{ pivot: [0, 2.62, 0], radius: 3.835, blades: 2, dir: 1 }],
  tail: { pivot: [-0.13, 1.14, 4.1], radius: 0.535, blades: 2, axis: 'x', ratio: 5.2 },
  box: [1.0, 0.85, 3.15],
  skids: [[-0.95, 0, -0.65], [0.95, 0, -0.65], [-0.95, 0, 0.6], [0.95, 0, 0.6]],
  eye: [0.28, 1.28, -0.8],
  tipHeight: 2.66, chaseDist: 14, chaseUp: 4.5
};
MODELS.r22 = function () {
  var m = newModel(), B = m.body, G = m.glass, Gr = m.gear;
  var white = hex(0xf1f1ec), red = hex(0xb5202a), blue = hex(0x1c3b86), dark = hex(0x2a2c30), grey = hex(0x8a8d92), black = hex(0x18191b);
  var secs = [
    S(-2.10, 0.93, 0.83, 0.04, 2.2), S(-2.02, 1.17, 0.62, 0.26, 2.2), S(-1.80, 1.48, 0.53, 0.44, 2.3),
    S(-1.40, 1.58, 0.50, 0.54, 2.5), S(-0.80, 1.63, 0.50, 0.57, 2.6), S(-0.10, 1.61, 0.50, 0.56, 2.6),
    S(0.40, 1.58, 0.50, 0.52, 2.6), S(0.90, 1.55, 0.56, 0.45, 2.4), S(1.40, 1.42, 0.64, 0.35, 2.3),
    S(1.90, 1.32, 0.86, 0.17, 2.1), S(2.60, 1.24, 0.98, 0.12, 2), S(3.60, 1.20, 1.05, 0.075, 2), S(4.15, 1.20, 1.09, 0.055, 2)
  ];
  var K = ringQ([8, 20, 33, 46, 58, 70, 80]);
  function isGlass(q) {
    var e = elev(q.a);
    return q.z > -2.09 && q.z < -0.09 && e > 6 && (e < 60 || q.z < -1.3);
  }
  loft(B, secs, { angles: K, G: G, glassCol: [24, 36, 48, 125], col: function (q) {
    if (isGlass(q)) return GLASS;
    return white;
  } });
  interior(B, secs, 0.04, -2.02, 0.4, K, isGlass);
  // engine / transmission cowling
  var cow = [S(-0.25, 1.60, 1.46, 0.26, 2.4), S(0.0, 1.76, 1.40, 0.34, 2.4), S(0.8, 1.76, 1.30, 0.35, 2.4), S(1.35, 1.58, 1.22, 0.25, 2.3)];
  loft(B, cow, { K: 20, color: white, caps: true });
  box(B, 0, 1.765, 0.9, 0.14, 0.008, 0.22, [150,152,156,255]);       // cooling grille
  // livery stripes (decals)
  mirror2(function (s) {
    rectPatch(B, secs, s, -1.95, 1.7, 0.66, 0.76, 14, 1, red, 0.012);
    rectPatch(B, secs, s, -1.9, 1.5, 0.56, 0.64, 14, 1, blue, 0.012);
    patch(B, secs, s, [[1.7, 0.74], [4.1, 1.12], [4.1, 1.19], [1.7, 0.82]], 6, 1, red, 0.012);
    patch(B, secs, s, [[1.5, 0.62], [4.1, 1.03], [4.1, 1.09], [1.5, 0.70]], 6, 1, blue, 0.012);
    panelLines(B, secs, s, -1.15, -0.09, 0.58, 1.50, grey, 0.02, 0.008);
    box(B, s * 0.575, 1.05, -0.2, 0.012, 0.02, 0.07, dark);   // door handle
  });
  // cockpit interior
  var seatCol = [48, 50, 56, 255];
  [-0.28, 0.28].forEach(function (x) { seat(B, x, 0.74, -0.62, 0.42, seatCol, 0.32); });
  box(B, 0, 0.56, -0.9, 0.44, 0.012, 0.55, dark);
  box(B, 0, 1.12, -1.6, 0.5, 0.11, 0.025, dark);       // instrument panel
  gauges(B, -0.36, 0.36, 1.10, -1.575, 7, 2);
  cyl(B, [0, 0.55, -0.9], [0, 1.28, -0.95], 0.015, 0.015, 5, dark, 0, true); // cyclic
  box(B, 0, 1.31, -0.95, 0.22, 0.012, 0.012, black);
  // mast, hub area
  mastHub(B, 1.75, 2.6, 0.04, grey);
  cyl(B, [0, 1.8, 0], [0, 1.95, 0], 0.07, 0.05, 10, dark, 0, true);
  // tail boom details, fins
  taper(B, 3.62, { x: 0, y: 1.40, hw: 0.012, hh: 0.20 }, 4.17, { x: 0, y: 1.34, hw: 0.012, hh: 0.16 }, white);    // upper fin
  taper(B, 3.55, { x: 0, y: 0.80, hw: 0.014, hh: 0.26 }, 4.18, { x: 0, y: 0.90, hw: 0.014, hh: 0.22 }, red);    // lower fin
  box(B, 0, 0.58, 4.15, 0.012, 0.045, 0.08, black);
  box(B, 0, 1.12, 3.78, 0.50, 0.012, 0.17, white);   // horizontal stabiliser
  [-1, 1].forEach(function (s) { box(B, s * 0.51, 1.19, 3.78, 0.012, 0.08, 0.13, white); });
  light(B, -0.51, 1.29, 3.78, RED, 0.03); light(B, 0.51, 1.29, 3.78, GREEN, 0.03); light(B, 0, 1.45, 4.19, WHITE);
  light(B, 0, 1.79, 0.9, RED, 0.03);
  antenna(B, [0.05, 1.22, 3.3], [0.04, 1.62, 3.38], 0.008, black);
  antenna(B, [0, 1.84, 1.2], [0.1, 2.0, 1.45], 0.007, black);
  // exhaust
  cyl(B, [0.2, 1.4, 1.45], [0.22, 1.28, 1.7], 0.07, 0.06, 8, dark, 0, true);
  // skids
  var sk = dark;
  [-1, 1].forEach(function (s) {
    path(Gr, [[s * 0.95, 0.16, 1.40], [s * 0.95, 0.05, 1.22], [s * 0.95, 0.045, -1.1], [s * 0.95, 0.1, -1.28], [s * 0.95, 0.24, -1.38]], 0.04, 8, sk);
    [-0.65, 0.6].forEach(function (z) {
      path(Gr, [[s * 0.95, 0.05, z], [s * 0.82, 0.34, z], [s * 0.42, 0.54, z]], 0.035, 8, sk);
    });
  });
  [-0.65, 0.6].forEach(function (z) { path(Gr, [[-0.42, 0.54, z], [0.42, 0.54, z]], 0.035, 8, sk, false); });
  // rotor
  var rg = { R: 3.835, n: 2, chord: 0.18, thick: 0.02, root: 0.45, col: hex(0x25272b), tipCol: hex(0xd9d9d2), tipLen: 0.35, tipCol2: red, tipLen2: 0.08, cone: 1.8, pitch: 4, discCol: [215, 220, 225, 255] };
  var rot = rotorMeshes(rg, function (H) {
    box(H, 0, -0.02, 0, 0.32, 0.04, 0.05, black);
    cyl(H, [0, -0.1, 0], [0, 0.07, 0], 0.07, 0.05, 10, grey, 0, true);
    [-1, 1].forEach(function (s) { cyl(H, [s * 0.1, 0.0, 0], [s * 0.45, 0.0, 0], 0.04, 0.035, 8, black, 0, true); });
  });
  rot.pivot = [0, 2.62, 0]; rot.dir = 1; rot.phase = 0;
  m.rotors.push(rot);
  // tail rotor
  var tg = { R: 0.535, n: 2, chord: 0.09, thick: 0.012, root: 0.06, col: hex(0x25272b), tipCol: hex(0xd9d9d2), tipLen: 0.1, cone: 0, pitch: 8, discCol: [215, 220, 225, 255] };
  var tr = rotorMeshes(tg, function (H) { cyl(H, [0, -0.04, 0], [0, 0.06, 0], 0.04, 0.04, 8, dark, 0, true); });
  tr.pivot = SPECS.r22.tail.pivot; tr.ratio = SPECS.r22.tail.ratio; tr.cant = 0;
  m.tail = tr;
  cyl(B, [-0.02, 1.14, 4.1], [-0.1, 1.14, 4.1], 0.045, 0.04, 8, dark, 0, true);
  return m;
};


/* ================= Bell 206B JetRanger ================= */
SPECS.b206 = {
  name: 'Bell 206B JetRanger', maker: 'Bell Helicopter',
  realLengthM: 9.5, realWidthM: 1.3, realHeightM: 2.9, mainRotorDiameterM: 10.16, tailRotorDiameterM: 1.65, seats: 5,
  rotors: [{ pivot: [0, 2.8, 0], radius: 5.08, blades: 2, dir: 1 }],
  tail: { pivot: [-0.17, 1.64, 7.0], radius: 0.825, blades: 2, axis: 'x', ratio: 5.2 },
  box: [0.85, 1.0, 3.8],
  skids: [[-1.0, 0, -0.5], [1.0, 0, -0.5], [-1.0, 0, 0.9], [1.0, 0, 0.9]],
  eye: [0.33, 1.55, -1.1],
  tipHeight: 2.86, chaseDist: 17, chaseUp: 5
};
MODELS.b206 = function () {
  var m = newModel(), B = m.body, G = m.glass, Gr = m.gear;
  var navy = hex(0x1b3166), white = hex(0xf2f2ee), gold = hex(0xe6b52e), dark = hex(0x24262a), grey = hex(0x8a8d92), black = hex(0x17181a), lgrey = hex(0xc9ccd0);
  var secs = [
    S(-2.40, 0.95, 0.82, 0.04, 2.2), S(-2.30, 1.10, 0.66, 0.26, 2.2), S(-2.00, 1.34, 0.56, 0.46, 2.3), S(-1.50, 1.66, 0.50, 0.58, 2.4),
    S(-1.10, 1.86, 0.48, 0.63, 2.5), S(-0.45, 1.98, 0.48, 0.66, 2.6), S(-0.35, 1.98, 0.48, 0.66, 2.6), S(0.40, 2.00, 0.48, 0.66, 2.6),
    S(0.50, 2.00, 0.48, 0.66, 2.6), S(1.20, 2.00, 0.50, 0.64, 2.6), S(1.40, 2.00, 0.51, 0.63, 2.6), S(2.00, 2.00, 0.58, 0.54, 2.4),
    S(2.60, 1.92, 0.72, 0.43, 2.3), S(3.20, 1.66, 1.00, 0.27, 2.1), S(4.50, 1.63, 1.13, 0.19, 2), S(6.00, 1.62, 1.27, 0.12, 2),
    S(7.00, 1.60, 1.38, 0.075, 2), S(7.15, 1.60, 1.40, 0.06, 2)
  ];
  var K = ringQ([8, 20, 33, 46, 58, 70, 80]);
  function isGlass(q) {
    var e = elev(q.a), z = q.z;
    if (e < 9) return false;
    if (z < -1.1) return z > -2.38;
    if (z > -0.45 && z < -0.35) return false;
    if (z > -1.1 && z < -0.45) return e < 64;
    if (z > -0.35 && z < 0.40) return e < 62;
    if (z > 0.5 && z < 1.2) return e < 58;
    return false;
  }
  loft(B, secs, { angles: K, G: G, glassCol: [22, 34, 46, 125], col: function (q) {
    if (isGlass(q)) return GLASS;
    if (q.z < 2.8) return q.y < 0.98 ? white : navy;
    return elev(q.a) < -28 ? white : navy;
  } });
  interior(B, secs, 0.04, -2.3, 1.4, K, isGlass);
  // transmission fairing / engine deck
  var cow = [S(-0.60, 2.0, 1.90, 0.30, 2.4), S(-0.2, 2.24, 1.85, 0.48, 2.4), S(0.6, 2.34, 1.80, 0.52, 2.4), S(1.4, 2.28, 1.80, 0.50, 2.4), S(2.1, 2.10, 1.80, 0.40, 2.3), S(2.75, 1.95, 1.78, 0.25, 2.2)];
  loft(B, cow, { K: 20, color: navy, caps: true });
  box(B, 0, 2.345, 1.35, 0.22, 0.012, 0.28, dark);                   // intake grille
  cyl(B, [0.02, 2.14, 2.1], [0.02, 2.2, 2.85], 0.15, 0.1, 10, dark, 0, true);   // exhaust
  // stripes
  mirror2(function (s) {
    rectPatch(B, secs, s, -2.2, 1.4, 0.97, 1.05, 16, 1, gold, 0.012);
    patch(B, secs, s, [[1.4, 0.97], [3.2, 1.0], [3.2, 1.06], [1.4, 1.05]], 4, 1, gold, 0.012);
    patch(B, secs, s, [[3.2, 1.30], [7.0, 1.455], [7.0, 1.50], [3.2, 1.36]], 8, 1, gold, 0.012);
    panelLines(B, secs, s, -1.08, -0.47, 0.60, 1.82, lgrey, 0.02, 0.008);
    panelLines(B, secs, s, -0.33, 1.35, 0.60, 1.85, lgrey, 0.02, 0.008);
    rectPatch(B, secs, s, 0.52, 1.18, 0.62, 0.98, 4, 2, navy, 0.01);
    box(B, s * 0.668, 1.22, -0.5, 0.012, 0.025, 0.07, grey);
    box(B, s * 0.658, 1.22, -0.28, 0.012, 0.025, 0.07, grey);
    box(B, s * 0.62, 0.66, 0.0, 0.02, 0.02, 0.17, dark);   // step
  });
  // nose details
  box(B, 0, 1.0, -2.375, 0.1, 0.05, 0.03, dark);                  // pitot / landing light
  light(B, 0, 0.98, -2.39, [255, 255, 235, 255], 0.05);
  // cockpit
  var sc = [50, 52, 58, 255];
  [-0.34, 0.34].forEach(function (x) { seat(B, x, 0.78, -1.0, 0.5, sc, 0.38); });
  seat(B, 0, 0.78, 0.35, 1.1, sc, 0.38);
  box(B, 0, 1.25, -1.78, 0.52, 0.12, 0.03, dark);                 // instrument panel
  gauges(B, -0.4, 0.4, 1.22, -1.75, 8, 2);
  box(B, 0, 0.95, -1.7, 0.1, 0.25, 0.1, dark);                    // console
  cyl(B, [0.34, 0.6, -1.4], [0.34, 1.1, -1.35], 0.015, 0.015, 5, dark, 0, true);
  // mast + swashplate
  cyl(B, [0, 2.3, 0], [0, 2.78, 0], 0.06, 0.05, 10, grey, 0, true);
  cyl(B, [0, 2.46, 0], [0, 2.53, 0], 0.16, 0.14, 12, black, 0, true);
  // tail: fins, stabiliser
  hexa(B, [[-0.014, 1.58, 6.15], [0.014, 1.58, 6.15], [0.014, 1.58, 7.16], [-0.014, 1.58, 7.16], [-0.014, 2.60, 6.80], [0.014, 2.60, 6.80], [0.014, 2.60, 7.14], [-0.014, 2.60, 7.14]], navy);
  hexa(B, [[-0.014, 1.38, 6.55], [0.014, 1.38, 6.55], [0.014, 1.38, 7.16], [-0.014, 1.38, 7.16], [-0.014, 0.92, 6.85], [0.014, 0.92, 6.85], [0.014, 0.92, 7.16], [-0.014, 0.92, 7.16]], navy);
  box(B, 0, 0.88, 7.0, 0.02, 0.04, 0.2, dark);
  box(Gr, 0, 1.47, 5.7, 0.74, 0.012, 0.23, navy);                    // stabiliser (counted with appendages)
  [-1, 1].forEach(function (s) { box(Gr, s * 0.75, 1.62, 5.7, 0.012, 0.22, 0.2, navy); });
  light(Gr, -0.75, 1.86, 5.7, RED); light(Gr, 0.75, 1.86, 5.7, GREEN); light(B, 0, 2.2, 7.17, WHITE);
  light(B, 0, 2.58, 6.95, RED, 0.04);
  antenna(B, [0.0, 2.0, 0.9], [0.1, 2.32, 0.9], 0.01, black);
  antenna(B, [0.04, 1.64, 4.0], [0.04, 1.98, 4.05], 0.007, black);
  // tail rotor gearbox
  cyl(B, [-0.02, 1.64, 7.0], [-0.12, 1.64, 7.0], 0.07, 0.06, 8, dark, 0, true);
  // skids
  [-1, 1].forEach(function (s) {
    path(Gr, [[s * 1.0, 0.18, 2.0], [s * 1.0, 0.055, 1.82], [s * 1.0, 0.05, -1.4], [s * 1.0, 0.13, -1.66], [s * 1.0, 0.32, -1.82]], 0.045, 8, dark);
    [-0.5, 0.9].forEach(function (z) {
      path(Gr, [[s * 1.0, 0.06, z], [s * 0.92, 0.32, z], [s * 0.64, 0.47, z]], 0.04, 8, dark);
      box(Gr, s * 0.64, 0.49, z, 0.08, 0.03, 0.08, dark);
    });
  });
  [-0.5, 0.9].forEach(function (z) { path(Gr, [[-0.64, 0.47, z], [0.64, 0.47, z]], 0.04, 8, dark, false); });
  // rotor
  var rg = { R: 5.08, n: 2, chord: 0.27, thick: 0.035, root: 0.5, col: hex(0x1b1c1f), tipCol: hex(0xe8e8e0), tipLen: 0.45, tipCol2: hex(0xb8232a), tipLen2: 0.1, cone: 2, pitch: 5 };
  var rot = rotorMeshes(rg, function (H) {
    box(H, 0, 0.0, 0, 0.55, 0.05, 0.1, black);
    cyl(H, [0, -0.06, 0], [0, 0.1, 0], 0.09, 0.07, 10, black, 0, true);
    [-1, 1].forEach(function (s) { box(H, s * 0.4, 0.0, 0, 0.18, 0.06, 0.09, grey); });
  });
  rot.pivot = [0, 2.8, 0]; rot.dir = 1; rot.phase = 0; m.rotors.push(rot);
  var tg = { R: 0.825, n: 2, chord: 0.14, thick: 0.015, root: 0.08, col: hex(0x1b1c1f), tipCol: hex(0xe8e8e0), tipLen: 0.14, cone: 0, pitch: 8 };
  var tr = rotorMeshes(tg, function (H) { cyl(H, [0, -0.05, 0], [0, 0.07, 0], 0.05, 0.05, 8, dark, 0, true); });
  tr.pivot = SPECS.b206.tail.pivot; tr.ratio = SPECS.b206.tail.ratio; tr.cant = 0; m.tail = tr;
  return m;
};


/* ================= Bell UH-1H Iroquois (Huey) ================= */
SPECS.uh1 = {
  name: 'Bell UH-1H Iroquois', maker: 'Bell Helicopter',
  realLengthM: 12.8, realWidthM: 2.6, realHeightM: 4.4, mainRotorDiameterM: 14.63, tailRotorDiameterM: 2.59, seats: 11,
  rotors: [{ pivot: [0, 4.2, 0], radius: 7.315, blades: 2, dir: 1 }],
  tail: { pivot: [-0.27, 3.1, 8.65], radius: 1.295, blades: 2, axis: 'x', ratio: 5.2 },
  box: [1.3, 1.4, 5.1],
  skids: [[-1.3, 0, -0.6], [1.3, 0, -0.6], [-1.3, 0, 1.4], [1.3, 0, 1.4]],
  eye: [0.5, 2.0, -2.2],
  tipHeight: 4.3, chaseDist: 24, chaseUp: 7
};
MODELS.uh1 = function () {
  var m = newModel(), B = m.body, G = m.glass, Gr = m.gear;
  var od = hex(0x5b6540), odDark = hex(0x454d30), black = hex(0x1a1b1c), dark = hex(0x2a2c2a), grey = hex(0x86897f), yellow = hex(0xd8b525), white = hex(0xe6e6da), seam = hex(0x2c321e);
  var secs = [
    S(-3.90, 1.45, 1.15, 0.054, 2.4), S(-3.75, 1.78, 0.95, 0.495, 2.5), S(-3.30, 2.14, 0.78, 0.925, 2.7), S(-2.60, 2.42, 0.72, 1.129, 2.9),
    S(-1.95, 2.52, 0.70, 1.193, 3.0), S(-1.85, 2.53, 0.70, 1.204, 3.0), S(-1.00, 2.60, 0.70, 1.236, 3.1), S(-0.90, 2.60, 0.70, 1.236, 3.1),
    S(-0.80, 2.60, 0.70, 1.236, 3.1), S(0.50, 2.65, 0.70, 1.236, 3.1), S(0.60, 2.65, 0.70, 1.236, 3.1), S(1.50, 2.65, 0.70, 1.236, 3.1),
    S(1.80, 2.65, 0.70, 1.236, 3.1), S(2.40, 2.72, 0.85, 1.183, 3.0), S(3.00, 2.60, 1.00, 0.85, 2.7), S(3.80, 2.30, 1.25, 0.5, 2.3),
    S(4.40, 2.10, 1.40, 0.34, 2.1), S(6.00, 2.02, 1.50, 0.24, 2), S(7.60, 2.00, 1.62, 0.15, 2), S(8.90, 2.00, 1.72, 0.07, 2)
  ];
  var K = ringQ([8, 18, 30, 42, 54, 64, 74, 82]);
  function isGlass(q) {
    var e = elev(q.a), z = q.z;
    if (z < -3.7) return false;
    if (z < -1.95) return (e > -12 && e < 84) || (z > -3.5 && z < -2.65 && e < -22 && e > -58);
    if (z > -1.95 && z < -1.85) return false;
    if (z > -1.85 && z < -1.0) return e > -9 && e < 62;
    if (z > -0.8 && z < 0.5) return e > -9 && e < 60;
    if (z > 0.6 && z < 1.5) return e > -9 && e < 60;
    return false;
  }
  loft(B, secs, { angles: K, G: G, glassCol: [26, 38, 34, 120], col: function (q) {
    if (isGlass(q)) return GLASS;
    return q.z > 7.0 && Math.abs(elev(q.a)) < 6 ? od : od;
  } });
  interior(B, secs, 0.05, -3.3, 1.8, K, isGlass, [60, 62, 56, 255]);
  // pylon fairing / engine deck
  var cow = [S(-0.95, 2.66, 2.56, 0.45, 2.6), S(-0.55, 2.95, 2.5, 0.72, 2.6), S(0.8, 3.06, 2.5, 0.76, 2.6), S(2.0, 2.98, 2.5, 0.72, 2.6), S(3.2, 2.72, 2.5, 0.52, 2.4), S(3.9, 2.4, 2.3, 0.3, 2.2)];
  loft(B, cow, { K: 22, color: od, caps: true });
  box(B, 0, 2.76, -0.93, 0.4, 0.1, 0.02, dark);                       // intake grille
  cyl(B, [0, 2.58, 3.3], [0, 2.7, 4.5], 0.2, 0.15, 10, dark, 0, true);  // exhaust
  [0.55, 0.0, -0.55].forEach(function (x) { box(B, x, 3.04, 1.0, 0.015, 0.012, 0.5, odDark); });
  // doors, panel lines, markings
  mirror2(function (s) {
    panelLines(B, secs, s, -1.88, -1.0, 0.85, 2.30, seam, 0.025, 0.01);          // pilot door
    panelLines(B, secs, s, -0.85, 1.65, 0.78, 2.46, seam, 0.025, 0.01);          // sliding cargo door
    rectPatch(B, secs, s, -0.85, 1.65, 2.28, 2.34, 8, 1, odDark, 0.012);        // door rail
    rectPatch(B, secs, s, -0.85, 1.65, 0.7, 0.78, 8, 1, odDark, 0.012);
    box(B, s * 1.165, 1.35, -1.1, 0.012, 0.04, 0.05, grey);
    box(B, s * 1.165, 1.25, -0.35, 0.012, 0.03, 0.07, grey);
    rectPatch(B, secs, s, 5.0, 6.2, 1.78, 1.92, 6, 1, yellow, 0.012);            // boom marking band
    rectPatch(B, secs, s, 6.5, 6.6, 1.68, 1.98, 1, 1, white, 0.012);
    rectPatch(B, secs, s, 2.0, 2.9, 1.2, 1.5, 4, 1, odDark, 0.01);
    rectPatch(B, secs, s, -0.6, 0.1, 0.9, 1.0, 4, 1, hex(0xd9d3a0), 0.012);
    light(B, s * 0.88, 1.06, -3.3, s > 0 ? GREEN : RED, 0.04);
  });
  // cockpit
  var sc = [58, 60, 56, 255];
  [-0.5, 0.5].forEach(function (x) { seat(B, x, 0.98, -1.5, 0.52, sc, 0.45); });
  box(B, 0, 0.8, 1.3, 0.9, 0.05, 0.22, sc);                          // troop bench
  box(B, 0, 1.2, 1.55, 0.9, 0.3, 0.04, sc);
  box(B, 0, 1.38, -2.7, 0.9, 0.16, 0.05, dark);                       // instrument panel
  box(B, 0, 1.58, -2.58, 1.0, 0.03, 0.14, dark);                     // glare shield
  gauges(B, -0.7, 0.7, 1.32, -2.64, 10, 3);
  box(B, 0, 1.35, -1.9, 0.12, 0.3, 0.2, dark);                       // console
  [-0.5, 0.5].forEach(function (x) { cyl(B, [x, 0.9, -2.0], [x, 1.55, -2.05], 0.02, 0.02, 5, black, 0, true); });
  // mast
  cyl(B, [0, 3.0, 0], [0, 4.2, 0], 0.1, 0.085, 10, grey, 0, true);
  cyl(B, [0, 3.45, 0], [0, 3.55, 0], 0.26, 0.2, 14, black, 0, true);   // swashplate
  box(B, 0.0, 3.5, 0.2, 0.05, 0.02, 0.2, black);
  // tail boom details
  cyl(B, [0, 1.8, 4.0], [0, 1.74, 8.3], 0.02, 0.02, 4, black, 0, true);   // tail rotor drive cover line
  // fin (swept) + tail rotor gearbox + horizontal stab
  hexa(B, [[-0.02, 1.98, 7.1], [0.02, 1.98, 7.1], [0.02, 1.98, 8.92], [-0.02, 1.98, 8.92], [-0.02, 3.62, 8.35], [0.02, 3.62, 8.35], [0.02, 3.62, 8.92], [-0.02, 3.62, 8.92]], od);
  hexa(B, [[-0.02, 1.5, 8.3], [0.02, 1.5, 8.3], [0.02, 1.5, 8.92], [-0.02, 1.5, 8.92], [-0.02, 1.98, 8.2], [0.02, 1.98, 8.2], [0.02, 1.98, 8.92], [-0.02, 1.98, 8.92]], od);
  box(B, 0, 3.12, 8.6, 0.12, 0.17, 0.2, dark);                         // tail gearbox
  box(B, 0, 1.8, 7.0, 0.95, 0.022, 0.26, od);                          // horizontal stabiliser
  box(B, 0, 1.58, 8.95, 0.03, 0.08, 0.04, dark);
  light(B, 0, 3.55, 8.93, WHITE); light(B, 0, 2.02, 6.0, RED, 0.05);
  cyl(B, [0.06, 2.0, 5.5], [0.06, 2.3, 5.6], 0.01, 0.007, 5, black, 0, true);
  cyl(B, [0.05, 2.74, -1.3], [0.05, 3.15, -1.5], 0.012, 0.008, 5, black, 0, true);
  // skids and cross tubes
  [-1, 1].forEach(function (s) {
    path(Gr, [[s * 1.3, 0.32, -2.34], [s * 1.3, 0.12, -2.0], [s * 1.3, 0.055, -1.7], [s * 1.3, 0.055, 2.3], [s * 1.3, 0.1, 2.65]], 0.05, 8, dark);
    [-0.6, 1.4].forEach(function (z) {
      path(Gr, [[s * 1.3, 0.06, z], [s * 1.2, 0.3, z], [s * 0.9, 0.52, z], [s * 0.7, 0.67, z]], 0.045, 8, dark);
    });
  });
  [-0.6, 1.4].forEach(function (z) { path(Gr, [[-0.7, 0.67, z], [0.7, 0.67, z]], 0.045, 8, dark, false); });
  // main rotor (+ Bell stabiliser bar)
  var rg = { R: 7.315, n: 2, chord: 0.53, thick: 0.06, root: 0.7, col: odDark, tipCol: black, tipLen: 0.55, tipCol2: yellow, tipLen2: 0.14, cone: 2.2, pitch: 5 };
  var rot = rotorMeshes(rg, function (H) {
    box(H, 0, 0, 0, 0.8, 0.07, 0.14, black);
    cyl(H, [0, -0.1, 0], [0, 0.22, 0], 0.12, 0.09, 12, black, 0, true);
    [-1, 1].forEach(function (s) { box(H, s * 0.62, 0, 0, 0.26, 0.09, 0.15, dark); });
    cyl(H, [0, 0.4, 0], [0, 0.22, 0], 0.03, 0.05, 6, grey, 0, true);
    cyl(H, [0, 0.4, -1.5], [0, 0.4, 1.5], 0.022, 0.022, 6, black, 0, true);       // Bell bar
    [-1, 1].forEach(function (s) {
      box(H, 0, 0.4, s * 1.28, 0.14, 0.014, 0.24, od);
      box(H, 0, 0.4, s * 1.28 + 0.0, 0.145, 0.012, 0.01, yellow);
      cyl(H, [0.0, 0.4, s * 0.5], [s * 0.0 + 0.55, 0.0, s * 0.0], 0.008, 0.008, 4, grey, 0, false);
    });
  });
  rot.pivot = [0, 4.2, 0]; rot.dir = 1; rot.phase = 0; m.rotors.push(rot);
  var tg = { R: 1.295, n: 2, chord: 0.21, thick: 0.025, root: 0.12, col: odDark, tipCol: black, tipLen: 0.2, tipCol2: yellow, tipLen2: 0.05, cone: 0, pitch: 8 };
  var tr = rotorMeshes(tg, function (H) { cyl(H, [0, -0.06, 0], [0, 0.1, 0], 0.07, 0.06, 8, dark, 0, true); box(H, 0, 0, 0, 0.14, 0.04, 0.06, black); });
  tr.pivot = SPECS.uh1.tail.pivot; tr.ratio = SPECS.uh1.tail.ratio; tr.cant = 0; m.tail = tr;
  return m;
};


/* ================= Sikorsky UH-60 Black Hawk ================= */
SPECS.uh60 = {
  name: 'Sikorsky UH-60 Black Hawk', maker: 'Sikorsky Aircraft',
  realLengthM: 15.3, realWidthM: 2.4, realHeightM: 5.1, mainRotorDiameterM: 16.36, tailRotorDiameterM: 3.35, seats: 13,
  rotors: [{ pivot: [0, 3.9, 0], radius: 8.18, blades: 4, dir: 1 }],
  tail: { pivot: [-0.42, 3.5, 10.0], radius: 1.675, blades: 4, axis: 'x', ratio: 4.6, cantDeg: 20 },
  box: [1.35, 1.6, 6.1],
  skids: [[-1.38, 0, 0.62], [1.38, 0, 0.62], [-0.12, 0, 8.3], [0.12, 0, 8.3]],
  eye: [0.5, 1.95, -3.2],
  tipHeight: 4.0, chaseDist: 28, chaseUp: 8
};
MODELS.uh60 = function () {
  var m = newModel(), B = m.body, G = m.glass, Gr = m.gear;
  var od = hex(0x4a5240), odDark = hex(0x333a2d), odLight = hex(0x58614c), black = hex(0x141516), dark = hex(0x25272a), grey = hex(0x7d8078), yellow = hex(0xd1ab2a), seam = hex(0x23281f);
  var secs = [
    S(-4.70, 1.50, 1.20, 0.06, 2.6), S(-4.55, 1.90, 0.98, 0.55, 2.8), S(-4.10, 2.17, 0.80, 0.98, 3.0), S(-3.60, 2.30, 0.77, 1.08, 3.2),
    S(-3.30, 2.36, 0.76, 1.14, 3.3), S(-2.45, 2.46, 0.75, 1.18, 3.4), S(-2.30, 2.47, 0.75, 1.18, 3.4), S(-1.00, 2.50, 0.75, 1.18, 3.4),
    S(-0.90, 2.50, 0.75, 1.18, 3.4), S(0.40, 2.52, 0.77, 1.18, 3.4), S(1.60, 2.54, 0.80, 1.18, 3.4), S(1.70, 2.54, 0.80, 1.18, 3.4),
    S(2.00, 2.55, 0.82, 1.17, 3.4), S(2.60, 2.55, 0.86, 1.14, 3.3), S(3.20, 2.55, 0.90, 1.08, 3.1), S(4.40, 2.50, 1.00, 0.92, 2.8),
    S(5.60, 2.46, 1.22, 0.74, 2.6), S(7.00, 2.46, 1.50, 0.52, 2.4), S(8.20, 2.56, 1.76, 0.36, 2.2), S(9.40, 2.64, 2.0, 0.24, 2), S(10.45, 2.64, 2.2, 0.12, 2)
  ];
  var K = ringQ([8, 20, 34, 48, 60, 72, 82]);
  function isGlass(q) {
    var e = elev(q.a), z = q.z;
    if (z < -4.6) return false;
    if (z < -3.3) return e > -4 && e < 80;
    if (z < -2.45) return e > -4 && e < 62;
    if (z > -2.3 && z < -1.0) return e > 6 && e < 56;
    if (z > -0.9 && z < 0.4) return false;
    if (z > 0.4 && z < 1.6) return e > 4 && e < 50;
    return false;
  }
  loft(B, secs, { angles: K, G: G, glassCol: [20, 32, 30, 125], col: function (q) {
    if (isGlass(q)) return GLASS;
    return od;
  } });
  interior(B, secs, 0.05, -3.3, 1.6, K, isGlass, [52, 54, 50, 255]);
  // engine nacelles and transmission fairing
  mirror2(function (s) {
    var nac = [S(-1.15, 2.86, 2.46, 0.2, 2.4, s * 0.56), S(-0.75, 3.12, 2.44, 0.42, 2.6, s * 0.56), S(0.0, 3.17, 2.44, 0.5, 2.6, s * 0.56), S(2.0, 3.15, 2.44, 0.5, 2.6, s * 0.56), S(3.1, 3.0, 2.44, 0.42, 2.5, s * 0.56), S(3.9, 2.8, 2.5, 0.22, 2.4, s * 0.56)];
    loft(B, nac, { K: 18, color: od, caps: true });
    ell(B, s * 0.56, 2.84, -1.17, 0.21, 0.2, 0.06, 10, 4, black);                       // intake
    cyl(B, [s * 0.56, 2.74, 3.8], [s * 0.56, 2.76, 4.35], 0.2, 0.15, 10, dark, 0, true); // IR suppressor / exhaust
    cyl(B, [s * 0.56, 2.74, 4.35], [s * 0.56, 2.76, 4.38], 0.13, 0.13, 8, black, 0, true);
    box(B, s * 0.56, 3.17, 1.4, 0.2, 0.008, 0.5, odDark);                               // cowl hatch
  });
  var tf = [S(-0.9, 3.12, 2.5, 0.12, 2.4), S(-0.3, 3.3, 2.5, 0.42, 2.5), S(0.7, 3.38, 2.5, 0.45, 2.5), S(1.8, 3.3, 2.5, 0.42, 2.5), S(2.8, 3.1, 2.5, 0.25, 2.4)];
  loft(B, tf, { K: 16, color: odLight, caps: true });
  // tail pylon (swept fin) and tail rotor gearbox
  hexa(B, [[-0.1, 2.5, 7.9], [0.1, 2.5, 7.9], [0.1, 2.5, 10.48], [-0.1, 2.5, 10.48], [-0.07, 4.85, 9.55], [0.07, 4.85, 9.55], [0.07, 4.85, 10.4], [-0.07, 4.85, 10.4]], od);
  box(B, 0, 3.62, 10.05, 0.17, 0.2, 0.22, odDark);
  cyl(B, [-0.12, 3.52, 10.05], [-0.34, 3.52 + 0.0, 10.05], 0.1, 0.08, 8, dark, 0, true);
  box(B, 0, 4.88, 9.98, 0.06, 0.04, 0.45, odDark);        // fin cap
  light(B, 0, 4.93, 10.42, WHITE, 0.04); light(B, 0, 2.05, 6.3, RED, 0.05);
  // stabilator
  box(Gr, 0, 2.55, 9.75, 2.19, 0.055, 0.48, od);
  box(Gr, 0, 2.55, 9.75, 0.22, 0.09, 0.3, odDark);
  // markings, doors, windows details
  mirror2(function (s) {
    panelLines(B, secs, s, -2.28, -1.02, 0.95, 2.2, seam, 0.025, 0.01);               // cockpit door
    panelLines(B, secs, s, -0.88, 2.0, 0.92, 2.28, seam, 0.025, 0.01);                // cabin sliding door
    rectPatch(B, secs, s, -0.88, 2.0, 2.26, 2.34, 8, 1, odDark, 0.012);
    rectPatch(B, secs, s, 2.1, 3.4, 1.1, 1.8, 4, 2, odLight, 0.01);
    rectPatch(B, secs, s, 4.0, 5.6, 1.6, 1.78, 4, 1, odDark, 0.01);
    box(B, s * 1.18, 1.55, -1.1, 0.012, 0.05, 0.05, grey);
    box(B, s * 1.18, 1.5, -0.55, 0.012, 0.04, 0.06, grey);
    box(B, s * 1.185, 1.5, 0.0, 0.012, 0.05, 0.12, black);
    light(B, s * 0.96, 1.42, -4.1, s > 0 ? GREEN : RED, 0.04);
    box(B, s * 1.02, 1.08, 0.45, 0.2, 0.025, 0.65, od);                              // step / sill
  });
  // windshield frame bars
  box(B, 0, 2.25, -4.25, 0.015, 0.2, 0.02, odDark);
  box(B, 0, 2.43, -3.0, 0.8, 0.012, 0.012, odDark);
  // cockpit interior
  var sc = [52, 54, 50, 255];
  [-0.55, 0.55].forEach(function (x) { seat(B, x, 1.02, -2.85, 0.55, sc, 0.5); });
  box(B, 0, 1.55, -3.95, 0.8, 0.18, 0.06, dark);
  box(B, 0, 1.78, -3.85, 0.8, 0.03, 0.12, dark);
  gauges(B, -0.8, 0.8, 1.48, -3.89, 12, 3);
  box(B, 0, 1.4, -2.9, 0.12, 0.3, 0.25, dark);
  [-0.55, 0.55].forEach(function (x) { cyl(B, [x, 0.95, -3.3], [x, 1.6, -3.35], 0.02, 0.02, 5, black, 0, true); });
  box(B, 0, 0.82, 0.5, 1.0, 0.04, 1.3, [74, 76, 70, 255]);                           // cabin floor
  [-1, 1].forEach(function (s) { box(B, s * 1.0, 1.3, 1.5, 0.05, 0.2, 0.5, sc); });
  // mast, hub fairing
  cyl(B, [0, 3.3, 0], [0, 3.9, 0], 0.14, 0.12, 12, grey, 0, true);
  cyl(B, [0, 3.38, 0], [0, 3.5, 0], 0.34, 0.3, 14, odDark, 0, true);
  // antennas / sensors
  box(B, 0, 2.58, -3.0, 0.015, 0.14, 0.15, black);                                    // wire cutter
  cyl(B, [0.15, 2.5, -1.9], [0.2, 2.9, -1.95], 0.012, 0.008, 5, black, 0, true);
  ell(B, 0, 2.6, 6.5, 0.2, 0.1, 0.35, 8, 4, dark);
  antenna(B, [0.0, 2.49, 4.0], [0.05, 2.9, 4.2], 0.012, black);
  box(B, 0.0, 0.72, -0.8, 0.12, 0.035, 0.3, dark);
  // landing gear: main wheels with oleo struts, tail wheel
  mirror2(function (s) {
    wheel(Gr, s * 1.38, 0.34, 0.62, 0.34, 0.22, black, grey);
    path(Gr, [[s * 1.16, 1.15, 1.25], [s * 1.32, 0.7, 0.9], [s * 1.38, 0.38, 0.62]], 0.065, 8, dark);
    path(Gr, [[s * 1.1, 0.95, 2.3], [s * 1.38, 0.38, 0.62]], 0.035, 6, dark, false);
    cyl(Gr, [s * 1.1, 0.8, 0.0], [s * 1.18, 1.05, 0.0], 0.06, 0.06, 6, dark, 0, true);
  });
  path(Gr, [[0, 1.95, 9.0], [0, 1.0, 8.55], [0, 0.26, 8.3]], 0.055, 8, dark);
  wheel(Gr, 0, 0.22, 8.3, 0.22, 0.12, black, grey);
  box(Gr, 0, 0.35, 8.3, 0.1, 0.03, 0.1, dark);
  // main rotor: four blades, elastomeric hub
  var rg = { R: 8.18, n: 4, chord: 0.53, thick: 0.05, root: 1.05, col: hex(0x1e1f20), tipCol: yellow, tipLen: 0.5, tipCol2: hex(0x2e2f31), tipLen2: 0.12, cone: 2.5, pitch: 5 };
  var rot = rotorMeshes(rg, function (H) {
    cyl(H, [0, -0.2, 0], [0, 0.25, 0], 0.4, 0.34, 16, hex(0x2d2f2b), 0, true);
    for (var k = 0; k < 4; k++) {
      H.xf(mRY(k * PI / 2), function () {
        cyl(H, [0.3, 0.02, 0], [1.1, 0.02, 0], 0.17, 0.14, 10, hex(0x33352f), 0, true);
        box(H, 0.75, 0.12, 0, 0.35, 0.04, 0.1, hex(0x1d1e1f));
        cyl(H, [0.5, -0.3, 0.1], [0.55, 0.0, 0.1], 0.02, 0.02, 4, grey, 0, false);
      });
    }
  });
  rot.pivot = [0, 3.9, 0]; rot.dir = 1; rot.phase = 0; m.rotors.push(rot);
  var tg = { R: 1.675, n: 4, chord: 0.24, thick: 0.03, root: 0.22, col: hex(0x1e1f20), tipCol: yellow, tipLen: 0.2, cone: 0, pitch: 8 };
  var tr = rotorMeshes(tg, function (H) { cyl(H, [0, -0.08, 0], [0, 0.12, 0], 0.15, 0.13, 10, dark, 0, true); });
  tr.pivot = SPECS.uh60.tail.pivot; tr.ratio = SPECS.uh60.tail.ratio; tr.cant = 20; m.tail = tr;
  return m;
};


/* ================= Boeing CH-47F Chinook ================= */
SPECS.ch47 = {
  name: 'Boeing CH-47F Chinook', maker: 'Boeing Rotorcraft Systems',
  realLengthM: 15.5, realWidthM: 3.8, realHeightM: 5.7, mainRotorDiameterM: 18.29, tailRotorDiameterM: 0, seats: 33,
  rotors: [{ pivot: [0, 4.4, -5.95], radius: 9.145, blades: 3, dir: 1 }, { pivot: [0, 5.6, 5.95], radius: 9.145, blades: 3, dir: -1 }],
  tail: null,
  box: [1.9, 1.75, 6.2],
  skids: [[-1.1, 0, -5.3], [1.1, 0, -5.3], [-1.65, 0, 3.2], [1.65, 0, 3.2]],
  eye: [0.55, 2.55, -7.0],
  tipHeight: 5.65, chaseDist: 40, chaseUp: 11
};
MODELS.ch47 = function () {
  var m = newModel(), B = m.body, G = m.glass, Gr = m.gear;
  var od = hex(0x4c553b), odDark = hex(0x363e2b), tan = hex(0x9c8d63), tanDark = hex(0x7f7250), black = hex(0x141516), dark = hex(0x26282a), grey = hex(0x7a7d76), seam = hex(0x242a1d), yellow = hex(0xd1ab2a);
  var secs = [
    S(-7.85, 2.30, 1.55, 0.08, 2.6), S(-7.70, 2.78, 1.05, 0.70, 3.0), S(-7.20, 3.14, 0.85, 1.08, 3.6), S(-6.85, 3.30, 0.85, 1.15, 3.8),
    S(-6.50, 3.40, 0.85, 1.18, 3.9), S(-5.75, 3.44, 0.85, 1.20, 4), S(-4.90, 3.42, 0.85, 1.20, 4), S(0.0, 3.42, 0.85, 1.20, 4),
    S(3.20, 3.40, 0.85, 1.20, 4), S(4.60, 3.38, 1.05, 1.20, 4), S(6.20, 3.34, 1.40, 1.15, 3.8), S(7.65, 3.30, 1.70, 1.10, 3.6)
  ];
  var K = ringQ([8, 20, 34, 48, 62, 76, 83]);
  function isGlass(q) {
    var e = elev(q.a), z = q.z;
    if (z < -7.8) return false;
    if (z < -6.85) return e > -2 && e < 70;
    if (z < -5.75) return e > 2 && e < 54;
    return false;
  }
  loft(B, secs, { angles: K, G: G, glassCol: [20, 30, 34, 130], capCol: [24, 26, 24, 255], caps: true, col: function (q) {
    if (isGlass(q)) return GLASS;
    return od;
  } });
  interior(B, secs, 0.06, -7.7, -5.75, K, isGlass, [48, 50, 46, 255]);
  // sponsons / fuel pods
  mirror2(function (s) {
    var sp = [S(-5.95, 1.62, 1.0, 0.12, 2.4, s * 1.5), S(-5.4, 2.07, 0.84, 0.5, 2.8, s * 1.4), S(-4.0, 2.18, 0.82, 0.62, 3.2, s * 1.28), S(2.0, 2.18, 0.82, 0.62, 3.2, s * 1.28), S(3.8, 2.08, 0.9, 0.5, 2.8, s * 1.4), S(4.3, 1.8, 1.05, 0.2, 2.4, s * 1.5)];
    loft(B, sp, { K: 18, color: od, caps: true });
    rectPatch(B, sp, s, -4.6, 3.2, 0.95, 1.12, 10, 1, tan, 0.012);                       // tan band
    // engine pods
    var en = [S(3.3, 4.0, 3.3, 0.1, 2.4, s * 0.95), S(3.7, 4.22, 3.28, 0.46, 2.6, s * 0.95), S(4.6, 4.52, 3.28, 0.58, 2.8, s * 0.95), S(6.6, 4.5, 3.34, 0.58, 2.8, s * 0.95), S(7.55, 4.2, 3.5, 0.42, 2.5, s * 0.95)];
    loft(B, en, { K: 18, color: od, caps: false });
    box(B, s * 0.95, 3.95, 7.56, 0.35, 0.3, 0.03, black);                                // exhaust
    ell(B, s * 0.95, 3.92, 3.35, 0.4, 0.36, 0.1, 10, 4, black);                          // intake
    // front gear
    wheel(Gr, s * 1.12, 0.36, -5.3, 0.36, 0.2, black, grey);
    path(Gr, [[s * 1.12, 0.9, -5.3], [s * 1.12, 0.36, -5.3]], 0.07, 8, dark);
    // rear gear
    wheel(Gr, s * 1.65, 0.44, 3.2, 0.44, 0.26, black, grey);
    path(Gr, [[s * 1.62, 0.85, 3.2], [s * 1.65, 0.44, 3.2]], 0.09, 8, dark);
    path(Gr, [[s * 1.5, 0.84, 2.4], [s * 1.65, 0.45, 3.2]], 0.04, 6, dark, false);
    light(B, s * 1.7, 1.9, -5.0, s > 0 ? GREEN : RED, 0.05);
  });
  // pylons
  var fp = [S(-7.3, 3.45, 3.3, 0.3, 2.6), S(-6.8, 3.88, 3.3, 0.55, 2.8), S(-5.9, 4.22, 3.3, 0.62, 2.8), S(-4.8, 4.0, 3.3, 0.58, 2.8), S(-4.2, 3.55, 3.3, 0.4, 2.6)];
  loft(B, fp, { K: 18, color: od, caps: true });
  var ap = [S(3.0, 3.6, 3.3, 0.45, 2.6), S(3.6, 4.6, 3.3, 0.8, 2.8), S(4.8, 5.34, 3.3, 0.95, 3), S(5.95, 5.5, 3.3, 0.98, 3), S(7.0, 5.36, 3.3, 0.92, 3), S(7.45, 4.8, 3.3, 0.74, 2.8), S(7.62, 4.2, 3.3, 0.4, 2.4)];
  loft(B, ap, { K: 20, color: od, caps: true });
  // rotor masts (static part)
  cyl(B, [0, 4.15, -5.95], [0, 4.4, -5.95], 0.24, 0.2, 12, grey, 0, true);
  cyl(B, [0, 5.35, 5.95], [0, 5.6, 5.95], 0.24, 0.2, 12, grey, 0, true);
  // doors, windows, ramp
  mirror2(function (s) {
    panelLines(B, secs, s, -4.7, -2.7, 1.0, 3.2, seam, 0.03, 0.01);                      // main cargo door
    rectPatch(B, secs, s, -4.5, -3.9, 2.3, 3.0, 2, 3, black, 0.014);
    panelLines(B, secs, s, -7.0, -5.8, 1.0, 3.2, seam, 0.03, 0.01);                      // cockpit door
    [[-2.2, 2.3], [-1.2, 2.3], [-0.2, 2.3], [0.8, 2.3], [1.8, 2.3]].forEach(function (p) { // small cabin windows
      rectPatch(G, secs, s, p[0] - 0.22, p[0] + 0.22, p[1] - 0.2, p[1] + 0.2, 1, 1, [20, 30, 34, 150, MAT_GLASS], 0.012, MAT_GLASS);
    });
    rectPatch(B, secs, s, -2.6, 5.2, 3.12, 3.22, 10, 1, tanDark, 0.01);
    rectPatch(B, secs, s, 5.2, 7.5, 1.3, 3.2, 3, 3, odDark, 0.01);
    box(B, s * 1.205, 2.2, -5.0, 0.012, 0.05, 0.05, grey);
  });
  // ramp area (rear)
  box(B, 0, 1.75, 6.5, 1.0, 0.03, 1.1, tanDark);
  box(B, 0, 3.0, 7.66, 1.0, 0.03, 0.03, dark);
  // cockpit interior
  var sc = [50, 52, 46, 255];
  [-0.55, 0.55].forEach(function (x) { seat(B, x, 1.55, -6.6, 0.55, sc, 0.5); });
  box(B, 0, 2.1, -7.45, 0.98, 0.2, 0.06, dark);
  gauges(B, -0.8, 0.8, 2.02, -7.39, 12, 3);
  box(B, 0, 1.95, -6.6, 0.12, 0.3, 0.25, dark);
  box(B, 0, 1.55, -5.5, 1.0, 0.03, 0.9, [74, 76, 70, 255]);
  // cockpit nose details / sensors / lights
  box(B, 0, 1.6, -7.88, 0.06, 0.06, 0.03, black);
  light(B, 0, 3.5, -7.1, WHITE, 0.05);
  light(B, 0, 3.52, 6.0, RED, 0.06);
  light(B, 0, 3.0, 7.7, WHITE, 0.05);
  box(B, 0.0, 0.84, -2.0, 0.16, 0.03, 0.5, dark);
  antenna(B, [0.0, 3.43, -3.0], [0.1, 3.9, -3.1], 0.015, black);
  antenna(B, [0.0, 3.43, 0.5], [0.0, 3.8, 0.55], 0.012, black);
  // hoist hooks
  box(B, 0.0, 0.8, 0.5, 0.05, 0.03, 0.05, dark);
  // rotors
  function chRotor(pz, py, dirv, phase) {
    var rg = { R: 9.145, n: 3, chord: 0.81, thick: 0.08, root: 1.6, col: hex(0x1f2020), tipCol: hex(0xd4d0c0), tipLen: 0.35, tipCol2: hex(0xb0262c), tipLen2: 0.15, cone: 2.5, pitch: 5, discCol: [210, 215, 218, 255] };
    var rot = rotorMeshes(rg, function (H) {
      cyl(H, [0, -0.25, 0], [0, 0.3, 0], 0.5, 0.42, 18, hex(0x30332c), 0, true);
      for (var k = 0; k < 3; k++) {
        H.xf(mRY(k * TAU / 3), function () {
          cyl(H, [0.3, 0.0, 0], [1.6, 0.0, 0], 0.2, 0.17, 10, hex(0x363a30), 0, true);
          box(H, 1.2, 0.17, 0, 0.45, 0.05, 0.12, hex(0x1d1e1f));
          cyl(H, [0.7, -0.25, 0.15], [0.75, 0.0, 0.15], 0.025, 0.025, 4, grey, 0, false);
        });
      }
    });
    rot.pivot = [0, py, pz]; rot.dir = dirv; rot.phase = phase; return rot;
  }
  m.rotors.push(chRotor(-5.95, 4.4, 1, 0));
  m.rotors.push(chRotor(5.95, 5.6, -1, PI / 3));
  return m;
};


/* ---------------- model assembly + renderer ---------------- */
var ALL_IDS = ['r22', 'b206', 'uh1', 'uh60', 'ch47'];
var BUILT = {};
function buildModel(id) {
  if (BUILT[id]) return BUILT[id];
  var m = MODELS[id]();
  BUILT[id] = m;
  return m;
}
// rotor assembly helper used by model files
function rotorMeshes(o, hubFn) {
  var hub = new Mesh();
  if (hubFn) hubFn(hub);
  return { hub: hub, blades: makeBlades(o), disc: makeDisc(o) };
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
'uniform vec2 uFog; uniform vec3 uSky; uniform float uTint; uniform float uAlpha;',
'void main(){',
' float dist = length(vRel); vec3 v = -vRel / max(dist,1e-4);',
' vec3 n = normalize(vN);',
' if (vM > 3.5) { if (dot(n, v) < 0.0) n = -n; }',
' vec3 sun = normalize(vec3(0.45,0.85,0.35));',
' float nd = max(dot(n,sun),0.0);',
' vec3 amb = mix(vec3(0.34,0.29,0.25), vec3(0.60,0.70,0.86), n.y*0.5+0.5);',
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
' float f = clamp((dist - uFog.x) / max(uFog.y - uFog.x, 1e-3), 0.0, 1.0);',
' col = mix(col, uSky, f);',
' gl_FragColor = vec4(col, vC.a * uAlpha);',
'}'
].join('\n');

var ATT = { pos: 2, nrm: 3, col: 4, mat: 5 };
var Q_TAIL = mRZ(-PI / 2);

function compile(gl, type, src) {
  var s = gl.createShader(type); gl.shaderSource(s, src); gl.compileShader(s);
  if (!gl.getShaderParameter(s, gl.COMPILE_STATUS)) throw new Error('HeliGL shader: ' + gl.getShaderInfoLog(s));
  return s;
}

function create(gl) {
  var prog = gl.createProgram();
  gl.attachShader(prog, compile(gl, gl.VERTEX_SHADER, VS));
  gl.attachShader(prog, compile(gl, gl.FRAGMENT_SHADER, FS));
  gl.bindAttribLocation(prog, ATT.pos, 'aPos'); gl.bindAttribLocation(prog, ATT.nrm, 'aNrm');
  gl.bindAttribLocation(prog, ATT.col, 'aCol'); gl.bindAttribLocation(prog, ATT.mat, 'aMat');
  gl.linkProgram(prog);
  if (!gl.getProgramParameter(prog, gl.LINK_STATUS)) throw new Error('HeliGL link: ' + gl.getProgramInfoLog(prog));
  var U = {};
  ['uVP', 'uModel', 'uCam', 'uFog', 'uSky', 'uTint', 'uAlpha'].forEach(function (n) { U[n] = gl.getUniformLocation(prog, n); });
  var cache = {};
  var thumbState = null;
  var sky = [0.56, 0.75, 0.94];

  function pack(meshes) { // meshes: array; returns {buf, ranges}
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
    var m = buildModel(id), list = [m.body, m.gear, m.glass], idx = { body: 0, gear: 1, glass: 2 };
    m.rotors.forEach(function (r, k) { r._i = list.length; list.push(r.hub, r.blades, r.disc); });
    if (m.tail) { m.tail._i = list.length; list.push(m.tail.hub, m.tail.blades, m.tail.disc); }
    var pk = pack(list);
    cache[id] = { m: m, buf: pk.buf, ranges: pk.ranges };
    return cache[id];
  }

  function draw(id, model, vp, cam, fogNear, fogFar, angle, blur, tint) {
    var G = getModel(id), m = G.m, R = G.ranges;
    blur = clamp(blur || 0, 0, 1); tint = clamp(tint || 0, 0, 1); angle = angle || 0;
    if (!(fogFar > fogNear)) { fogNear = 1e8; fogFar = 2e8; }
    // ---- save state
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
    function rotMat(r) { return mMul(mT(r.pivot[0], r.pivot[1], r.pivot[2]), mRY((r.dir || 1) * angle + (r.phase || 0))); }
    function tailMat(t) { return mMul(mT(t.pivot[0], t.pivot[1], t.pivot[2]), mMul(mRZ((t.cant || 0) * PI / 180), mMul(mRX(angle * t.ratio), Q_TAIL))); }
    var bladeOpaque = blur < 0.02;
    // opaque
    setM(mIdent()); drawRange(0, 1); drawRange(1, 1);
    m.rotors.forEach(function (r) { setM(rotMat(r)); drawRange(r._i, 1); if (bladeOpaque) drawRange(r._i + 1, 1); });
    if (m.tail) { setM(tailMat(m.tail)); drawRange(m.tail._i, 1); if (bladeOpaque) drawRange(m.tail._i + 1, 1); }
    // blended
    gl.enable(gl.BLEND); gl.depthMask(false);
    setM(mIdent()); drawRange(2, 1);
    var ba = 1 - 0.86 * blur;
    m.rotors.forEach(function (r) {
      setM(rotMat(r));
      if (!bladeOpaque) drawRange(r._i + 1, ba);
      if (blur > 0) drawRange(r._i + 2, blur);
    });
    if (m.tail) {
      setM(tailMat(m.tail));
      if (!bladeOpaque) drawRange(m.tail._i + 1, ba);
      if (blur > 0) drawRange(m.tail._i + 2, blur);
    }
    // ---- restore
    [ATT.pos, ATT.nrm, ATT.col, ATT.mat].forEach(function (i) { if (!st.en[i]) gl.disableVertexAttribArray(i); });
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
    var sp = SPECS[id], mdl = buildModel(id);
    var spin = 1.45; // blades roughly along the viewing direction (foreshortened) so the aircraft fills the frame
    var pts = [], bb = mdl.body.bbox(), gb = mdl.gear.bbox(), i, j;
    [bb, gb].forEach(function (q) { for (i = 0; i < 8; i++) pts.push([i & 1 ? q.max[0] : q.min[0], i & 2 ? q.max[1] : q.min[1], i & 4 ? q.max[2] : q.min[2]]); });
    mdl.rotors.forEach(function (r) {
      var rr = sp.rotors[mdl.rotors.indexOf(r)].radius, nb = sp.rotors[mdl.rotors.indexOf(r)].blades;
      for (j = 0; j < nb; j++) { var a = (r.dir || 1) * spin + (r.phase || 0) + j * TAU / nb; pts.push([r.pivot[0] + rr * Math.cos(a), r.pivot[1], r.pivot[2] - rr * Math.sin(a)]); }
      pts.push([r.pivot[0], r.pivot[1] + 0.3, r.pivot[2]]);
    });
    if (mdl.tail) pts.push([mdl.tail.pivot[0], mdl.tail.pivot[1] + sp.tail.radius, mdl.tail.pivot[2]], [mdl.tail.pivot[0], mdl.tail.pivot[1] - sp.tail.radius, mdl.tail.pivot[2]]);
    var ctr = [0, 0, 0]; pts.forEach(function (p) { ctr = add(ctr, p); }); ctr = mul(ctr, 1 / pts.length);
    var fov = 30 * PI / 180, th = Math.tan(fov / 2), asp = w / h, dir = norm([-0.80, 0.40, -1.0]), dist = 0, eye, vpm, k;
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
      var cx = (mnx + mxx) / 2, cy = (mny + mxy) / 2, ext = Math.max((mxx - mnx) / 2, (mxy - mny) / 2) / 0.92;
      var right = [view[0], view[4], view[8]], up = [view[1], view[5], view[9]];
      ctr = add(ctr, add(mul(right, cx * th * asp * dist), mul(up, cy * th * dist)));
      dist = dist * clamp(ext, 0.6, 1.6);
    }
    eye = add(ctr, mul(dir, dist));
    vpm = mMul(mPersp(fov, asp, dist * 0.05, dist * 6), mLookAt(eye, ctr, [0, 1, 0]));
    window.__thumbDbg = { dist: dist, ctr: ctr, eye: eye, n: pts.length };
    var vp = new Float32Array(vpm), md = new Float32Array(mIdent());
    thumbState.r.draw(id, md, vp, eye, 1e8, 2e8, spin, 0, 0);
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

var HeliGL = {
  IDS: ALL_IDS, SPECS: SPECS, create: create,
  _buildForTest: function (id) {
    var m = buildModel(id), out = { body: m.body, gear: m.gear, glass: m.glass, rotors: m.rotors, tail: m.tail };
    var tris = m.body.tcount() + m.gear.tcount() + m.glass.tcount();
    m.rotors.forEach(function (r) { tris += r.hub.tcount() + r.blades.tcount() + r.disc.tcount(); });
    if (m.tail) tris += m.tail.hub.tcount() + m.tail.blades.tcount() + m.tail.disc.tcount();
    out.triangles = tris; return out;
  }
};
if (typeof window !== 'undefined') window.HeliGL = HeliGL;
if (typeof module !== 'undefined' && module.exports) module.exports = HeliGL;
if (typeof globalThis !== 'undefined' && typeof window === 'undefined') globalThis.HeliGL = HeliGL;
})(typeof window !== 'undefined' ? window : this);
