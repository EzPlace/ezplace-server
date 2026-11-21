// Builds the six electric rides from scratch (no downloaded models): tubes, tyres, spokes and panels sized from the real vehicles' published dimensions.
// Output: the same MDL1 binary the real car models use (wheel parts 1 = front wheel, 3 = rear wheel so they spin and steer).
//   node tools/make_rides.js [outDir]        (default: car_snd/)
const fs = require('fs'), path = require('path');
const OUT = process.argv[2] || path.join(__dirname, '..', 'car_snd');
// ---------------------------------------------------------------- the mesh builder
let V, T;
const sub = (a, b) => [a[0] - b[0], a[1] - b[1], a[2] - b[2]], add = (a, b) => [a[0] + b[0], a[1] + b[1], a[2] + b[2]], mul = (a, k) => [a[0] * k, a[1] * k, a[2] * k];
const dot = (a, b) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2], cross = (a, b) => [a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]];
const len = a => Math.hypot(a[0], a[1], a[2]), norm = a => { const l = len(a) || 1; return [a[0] / l, a[1] / l, a[2] / l]; };
let ST;                                                    // the current style: part, class, colour
const S = o => Object.assign({}, ST, o || {});
function vert(p, n, s) { V.push({ p, n: norm(n), part: s.part, cls: s.cls, col: s.col }); return V.length - 1; }
function tri(a, b, c) {
  const n = cross(sub(V[b].p, V[a].p), sub(V[c].p, V[a].p)), nn = add(add(V[a].n, V[b].n), V[c].n);
  if (dot(n, nn) < 0) T.push([a, c, b, V[a].cls]); else T.push([a, b, c, V[a].cls]);
}
const quad = (a, b, c, d) => { tri(a, b, c); tri(a, c, d); };
function rotXYZ(p, r) {
  let [x, y, z] = p, c, s;
  if (r[0]) { c = Math.cos(r[0]); s = Math.sin(r[0]); [y, z] = [y * c - z * s, y * s + z * c]; }
  if (r[1]) { c = Math.cos(r[1]); s = Math.sin(r[1]); [x, z] = [x * c + z * s, -x * s + z * c]; }
  if (r[2]) { c = Math.cos(r[2]); s = Math.sin(r[2]); [x, y] = [x * c - y * s, x * s + y * c]; }
  return [x, y, z];
}
function box(c, sz, o) {                                   // a flat-shaded box, optionally turned
  o = S(o); const r = o.rot || [0, 0, 0], h = [sz[0] / 2, sz[1] / 2, sz[2] / 2];
  const faces = [[[1, 0, 0], [[1, -1, -1], [1, 1, -1], [1, 1, 1], [1, -1, 1]]], [[-1, 0, 0], [[-1, -1, 1], [-1, 1, 1], [-1, 1, -1], [-1, -1, -1]]], [[0, 1, 0], [[-1, 1, -1], [-1, 1, 1], [1, 1, 1], [1, 1, -1]]],
    [[0, -1, 0], [[-1, -1, 1], [-1, -1, -1], [1, -1, -1], [1, -1, 1]]], [[0, 0, 1], [[-1, -1, 1], [1, -1, 1], [1, 1, 1], [-1, 1, 1]]], [[0, 0, -1], [[1, -1, -1], [-1, -1, -1], [-1, 1, -1], [1, 1, -1]]]];
  for (const [n, cs] of faces) { const nn = rotXYZ(n, r), ids = cs.map(k => vert(add(c, rotXYZ([k[0] * h[0], k[1] * h[1], k[2] * h[2]], r)), nn, o)); quad(ids[0], ids[1], ids[2], ids[3]); }
}
function frame(axis) {                                     // an orthonormal pair perpendicular to an axis
  const a = Math.abs(axis[1]) < 0.9 ? [0, 1, 0] : [1, 0, 0], u = norm(cross(axis, a)), v = cross(axis, u); return [u, v];
}
function cyl(p0, p1, r0, r1, o) {                          // a tube or cone between two points, smooth sides and flat caps
  o = S(o); const seg = o.seg || 10, ax = norm(sub(p1, p0)), [u, v] = frame(ax), ring = [[], []];
  for (let e = 0; e < 2; e++) for (let i = 0; i < seg; i++) {
    const a = i / seg * Math.PI * 2, d = add(mul(u, Math.cos(a)), mul(v, Math.sin(a))), r = e ? r1 : r0, base = e ? p1 : p0;
    ring[e].push(vert(add(base, mul(d, r)), add(d, mul(ax, (r0 - r1) / Math.max(1e-6, len(sub(p1, p0))))), o));
  }
  for (let i = 0; i < seg; i++) { const j = (i + 1) % seg; quad(ring[0][i], ring[0][j], ring[1][j], ring[1][i]); }
  if (o.caps !== false) for (let e = 0; e < 2; e++) {
    const n = mul(ax, e ? 1 : -1), c = vert(e ? p1 : p0, n, o), rr = [];
    for (let i = 0; i < seg; i++) { const a = i / seg * Math.PI * 2, d = add(mul(u, Math.cos(a)), mul(v, Math.sin(a))); rr.push(vert(add(e ? p1 : p0, mul(d, e ? r1 : r0)), n, o)); }
    for (let i = 0; i < seg; i++) tri(c, rr[i], rr[(i + 1) % seg]);
  }
}
const tube = (p0, p1, r, o) => cyl(p0, p1, r, r, o);
function torus(c, R, rx, ry, o) {                          // a ring around the x axis: R to the middle of the tyre, rx half its width, ry half its depth
  o = S(o); const sa = o.segA || 28, sb = o.segB || 10, id = [];
  for (let i = 0; i < sa; i++) { const a = i / sa * Math.PI * 2, ca = Math.cos(a), sa_ = Math.sin(a); id.push([]);
    for (let j = 0; j < sb; j++) { const b = j / sb * Math.PI * 2, cb = Math.cos(b), sb_ = Math.sin(b), rad = R + ry * cb;
      id[i].push(vert([c[0] + rx * sb_, c[1] + rad * ca, c[2] + rad * sa_], [sb_ / rx, cb * ca / ry, cb * sa_ / ry], o)); } }
  for (let i = 0; i < sa; i++) for (let j = 0; j < sb; j++) { const i2 = (i + 1) % sa, j2 = (j + 1) % sb; quad(id[i][j], id[i2][j], id[i2][j2], id[i][j2]); }
}
function ellipsoid(c, r, o) {
  o = S(o); const sa = o.seg || 12, sb = Math.max(4, sa / 2 | 0), id = [];
  for (let i = 0; i <= sb; i++) { const t = i / sb * Math.PI; id.push([]); for (let j = 0; j < sa; j++) { const a = j / sa * Math.PI * 2, n = [Math.sin(t) * Math.cos(a), Math.cos(t), Math.sin(t) * Math.sin(a)]; id[i].push(vert([c[0] + n[0] * r[0], c[1] + n[1] * r[1], c[2] + n[2] * r[2]], [n[0] / r[0], n[1] / r[1], n[2] / r[2]], o)); } }
  for (let i = 0; i < sb; i++) for (let j = 0; j < sa; j++) quad(id[i][j], id[i + 1][j], id[i + 1][(j + 1) % sa], id[i][(j + 1) % sa]);
}
function prism(pf, x0, x1, o) {                            // a side profile [[z, y], ...] (star-shaped) pushed across from x0 to x1: seats, panels, fenders, decks
  o = S(o); const n = pf.length, cz = pf.reduce((s, p) => s + p[0], 0) / n, cy = pf.reduce((s, p) => s + p[1], 0) / n;
  for (const [x, nx] of [[x1, 1], [x0, -1]]) { const c = vert([x, cy, cz], [nx, 0, 0], o), ids = pf.map(p => vert([x, p[1], p[0]], [nx, 0, 0], o)); for (let i = 0; i < n; i++) tri(c, ids[i], ids[(i + 1) % n]); }
  for (let i = 0; i < n; i++) {
    const a = pf[i], b = pf[(i + 1) % n], ez = b[0] - a[0], ey = b[1] - a[1]; let nz = ey, ny = -ez; const mz = (a[0] + b[0]) / 2 - cz, my = (a[1] + b[1]) / 2 - cy; if (nz * mz + ny * my < 0) { nz = -nz; ny = -ny; }
    const ids = [vert([x0, a[1], a[0]], [0, ny, nz], o), vert([x1, a[1], a[0]], [0, ny, nz], o), vert([x1, b[1], b[0]], [0, ny, nz], o), vert([x0, b[1], b[0]], [0, ny, nz], o)]; quad(ids[0], ids[1], ids[2], ids[3]);
  }
}
function slab(p0, p1, w, th, o, cx) {                      // a flat strip along a line in the side view [y, z], w wide
  const d = [0, p1[0] - p0[0], p1[1] - p0[1]], L = Math.hypot(d[1], d[2]); box([cx || 0, (p0[0] + p1[0]) / 2, (p0[1] + p1[1]) / 2], [w, th, L], Object.assign({ rot: [Math.atan2(-d[1], d[2]), 0, 0] }, o || {}));
}
function arc(cy, cz, r, a0, a1, n) { const pts = []; for (let i = 0; i <= n; i++) { const a = a0 + (a1 - a0) * i / n; pts.push([cy + r * Math.cos(a), cz + r * Math.sin(a)]); } return pts; }   // a is measured from straight up, towards +z
// ---------------------------------------------------------------- wheels (centred on x = 0, y = radius)
const RUBBER = [24, 24, 27], METAL = [185, 188, 196], DARKM = [60, 62, 68];
function wheel(z, R, tw, part, o) {                        // R outer radius, tw tyre half-width; o: rim fraction, spokes, solid, discR
  o = o || {}; const cy = R, ry = o.section || tw * 1.1, rimR = R - 2 * ry - 0.005 + (o.rimUp || 0);
  torus([0, cy, z], R - ry, tw, ry, S({ part, cls: 4, col: RUBBER, segA: 32, segB: 10 }));
  torus([0, cy, z], rimR, 0.012, 0.012, S({ part, cls: 5, col: o.rimCol || METAL, segA: 32, segB: 6 }));
  const hubR = o.hubR || 0.03;
  cyl([-(o.hubW || 0.05), cy, z], [(o.hubW || 0.05), cy, z], hubR, hubR, S({ part, cls: 5, col: o.hubCol || METAL, seg: 12 }));
  if (o.solid) {                                         // a cast wheel: a dished disc with a few fat spokes showing
    cyl([-0.012, cy, z], [0.012, cy, z], rimR - 0.004, rimR - 0.004, S({ part, cls: 7, col: o.discCol || DARKM, seg: 24 }));
    for (let k = 0; k < 5; k++) { const a = k / 5 * Math.PI * 2; tube([0.016, cy, z], [0.016, cy + Math.cos(a) * (rimR - 0.01), z + Math.sin(a) * (rimR - 0.01)], 0.012, S({ part, cls: 5, col: o.rimCol || METAL, seg: 5 })); }
  } else {
    const n = o.spokes || 32;
    for (let k = 0; k < n; k++) { const a = k / n * Math.PI * 2, s = (k % 2 ? 1 : -1) * 0.014; tube([s * 0.6, cy + Math.cos(a) * hubR, z + Math.sin(a) * hubR], [-s * 0.3, cy + Math.cos(a) * rimR, z + Math.sin(a) * rimR], 0.0035, S({ part, cls: 5, col: [150, 152, 160], seg: 4, caps: false })); }
  }
  if (o.disc) cyl([o.discX || 0.035, cy, z], [(o.discX || 0.035) + 0.004, cy, z], o.disc, o.disc, S({ part, cls: 5, col: [120, 122, 130], seg: 20 }));
}
// ---------------------------------------------------------------- electric dirt bikes (Sur-Ron Light Bee X, EridePro SS 3.0, Yozma IN10, 72V Razor)
function dirtbike(o) {
  const wb = o.wb, zf = -wb / 2, zr = wb / 2, rF = o.rF, rR = o.rR, sy = o.seat / 0.83, sz = wb / 1.255;   // the frame is drawn for a Sur-Ron and stretched to fit
  const P = (x, y, z) => [x * (o.sx || 1), y * sy, z * sz], pl = o.plastic, fr = o.frame || [38, 38, 42], FRC = o.frameMetal ? { cls: 5, col: fr } : { cls: 7, col: fr };
  // wheels
  wheel(zf, rF, o.twF, 1, { spokes: 36, disc: 0.1, section: o.twF * 1.15 });
  wheel(zr, rR, o.twR, 3, { spokes: 36, disc: 0.095, discX: -0.04, section: o.twR * 1.15 });
  // fork, bars
  const rake = o.rake || 0.47, topY = o.bar - 0.09, Lf = (topY - rF) / Math.cos(rake), top = [0, rF + Lf * Math.cos(rake), zf + Lf * Math.sin(rake)], axle = [0, rF, zf];
  for (const x of [-0.085, 0.085]) {
    const a = [x, rF, zf], b = [x, top[1], top[2]], mid = add(a, mul(sub(b, a), 0.5));
    tube(a, mid, 0.0185, S({ cls: 5, col: o.forkLower || [196, 160, 50], seg: 8 })); tube(mid, b, 0.026, S({ cls: 7, col: [28, 28, 32], seg: 8 }));
  }
  tube([-0.1, top[1] - 0.02, top[2]], [0.1, top[1] - 0.02, top[2]], 0.02, S({ cls: 5, col: [150, 152, 158], seg: 8 }));            // the triple clamp
  const bz = top[2] + 0.03, by = top[1] + 0.07, bw = o.barW / 2;
  tube([-bw * 0.3, by, bz], [bw * 0.3, by, bz], 0.013, S({ cls: 5, col: [160, 162, 168], seg: 8 }));
  for (const sgn of [-1, 1]) { tube([sgn * bw * 0.3, by, bz], [sgn * bw * 0.78, by + 0.02, bz + 0.05], 0.012, S({ cls: 5, col: [160, 162, 168], seg: 8 })); tube([sgn * bw * 0.78, by + 0.02, bz + 0.05], [sgn * bw, by + 0.025, bz + 0.07], 0.0185, S({ cls: 7, col: [20, 20, 22], seg: 8 }));
    tube([sgn * bw * 0.62, by + 0.02, bz + 0.03], [sgn * bw * 0.62, by + 0.02, bz - 0.07], 0.007, S({ cls: 5, col: [200, 200, 205], seg: 5 })); }   // brake and clutch levers
  tube([0, top[1], top[2]], [0, by, bz], 0.02, S({ cls: 5, col: [150, 152, 158], seg: 8 }));
  box([0, by - 0.03, bz + 0.02], [0.1, 0.05, 0.05], S({ cls: 7, col: [25, 25, 28] }));                                                // the display
  // number plate and headlight at the front
  const hz = top[2] - 0.06, hy = rF + Lf * 0.78 * Math.cos(rake) + 0.04;
  box([0, hy, hz - 0.04 * 0], [0.2, 0.2, 0.025], S({ rot: [-rake, 0, 0], cls: 0, col: pl }));
  box([0, hy + 0.06, hz - 0.02], [0.09, 0.06, 0.02], S({ rot: [-rake, 0, 0], cls: 2, col: [255, 250, 225] }));
  // front fender
  const fp = arc(rF, zf, rF + 0.05, -1.5, 0.45, 7); for (let i = 0; i < fp.length - 1; i++) slab(fp[i], fp[i + 1], 0.11, 0.012, S({ cls: 0, col: pl }));
  // the frame: head tube, twin spars, battery/tank, swingarm, shock
  const head = [0, top[1] - 0.05, top[2] + 0.04], sp = P(0, 0.80, 0.42), pivot = P(0, 0.40, 0.30), low = P(0, 0.33, -0.12), mot = P(0, 0.42, 0.10);
  tube([0, top[1] - 0.17, top[2] + 0.08], [0, top[1] + 0.0, top[2] - 0.005], 0.03, S(FRC));
  for (const x of [-0.07, 0.07]) {
    tube([x, head[1] - 0.02, head[2] + 0.02], [x, sp[1] - 0.04, sp[2] - 0.12], 0.024, S(FRC));
    tube([x, head[1] - 0.1, head[2] + 0.04], [x, low[1] + 0.02, low[2]], 0.024, S(FRC));
    tube([x, low[1] + 0.02, low[2]], [x, pivot[1], pivot[2] - 0.03], 0.022, S(FRC));
    tube([x, sp[1] - 0.04, sp[2] - 0.12], [x, pivot[1] + 0.06, pivot[2] - 0.04], 0.02, S(FRC));
    tube([x * 1.05, pivot[1], pivot[2]], [x * 1.05, rR, zr], 0.022, S({ cls: 5, col: o.swing || METAL, seg: 8 }));                 // swingarm
  }
  tube([-0.075, pivot[1], pivot[2]], [0.075, pivot[1], pivot[2]], 0.026, S({ cls: 5, col: METAL, seg: 8 }));
  tube([-0.075, rR, zr], [0.075, rR, zr], 0.02, S({ cls: 5, col: METAL, seg: 8 }));                                                // the axle
  tube(P(0, 0.80, 0.34), P(0, 0.47, 0.44), 0.025, S({ cls: 5, col: [210, 170, 40], seg: 8 }));                                    // the rear shock
  cyl(P(0, 0.80, 0.34), P(0, 0.62, 0.395), 0.03, 0.03, S({ cls: 7, col: [30, 30, 34], seg: 8 }));
  if (o.battery !== false) { const bw = o.batW || 0.17; box(P(0, 0.58, 0.04), [bw, 0.44 * sy, 0.52 * sz], S(FRC.cls === 5 ? { cls: 5, col: [150, 154, 162] } : { cls: 7, col: [48, 50, 56] })); box(P(0, 0.58, 0.04), [bw + 0.012, 0.3 * sy, 0.36 * sz], S({ cls: 7, col: [26, 26, 30] })); }
  cyl([-0.075, mot[1], mot[2]], [0.075, mot[1], mot[2]], 0.1 * Math.min(1.3, sy), 0.1 * Math.min(1.3, sy), S({ cls: 5, col: [105, 108, 116], seg: 14 }));                  // the motor
  box(P(0, 0.34, 0.20), [0.16, 0.1, 0.34 * sz], S({ cls: 7, col: [32, 32, 36] }));                                                  // skid plate and controller
  // panels, seat, tail
  const sy0 = o.seat;
  prism([[sz * -0.18, sy0 - 0.05], [sz * -0.14, sy0 + 0.01], [sz * 0.30, sy0 + 0.02], [sz * 0.66, sy0 + 0.04], [sz * 0.68, sy0 - 0.01], [sz * 0.40, sy0 - 0.07]], -0.1, 0.1, S({ cls: 6, col: [26, 26, 29] }));   // the seat
  prism([[sz * -0.30, sy0 + 0.05], [sz * -0.12, sy0 + 0.1], [sz * 0.05, sy0 - 0.02], [sz * -0.04, sy0 - 0.26], [sz * -0.26, sy0 - 0.2]], -0.105, 0.105, S({ cls: 0, col: pl }));                                   // shrouds around the battery
  prism([[sz * 0.28, sy0 - 0.02], [sz * 0.72, sy0 + 0.02], [sz * 0.92, sy0 - 0.06], [sz * 0.9, sy0 - 0.1], [sz * 0.45, sy0 - 0.1]], -0.075, 0.075, S({ cls: 0, col: pl }));                                              // the tail panel
  box([0, sy0 - 0.07, zr + sz * 0.3], [0.07, 0.03, 0.025], S({ cls: 3, col: [200, 20, 20] }));
  for (const x of [-0.13, 0.13]) tube([x - 0.04, 0.3 * sy, zr * 0.2], [x + 0.04, 0.3 * sy, zr * 0.2], 0.012, S({ cls: 5, col: [190, 190, 196], seg: 6 }));   // footpegs
  if (o.chain) {                                                                                                                     // chain drive from the motor sprocket back to the wheel
    tube([0.095, mot[1], mot[2]], [0.095, rR, zr], 0.008, S({ cls: 7, col: [50, 50, 54], seg: 5, caps: false }));
    tube([0.095, mot[1] - 0.045, mot[2]], [0.095, rR - 0.045, zr], 0.008, S({ cls: 7, col: [50, 50, 54], seg: 5, caps: false }));
    cyl([0.09, rR, zr], [0.1, rR, zr], 0.05, 0.05, S({ cls: 5, col: [160, 160, 168], seg: 12 }));
  }
  if (o.extraBattery) box(P(0, 0.62, 0.55), [0.14, 0.14, 0.3], S({ cls: 7, col: [34, 34, 38] }));
  return { wheels: { FL: [0, rF, zf], FR: [0, rF, zf], RL: [0, rR, zr], RR: [0, rR, zr] }, parts: {} };
}
// ---------------------------------------------------------------- the Kukirin G4: a stand-up scooter with 11 inch off-road tyres and suspension at both ends
function scooter(o) {
  const wb = o.wb, zf = -wb / 2, zr = wb / 2, R = o.R, tw = o.tw, pl = o.plastic;
  wheel(zf, R, tw, 1, { solid: true, hubR: 0.045, hubW: 0.075, section: tw * 1.25, discCol: [40, 42, 46], rimCol: [200, 110, 30] });
  wheel(zr, R, tw, 3, { solid: true, hubR: 0.045, hubW: 0.075, section: tw * 1.25, discCol: [40, 42, 46], rimCol: [200, 110, 30] });
  const deckY = o.deckY, dz0 = zf + 0.2, dz1 = zr - 0.18;
  prism([[dz0, deckY - 0.045], [dz0, deckY + 0.02], [dz1, deckY + 0.02], [dz1, deckY - 0.045]], -0.11, 0.11, S({ cls: 0, col: [34, 34, 38] }));        // the deck (the battery sits inside it)
  prism([[dz0 + 0.02, deckY + 0.02], [dz0 + 0.02, deckY + 0.032], [dz1 - 0.02, deckY + 0.032], [dz1 - 0.02, deckY + 0.02]], -0.095, 0.095, S({ cls: 6, col: [20, 20, 22] }));   // grip tape
  // front: a stem that leans back to the rider, a hinge, bars and a display
  const stemB = [0, deckY + 0.02, zf + 0.22], stemT = [0, o.bar - 0.06, zf + 0.30];
  tube(stemB, stemT, 0.024, S({ cls: 0, col: [36, 36, 40], seg: 10 }));
  tube([-0.06, stemB[1] + 0.12, stemB[2] + 0.01], [0.06, stemB[1] + 0.12, stemB[2] + 0.01], 0.018, S({ cls: 5, col: [210, 120, 35], seg: 8 }));      // the folding clamp
  for (const x of [-0.05, 0.05]) {                                                                                                          // the fork and its spring
    tube([x, R, zf], [x, deckY + 0.04, zf + 0.2], 0.016, S({ cls: 5, col: [210, 120, 35], seg: 8 }));
    cyl([x, R + 0.06, zf + 0.012], [x, R + 0.2, zf + 0.06], 0.022, 0.022, S({ cls: 5, col: [150, 152, 160], seg: 8 }));
  }
  const ba = [stemT[0], stemT[1] + 0.02, stemT[2] + 0.02], bw = o.barW / 2;
  tube([-bw, ba[1], ba[2]], [bw, ba[1], ba[2]], 0.012, S({ cls: 5, col: [180, 182, 188], seg: 8 }));
  for (const sgn of [-1, 1]) { tube([sgn * bw * 0.72, ba[1], ba[2]], [sgn * bw, ba[1], ba[2] + 0.01], 0.0185, S({ cls: 7, col: [20, 20, 22], seg: 8 })); tube([sgn * bw * 0.6, ba[1], ba[2]], [sgn * bw * 0.6, ba[1] + 0.005, ba[2] - 0.1], 0.006, S({ cls: 5, col: [210, 120, 35], seg: 5 })); }
  box([0, ba[1] + 0.03, ba[2] + 0.005], [0.12, 0.045, 0.08], S({ rot: [-0.5, 0, 0], cls: 7, col: [18, 18, 22] }));                       // the display
  box([0, ba[1] + 0.046, ba[2] - 0.012], [0.09, 0.012, 0.05], S({ rot: [-0.5, 0, 0], cls: 2, col: [120, 230, 120] }));
  box([0, stemT[1] - 0.04, stemT[2] - 0.045], [0.1, 0.07, 0.045], S({ cls: 2, col: [255, 250, 225] }));                                       // the headlight
  box([0, 0.2, zf + 0.03], [0.04, 0.012, 0.02], S({ cls: 7, col: [20, 20, 20] }));
  // rear: a swingarm with a spring, the brake light and the fenders
  for (const x of [-0.06, 0.06]) { tube([x, deckY - 0.01, zr - 0.2], [x, R, zr], 0.018, S({ cls: 5, col: [210, 120, 35], seg: 8 })); cyl([x, deckY + 0.14, zr - 0.18], [x, R + 0.04, zr - 0.04], 0.022, 0.022, S({ cls: 5, col: [150, 152, 160], seg: 8 })); }
  const ff = arc(R, zf, R + 0.04, -1.6, 0.6, 8); for (let i = 0; i < ff.length - 1; i++) slab(ff[i], ff[i + 1], 0.13, 0.012, S({ cls: 0, col: [34, 34, 38] }));
  const rf = arc(R, zr, R + 0.045, -0.6, 1.6, 8); for (let i = 0; i < rf.length - 1; i++) slab(rf[i], rf[i + 1], 0.14, 0.012, S({ cls: 0, col: [34, 34, 38] }));
  box([0, R + 0.09, zr + R + 0.02], [0.08, 0.025, 0.02], S({ cls: 3, col: [210, 25, 25] }));
  for (const x of [-0.13, 0.13]) box([x, deckY - 0.01, dz0 + 0.1], [0.012, 0.012, 0.18], S({ cls: 5, col: [210, 120, 35] }));               // orange side strips
  return { wheels: { FL: [0, R, zf], FR: [0, R, zf], RL: [0, R, zr], RR: [0, R, zr] }, parts: {} };
}
// ---------------------------------------------------------------- the Macfox X1S: a step-through fat tyre e-bike (20 x 4 inch), banana seat, suspension fork and a rear battery rack
function ebike(o) {
  const wb = o.wb, zf = -wb / 2, zr = wb / 2, R = o.R, tw = o.tw, pl = o.plastic;
  wheel(zf, R, tw, 1, { spokes: 32, hubR: 0.035, hubW: 0.075, section: tw * 1.1, disc: 0.09, rimCol: [200, 200, 205] });
  wheel(zr, R, tw, 3, { spokes: 32, hubR: 0.05, hubW: 0.08, section: tw * 1.1, rimCol: [200, 200, 205], disc: 0.09 });
  const rake = 0.40, topY = 0.98, Lf = (topY - R) / Math.cos(rake), top = [0, R + Lf * Math.cos(rake), zf + Lf * Math.sin(rake)];
  for (const x of [-0.07, 0.07]) { tube([x, R, zf], [x, R + Lf * 0.5 * Math.cos(rake), zf + Lf * 0.5 * Math.sin(rake)], 0.018, S({ cls: 5, col: [190, 192, 198], seg: 8 })); tube([x, R + Lf * 0.5 * Math.cos(rake), zf + Lf * 0.5 * Math.sin(rake)], [x, top[1], top[2]], 0.026, S({ cls: 0, col: pl, seg: 8 })); }
  tube([-0.08, top[1] - 0.01, top[2]], [0.08, top[1] - 0.01, top[2]], 0.02, S({ cls: 0, col: pl, seg: 8 }));
  const bz = top[2] + 0.04, by = top[1] + 0.12, bw = o.barW / 2;
  tube([0, top[1], top[2]], [0, by - 0.02, bz - 0.01], 0.022, S({ cls: 0, col: pl, seg: 8 }));
  tube([-bw * 0.25, by, bz], [bw * 0.25, by, bz], 0.013, S({ cls: 5, col: [190, 192, 198], seg: 8 }));
  for (const sgn of [-1, 1]) { tube([sgn * bw * 0.25, by, bz], [sgn * bw * 0.7, by + 0.03, bz + 0.1], 0.012, S({ cls: 5, col: [190, 192, 198], seg: 8 })); tube([sgn * bw * 0.7, by + 0.03, bz + 0.1], [sgn * bw, by + 0.035, bz + 0.14], 0.0185, S({ cls: 7, col: [20, 20, 22], seg: 8 })); tube([sgn * bw * 0.6, by + 0.03, bz + 0.08], [sgn * bw * 0.6, by + 0.03, bz - 0.04], 0.006, S({ cls: 5, col: [200, 200, 205], seg: 5 })); }
  box([0, by + 0.04, bz + 0.01], [0.09, 0.035, 0.05], S({ rot: [-0.4, 0, 0], cls: 7, col: [18, 18, 22] }));
  box([0, top[1] - 0.12, top[2] - 0.06], [0.09, 0.09, 0.06], S({ cls: 2, col: [255, 250, 225] }));                                                  // the headlight
  const fp = arc(R, zf, R + 0.045, -1.5, 0.5, 7); for (let i = 0; i < fp.length - 1; i++) slab(fp[i], fp[i + 1], 0.2, 0.012, S({ cls: 0, col: pl }));
  const rp = arc(R, zr, R + 0.045, -0.5, 1.5, 8); for (let i = 0; i < rp.length - 1; i++) slab(rp[i], rp[i + 1], 0.2, 0.012, S({ cls: 0, col: pl }));
  // the frame: a step-through down tube, seat tube, rear triangle and a big battery on the rack
  const hd = [0, top[1] - 0.1, top[2] + 0.03], bb = [0, 0.38, zr - 0.62], st = [0, 0.84, zr - 0.38], seatT = [0, 0.97, zr - 0.30];
  for (const x of [-0.035, 0.035]) tube([x, hd[1], hd[2]], [x, bb[1], bb[2]], 0.03, S({ cls: 0, col: pl, seg: 8 }));
  tube(bb, st, 0.028, S({ cls: 0, col: pl, seg: 8 })); tube(st, seatT, 0.022, S({ cls: 5, col: [190, 192, 198], seg: 8 }));
  for (const x of [-0.07, 0.07]) { tube([x, bb[1] + 0.02, bb[2]], [x, R, zr], 0.018, S({ cls: 0, col: pl, seg: 8 })); tube([x, st[1] - 0.02, st[2]], [x, R + 0.02, zr], 0.016, S({ cls: 0, col: pl, seg: 8 })); }
  tube([-0.12, R, zr], [0.12, R, zr], 0.02, S({ cls: 5, col: [190, 192, 198], seg: 8 }));
  cyl([-0.06, bb[1], bb[2]], [0.06, bb[1], bb[2]], 0.045, 0.045, S({ cls: 5, col: [170, 172, 180], seg: 10 }));                                      // the bottom bracket
  cyl([0.07, bb[1], bb[2]], [0.077, bb[1], bb[2]], 0.09, 0.09, S({ cls: 5, col: [180, 182, 190], seg: 16 }));                                         // chainring
  tube([-0.12, bb[1] - 0.05, bb[2] - 0.025], [-0.12, bb[1] - 0.2, bb[2] - 0.04], 0.007, S({ cls: 7, col: [30, 30, 34], seg: 5 })); box([-0.15, bb[1] - 0.2, bb[2] - 0.04], [0.07, 0.015, 0.1], S({ cls: 7, col: [30, 30, 34] }));
  tube([0.12, bb[1] + 0.05, bb[2] + 0.025], [0.12, bb[1] + 0.2, bb[2] + 0.04], 0.007, S({ cls: 7, col: [30, 30, 34], seg: 5 })); box([0.15, bb[1] + 0.2, bb[2] + 0.04], [0.07, 0.015, 0.1], S({ cls: 7, col: [30, 30, 34] }));
  tube([0.06, bb[1], bb[2]], [0.06, R, zr], 0.006, S({ cls: 7, col: [40, 40, 44], seg: 4, caps: false }));                                          // the chain
  prism([[zr - 0.74, 1.0], [zr - 0.62, 1.04], [zr - 0.1, 0.98], [zr + 0.14, 0.93], [zr + 0.1, 0.87], [zr - 0.4, 0.86], [zr - 0.7, 0.9]], -0.11, 0.11, S({ cls: 6, col: [28, 28, 32] }));   // the long banana seat
  box([0, 0.78, zr - 0.18], [0.12, 0.17, 0.5], S({ cls: 0, col: pl }));                                                                              // the battery pack under the seat
  box([0, 0.9, zr + 0.18], [0.16, 0.025, 0.14], S({ cls: 7, col: [30, 30, 34] }));
  box([0, 0.7, zr + 0.34], [0.07, 0.03, 0.025], S({ cls: 3, col: [210, 25, 25] }));
  tube([0.06, R + 0.19, zr - 0.35], [0.06, R + 0.02, zr - 0.02], 0.02, S({ cls: 5, col: [200, 110, 30], seg: 8 }));                                    // a rear shock
  return { wheels: { FL: [0, R, zf], FR: [0, R, zf], RL: [0, R, zr], RR: [0, R, zr] }, parts: {} };
}
// ---------------------------------------------------------------- the six, sized from published specs
const RIDES = {
  surron:  () => dirtbike({ wb: 1.255, rF: 0.31, rR: 0.305, twF: 0.04, twR: 0.045, seat: 0.83, bar: 1.08, barW: 0.78, plastic: [40, 170, 85], frame: [150, 154, 162], frameMetal: true, swing: [195, 198, 205], batW: 0.18 }),
  eride:   () => dirtbike({ wb: 1.3, rF: 0.311, rR: 0.305, twF: 0.036, twR: 0.045, seat: 0.86, bar: 1.12, barW: 0.80, plastic: [235, 85, 25], frame: [34, 34, 38], batW: 0.2, extraBattery: true }),
  yozma:   () => dirtbike({ wb: 1.16, rF: 0.25, rR: 0.25, twF: 0.05, twR: 0.055, seat: 0.71, bar: 0.97, barW: 0.72, plastic: [215, 35, 40], frame: [38, 38, 42], batW: 0.2, sx: 1.05, rake: 0.5 }),
  razor:   () => dirtbike({ wb: 1.05, rF: 0.2, rR: 0.19, twF: 0.032, twR: 0.034, seat: 0.74, bar: 1.02, barW: 0.71, plastic: [245, 125, 20], frame: [28, 28, 32], batW: 0.2, chain: true, extraBattery: true, rake: 0.5, forkLower: [190, 190, 196] }),
  kukirin: () => scooter({ wb: 1.04, R: 0.14, tw: 0.045, deckY: 0.19, bar: 1.235, barW: 0.66, plastic: [34, 34, 38] }),
  macfox:  () => ebike({ wb: 1.2, R: 0.33, tw: 0.05, barW: 0.69, plastic: [32, 32, 38] })
};
const FRIENDLY = { surron: 'Sur-Ron Light Bee X', eride: 'EridePro SS 3.0', yozma: 'Yozma IN10', razor: '72V Modded Razor', kukirin: 'Kukirin G4', macfox: 'Macfox X1S' };
function build(id) {
  V = []; T = []; ST = { part: 0, cls: 0, col: [200, 200, 200] };
  const info = RIDES[id](), wheels = info.wheels;
  // order: opaque first, glass last (none here), triangles grouped by class so the ranges are tidy
  T.sort((a, b) => (a[3] === 1) - (b[3] === 1) || a[3] - b[3]);
  // centre: x = 0 already; z so the wheel centres are balanced
  const zc = (wheels.FL[2] + wheels.RL[2]) / 2; if (Math.abs(zc) > 1e-9) { V.forEach(v => { v.p[2] -= zc; }); for (const k of Object.keys(wheels)) wheels[k][2] -= zc; }
  const mn = [1e9, 1e9, 1e9], mx = [-1e9, -1e9, -1e9]; V.forEach(v => { for (let k = 0; k < 3; k++) { mn[k] = Math.min(mn[k], v.p[k]); mx[k] = Math.max(mx[k], v.p[k]); } });
  const pad = 1e-4, buf = Buffer.alloc(V.length * 16 + T.length * 6);
  V.forEach((v, i) => { const o = i * 16; for (let k = 0; k < 3; k++) buf.writeUInt16LE(Math.max(0, Math.min(65535, Math.round((v.p[k] - mn[k]) / (mx[k] - mn[k] + pad) * 65535))), o + k * 2);
    for (let k = 0; k < 3; k++) buf.writeInt8(Math.max(-127, Math.min(127, Math.round(v.n[k] * 127))), o + 6 + k); buf.writeUInt8(v.part, o + 9); for (let k = 0; k < 3; k++) buf.writeUInt8(v.col[k], o + 10 + k); buf.writeUInt8(v.cls, o + 13); });
  const ranges = []; T.forEach((t, i) => { const r = ranges[ranges.length - 1]; if (r && r.cls === t[3]) r.count += 3; else ranges.push({ cls: t[3], part: 0, first: i * 3, count: 3 }); });
  T.forEach((t, i) => { for (let k = 0; k < 3; k++) buf.writeUInt16LE(t[k], V.length * 16 + (i * 3 + k) * 2); });
  const W = mx[0] - mn[0], meta = { id, version: 1, verts: V.length, tris: T.length, min: mn, max: mx, ranges, parts: Object.fromEntries(Object.entries(wheels).map(([k, w]) => ['wheel' + k, w.map(x => +x.toFixed(3))])),
    wheels: Object.fromEntries(Object.entries(wheels).map(([k, w]) => [k, w.map(x => +x.toFixed(3))])), length: +(mx[2] - mn[2]).toFixed(3), width: +W.toFixed(3), height: +(mx[1] - mn[1]).toFixed(3), noseZ: +mn[2].toFixed(3), tailZ: +mx[2].toFixed(3) };
  const mj = Buffer.from(JSON.stringify(meta)), head = Buffer.alloc(8); head.writeUInt32LE(0x4D444C31, 0); head.writeUInt32LE(mj.length, 4);
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, id + '.bin'), Buffer.concat([head, mj, Buffer.alloc((4 - mj.length % 4) % 4), buf]));
  console.log(FRIENDLY[id].padEnd(22), V.length + ' verts', T.length + ' tris', Math.round(buf.length / 1024) + ' KB', 'L ' + meta.length + ' W ' + meta.width + ' H ' + meta.height);
}
for (const id of Object.keys(RIDES)) build(id);
