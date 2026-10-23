// Builds the small, precompressed copy of index.html that the server sends (index.min.html + .br + .gz + a hash of the source).
// Run from the project folder:  node tools/build_index.js
// Needs: npm install terser clean-css   (in a temp folder; pass its node_modules path as NODE_PATH)
const fs = require('fs'), zlib = require('zlib'), crypto = require('crypto');
const { minify } = require('terser');
const CleanCSS = require('clean-css');
(async () => {
  const src = fs.readFileSync('index.html', 'utf8');
  const hash = crypto.createHash('md5').update(src).digest('hex');
  let out = src, jsBefore = 0, jsAfter = 0, cssBefore = 0, cssAfter = 0;
  const scripts = [...src.matchAll(/<script(?![^>]*\bsrc=)([^>]*)>([\s\S]*?)<\/script>/g)];
  for (const m of scripts) {
    if (/type\s*=\s*["']?(?!text\/javascript|module)/.test(m[1])) continue;     // leave JSON / template blocks alone
    const r = await minify(m[2], { compress: { passes: 1 }, mangle: { toplevel: false }, format: { comments: false } });
    if (r.error || !r.code) throw new Error('terser failed: ' + (r.error || 'empty'));
    jsBefore += m[2].length; jsAfter += r.code.length;
    out = out.replace(m[0], () => '<script' + m[1] + '>' + r.code + '</script>');
  }
  for (const m of [...src.matchAll(/<style([^>]*)>([\s\S]*?)<\/style>/g)]) {
    const r = new CleanCSS({ level: 1 }).minify(m[2]);
    if (r.errors.length) throw new Error('clean-css failed: ' + r.errors.join(';'));
    cssBefore += m[2].length; cssAfter += r.styles.length;
    out = out.replace(m[0], () => '<style' + m[1] + '>' + r.styles + '</style>');
  }
  const buf = Buffer.from(out, 'utf8');
  const br = zlib.brotliCompressSync(buf, { params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 11, [zlib.constants.BROTLI_PARAM_SIZE_HINT]: buf.length } });
  const gz = zlib.gzipSync(buf, { level: 9 });
  fs.writeFileSync('index.min.html', buf); fs.writeFileSync('index.min.html.br', br); fs.writeFileSync('index.min.html.gz', gz);
  fs.writeFileSync('index.min.meta.json', JSON.stringify({ source_md5: hash, min_bytes: buf.length, br_bytes: br.length, gz_bytes: gz.length }));
  console.log(JSON.stringify({ source: src.length, js: [jsBefore, jsAfter], css: [cssBefore, cssAfter], min: buf.length, gz: gz.length, br: br.length, original_gz: zlib.gzipSync(Buffer.from(src), { level: 9 }).length }));
})().catch(e => { console.error('BUILD FAILED', e.message); process.exit(1); });
