// 二期聚合器 v1：定时取悟道简报 → 摊平成现行 27 键契约 → 提交成静态 JSON。
// 慢件（Pipeworx 板块/涨停池/龙虎榜）在同一次运行里顺手落缓存，供 #15 扩槽那轮直接读。
// 本地干跑（零 key、零网络，用已落盘的悟道原文验摊平）：
//   node aggregate.mjs --local --type closing --wd-file ../2026-10-20/_probe/wd_closing_20261020.txt

const argv = process.argv.slice(2);
const arg = (name, dflt) => {
  const i = argv.indexOf('--' + name);
  return i >= 0 && argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : (argv.includes('--' + name) ? true : dflt);
};

const WUDAO_URL = 'https://stock.quicktiny.cn/api/mcp';
const PIPEWORX_URL = 'https://gateway.pipeworx.io/china-stocks/mcp';
const PERIOD_NAME = { opening: '盘前简报', midday: '午盘简报', closing: '收盘简报', evening: '晚间简报' };
const OUT = arg('out', 'data');

const cstNow = () => new Date(Date.now() + 8 * 3600 * 1000);
const cstDate = () => cstNow().toISOString().slice(0, 10);

// CST 时刻挑「最近已生成的一期」；上游标称 09:00/12:00/15:30/21:00，只有 15:30 那次实测收到过。
function guessType() {
  const t = cstNow().toISOString().slice(11, 16);
  if (t >= '21:00') return 'evening';
  if (t >= '15:30') return 'closing';
  if (t >= '12:00') return 'midday';
  if (t >= '09:00') return 'opening';
  return 'closing';
}

// MCP over Streamable HTTP。完成判据只认 JSON.parse 成功拿到 result/error：
// 上游不关流，按 "result" 文本出现与否判会测出假完成（10-20 踩过两次）。
async function mcpCall(url, name, args, capMs, headers) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), capMs);
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json, text/event-stream', ...headers },
      body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }),
      signal: ac.signal
    });
    const ct = res.headers.get('content-type') || '';
    if (ct.includes('json')) return await res.json();
    const reader = res.body.getReader(), td = new TextDecoder();
    let buf = '';
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += td.decode(value, { stream: true });
      for (const line of buf.split('\n')) {
        if (!line.startsWith('data: ')) continue;
        try {
          const o = JSON.parse(line.slice(6));
          if (o.result || o.error) return o;
        } catch (e) { /* 片段还不是完整 JSON，继续读 */ }
      }
    }
    throw new Error('未收到完整出参（' + capMs + 'ms 内）');
  } finally {
    clearTimeout(timer);
  }
}

const textOf = (rpc) => {
  const r = rpc && rpc.result;
  if (!r) throw new Error('出参里没有 result：' + JSON.stringify(rpc && rpc.error).slice(0, 200));
  const t = r.content && r.content[0] && r.content[0].text;
  if (!t) throw new Error('result.content[0].text 为空');
  try { return JSON.parse(t); } catch (e) { return t; }
};

// 平台 toolCall 回放与直连 MCP 两种形状都吃
function pickItem(payload) {
  const p = typeof payload === 'string' ? JSON.parse(payload) : payload;
  if (p && p.data && Array.isArray(p.data.items)) return p.data.items[0] || null;
  if (p && Array.isArray(p.items)) return p.items[0] || null;
  if (p && p.marketSummary) return p;
  return null;
}

function splitSentence(s) {
  const m = s.match(/^[^，、]*[，、]/);
  return m ? { t: s.slice(0, m[0].length - 1), s: s.slice(m[0].length) } : { t: s, s: '' };
}

// 屏的容量档：主标题 18px 单行 ≈15 字、副标题 16px 单行 ≈17 字（全角算 1、半角 2 算 1）。
function wlen(s) {
  let full = 0, half = 0;
  for (const ch of String(s || '')) { if (/[\x00-\xff]/.test(ch)) half++; else full++; }
  return full + Math.ceil(half / 2);
}
const CAP = { t: 15, s: 17 };

// 摊法乙用的三件套：切段（保留原标点）→ 超长段从中间断（断点不落在数字/字母/百分号串里）→ 贪心合并回栏宽。
function pieces(text) {
  const out = [];
  for (const m of String(text).matchAll(/[^，、]+[，、]?/g)) {
    const sep = (m[0].match(/[，、]$/) || [''])[0];
    const txt = m[0].slice(0, m[0].length - sep.length).trim();
    if (txt) out.push({ txt, sep });
  }
  return out;
}

// 断点取「尽量装满栏宽」的前缀，再往回退到数串边界（不把 11 / 75.3 这类数字与紧随其后的半角劈开），
// 这样 r6 那种 19 字主标题会断成「股票ETF已连续11个交易日」+「净流入（新闻7）」，而不是从数字中间切开。
function splitLong(txt, cap) {
  const cs = [...txt];
  let acc = 0, i = 0;
  for (; i < cs.length; i++) {
    const w = /[\x00-\xff]/.test(cs[i]) ? 0.5 : 1;
    if (acc + w > cap) break;
    acc += w;
  }
  while (i > 1 && i < cs.length && /[\x00-\xff]/.test(cs[i - 1]) && /[\x00-\xff]/.test(cs[i])) i--;
  // 别把数字和单位拆开：前半以数字收尾、后半以汉字起头时（「…创近9 / 个月天量」），整段数字挪到后半。
  while (i > 0 && i < cs.length && /\d/.test(cs[i - 1]) && !/[\x00-\xff]/.test(cs[i])) i--;
  if (i <= 0 || i >= cs.length) return [txt, ''];
  return [cs.slice(0, i).join(''), cs.slice(i).join('')];
}

// seq 非空 = 这条素材的首行（带序号、占主标题位）；续行的字放哪一档由 fill 决定：
//   'sub'  = 续行只写副标题位（17 字档，16px 小字），主位留空 ⇒ 需要模板把 a:if 放宽成 {{rNt || rNs}}
//   'main' = 续行当主标题写（15 字档，18px），不动模板就能画，只是和正经标题同字号
function rowsOf(text, seq, fill) {
  const ps = pieces(text);
  if (!ps.length) return [];
  let head = ps.shift();
  while (wlen(head.txt) > CAP.t) {
    const [a, b] = splitLong(head.txt, CAP.t);
    head = { txt: a, sep: '' };
    ps.unshift({ txt: b, sep: '' });
  }
  const rows = [{ n: seq, t: head.txt, s: '' }];
  let buf = '', bufSep = '';
  const flush = () => {
    if (!buf) return;
    if (!rows[0].s) rows[0].s = buf;
    else rows.push(fill === 'main' ? { n: '', t: buf, s: '' } : { n: '', t: '', s: buf });
    buf = ''; bufSep = '';
  };
  // 首行的副标题位永远是 17 字档；后面的续行按 fill 决定用 17（副位）还是 15（主位）
  const limit = () => (rows[0].s === '' ? CAP.s : fill === 'main' ? CAP.t : CAP.s);
  for (const p of ps) {
    let t = p.txt;
    if (buf && wlen(buf + bufSep + t) > limit()) flush();
    while (wlen(t) > limit()) { const [a, b] = splitLong(t, limit()); buf = a; flush(); t = b; }
    buf = buf ? buf + bufSep + t : t;
    bufSep = p.sep;
  }
  flush();
  return rows;
}

// 一屏两栏 × 3 行 ⇒ 四个槽箱：左页1(r1–r3)、右页1(r4–r6)、左页2(r7)、右页2(r8)。
// 配额按「总行数在两栏间配平」算，不是先填满左栏：3 条摊成 2+1，8 条摊成 3+3+1+1。
function caps(total) {
  const t = Math.min(total, 8);
  if (t <= 6) return [Math.ceil(t / 2), t - Math.ceil(t / 2), 0, 0];
  const rem = t - 6;
  return [3, 3, Math.ceil(rem / 2), rem - Math.ceil(rem / 2)];
}

// 一个话题的续行只待在它开头那一栏里，装不下就整段丢——绝不留到下一栏（那正是「一条话跨栏」）。
function packGroups(groups, title, meta) {
  const BIN = [[1, 2, 3], [4, 5, 6], [7], [8]];
  const cap = caps(groups.reduce((a, g) => a + g.length, 0));
  const slot = {};
  let bi = 0;
  for (const g of groups) {
    while (bi < 4 && Object.keys(slot).filter(k => BIN[bi].includes(+k)).length >= cap[bi]) bi++;
    if (bi >= 4) break;
    for (const r of g) {
      const used = BIN[bi].filter(i => slot[i]).length;
      if (used >= cap[bi]) break;
      slot[BIN[bi][used]] = r;
    }
  }
  const kept = [];
  for (let i = 1; i <= 8; i++) if (slot[i]) kept.push({ i, r: slot[i] });
  const out = {};
  let k = 0;
  for (const x of kept) if (x.r.n === HEAD) x.r.n = String(++k);
  out.title = title;
  out.meta = meta;
  for (let i = 1; i <= 8; i++) {
    const r = slot[i];
    out['r' + i + 'n'] = r ? (r.n === undefined ? String(i) : r.n) : '';
    out['r' + i + 't'] = r ? r.t : '';
    out['r' + i + 's'] = r ? r.s : '';
  }
  out.hasNext = (out.r7t || out.r7s || out.r8t || out.r8s) ? 'yes' : 'no';
  const over = kept.map(x =>
    (wlen(x.r.t) > CAP.t ? 'r' + x.i + 't 主 ' + wlen(x.r.t) + '/15 ' : '') +
    (x.r.s && wlen(x.r.s) > CAP.s ? 'r' + x.i + 's 副 ' + wlen(x.r.s) + '/17 ' : '')).join('');
  return { keys: out, rows: kept.length, topics: k, over };
}

function pack(rows, title, meta) {
  const kept = rows.slice(0, 8);
  const out = {};
  let k = 0;
  for (const r of kept) if (r.n === HEAD) r.n = String(++k);   // 续行不带号，所以号数跟着「素材」走
  out.title = title;
  out.meta = meta;
  for (let i = 1; i <= 8; i++) {
    const r = kept[i - 1];
    out['r' + i + 'n'] = r ? (r.n === undefined ? String(i) : r.n) : '';
    out['r' + i + 't'] = r ? r.t : '';
    out['r' + i + 's'] = r ? r.s : '';
  }
  // 第 1 页 r1–r6、第 2 页 r7–r8（模板 24 行的 on-key-next 只改页码游标）
  out.hasNext = (out.r7t || out.r7s || out.r8t || out.r8s) ? 'yes' : 'no';
  const over = kept.map((r, i) =>
    (wlen(r.t) > CAP.t ? 'r' + (i + 1) + 't 主 ' + wlen(r.t) + '/15 ' : '') +
    (r.s && wlen(r.s) > CAP.s ? 'r' + (i + 1) + 's 副 ' + wlen(r.s) + '/17 ' : '')).join('');
  return { keys: out, rows: kept.length, over };
}

const HEAD = '\u0000head';   // rowsOf 用它标「这条素材的首行」，由 pack 换成连续序号

// 一条素材一行（甲式：不改写、不拆段，超栏交给屏上省略号）
// 原文里的「（新闻2、4）」是上游给模型看的引用标记，不能上屏 ⇒ 取素材时一律摘掉。
const clean = (s) => String(s).replace(/[（(]\s*新闻[^）)]*[)）]/g, '').replace(/[，、；]+$/, '').trim();

function sourcesOf(it) {
  const cf = ((it.marketSummary && it.marketSummary.capitalFlow) || '').replace(/\n/g, '').trim();
  const sources = [];
  if (cf) for (const s of cf.split('。').map(clean).filter(Boolean)) sources.push(s);
  else for (const s of (it.coreSummary || '').replace(/\n/g, '').split(/[。；]/).map(clean).filter(Boolean)) sources.push(s);
  // 10-21 用户裁 A：hotTopics 无论 capitalFlow 有没有都追加（原先只在 cf 非空时才带，
  // 而盘前那期 cf 恒空 ⇒ 四条题材名整段丢掉、8 个槽只填 3 个）。题材名是原文照抄，不是拼句。
  for (const h of (it.hotTopics || []).map(h => clean(h.title || '')).filter(Boolean)) sources.push(h);
  return sources;
}

// 现行一期口径（甲：照搬悟道原文，第一个逗号前当主标题、剩下整段当副标题）
function flatten(it) {
  const rows = sourcesOf(it).map(s => splitSentence(s));
  const p = pack(rows, titleOf(it), metaOf(it, Math.min(rows.length, 8)));
  p.chosen = 'jia（每条一行、副标题整段照搬）';
  return p;
}

// 乙口径（10-21 用户裁：拆段贪心，只有素材首行带序号）。四档：
//   one  = **家默认**：一条素材只占一行（主标题 + 第一个 ≤17 字块）⇒ 屏上条数 = 话题数，绝不出现半句
//   main = 续行当主标题写（18px、主位有字）⇒ 一句不丢，但 8 行上限会把尾部话题整条挤掉（留给 --fill 手动 A/B）
//   sub  = 续行只写副标题位（16px 小字、主位留空）⇒ 主标题空的行被模板 a:if="{{rNt}}" 整行不画，等于丢内容，不可用
//   auto = 和 one 同义。Actions 那步传的是 `--fill auto`（yml 里的兜底值），所以「默认摊法」必须落在 auto 上才作数
function flattenYi(it, fill) {
  const src = sourcesOf(it);
  const groupsOf = (f) => src.map(s => rowsOf(s, HEAD, f));
  let groups, chosen = fill;
  if (fill === 'main' || fill === 'sub') groups = groupsOf(fill);
  else { groups = groupsOf('sub').map(g => [g[0]]); chosen = fill === 'auto' ? 'auto→one' : fill; }
  const p = packGroups(groups, titleOf(it), '');
  p.keys.meta = metaOf(it, p.topics);
  p.chosen = chosen;
  return p;
}

function titleOf(it) {
  const date = /^\d{4}-\d{2}-\d{2}$/.test(String(it.date)) ? it.date : '';
  return (PERIOD_NAME[it.type] || '简报') + (date ? ' · ' + date.slice(5) : '');
}
function metaOf(it, n) {
  return '共 ' + n + ' 条 · ' + (/^\d{2}:\d{2}$/.test(String(it.time)) ? it.time : '');
}

// 悟道空窗用的 B 式（Pipeworx 免 key）：**每行都是拼句，不是任何人的原文**，
// 所以只有拿不到悟道原文的那几段才走这里；句式钉成 `名称+数值+单位`，板块行用已定版的丙式。
function flattenPw(snap, sector) {
  const idx = {};
  for (const i of snap.indices || []) idx[i.cn_name] = i;
  const pct = (v) => (v >= 0 ? '+' : '') + v + '%';
  const yi = (v) => (v / 1e8).toFixed(1);
  const wan = (v) => (v / 1e12).toFixed(2);
  const rows = [];
  const sh = idx['上证指数'], szy = idx['深证成指'], cyb = idx['创业板指'], kc = idx['科创50'];
  if (sh) rows.push({ t: `沪指 ${sh.level}点 ${pct(sh.change_pct)}`,
    s: szy && cyb ? `深成指${pct(szy.change_pct)} 创业板${pct(cyb.change_pct)}` : '' });
  if (sh && szy) rows.push({ t: `两市成交${wan(sh.turnover_yuan + szy.turnover_yuan)}万亿`,
    s: kc ? `科创50 ${pct(kc.change_pct)}` : '' });
  if (snap.limit_up_count != null) rows.push({ t: `涨停${snap.limit_up_count}只 跌停${snap.limit_down_count}只`,
    s: /^\d{4}-\d{2}-\d{2}$/.test(String(snap.limit_counts_date)) && snap.limit_counts_date !== snap.as_of.slice(0, 10)
      ? `${snap.limit_counts_date.slice(5)}口径` : '' });
  // 丙式板块行：主「<行业> 净流入<x.x亿>」、副「占比<x.x%> · <MM-DD>口径」；data_as_of 是当天就不写日期段。
  const today = String(snap.as_of || '').slice(0, 10);
  for (const s of (sector && sector.sectors || []).slice(0, 5)) {
    const asOf = String(sector.data_as_of || sector.date || '');
    rows.push({ t: `${s.industry} 净流入${yi(s.net_inflow_cny)}亿`,
      s: `占比${(Math.round(s.net_inflow_ratio_pct * 10) / 10).toFixed(1)}%` +
         (asOf && asOf !== today ? ' · ' + asOf.slice(5) + '口径' : '') });
  }
  const asOf = String(snap.as_of || '');
  return pack(rows, '早盘速览 · ' + (asOf.slice(5, 10) || ''),
    '共 ' + Math.min(rows.length, 8) + ' 条 · ' + asOf.slice(11, 16));
}

async function main() {
  const fs = (await import('fs')).default;
  const source = arg('source', 'wudao');

  if (source === 'pipeworx') {
    const pf = arg('pw-file');
    let snap, sector;
    if (pf) {
      const pw = JSON.parse(fs.readFileSync(pf, 'utf8'));
      snap = JSON.parse(JSON.parse(pw.ashares_market_snapshot.payload).result.content[0].text);
      sector = JSON.parse(JSON.parse(pw.ashares_sector_capital_flow.payload).result.content[0].text);
    } else {
      snap = textOf(await mcpCall(PIPEWORX_URL, 'ashares_market_snapshot', {}, 180000));
      sector = textOf(await mcpCall(PIPEWORX_URL, 'ashares_sector_capital_flow', { limit: 20 }, 180000));
    }
    const p = flattenPw(snap, sector);
    fs.mkdirSync(OUT, { recursive: true });
    const name = arg('name', 'brief-open.json');
    fs.writeFileSync(OUT + '/' + name, JSON.stringify(p.keys, null, 2) + '\n');
    console.log(name + ' 写出：' + p.rows + ' 行、非空 ' + Object.values(p.keys).filter(v => v !== '').length + ' 键'
      + (p.over ? ' ｜ 超栏 ' + p.over : ' ｜ 无超栏'));
    return;
  }

  const local = arg('local', false);
  const type = arg('type', 'auto') === 'auto' ? guessType() : arg('type');

  let item;
  if (local) {
    const f = arg('wd-file');
    if (!f) throw new Error('--local 要配 --wd-file <悟道 briefings 落盘文件>');
    item = pickItem(JSON.parse(fs.readFileSync(f, 'utf8')));
  } else {
    const key = process.env.WUDAO_API_KEY;
    if (!key) throw new Error('缺 WUDAO_API_KEY（在仓库 Settings→Secrets 里填，别写进文件或提交）');
    item = pickItem(textOf(await mcpCall(WUDAO_URL, 'briefings',
      { type, detailLevel: 'standard', format: 'json' }, 20000,
      { Authorization: 'Bearer ' + key })));
  }
  if (!item) throw new Error('悟道没返回这一期的 items（' + type + '）：' + cstDate() + ' 未到生成点或上游停服');

  fs.mkdirSync(OUT + '/cache', { recursive: true });
  // 悟道原始包只在本地留档（给 --local 复摊用，省额度）；Actions 里不写，免得仓库天天多塞一份上游原文。
  if (!local && !process.env.GITHUB_ACTIONS) fs.writeFileSync(OUT + '/cache/briefings-' + type + '-' + cstDate() + '.json', JSON.stringify(item));
  const mode = arg('flatten', 'yi');
  const fill = arg('fill', 'one');
  const p = mode === 'jia' ? flatten(item) : flattenYi(item, fill);
  fs.writeFileSync(OUT + '/brief-' + item.type + '.json', JSON.stringify(p.keys, null, 2) + '\n');
  console.log('brief-' + item.type + '.json 写出（摊法 ' + mode + '，' + p.chosen + '），27 键（非空 '
    + Object.values(p.keys).filter(v => v !== '').length + '）'
    + ' ｜ ' + p.keys.title + ' ｜ ' + p.keys.meta + (p.over ? ' ｜ 超栏 ' + p.over : ' ｜ 无超栏'));
  if (arg('show', false)) {
    for (let i = 1; i <= 8; i++) {
      const n = p.keys['r' + i + 'n'], t = p.keys['r' + i + 't'], s = p.keys['r' + i + 's'];
      console.log('  r' + i + ' 号' + (n || '-').padEnd(3)
        + ' 主' + String(wlen(t)).padStart(2) + '/15 「' + t + '」'
        + ' 副' + String(wlen(s)).padStart(2) + '/17 「' + s + '」');
    }
    console.log('  hasNext=' + p.keys.hasNext);
  }

  if (local) return;

  // 慢件：只落缓存，绝不参与上面那份 27 键（现行页一帧都不该等它）。
  // billboard 必须显式传 date（不传会回跨日期混排的历史榜），这点和 briefings「一律不传 date」相反。
  const pw = (name, args, capMs) => mcpCall(PIPEWORX_URL, name, args, capMs, {})
    .then(r => {
      fs.writeFileSync(OUT + '/cache/' + name + '-' + cstDate() + '.json', JSON.stringify(textOf(r)));
      console.log(name + ' 已缓存');
    })
    .catch(e => { fs.writeFileSync(OUT + '/cache/' + name + '-' + cstDate() + '.failed', String(e.message).slice(0, 200)); console.log(name + ' 未拿到完整包：' + e.message); });
  await Promise.all([
    pw('ashares_sector_capital_flow', { limit: 20 }, 180000),
    pw('ashares_limit_up', { date: cstDate() }, 180000),
    pw('ashares_billboard', { date: cstDate(), limit: 30 }, 180000)
  ]);
}

main().catch(e => { console.error('FAIL: ' + e.message); process.exit(1); });
