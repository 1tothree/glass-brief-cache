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
// 这里只用来在构建日志里报超栏，**不截断**：装不下的部分由屏上省略号收尾（用户 10-19 定）。
function wlen(s) {
  let full = 0, half = 0;
  for (const ch of String(s || '')) { if (/[\x00-\xff]/.test(ch)) half++; else full++; }
  return full + Math.ceil(half / 2);
}

function pack(rows, title, meta) {
  const kept = rows.slice(0, 8);
  const out = {};
  out.title = title;
  out.meta = meta;
  for (let i = 1; i <= 8; i++) {
    const r = kept[i - 1];
    out['r' + i + 'n'] = r ? String(i) : '';
    out['r' + i + 't'] = r ? r.t : '';
    out['r' + i + 's'] = r ? r.s : '';
  }
  out.hasNext = out.r7t ? 'yes' : 'no';
  const over = kept.map((r, i) =>
    (wlen(r.t) > 15 ? 'r' + (i + 1) + 't 主 ' + wlen(r.t) + '/15 ' : '') +
    (r.s && wlen(r.s) > 17 ? 'r' + (i + 1) + 's 副 ' + wlen(r.s) + '/17 ' : '')).join('');
  return { keys: out, rows: kept.length, over };
}

// 现行一期口径（A：照搬悟道原文，不改写、不数字数，超出部分交给屏上省略号）
function flatten(it) {
  const cf = ((it.marketSummary && it.marketSummary.capitalFlow) || '').replace(/\n/g, '').trim();
  const hot = (it.hotTopics || []).map(h => h.title).filter(Boolean);
  const rows = [];
  if (cf) {
    for (const s of cf.split('。').map(x => x.trim()).filter(Boolean)) rows.push(splitSentence(s));
    for (const h of hot) rows.push({ t: h, s: '' });
  } else {
    for (const s of (it.coreSummary || '').replace(/\n/g, '').split(/[。；]/).map(x => x.trim()).filter(Boolean)) {
      rows.push(splitSentence(s));
    }
  }
  const date = /^\d{4}-\d{2}-\d{2}$/.test(String(it.date)) ? it.date : '';
  const title = (PERIOD_NAME[it.type] || '简报') + (date ? ' · ' + date.slice(5) : '');
  const meta = '共 ' + Math.min(rows.length, 8) + ' 条 · ' + (/^\d{2}:\d{2}$/.test(String(it.time)) ? it.time : '');
  return pack(rows, title, meta);
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
  const p = flatten(item);
  fs.writeFileSync(OUT + '/brief-' + item.type + '.json', JSON.stringify(p.keys, null, 2) + '\n');
  console.log('brief-' + item.type + '.json 写出，27 键（非空 ' + Object.values(p.keys).filter(v => v !== '').length + '）'
    + ' ｜ ' + p.keys.title + ' ｜ ' + p.keys.meta + (p.over ? ' ｜ 超栏 ' + p.over : ''));

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
