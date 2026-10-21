// jsdelivr 边缘刷新（替代 yml 里那段 `sleep 300` + 无差别 purge）
//
// 10-21 实测出来的三条真相，都是运行 7（id 39573694712）当场翻的车：
//   ① 「commit 后等满 5 分钟再 purge」不够。工作流 09:01:21Z 提交、09:06:23Z 按 sleep 300 打 purge，
//      隔了 5 分 02 秒，结果 closing 边缘仍是 363 字节空壳，而**本来 15:5x 就推对的盘前那份被反向刷回
//      604 字节/共 3 条的旧稿**。我在 09:10:15Z 与 09:15:55Z 手动补打，两次都 `throttled:false`
//      （所以那 ~42 分钟频控不是硬指标，别拿它当理由少打），回读立刻 `age 0` 变新版。
//   ② 真正作数的判据只有一个：**回读到的字节 == 仓库里那份字节**。purge 端点哪怕被频控也回
//      `status:finished`，返回值不可信。
//   ③ jsdelivr 的 `/gh/…@main/` 除了缓存文件，还缓存「分支→commit」这一层解析；被陈旧解析坑住时，
//      purge 会顺带强制重解析（同一 URL 换成固定 commit sha 则一直是新版，可对照）。
//
// 于是这里的口径：
//   - 内容本来就一致的文件一个 purge 都不打（无差别 purge 才是把没变的文件刷回旧版的元凶）。
//   - 要打的文件做有界重试：最多 3 次，每次先看 raw 有没有翻篇，翻篇才 purge，purge 后预热两连打，
//     回读还是旧版就再等 5 分钟打第二次。
//   - 任何一步失败只打警告、不抛错：取数和提交已经成功，不能让刷新一失败就把整轮标红。
//   - 冷读实测 2.0–5.2 秒，超插件 3000 ms 闸门，所以预热这两下不是可选项。

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

const OWNER = process.env.GH_OWNER || '1tothree';
const REPO = process.env.GH_REPO || 'glass-brief-cache';
const REF = process.env.GH_REF || 'main';
const TYPES = (process.env.BRIEF_TYPES || 'opening,midday,closing,evening').split(',').map(s => s.trim());
const ATTEMPTS = Number(process.env.REFRESH_ATTEMPTS || 3);
const GAP_MS = Number(process.env.REFRESH_GAP_MS || 300000); // 两次 purge 之间隔 5 分钟

const P = t => `gh/${OWNER}/${REPO}@${REF}/data/brief-${t}.json`;
const EDGE = t => `https://cdn.jsdelivr.net/${P(t)}`;
const PURGE = t => `https://purge.jsdelivr.net/${P(t)}`;
const RAW = t => `https://raw.githubusercontent.com/${OWNER}/${REPO}/${REF}/data/brief-${t}.json`;

const sleep = ms => new Promise(r => setTimeout(r, ms));
const sha = b => crypto.createHash('sha256').update(b).digest('hex').slice(0, 12);

async function get(url, tries = 3) {
  for (let i = 0; i < tries; i++) {
    try {
      const res = await fetch(url, { headers: { 'user-agent': 'brief-aggregator' } });
      if (!res.ok) throw new Error('http ' + res.status);
      return Buffer.from(await res.arrayBuffer());
    } catch (e) {
      if (i === tries - 1) return null;
      await sleep(3000); // raw 从某些网络会 Connection reset
    }
  }
  return null;
}

// purge 端点是 GET，响应形如 {"paths":{"/gh/…":{"throttled":false,"providers":{"CF":true,"FY":true}}}}
async function purge(t) {
  try {
    const j = await (await fetch(PURGE(t))).json();
    const one = (j.paths || {})[`/${P(t)}`] || Object.values(j.paths || {})[0] || {};
    return { throttled: !!one.throttled, reset: Number(one.throttlingReset) || 0, providers: Object.keys(one.providers || {}).join('/') };
  } catch { return { throttled: null, reset: 0, providers: '' }; }
}

async function edgeMatches(t, want) {
  const b = await get(EDGE(t));
  return { ok: !!b && sha(b) === want.sha, bytes: b };
}

async function refreshOne(t, want) {
  for (let a = 1; a <= ATTEMPTS; a++) {
    if (a > 1) {
      console.log(`[retry] ${t} 第 ${a}/${ATTEMPTS} 次 ⇒ 先等 ${Math.round(GAP_MS / 1000)}s（让上一发 purge 的陈旧解析彻底过期）`);
      await sleep(GAP_MS);
    }
    const raw = await get(RAW(t));
    if (!raw) console.log(`[raw?] ${t} raw 三次都没拿到（第 ${a} 次），仍照打 purge`);
    else if (sha(raw) !== want.sha) {
      console.log(`[raw-old] ${t} raw 还是旧版 ${raw.length}B ${sha(raw)} ≠ 仓库 ${want.size}B ${want.sha} ⇒ 这轮不打 purge`);
      continue;
    } else console.log(`[raw-ok] ${t} raw 已是新版 ${raw.length}B ${sha(raw)} (${new Date().toISOString()})`);

    const r = await purge(t);
    console.log(`[purge] ${t} 第 ${a} 次 throttled=${r.throttled} providers=${r.providers}`);
    if (r.throttled) {
      const w = Math.min(r.reset + 30, GAP_MS / 1000);
      console.log(`[throttle] ${t} 频控回 ${r.reset}s ⇒ 等 ${w}s 再校验`);
      await sleep(w * 1000);
    }
    await sleep(3000);
    for (let i = 1; i <= 2; i++) { // 预热：冷读 2.0–5.2s 会顶穿插件 3000ms 闸门
      const b = await get(EDGE(t));
      console.log(`[warm] ${t} try${i} ${b ? b.length + 'B ' + sha(b) : 'ERR'}`);
      await sleep(1500);
    }
    const chk = await edgeMatches(t, want);
    console.log(`[check] ${t} ${chk.ok ? 'OK' : 'STALE'} 边缘 ${chk.bytes ? chk.bytes.length + 'B ' + sha(chk.bytes) : '拿不到'} vs 仓库 ${want.size}B ${want.sha}`);
    if (chk.ok) return true;
  }
  return false;
}

async function main() {
  const want = {};
  for (const t of TYPES) {
    const p = path.resolve('data', `brief-${t}.json`);
    if (!fs.existsSync(p)) { console.log(`[skip] ${t} 工作树里没有 data/brief-${t}.json`); continue; }
    const b = fs.readFileSync(p);
    want[t] = { sha: sha(b), size: b.length };
  }
  if (!Object.keys(want).length) { console.log('[skip] 没有任何可比对的 brief-*.json'); return; }

  const need = [];
  for (const t of Object.keys(want)) {
    const chk = await edgeMatches(t, want[t]);
    console.log(chk.ok
      ? `[fresh] ${t} 边缘已是新版 ${chk.bytes.length}B ${sha(chk.bytes)} ⇒ 不打 purge`
      : `[stale] ${t} 边缘 ${chk.bytes ? chk.bytes.length + 'B ' + sha(chk.bytes) : '拿不到'} ≠ 仓库 ${want[t].size}B ${want[t].sha}`);
    if (!chk.ok) need.push(t);
  }
  if (!need.length) { console.log(`[ok] ${new Date().toISOString()} 四份边缘全部与仓库一致，无需刷新`); return; }

  const failed = [];
  for (const t of need) if (!(await refreshOne(t, want[t]))) failed.push(t);

  if (failed.length) {
    console.log(`[warn] 打完 ${ATTEMPTS} 次仍陈旧：${failed.join(',')} ⇒ 下一轮还会重试（脚本每轮都先比对内容，不会漏）`);
    console.log(`[hint] 要立刻救回来就手动打 https://purge.jsdelivr.net/${P(failed[0])} 并回读，或直接再 dispatch 一次本工作流`);
  } else {
    console.log(`[ok] ${new Date().toISOString()} 需要刷新的 ${need.length} 份全部对齐仓库`);
  }
}

main().catch(e => { console.log('[warn] 刷新步骤异常（不影响已提交的数据）：' + e.message); });
