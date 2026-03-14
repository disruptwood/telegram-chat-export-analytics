from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Iterable

from .models import TelegramMessage
from .participants import canonical_sender_id, collect_sender_profiles, filter_messages_for_participants


HEATMAP_BUBBLES_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Telegram Chat Activity Bubbles</title>
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body { background:#08090f; overflow:hidden; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; }
canvas { display:block; }
#loading {
  position:fixed; top:0; left:0; width:100%; height:100%;
  display:flex; flex-direction:column; align-items:center; justify-content:center;
  color:#6ec6ff; font-size:1.1em; z-index:100; background:#08090f; gap:12px;
}
#loading .bar { width:200px; height:4px; background:#1a1d27; border-radius:2px; overflow:hidden; }
#loading .bar-fill { height:100%; width:0; background:#6ec6ff; transition:width 0.3s; }
#controls {
  position:fixed; bottom:52px; right:16px; z-index:20; display:flex; gap:6px;
}
#controls button {
  background:rgba(20,24,36,0.85); border:1px solid #2a2d3a; color:#aaa;
  padding:6px 12px; border-radius:6px; cursor:pointer; font-size:0.8em;
  backdrop-filter:blur(6px);
}
#controls button:hover { color:#fff; border-color:#6ec6ff; }
#controls button.recording { background:#c62828; color:#fff; border-color:#c62828; animation:pulse 1s infinite; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.6} }
</style>
</head>
<body>

<div id="loading">
  <div>Loading daily activity...</div>
  <div class="bar"><div class="bar-fill" id="loadBar"></div></div>
</div>

<canvas id="c"></canvas>

<div id="controls">
  <button id="btnSpeed" onclick="cycleSpeed()">1x</button>
  <button id="btnPause" onclick="togglePause()">Pause</button>
  <button id="btnRestart" onclick="restart()">Restart</button>
  <button id="btnRecord" onclick="toggleRecord()">Record</button>
</div>

<script>
const DECAY = 0.93;
const DAY_MS = 55;
const HEAT_BOOST = 1.0;
const MIN_HEAT_SHOW = 2.0;
const MAX_BUBBLES = 35;
const MAX_R = 52;
const MIN_R = 8;
const ARENA_RATIO = 0.38;
const TIMELINE_H = 40;
const LERP = 0.12;

const canvas = document.getElementById('c');
const ctx = canvas.getContext('2d');
let W, H, cx, cy, arenaR;
let DATA = null;
let dayIdx = 0, dayT = 0, lastTime = 0;
let paused = false, speedMult = 1;

let senderHeat = {};
let senderColor = {};
let bubbles = {};
let activeBubbleIds = [];

let mediaRecorder = null, recordedChunks = [], isRecording = false;

let hudDate = '', hudMsgs = 0, hudActive = 0;

const MONTHS = ['', 'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

function resize() {
  W = canvas.width = window.innerWidth;
  H = canvas.height = window.innerHeight;
  cx = W / 2;
  cy = (H - TIMELINE_H) / 2 - 10;
  arenaR = Math.min(W, H - TIMELINE_H - 30) * ARENA_RATIO;
}

function makeColor(id) {
  let h = 0;
  for (let i = 0; i < id.length; i++) h = ((h << 5) - h + id.charCodeAt(i)) | 0;
  return {
    hue: ((h % 360) + 360) % 360,
    sat: 55 + (h % 25),
    lit: 45 + ((h >> 8) % 18),
  };
}

function hsl(c, a) {
  if (a !== undefined) return `hsla(${c.hue},${c.sat}%,${c.lit}%,${a})`;
  return `hsl(${c.hue},${c.sat}%,${c.lit}%)`;
}

async function init() {
  resize();
  window.addEventListener('resize', resize);

  const res = await fetch('daily_all_senders.json');
  const reader = res.body.getReader();
  const contentLength = +res.headers.get('Content-Length') || 3000000;
  let received = 0;
  const chunks = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
    received += value.length;
    document.getElementById('loadBar').style.width = Math.min(100, received/contentLength*100) + '%';
  }
  const blob = new Blob(chunks);
  const text = await blob.text();
  DATA = JSON.parse(text);
  document.getElementById('loading').style.display = 'none';

  for (const id in DATA.names) {
    senderColor[id] = makeColor(id);
    senderHeat[id] = 0;
  }

  applyDay(0);
  requestAnimationFrame(loop);
}

function applyDay(idx) {
  const dayStr = DATA.days[idx];
  let totalMsgs = 0;
  let activeToday = 0;

  for (const id in DATA.counts) {
    senderHeat[id] *= DECAY;
    const msgs = DATA.counts[id][idx] || 0;
    if (msgs > 0) {
      senderHeat[id] += msgs * HEAT_BOOST;
      totalMsgs += msgs;
      activeToday++;
    }
  }

  const sorted = Object.entries(senderHeat)
    .filter(([_, h]) => h >= MIN_HEAT_SHOW)
    .sort((a, b) => b[1] - a[1])
    .slice(0, MAX_BUBBLES);

  activeBubbleIds = sorted.map(([id]) => id);
  const maxHeat = sorted.length > 0 ? sorted[0][1] : 1;
  const activeSet = new Set(activeBubbleIds);

  for (const [id, heat] of sorted) {
    const frac = Math.sqrt(heat / maxHeat);
    const tr = MIN_R + frac * (MAX_R - MIN_R);
    if (!bubbles[id]) {
      const angle = Math.random() * Math.PI * 2;
      bubbles[id] = {
        x: cx + Math.cos(angle) * (arenaR + 40),
        y: cy + Math.sin(angle) * (arenaR + 40),
        r: 2, tr, vx: 0, vy: 0, heat, name: DATA.names[id],
      };
    } else {
      bubbles[id].tr = tr;
      bubbles[id].heat = heat;
      bubbles[id].name = DATA.names[id];
    }
  }

  for (const id in bubbles) {
    if (!activeSet.has(id)) {
      bubbles[id].tr = 0;
      bubbles[id].heat = senderHeat[id] || 0;
    }
  }

  for (const id in bubbles) {
    if (bubbles[id].r < 0.5 && bubbles[id].tr < 0.5) delete bubbles[id];
  }

  const d = new Date(dayStr + 'T00:00:00');
  hudDate = `${d.getDate()} ${MONTHS[d.getMonth()+1]} ${d.getFullYear()}`;
  hudMsgs = totalMsgs;
  hudActive = activeToday;
}

function simulate() {
  const bubs = Object.values(bubbles).filter(b => b.r > 1);
  for (const b of bubs) {
    const dx = cx - b.x, dy = cy - b.y;
    const dist = Math.sqrt(dx*dx + dy*dy) || 1;
    const pull = 0.006;
    b.vx += (dx/dist) * pull * dist * 0.015;
    b.vy += (dy/dist) * pull * dist * 0.015;
  }
  for (let i = 0; i < bubs.length; i++) {
    for (let j = i + 1; j < bubs.length; j++) {
      const a = bubs[i], b = bubs[j];
      let dx = a.x - b.x, dy = a.y - b.y;
      let dist = Math.sqrt(dx*dx + dy*dy) || 1;
      const minD = a.r + b.r + 2;
      if (dist < minD) {
        const push = (minD - dist) * 0.15;
        const nx = dx / dist, ny = dy / dist;
        a.vx += nx * push; a.vy += ny * push;
        b.vx -= nx * push; b.vy -= ny * push;
      }
    }
  }
  for (const b of bubs) {
    const dx = b.x - cx, dy = b.y - cy;
    const dist = Math.sqrt(dx*dx + dy*dy);
    if (dist + b.r > arenaR - 2) {
      const over = dist + b.r - arenaR + 2;
      b.vx -= (dx / dist) * over * 0.1;
      b.vy -= (dy / dist) * over * 0.1;
    }
  }
  for (const b of Object.values(bubbles)) {
    b.vx *= 0.82;
    b.vy *= 0.82;
    b.x += b.vx;
    b.y += b.vy;
    b.r += (b.tr - b.r) * LERP;
  }
}

function draw() {
  ctx.fillStyle = '#08090f';
  ctx.fillRect(0, 0, W, H);

  const rg = ctx.createRadialGradient(cx, cy, 0, cx, cy, arenaR * 1.5);
  rg.addColorStop(0, 'rgba(20,32,50,0.16)');
  rg.addColorStop(1, 'rgba(6,8,12,0)');
  ctx.fillStyle = rg;
  ctx.beginPath();
  ctx.arc(cx, cy, arenaR * 1.55, 0, Math.PI * 2);
  ctx.fill();

  ctx.strokeStyle = 'rgba(120,160,220,0.10)';
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.arc(cx, cy, arenaR, 0, Math.PI * 2);
  ctx.stroke();

  const drawList = Object.entries(bubbles)
    .filter(([_, b]) => b.r > 1)
    .sort((a, b) => a[1].r - b[1].r);

  for (const [id, bubble] of drawList) {
    const color = senderColor[id];
    const alpha = Math.max(0.15, Math.min(0.85, bubble.r / MAX_R));
    const gradient = ctx.createRadialGradient(
      bubble.x - bubble.r * 0.3,
      bubble.y - bubble.r * 0.3,
      bubble.r * 0.1,
      bubble.x,
      bubble.y,
      bubble.r
    );
    gradient.addColorStop(0, hsl(color, Math.min(1, alpha + 0.15)));
    gradient.addColorStop(1, hsl(color, Math.max(0.10, alpha - 0.20)));
    ctx.fillStyle = gradient;
    ctx.beginPath();
    ctx.arc(bubble.x, bubble.y, bubble.r, 0, Math.PI * 2);
    ctx.fill();

    if (bubble.r >= 16) {
      ctx.fillStyle = 'rgba(255,255,255,0.92)';
      ctx.font = `${Math.max(10, Math.min(16, bubble.r * 0.30))}px sans-serif`;
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.fillText(bubble.name.slice(0, 18), bubble.x, bubble.y);
    }
  }

  ctx.fillStyle = 'rgba(255,255,255,0.95)';
  ctx.font = '600 22px sans-serif';
  ctx.textAlign = 'left';
  ctx.fillText(hudDate, 16, 36);
  ctx.font = '14px sans-serif';
  ctx.fillStyle = 'rgba(190,210,255,0.85)';
  ctx.fillText(`${hudMsgs} messages   ${hudActive} active senders`, 16, 58);
}

function loop(ts) {
  if (!lastTime) lastTime = ts;
  const dt = ts - lastTime;
  lastTime = ts;
  if (!paused && DATA) {
    dayT += dt * speedMult;
    while (dayT >= DAY_MS) {
      dayT -= DAY_MS;
      dayIdx++;
      if (dayIdx >= DATA.days.length) {
        dayIdx = DATA.days.length - 1;
        paused = true;
        break;
      }
      applyDay(dayIdx);
    }
    simulate();
  }
  draw();
  requestAnimationFrame(loop);
}

function cycleSpeed() {
  const options = [1, 2, 4, 8];
  speedMult = options[(options.indexOf(speedMult) + 1) % options.length];
  document.getElementById('btnSpeed').textContent = `${speedMult}x`;
}

function togglePause() {
  paused = !paused;
  document.getElementById('btnPause').textContent = paused ? 'Play' : 'Pause';
}

function restart() {
  if (!DATA) return;
  dayIdx = 0;
  dayT = 0;
  senderHeat = {};
  bubbles = {};
  for (const id in DATA.names) senderHeat[id] = 0;
  applyDay(0);
  paused = false;
  document.getElementById('btnPause').textContent = 'Pause';
}

async function toggleRecord() {
  const btn = document.getElementById('btnRecord');
  if (!isRecording) {
    const stream = canvas.captureStream(60);
    mediaRecorder = new MediaRecorder(stream, { mimeType: 'video/webm;codecs=vp9' });
    recordedChunks = [];
    mediaRecorder.ondataavailable = event => { if (event.data.size > 0) recordedChunks.push(event.data); };
    mediaRecorder.onstop = () => {
      const blob = new Blob(recordedChunks, { type: 'video/webm' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = 'telegram-chat-activity-bubbles.webm';
      link.click();
      URL.revokeObjectURL(url);
    };
    mediaRecorder.start();
    isRecording = true;
    btn.classList.add('recording');
    btn.textContent = 'Stop';
  } else {
    mediaRecorder.stop();
    isRecording = false;
    btn.classList.remove('recording');
    btn.textContent = 'Record';
  }
}

init();
</script>
</body>
</html>
"""


@dataclass(frozen=True, slots=True)
class DailySenderSeries:
    names: dict[str, str]
    days: list[str]
    counts: dict[str, list[int]]


def build_daily_sender_series(
    messages: Iterable[TelegramMessage],
    *,
    include_non_human: bool = False,
) -> DailySenderSeries:
    filtered_messages = sorted(
        filter_messages_for_participants(messages, include_non_human=include_non_human),
        key=lambda item: (item.date, item.id),
    )
    if not filtered_messages:
        return DailySenderSeries(names={}, days=[], counts={})

    profiles = collect_sender_profiles(filtered_messages)
    first_day = filtered_messages[0].date.date()
    last_day = filtered_messages[-1].date.date()
    total_days = (last_day - first_day).days + 1
    days = [(first_day + timedelta(days=offset)).isoformat() for offset in range(total_days)]
    day_index = {day: index for index, day in enumerate(days)}
    sender_ids = sorted(profiles, key=lambda item: (profiles[item].display_name.casefold(), item))
    counts = {sender_id: [0] * total_days for sender_id in sender_ids}

    for message in filtered_messages:
        sender_id = canonical_sender_id(message)
        counts[sender_id][day_index[message.date.date().isoformat()]] += 1

    names = {sender_id: profiles[sender_id].display_name for sender_id in sender_ids}
    return DailySenderSeries(names=names, days=days, counts=counts)


def write_daily_sender_series_json(series: DailySenderSeries, destination: str | Path) -> Path:
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    destination_path.write_text(
        json.dumps({"names": series.names, "days": series.days, "counts": series.counts}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return destination_path


def write_heatmap_bubbles_html(destination: str | Path) -> Path:
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    destination_path.write_text(HEATMAP_BUBBLES_HTML, encoding="utf-8")
    return destination_path
