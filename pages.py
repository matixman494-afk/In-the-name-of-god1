# pages.py
# ══════════════════════════════════════════════════════════════════════════════
# صفحات HTML پنل — ساده و بدون فریم‌ورک فرانت‌اند، فقط HTML/CSS/JS خام.
# قصداً مینیمال نگه داشته شده تا راحت قابل توسعه باشه.
# ══════════════════════════════════════════════════════════════════════════════

BASE_CSS = """
:root{
  --bg:#0B0B12; --card:#15151F; --card-b:#242433; --t1:#F2F0FA; --t2:#9C9AB0;
  --accent:#7C5CFC; --accent2:#5B3FD9;
  --green:#10B981; --red:#EF4444; --amber:#F59E0B; --blue:#3B82F6;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--t1);font-family:'Vazirmatn',Tahoma,sans-serif;direction:rtl}
.wrap{max-width:960px;margin:0 auto;padding:24px 16px}
.card{background:var(--card);border:1px solid var(--card-b);border-radius:16px;padding:20px;margin-bottom:16px}
h1,h2{margin:0 0 12px}
input,select{width:100%;padding:11px 13px;border-radius:10px;border:1px solid var(--card-b);
  background:#0F0F18;color:var(--t1);font-size:14px;margin-bottom:10px}
label{font-size:12.5px;color:var(--t2);display:block;margin-bottom:4px}
.btn{padding:10px 16px;border-radius:10px;border:none;cursor:pointer;font-size:13.5px;font-weight:600;
  background:linear-gradient(135deg,var(--accent),var(--accent2));color:#fff}
.btn:hover{filter:brightness(1.08)}
.btn-sm{padding:6px 10px;font-size:11.5px;border-radius:8px}
.btn-green{background:linear-gradient(135deg,#10B981,#059669)}
.btn-red{background:linear-gradient(135deg,#EF4444,#DC2626)}
.btn-blue{background:linear-gradient(135deg,#3B82F6,#2563EB)}
.btn-outline{background:transparent;border:1px solid var(--card-b);color:var(--t2)}
.row{display:flex;gap:8px;flex-wrap:wrap}
.link-row{display:flex;justify-content:space-between;align-items:center;gap:10px;
  padding:13px;border:1px solid var(--card-b);border-radius:12px;margin-bottom:10px;flex-wrap:wrap}
.tag{padding:3px 9px;border-radius:999px;font-size:11px;font-weight:600}
.tag-on{background:rgba(16,185,129,.15);color:#34D399}
.tag-off{background:rgba(239,68,68,.15);color:#F87171}
.muted{color:var(--t2);font-size:12.5px}
.err{color:#F87171;font-size:13px;margin-top:6px;min-height:16px}
.center{display:flex;align-items:center;justify-content:center;min-height:100vh}
"""

LOGIN_HTML = f"""<!doctype html>
<html lang="fa" dir="rtl"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>ورود | Matix</title>
<style>{BASE_CSS}</style>
</head><body>
<div class="center">
  <div class="card" style="width:100%;max-width:360px">
    <h1 style="text-align:center">🔐 Matix</h1>
    <p class="muted" style="text-align:center;margin-bottom:18px">برای مدیریت لینک‌ها وارد شو</p>
    <label>رمز عبور</label>
    <input id="pw" type="password" placeholder="رمز پنل" onkeydown="if(event.key==='Enter')doLogin()">
    <button class="btn" style="width:100%" onclick="doLogin()">ورود</button>
    <div class="err" id="err"></div>
  </div>
</div>
<script>
async function doLogin(){{
  const pw = document.getElementById('pw').value;
  const errEl = document.getElementById('err');
  errEl.textContent = '';
  try {{
    const res = await fetch('/api/login', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body: JSON.stringify({{password: pw}})}});
    if (res.ok) {{ location.href = '/dashboard'; }}
    else {{ const d = await res.json(); errEl.textContent = d.detail || 'رمز اشتباهه'; }}
  }} catch(e) {{ errEl.textContent = 'خطا در اتصال به سرور'; }}
}}
</script>
</body></html>
"""

DASHBOARD_HTML = f"""<!doctype html>
<html lang="fa" dir="rtl"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>داشبورد | Matix</title>
<style>{BASE_CSS}</style>
</head><body>
<div class="wrap">
  <div class="row" style="justify-content:space-between;align-items:center;margin-bottom:18px">
    <h1>🛡 Matix</h1>
    <button class="btn btn-outline btn-sm" onclick="logout()">خروج</button>
  </div>

  <div class="card">
    <h2>➕ ساخت لینک جدید</h2>
    <div class="row">
      <div style="flex:2;min-width:160px"><label>عنوان</label><input id="c-label" placeholder="مثلاً: کاربر ۱"></div>
      <div style="flex:1;min-width:100px"><label>حجم (GB، 0=نامحدود)</label><input id="c-limit" type="number" value="0" min="0" step="0.5"></div>
      <div style="flex:1;min-width:100px"><label>مدت (روز، 0=نامحدود)</label><input id="c-days" type="number" value="0" min="0"></div>
    </div>
    <button class="btn" onclick="createLink()">ساخت لینک</button>
  </div>

  <div class="card">
    <div class="row" style="justify-content:space-between;align-items:center">
      <h2>📋 لینک‌ها</h2>
      <span class="muted" id="count"></span>
    </div>
    <div id="links"></div>
  </div>

  <div class="card">
    <h2>🔑 تغییر رمز پنل</h2>
    <div class="row">
      <div style="flex:1;min-width:160px"><label>رمز جدید</label><input id="pw-new" type="password" placeholder="حداقل ۴ کاراکتر"></div>
      <div style="flex:1;min-width:160px"><label>تکرار رمز جدید</label><input id="pw-confirm" type="password"></div>
    </div>
    <button class="btn" onclick="changePassword()">ذخیره رمز جدید</button>
    <div class="err" id="pw-err"></div>
  </div>

  <div class="card">
    <h2>🤖 تنظیمات ربات تلگرام</h2>
    <p class="muted" style="margin-top:-6px;margin-bottom:14px">
      توکن رو از <a href="https://t.me/BotFather" target="_blank" style="color:var(--blue)">@BotFather</a> بگیر،
      آیدی عددیت رو از <a href="https://t.me/userinfobot" target="_blank" style="color:var(--blue)">@userinfobot</a>.
    </p>
    <label>توکن ربات</label>
    <input id="tg-token" placeholder="1234567890:AAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx">
    <label>آیدی عددی ادمین‌ها (با کاما جدا کن)</label>
    <input id="tg-admins" placeholder="123456789, 987654321">
    <button class="btn" onclick="saveTelegramSettings()">ذخیره تنظیمات ربات</button>
    <span class="muted" id="tg-status" style="margin-right:8px"></span>
  </div>

  <div class="card">
    <h2>📢 کانال‌های عضویت اجباری</h2>
    <p class="muted" style="margin-top:-6px;margin-bottom:14px">
      کاربر عادی تا عضو نشدن توی همه‌ی این کانال‌ها نمی‌تونه از ربات استفاده کنه.
      ⚠️ ربات باید توی هر کانالی که اضافه می‌کنی ادمین باشه.
    </p>
    <div id="channels"></div>
    <div class="row" style="margin-top:10px">
      <div style="flex:1;min-width:140px"><label>آیدی (@channel یا -100...)</label><input id="ch-id" placeholder="@mychannel"></div>
      <div style="flex:1;min-width:140px"><label>اسم نمایشی</label><input id="ch-title" placeholder="کانال اطلاع‌رسانی"></div>
      <div style="flex:1;min-width:140px"><label>لینک عضویت (اختیاری)</label><input id="ch-url" placeholder="https://t.me/mychannel"></div>
    </div>
    <button class="btn" onclick="addChannel()">افزودن کانال</button>
    <div class="err" id="ch-err"></div>
  </div>
</div>

<script>
function esc(s){{ return (s||'').replace(/[&<>"']/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}})[c]); }}

async function logout(){{ await fetch('/api/logout', {{method:'POST'}}); location.href='/login'; }}

async function loadLinks(){{
  const res = await fetch('/api/links');
  if (res.status === 401) {{ location.href='/login'; return; }}
  const data = await res.json();
  const box = document.getElementById('links');
  document.getElementById('count').textContent = data.links.length + ' لینک';
  if (!data.links.length){{ box.innerHTML = '<p class="muted">هنوز لینکی نساختی.</p>'; return; }}
  box.innerHTML = data.links.map(l => `
    <div class="link-row">
      <div style="flex:1;min-width:200px">
        <div><b>${{esc(l.label)}}</b> <span class="tag ${{l.allowed?'tag-on':'tag-off'}}">${{l.allowed?'فعال':'غیرفعال/منقضی'}}</span></div>
        <div class="muted">مصرف: ${{l.used_fmt}} / ${{l.limit_fmt}} — انقضا: ${{l.expires_at ? l.expires_at.split('T')[0] : 'نامحدود'}}</div>
      </div>
      <div class="row">
        <button class="btn btn-sm btn-blue" onclick="copyLink('${{l.uuid}}')">کپی لینک</button>
        <button class="btn btn-sm ${{l.active?'btn-red':'btn-green'}}" onclick="toggleLink('${{l.uuid}}', ${{!l.active}})">${{l.active?'غیرفعال کن':'فعال کن'}}</button>
        <button class="btn btn-sm btn-outline" onclick="resetUsage('${{l.uuid}}')">ریست مصرف</button>
        <button class="btn btn-sm btn-red" onclick="deleteLink('${{l.uuid}}')">حذف</button>
      </div>
    </div>
  `).join('');
  window._links = Object.fromEntries(data.links.map(l => [l.uuid, l.vless_link]));
}}

async function createLink(){{
  const label = document.getElementById('c-label').value || 'کانفیگ جدید';
  const limit_gb = parseFloat(document.getElementById('c-limit').value || '0');
  const expires_days = parseInt(document.getElementById('c-days').value || '0');
  const res = await fetch('/api/links', {{method:'POST', headers:{{'Content-Type':'application/json'}},
    body: JSON.stringify({{label, limit_gb, expires_days}})}});
  if (res.ok){{
    document.getElementById('c-label').value = '';
    await loadLinks();
  }}
}}

function copyLink(uid){{
  const link = window._links[uid];
  navigator.clipboard.writeText(link).then(() => alert('لینک کپی شد ✓'));
}}

async function toggleLink(uid, active){{
  await fetch(`/api/links/${{uid}}`, {{method:'PATCH', headers:{{'Content-Type':'application/json'}}, body: JSON.stringify({{active}})}});
  loadLinks();
}}

async function resetUsage(uid){{
  await fetch(`/api/links/${{uid}}`, {{method:'PATCH', headers:{{'Content-Type':'application/json'}}, body: JSON.stringify({{reset_usage: true}})}});
  loadLinks();
}}

async function deleteLink(uid){{
  if (!confirm('مطمئنی می‌خوای این لینک حذف بشه؟')) return;
  await fetch(`/api/links/${{uid}}`, {{method:'DELETE'}});
  loadLinks();
}}

// ── تغییر رمز پنل ──────────────────────────────────────────────────────────
async function changePassword(){{
  const pw = document.getElementById('pw-new').value;
  const confirm_pw = document.getElementById('pw-confirm').value;
  const errEl = document.getElementById('pw-err');
  errEl.textContent = '';
  if (pw !== confirm_pw){{ errEl.textContent = 'رمزها یکی نیستن'; return; }}
  const res = await fetch('/api/change-password', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body: JSON.stringify({{new_password: pw}})}});
  if (res.ok){{
    document.getElementById('pw-new').value = '';
    document.getElementById('pw-confirm').value = '';
    alert('رمز جدید ذخیره شد ✓');
  }} else {{ const d = await res.json(); errEl.textContent = d.detail || 'خطا'; }}
}}

// ── تنظیمات ربات تلگرام ────────────────────────────────────────────────────
async function loadTelegramSettings(){{
  const res = await fetch('/api/settings/telegram');
  if (res.status === 401){{ location.href='/login'; return; }}
  const data = await res.json();
  document.getElementById('tg-token').value = data.bot_token || '';
  document.getElementById('tg-admins').value = (data.admin_ids || []).join(', ');
  document.getElementById('tg-status').textContent = data.bot_active ? '🟢 ربات روشنه' : '🔴 توکن تنظیم نشده';
}}

async function saveTelegramSettings(){{
  const bot_token = document.getElementById('tg-token').value.trim();
  const admin_ids = document.getElementById('tg-admins').value.trim();
  const res = await fetch('/api/settings/telegram', {{method:'POST', headers:{{'Content-Type':'application/json'}},
    body: JSON.stringify({{bot_token, admin_ids}})}});
  if (res.ok){{ alert('تنظیمات ربات ذخیره شد ✓ (تا ۳۰ ثانیه طول می‌کشه اعمال بشه)'); loadTelegramSettings(); }}
  else {{ alert('خطا در ذخیره‌ی تنظیمات'); }}
}}

// ── کانال‌های عضویت اجباری ──────────────────────────────────────────────────
async function loadChannels(){{
  const res = await fetch('/api/settings/channels');
  if (res.status === 401){{ location.href='/login'; return; }}
  const data = await res.json();
  const box = document.getElementById('channels');
  if (!data.channels.length){{ box.innerHTML = '<p class="muted">هنوز کانالی اضافه نشده.</p>'; return; }}
  box.innerHTML = data.channels.map((ch, i) => `
    <div class="link-row">
      <div><b>${{esc(ch.title)}}</b> <span class="muted">(${{esc(ch.id)}})</span></div>
      <button class="btn btn-sm btn-red" onclick="deleteChannel(${{i}})">حذف</button>
    </div>
  `).join('');
}}

async function addChannel(){{
  const id = document.getElementById('ch-id').value.trim();
  const title = document.getElementById('ch-title').value.trim();
  const url = document.getElementById('ch-url').value.trim();
  const errEl = document.getElementById('ch-err');
  errEl.textContent = '';
  const res = await fetch('/api/settings/channels', {{method:'POST', headers:{{'Content-Type':'application/json'}},
    body: JSON.stringify({{id, title, url}})}});
  if (res.ok){{
    document.getElementById('ch-id').value = '';
    document.getElementById('ch-title').value = '';
    document.getElementById('ch-url').value = '';
    loadChannels();
  }} else {{ const d = await res.json(); errEl.textContent = d.detail || 'خطا'; }}
}}

async function deleteChannel(i){{
  if (!confirm('این کانال از لیست عضویت اجباری حذف بشه؟')) return;
  await fetch(`/api/settings/channels/${{i}}`, {{method:'DELETE'}});
  loadChannels();
}}

loadLinks();
loadTelegramSettings();
loadChannels();
setInterval(loadLinks, 15000);
</script>
</body></html>
"""
