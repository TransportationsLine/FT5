const CACHE = 'FT5-v4';
const ASSETS = ['./', './index.html', './manifest.json', './icon-192.png', './icon-512.png'];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(ASSETS)));
  self.skipWaiting();
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', e => {
  const req = e.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);

  // لا نخزّن طلبات السيرفر (Google Apps Script)
  if (url.hostname.includes('script.google.com') ||
      url.hostname.includes('googleusercontent.com')) return;

  // لا نخزّن فحص صحة الخادم
  if (url.pathname === '/health') return;

  // الشبكة أولاً، ثم الكاش عند انقطاع الإنترنت
  e.respondWith(
    fetch(req)
      .then(res => {
        // نخزّن الردود السليمة فقط (لا 404 ولا أخطاء)
        if (res.ok || res.type === 'opaque') {
          const copy = res.clone();
          caches.open(CACHE).then(c => c.put(req, copy)).catch(() => {});
        }
        return res;
      })
      .catch(() => caches.match(req).then(r => r || caches.match('./index.html')))
  );
});
