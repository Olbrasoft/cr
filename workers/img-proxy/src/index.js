export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (!path.startsWith('/img/')) {
      return new Response('Not Found', { status: 404 });
    }

    // If resize requested (?w=360), pass through to origin (Axum handles resize).
    if (url.searchParams.has('w')) {
      return fetch(request);
    }

    const key = path.slice(5);
    if (!key) {
      return new Response('Not Found', { status: 404 });
    }

    // SEO municipality URLs: {orp}/{municipality}/{photo}.webp
    // These need DB lookup (Axum handles), pass through to origin.
    const knownPrefixes = ['municipalities/', 'landmarks/', 'pools/', 'regions/', 'videos/'];
    const isKnownPrefix = knownPrefixes.some(p => key.startsWith(p));
    const segmentCount = key.split('/').length;
    if (!isKnownPrefix && segmentCount === 3) {
      return fetch(request);
    }

    let object = await env.IMAGES.get(key);

    // Fallback for SEO landmark URLs:
    // landmarks/{slug}-{catalog_id}.{ext} → landmarks/{catalog_id}.{ext}
    // landmarks/{slug}-{catalog_id}_0002.{ext} → landmarks/{catalog_id}_0002.{ext}
    if (!object && key.startsWith('landmarks/')) {
      const match = key.match(/landmarks\/.*-(\d{10,}(?:_\d+)?)\.(\w+)$/);
      if (match) {
        const fallbackKey = `landmarks/${match[1]}.${match[2]}`;
        object = await env.IMAGES.get(fallbackKey);
      }
    }

    if (!object) {
      return new Response('Not Found', { status: 404 });
    }

    const headers = new Headers();
    object.writeHttpMetadata(headers);
    headers.set('etag', object.httpEtag);
    headers.set('cache-control', 'public, max-age=86400, s-maxage=604800');

    return new Response(object.body, { headers });
  },
};
