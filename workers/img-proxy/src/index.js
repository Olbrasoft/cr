export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (!path.startsWith('/img/')) {
      return new Response('Not Found', { status: 404 });
    }

    const key = path.slice(5);
    if (!key) {
      return new Response('Not Found', { status: 404 });
    }

    let object = await env.IMAGES.get(key);

    // Fallback for SEO landmark URLs: landmarks/{slug}-{catalog_id}.{ext} → landmarks/{catalog_id}.{ext}
    if (!object && key.startsWith('landmarks/')) {
      const match = key.match(/landmarks\/.*-(\d{10,})\.(\w+)$/);
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
