export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    // Strip /img/ prefix to get the R2 object key
    if (!path.startsWith('/img/')) {
      return new Response('Not Found', { status: 404 });
    }

    const key = path.slice(5); // Remove "/img/"
    if (!key) {
      return new Response('Not Found', { status: 404 });
    }

    const object = await env.IMAGES.get(key);
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
