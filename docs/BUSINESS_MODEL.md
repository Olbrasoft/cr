# Business Model

> Monetization strategy and revenue channels for CeskaRepublika.wiki.

---

## Revenue Channels Overview

```
Revenue Sources
+-- 1. Accommodation reservation commissions (primary)
+-- 2. Real estate commissions / leads
+-- 3. Premium listings (future)
+-- 4. Contextual advertising (supplementary, future)
```

---

## 1. Accommodation Reservation Commissions (Primary)

### How It Works

1. Visitor on a municipality/monument page sees "Accommodation nearby"
2. Fills out reservation form directly on the portal
3. Reservation is sent via **Previo.cz** API
4. Previo processes and confirms the reservation with the accommodation facility
5. We receive a **commission** for each completed reservation

### Key Partner: Previo.cz

- Leading Czech reservation system with thousands of connected accommodation facilities
- API provides:
  - Real-time availability queries
  - Price queries
  - Reservation submission
- Each accommodation facility in our database can have a binding to a Previo ID

### Contextual Advantage

Unlike pure accommodation portals, we offer accommodation **in context** — the visitor came looking for information about a city or monument, and accommodation is offered naturally. Conversion rate should be higher because the intent to travel already exists.

---

## 2. Real Estate Commissions and Leads

### How It Works

- Municipality pages display current real estate listings from that area
- Data comes from aggregators (Digi-reality etc.)
- We earn commission for bringing customers (clicks, inquiries)

### Cooperation Models

| Model | Description |
|-------|------------|
| **CPC (cost per click)** | Payment per click to listing detail at the real estate agency |
| **CPL (cost per lead)** | Payment per submitted inquiry |
| **Transaction commission** | Percentage of completed transaction (hardest to negotiate) |
| **Weekly reports** | Sending inquiries to real estate agencies with interest overview in their area |

### Contextual Value

A visitor browsing a municipality page potentially cares about life in that locality. Displaying real estate listings is natural and relevant — unlike generic advertising.

---

## 3. Premium Listings (Future)

After achieving sufficient traffic, offer accommodation operators and real estate agencies premium presentation options.

| Service | Description |
|---------|------------|
| **Highlighted listing** | Facility shown in top positions |
| **Extended listing** | More photos, longer description, special badge |
| **Priority display** | Facility shown first on nearest monument detail |
| **Monthly flat fee** | Fixed payment for premium presentation |

### Owner Takeover as Gateway

Once an operator takes over management of their listing, they have a reason to invest in improving it. This opens the door to premium service offerings.

---

## 4. Contextual Advertising (Supplementary, Future)

- At high traffic (tens of thousands of unique visitors per month), advertising systems can be engaged
- Prefer contextual advertising (relevant to page content) over generic display ads

---

## Operating Economics

### Costs (Low-Budget Start)

| Item | Estimated Cost |
|------|---------------|
| Hosting (VPS) | Minimal / free tier initially, ~4 EUR/month when growing |
| Domain | ~15 USD/year (ceskarepublika.wiki) |
| Image storage | Free tier (Cloudflare R2) |
| CDN | Free tier (Cloudflare) |
| Email | Free tier (Cloudflare Email Routing) |
| **Total at start** | **~20 USD/year** + development time |

### Break-Even Strategy

1. **Phase 0:** Zero costs (free tier hosting, own work)
2. **Phase 1:** Minimal costs (~50 EUR/year), building content and SEO
3. **Phase 4:** Previo integration, first accommodation commissions
4. **Phase 5:** Real estate added, revenue diversification
5. **Phase 6+:** Premium services, scaling

---

## Competitive Advantage

| Aspect | CeskaRepublika.wiki | Booking.com | hrady.cz | Real estate portals |
|--------|---------------------|-------------|----------|---------------------|
| Territorial hierarchy | Complete (6,258 municipalities) | No | Partial | No |
| Monuments | Yes | No | Yes | No |
| Accommodation | Yes (via Previo) | Yes | No | No |
| Real estate | Yes | No | No | Yes |
| Everything in one place | Yes | No | No | No |
| Wiki character | Yes | No | No | No |
| SEO optimization | Maximum | Good | Medium | Varies |
