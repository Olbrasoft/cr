# UI Wireframes

> ASCII wireframes for key page types. Design principles from the proof of concept.

---

## UX Principles

1. **Simplicity:** Clean, clear design without information overload
2. **Mobile first:** Responsive design — majority of visitors from mobile
3. **Speed:** Fast loading — no heavy frameworks, minimal JavaScript
4. **Accessibility:** Semantic HTML, good contrast, keyboard navigation
5. **Encyclopedic character:** Design matching a wiki/encyclopedic portal

---

## Homepage (`/`)

```
+------------------------------------------------------------+
|  Logo: CeskaRepublika.wiki                      [Search]   |
|  Encyclopedia of the Czech Republic                        |
+------------------------------------------------------------+
|                                                            |
|              +---------------------+                       |
|              |   Interactive map   |                       |
|              |   of Czech Republic |                       |
|              |   (14 regions)      |                       |
|              |   clickable SVG     |                       |
|              +---------------------+                       |
|                                                            |
|  Regions of the Czech Republic:                            |
|  +----------+----------+----------+----------+             |
|  | Praha    |Stredoces.|Jihocesky |Plzensky  |             |
|  +----------+----------+----------+----------+             |
|  |Karlovar. |Ustecky   |Liberecky |Kralovehr.|             |
|  +----------+----------+----------+----------+             |
|  |Pardubicky|Vysocina  |Jihomorav.|Olomoucky |             |
|  +----------+----------+----------+----------+             |
|  |Zlinsky   |Moravsko- |          |          |             |
|  |          |slezsky   |          |          |             |
|  +----------+----------+----------+----------+             |
|                                                            |
|  [Search] Find a municipality, monument, or accommodation  |
|                                                            |
+------------------------------------------------------------+
```

---

## Region Page (`/stredocesky-kraj/`)

```
+------------------------------------------------------------+
|  Breadcrumb: Home > Stredocesky kraj                       |
+------------------------------------------------------------+
|                                                            |
|  # Stredocesky kraj                                        |
|  12 districts | 26 ORP | 1,144 municipalities             |
|                                                            |
|  [Brief region description]                                |
|                                                            |
|  +---------------------+                                   |
|  |   Region map        |                                   |
|  |   with ORP areas    |                                   |
|  +---------------------+                                   |
|                                                            |
|  ORP:                                                      |
|  +-- Benesov (145 municipalities)                          |
|  +-- Beroun (85 municipalities)                            |
|  +-- Kladno (98 municipalities)                            |
|  +-- ...                                                   |
|                                                            |
|  Monuments in region:                                      |
|  [Karlstejn] [Konopiste] [Cesky Sternberk] ...            |
|                                                            |
|  Accommodation: -> /ubytovani-stredocesky-kraj/            |
|  Real estate: -> /reality-stredocesky-kraj/                |
|                                                            |
+------------------------------------------------------------+
```

---

## Municipality Page (`/stredocesky-kraj/benesov/benesov/`)

```
+------------------------------------------------------------+
|  Breadcrumb: Home > Stredocesky > Benesov (ORP) > Benesov  |
+------------------------------------------------------------+
|                                                            |
|  # Benesov                                                 |
|  Municipality in ORP Benesov, district Benesov,            |
|  Stredocesky kraj                                          |
|                                                            |
|  [Municipality description, basic info]                    |
|                                                            |
|  +---------------------+                                   |
|  |   Location map      |                                   |
|  +---------------------+                                   |
|                                                            |
|  --- Monuments in Benesov ---                              |
|  [Zamek Konopiste] [Kostel sv. Anny] ...                   |
|  -> All monuments: /pamatky-benesov/                       |
|                                                            |
|  --- Accommodation in Benesov ---                          |
|  [Hotel Posta ***] [Penzion u Zamku] ...                   |
|  -> All accommodation: /ubytovani-benesov/                 |
|                                                            |
|  --- Real estate in Benesov ---                            |
|  [Sale: 3+1 flat, 3,500,000 CZK] [Rent: 2+kk] ...        |
|  -> All real estate: /reality-benesov/                     |
|                                                            |
+------------------------------------------------------------+
```

---

## Accommodation Listing (`/ubytovani-benesov/`)

```
+------------------------------------------------------------+
|  Breadcrumb: Home > Accommodation > Benesov                |
+------------------------------------------------------------+
|                                                            |
|  # Accommodation in Benesov                                |
|  23 accommodation facilities                               |
|                                                            |
|  Filter: [Type v] [Price v] [Rating v]                     |
|                                                            |
|  +------------------------------------------+              |
|  | [photo] Hotel Posta ****                 |              |
|  |         from 1,200 CZK/night             |              |
|  |         Benesov, city center              |              |
|  |         [View] [Reserve]                 |              |
|  +------------------------------------------+              |
|  | [photo] Penzion u Zamku ***              |              |
|  |         from 800 CZK/night               |              |
|  |         Benesov, near Konopiste chateau   |              |
|  |         [View] [Reserve]                 |              |
|  +------------------------------------------+              |
|                                                            |
|  ... more results (infinite scroll / pagination)           |
|                                                            |
|  [Pagination: 1 2 3 ... 5 Next ->]                        |
|  [Switch to: o Infinite scroll / * Pagination]             |
|                                                            |
+------------------------------------------------------------+
```

---

## Search (`/hledani/`)

```
+------------------------------------------------------------+
|  [Search: romantic accommodation near a castle       ]     |
|                                                            |
|  Suggestions:                                              |
|  +-- Hrad Karlstejn (monument)                             |
|  +-- Hrad Bezdez (monument)                                |
|  +-- Hotel u Hradu, Cesky Krumlov (accommodation)          |
|  +-- Hradec Kralove (city)                                 |
|                                                            |
|  Search results:                                           |
|  [Municipalities] [Monuments] [Accommodation] [All]        |
|                                                            |
|  [combined results from all modules]                       |
|                                                            |
+------------------------------------------------------------+
```

---

## Pagination Strategy

### For Search Engine Robots (SEO)
- Classic pagination with numeric URLs: `?strana=1`, `?strana=2`, `?strana=3`
- Each page has its own URL and is fully indexable
- Navigation links "Previous / Next" + page numbers
- `<link rel="prev">` and `<link rel="next">` in HTML head

### For Users
- **Default mode:** Infinite scrolling — more records load automatically when reaching the end
- **Switch option:** User can switch to classic pagination (toggle)
- **Preference saved:** User's choice remembered (cookie/localStorage)

### Implementation Principle
```
Page /ubytovani-praha/
  -> Server sends first page of results (full HTML — for robots and people)
  -> Browser detects scroll and loads ?strana=2 asynchronously (for people only)
  -> Robot sees link to ?strana=2 and follows it separately (gets full HTML)
```

---

## Navigation Elements

### Header (every page)
- Logo and portal name
- Main navigation: Regions | Monuments | Accommodation | Real Estate
- Search field
- Login (future)

### Breadcrumb
- Always present on all pages except homepage
- Shows position in hierarchy
- Clickable links at each level

### Footer
- Links to static pages (About, Contact, Terms, Privacy)
- Copyright
- Contact email: info@ceskarepublika.wiki
