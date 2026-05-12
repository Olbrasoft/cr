use super::*;

pub async fn voices(State(state): State<AppState>) -> WebResult<impl IntoResponse> {
    let tmpl = VoicesTemplate {
        img: state.image_base_url.clone(),
        voices: voice_library(),
    };
    Ok(Html(tmpl.render()?))
}

pub async fn prevod_textu_na_hlas(State(state): State<AppState>) -> WebResult<impl IntoResponse> {
    let tmpl = PrevodTextuNaHlasTemplate {
        img: state.image_base_url.clone(),
        voices: voice_library(),
    };
    Ok(Html(tmpl.render()?))
}

pub(crate) struct VoiceEntry {
    pub(crate) slug: &'static str,
    pub(crate) name: &'static str,
    pub(crate) years: &'static str,
    pub(crate) role: &'static str,
    pub(crate) bio: &'static str,
    pub(crate) duration: &'static str,
    /// Verbatim personal intro that both Chatterbox and OmniVoice
    /// pronounce. Same source for both stacks so the comparison page
    /// can show identical text under each pair of audio players.
    pub(crate) intro_text: &'static str,
}

pub(crate) fn voice_library() -> Vec<VoiceEntry> {
    vec![
        VoiceEntry {
            slug: "moravec",
            name: "Miroslav Moravec",
            years: "1939–2009",
            role: "herec, dabér",
            bio: "Hlas Pierra Richarda a Louise de Funèse v českém znění. Daboval přes 600 rolí.",
            duration: "11,0 s",
            intro_text: "Dobrý den, jmenuji se Miroslav Moravec. Jsem herec a dabér, narodil jsem se v roce devatenáct set třicet devět. Mluvil jsem Pierra Richarda a Louise de Funèse.",
        },
        VoiceEntry {
            slug: "bartoska",
            name: "Jiří Bartoška",
            years: "1947–2025",
            role: "herec, prezident MFF Karlovy Vary",
            bio: "Filmový a divadelní herec. Od roku 1994 až do své smrti v květnu 2025 prezident Mezinárodního filmového festivalu v Karlových Varech.",
            duration: "9,1 s",
            intro_text: "Dobrý den, jsem Jiří Bartoška, narozený v roce tisíc devět set čtyřicet sedm. Herec a prezident Mezinárodního filmového festivalu v Karlových Varech.",
        },
        VoiceEntry {
            slug: "rimsky",
            name: "Pavel Rímský",
            years: "*1949",
            role: "herec, dabér",
            bio: "Herec Vinohradského divadla a dabér — propůjčil hlas mj. Sylvestru Stallonemu, Lucianu Pavarottimu a dalším.",
            duration: "10,8 s",
            intro_text: "Dobrý den, jmenuji se Pavel Rímský, narodil jsem se v roce tisíc devět set čtyřicet devět. Jsem herec a dabér, hlas Luciana Pavarottiho v českém znění.",
        },
        VoiceEntry {
            slug: "rossner",
            name: "Boris Rösner",
            years: "1951–2006",
            role: "herec, dabér",
            bio: "Charakterní herec Národního divadla a TV.",
            duration: "7,8 s",
            intro_text: "Dobrý den, jsem Boris Rössner, narodil jsem se v roce tisíc devět set padesát jedna. Český herec a dabér, hrál jsem v Národním divadle.",
        },
        VoiceEntry {
            slug: "lukavsky",
            name: "Radovan Lukavský",
            years: "1919–2008",
            role: "herec, profesor DAMU",
            bio: "Jedna z legend Národního divadla. Dlouholetý profesor a vedoucí katedry herectví na DAMU.",
            duration: "13,1 s",
            intro_text: "Dobrý den, jmenuji se Radovan Lukavský, narodil jsem se v roce tisíc devět set devatenáct. Herec Národního divadla a dlouholetý profesor DAMU.",
        },
        VoiceEntry {
            slug: "kostka",
            name: "Petr Kostka",
            years: "*1938",
            role: "herec, dabér",
            bio: "Herec Národního divadla, recitátor poezie, dabér.",
            duration: "10,1 s",
            intro_text: "Dobrý den, jsem Petr Kostka, narozený v roce devatenáct set třicet osm. Herec Národního divadla, dabér a recitátor poezie.",
        },
        VoiceEntry {
            slug: "stransky",
            name: "Martin Stránský",
            years: "*1970",
            role: "herec, dabér",
            bio: "Český herec a dabér.",
            duration: "6,6 s",
            intro_text: "Dobrý den, jmenuji se Martin Stránský, narodil jsem se v roce tisíc devět set sedmdesát. Jsem herec a dabér.",
        },
        VoiceEntry {
            slug: "adamovska",
            name: "Zlata Adamovská",
            years: "*1959",
            role: "herečka",
            bio: "Známá ze seriálu Ulice, Sanitka a desítek filmů.",
            duration: "10,6 s",
            intro_text: "Dobrý den, jsem Zlata Adamovská, narozená v roce tisíc devět set padesát devět. Diváci mě znají ze seriálu Ulice a z desítek filmů a divadelních rolí.",
        },
        VoiceEntry {
            slug: "balzerova",
            name: "Eliška Balzerová",
            years: "*1949",
            role: "herečka",
            bio: "Vrchní, prchni!, Nemocnice na kraji města. Herečka Vinohradského divadla.",
            duration: "10,7 s",
            intro_text: "Dobrý den, jmenuji se Eliška Balzerová, narodila jsem se v roce tisíc devět set čtyřicet devět. Hrála jsem ve filmech Vrchní, prchni a Nemocnice na kraji města.",
        },
        VoiceEntry {
            slug: "boudova",
            name: "Nela Boudová",
            years: "*1967",
            role: "herečka",
            bio: "Česká herečka v televizních seriálech a filmech.",
            duration: "7,8 s",
            intro_text: "Dobrý den, jsem Nela Boudová, narozená v roce tisíc devět set šedesát sedm. Česká herečka, hrála jsem v televizních seriálech i ve filmech.",
        },
        VoiceEntry {
            slug: "postlerova",
            name: "Simona Postlerová",
            years: "1964–2024",
            role: "herečka",
            bio: "Herečka Národního divadla v Praze.",
            duration: "8,9 s",
            intro_text: "Dobrý den, jmenuji se Simona Postlerová, narodila jsem se v roce tisíc devět set šedesát čtyři. Herečka Národního divadla v Praze.",
        },
        VoiceEntry {
            slug: "cerna",
            name: "Dana Černá",
            years: "*1970",
            role: "herečka, dabérka",
            bio: "Česká dabérka a rozhlasová herečka.",
            duration: "9,3 s",
            intro_text: "Dobrý den, jsem Dana Černá, česká herečka a dabérka. Pracovala jsem v rozhlase a propůjčila hlas mnoha postavám v dabingu.",
        },
        VoiceEntry {
            slug: "syslova",
            name: "Dana Syslová",
            years: "*1945",
            role: "herečka, dabérka",
            bio: "Dlouholetá členka Pražských městských divadel. Český hlas Meryl Streep, Susan Sarandon a Helen Mirren.",
            duration: "10,5 s",
            intro_text: "Dobrý den, jmenuji se Dana Syslová, narodila jsem se v roce tisíc devět set čtyřicet pět. Herečka a dabérka, dlouholetá členka Pražských městských divadel.",
        },
        VoiceEntry {
            slug: "fialova",
            name: "Květa Fialová",
            years: "1929–2017",
            role: "herečka",
            bio: "Tornádo Lou v Limonádovém Joeovi. Herečka Vinohradského divadla.",
            duration: "8,7 s",
            intro_text: "Dobrý den, jsem Květa Fialová, narozená v roce tisíc devět set dvacet devět. Herečka, diváci mě znají jako Tornádo Lou z Limonádového Joea.",
        },
        VoiceEntry {
            slug: "dockalova",
            name: "Tereza Dočkalová",
            years: "*1988",
            role: "herečka",
            bio: "Mladá generace, hraje v seriálu Specialisté.",
            duration: "9,3 s",
            intro_text: "Dobrý den, jmenuji se Tereza Dočkalová, narodila jsem se v roce tisíc devět set osmdesát osm. Mladá česká herečka, hraji v seriálu Specialisté.",
        },
        VoiceEntry {
            slug: "sverak",
            name: "Zdeněk Svěrák",
            years: "*1936",
            role: "herec, scenárista, dramatik",
            bio: "Autor Cimrmana, scénáře k filmům Vesničko má středisková a Kolja (Oscar 1996).",
            duration: "13,1 s",
            intro_text: "Dobrý den, jsem Zdeněk Svěrák, narozený v roce tisíc devět set třicet šest. Herec a scenárista, autor Cimrmana a scénářů Vesničko má středisková a Kolja.",
        },
        VoiceEntry {
            slug: "hartl",
            name: "Patrik Hartl",
            years: "*1976",
            role: "režisér, spisovatel",
            bio: "Divadelní režisér, scenárista, spisovatel.",
            duration: "10,1 s",
            intro_text: "Dobrý den, jmenuji se Patrik Hartl, narodil jsem se v roce tisíc devět set sedmdesát šest. Jsem režisér, divadelní autor a spisovatel.",
        },
        VoiceEntry {
            slug: "donutil",
            name: "Miroslav Donutil",
            years: "*1951",
            role: "herec",
            bio: "Herec Národního divadla, talk show Na kus řeči s Miroslavem Donutilem.",
            duration: "9,8 s",
            intro_text: "Dobrý den, jsem Miroslav Donutil, narozený v roce tisíc devět set padesát jedna. Herec Národního divadla, znám z talk show Na kus řeči s Miroslavem Donutilem.",
        },
        VoiceEntry {
            slug: "issova_26",
            name: "Klára Issová",
            years: "*1979",
            role: "herečka",
            bio: "Anděl Exit, zahraniční produkce.",
            duration: "12,7 s",
            intro_text: "Dobrý den, jmenuji se Klára Issová, narodila jsem se v roce tisíc devět set sedmdesát devět. Česká herečka, hrála jsem ve filmech Anděl Exit a v zahraničních produkcích.",
        },
        VoiceEntry {
            slug: "junak_61",
            name: "Zdeněk Junák",
            years: "*1951",
            role: "herec",
            bio: "Herec brněnské Mahenovy činohry Národního divadla v Brně.",
            duration: "10,1 s",
            intro_text: "Dobrý den, jsem Zdeněk Junák, narozený v roce devatenáct set padesát jedna. Herec brněnské Mahenovy činohry Národního divadla v Brně.",
        },
        VoiceEntry {
            slug: "stastny_60",
            name: "Jan Šťastný",
            years: "*1965",
            role: "herec, dabér",
            bio: "Český herec a dabér.",
            duration: "9,7 s",
            intro_text: "Dobrý den, jmenuji se Jan Šťastný, narodil jsem se v roce tisíc devět set šedesát pět. Jsem herec a dabér, hraji v televizních seriálech a divadlech.",
        },
        VoiceEntry {
            slug: "vondrackova_31",
            name: "Lucie Vondráčková",
            years: "*1980",
            role: "zpěvačka, herečka",
            bio: "Zpěvačka a herečka, dcera zpěváka Jiřího Vondráčka.",
            duration: "9,4 s",
            intro_text: "Dobrý den, jsem Lucie Vondráčková, narozená v roce tisíc devět set osmdesát. Zpěvačka a herečka, dcera zpěváka Jiřího Vondráčka.",
        },
    ]
}
