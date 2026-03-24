/// Generates a URL-safe slug from a Czech name.
///
/// Rules:
/// 1. Lowercase everything
/// 2. Remove Czech diacritics (č→c, ř→r, š→s, ž→z, etc.)
/// 3. Replace spaces with hyphens
/// 4. Keep existing hyphens
/// 5. Remove any non-alphanumeric, non-hyphen characters
/// 6. Collapse multiple consecutive hyphens into one
/// 7. Trim leading/trailing hyphens
pub fn slug_from_name(name: &str) -> String {
    let lowered = name.to_lowercase();

    let mut result = String::with_capacity(lowered.len());
    for ch in lowered.chars() {
        match ch {
            'á' | 'à' => result.push('a'),
            'č' => result.push('c'),
            'ď' => result.push('d'),
            'é' | 'ě' | 'è' => result.push('e'),
            'í' | 'ì' => result.push('i'),
            'ň' => result.push('n'),
            'ó' | 'ò' => result.push('o'),
            'ř' => result.push('r'),
            'š' => result.push('s'),
            'ť' => result.push('t'),
            'ú' | 'ů' | 'ù' => result.push('u'),
            'ý' | 'ỳ' => result.push('y'),
            'ž' => result.push('z'),
            ' ' => result.push('-'),
            '-' => result.push('-'),
            c if c.is_ascii_alphanumeric() => result.push(c),
            _ => {}
        }
    }

    // Collapse multiple hyphens and trim
    let mut collapsed = String::with_capacity(result.len());
    let mut prev_hyphen = false;
    for ch in result.chars() {
        if ch == '-' {
            if !prev_hyphen {
                collapsed.push('-');
            }
            prev_hyphen = true;
        } else {
            collapsed.push(ch);
            prev_hyphen = false;
        }
    }

    let collapsed = collapsed.trim_matches('-');
    collapsed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_czech_diacritics() {
        assert_eq!(slug_from_name("Středočeský kraj"), "stredocesky-kraj");
        assert_eq!(slug_from_name("Jihomoravský kraj"), "jihomoravsky-kraj");
        assert_eq!(slug_from_name("Olomoucký kraj"), "olomoucky-kraj");
    }

    #[test]
    fn region_slugs() {
        assert_eq!(slug_from_name("Hlavní město Praha"), "hlavni-mesto-praha");
        assert_eq!(slug_from_name("Středočeský kraj"), "stredocesky-kraj");
        assert_eq!(slug_from_name("Jihočeský kraj"), "jihocesky-kraj");
        assert_eq!(slug_from_name("Plzeňský kraj"), "plzensky-kraj");
        assert_eq!(slug_from_name("Karlovarský kraj"), "karlovarsky-kraj");
        assert_eq!(slug_from_name("Ústecký kraj"), "ustecky-kraj");
        assert_eq!(slug_from_name("Liberecký kraj"), "liberecky-kraj");
        assert_eq!(
            slug_from_name("Královéhradecký kraj"),
            "kralovehradecky-kraj"
        );
        assert_eq!(slug_from_name("Pardubický kraj"), "pardubicky-kraj");
        assert_eq!(slug_from_name("Kraj Vysočina"), "kraj-vysocina");
        assert_eq!(slug_from_name("Jihomoravský kraj"), "jihomoravsky-kraj");
        assert_eq!(slug_from_name("Olomoucký kraj"), "olomoucky-kraj");
        assert_eq!(slug_from_name("Zlínský kraj"), "zlinsky-kraj");
        assert_eq!(
            slug_from_name("Moravskoslezský kraj"),
            "moravskoslezsky-kraj"
        );
    }

    #[test]
    fn existing_hyphens_preserved() {
        assert_eq!(slug_from_name("Frýdek-Místek"), "frydek-mistek");
    }

    #[test]
    fn complex_names_with_hyphens_and_spaces() {
        assert_eq!(
            slug_from_name("Brandýs nad Labem-Stará Boleslav"),
            "brandys-nad-labem-stara-boleslav"
        );
    }

    #[test]
    fn names_with_prepositions() {
        assert_eq!(slug_from_name("Žďár nad Sázavou"), "zdar-nad-sazavou");
        assert_eq!(slug_from_name("Ústí nad Labem"), "usti-nad-labem");
        assert_eq!(slug_from_name("Dvůr Králové nad Labem"), "dvur-kralove-nad-labem");
    }

    #[test]
    fn simple_names() {
        assert_eq!(slug_from_name("Brno"), "brno");
        assert_eq!(slug_from_name("Praha"), "praha");
        assert_eq!(slug_from_name("Adamov"), "adamov");
    }

    #[test]
    fn ring_u_handled() {
        assert_eq!(slug_from_name("Růžová"), "ruzova");
        assert_eq!(slug_from_name("Důl"), "dul");
    }

    #[test]
    fn multiple_spaces_collapse() {
        assert_eq!(slug_from_name("Foo  Bar"), "foo-bar");
    }

    #[test]
    fn leading_trailing_hyphens_trimmed() {
        assert_eq!(slug_from_name(" Praha "), "praha");
        assert_eq!(slug_from_name("-Praha-"), "praha");
    }

    #[test]
    fn empty_string() {
        assert_eq!(slug_from_name(""), "");
    }

    #[test]
    fn only_diacritics() {
        assert_eq!(slug_from_name("Řčšžťďňě"), "rcsztdne");
    }

    #[test]
    fn numeric_parts_preserved() {
        assert_eq!(slug_from_name("Praha 1"), "praha-1");
    }

    #[test]
    fn special_characters_removed() {
        assert_eq!(slug_from_name("Město (okres)"), "mesto-okres");
    }

    #[test]
    fn cesky_krumlov() {
        assert_eq!(slug_from_name("Český Krumlov"), "cesky-krumlov");
    }
}
