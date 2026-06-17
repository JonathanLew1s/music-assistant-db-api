// Camelot wheel notation. C major = 8B, A minor = 8A.
//
// Formula derived from the circle of fifths:
//   major: ((idx * 7 + 8) % 12), where 0 → 12
//   minor: relative major is +3 semitones, so (((idx + 3) * 7 + 8) % 12), 0 → 12
const NOTE_ORDER: [&str; 12] = ["C","C#","D","D#","E","F","F#","G","G#","A","A#","B"];

fn normalise_key(key: &str) -> &str {
    match key {
        "Db" => "C#", "Eb" => "D#", "Gb" => "F#", "Ab" => "G#", "Bb" => "A#",
        other => other,
    }
}

pub fn to_camelot(key: &str, mode: &str) -> Option<String> {
    let tonic = normalise_key(key.trim());
    let idx = NOTE_ORDER.iter().position(|&n| n == tonic)?;
    let raw = match mode.trim().to_lowercase().as_str() {
        "major" => (idx * 7 + 8) % 12,
        "minor" => ((idx + 3) * 7 + 8) % 12,
        _ => return None,
    };
    let n = if raw == 0 { 12 } else { raw };
    let suffix = if mode.trim().to_lowercase() == "major" { "B" } else { "A" };
    Some(format!("{n}{suffix}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn camelot_c_major() {
        assert_eq!(to_camelot("C", "major"), Some("8B".into()));
    }

    #[test]
    fn camelot_a_minor() {
        assert_eq!(to_camelot("A", "minor"), Some("8A".into()));
    }

    #[test]
    fn camelot_d_sharp_minor() {
        assert_eq!(to_camelot("D#", "minor"), Some("2A".into()));
    }

    #[test]
    fn camelot_g_major() {
        assert_eq!(to_camelot("G", "major"), Some("9B".into()));
    }

    #[test]
    fn camelot_e_major() {
        // E major = 12B
        assert_eq!(to_camelot("E", "major"), Some("12B".into()));
    }

    #[test]
    fn camelot_enharmonic() {
        assert_eq!(to_camelot("Bb", "major"), to_camelot("A#", "major"));
    }

    #[test]
    fn camelot_unknown_key() {
        assert_eq!(to_camelot("X", "major"), None);
    }
}
