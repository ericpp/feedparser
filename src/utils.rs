use std::collections::HashMap;

use chrono::DateTime;
use md5;
use serde_json::{json, Value as JsonValue};

use crate::parser_state::{ParserState, ValueRecipient};



pub fn clean_string(s: &str) -> String {
    s.trim().replace(r#"(\r\n|\n|\r)"#, "")
}

pub fn truncate_string(s: &str, length: usize) -> String {
    s.chars().take(length).collect()
}

pub fn truncate_int(number: i32) -> i32 {
    number.clamp(-2147483647, 2147483647)
}

fn contains_non_latin_codepoints(s: &str) -> bool {
    s.chars().any(|c| c as u32 > 0x00FF)
}

fn replace_non_latin_characters(s: &str) -> String {
    s.replace(r#"[^\x00-\x80]"#, " ")
}

pub fn sanitize_url(url: &str) -> String {
    if url.is_empty() {
        return String::new();
    }

    if contains_non_latin_codepoints(url) {
        let encoded = urlencoding::encode(&url);
        let mut new_url = truncate_string(&encoded, 768);

        if contains_non_latin_codepoints(&new_url) {
            new_url = replace_non_latin_characters(&new_url);
        }

        return truncate_string(&new_url, 768);
    }

    truncate_string(url, 768)
}

pub fn pub_date_to_timestamp(pub_date: &str) -> i64 {
    let pub_date_str = pub_date.trim();
    if pub_date_str.is_empty() {
        return 0; // bad pub date, return 0
    }

    if let Ok(num) = pub_date_str.parse::<i64>() {
        return num; // already a timestamp
    }

    // parse rfc 2882 (rss spec) and iso 8601 (rfc 3339)
    DateTime::parse_from_rfc2822(pub_date_str)
        .or_else(|_| DateTime::parse_from_rfc3339(pub_date_str))
        .map(|dt| dt.timestamp())
        .unwrap_or(0) // return timestamp or 0 if error
}



pub fn calculate_update_frequency(pubdates: &[i64]) -> i32 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    
    let time_400_days_ago = now - (400 * 24 * 60 * 60);
    let time_200_days_ago = now - (200 * 24 * 60 * 60);
    let time_100_days_ago = now - (100 * 24 * 60 * 60);
    let time_40_days_ago = now - (40 * 24 * 60 * 60);
    let time_20_days_ago = now - (20 * 24 * 60 * 60);
    let time_10_days_ago = now - (10 * 24 * 60 * 60);
    let time_5_days_ago = now - (5 * 24 * 60 * 60);

    if pubdates.iter().filter(|&&time| time > time_400_days_ago).count() == 0 {
        return 9;
    }
    if pubdates.iter().filter(|&&time| time > time_200_days_ago).count() == 0 {
        return 8;
    }
    if pubdates.iter().filter(|&&time| time > time_100_days_ago).count() == 0 {
        return 7;
    }
    if pubdates.iter().filter(|&&time| time > time_5_days_ago).count() > 1 {
        return 1;
    }
    if pubdates.iter().filter(|&&time| time > time_10_days_ago).count() > 1 {
        return 2;
    }
    if pubdates.iter().filter(|&&time| time > time_20_days_ago).count() > 1 {
        return 3;
    }
    if pubdates.iter().filter(|&&time| time > time_40_days_ago).count() > 1 {
        return 4;
    }
    if pubdates.iter().filter(|&&time| time > time_100_days_ago).count() > 1 {
        return 5;
    }
    if pubdates.iter().filter(|&&time| time > time_200_days_ago).count() > 1 {
        return 6;
    }
    if pubdates.iter().filter(|&&time| time > time_400_days_ago).count() >= 1 {
        return 7;
    }
    0
}

//Get a mime-type string for an unknown media enclosure
pub fn guess_enclosure_type(url: &str) -> String {
    if url.contains(".m4v") {
        return "video/mp4".to_string();
    }
    if url.contains(".mp4") {
        return "video/mp4".to_string();
    }
    if url.contains(".avi") {
        return "video/avi".to_string();
    }
    if url.contains(".mov") {
        return "video/quicktime".to_string();
    }
    if url.contains(".mp3") {
        return "audio/mpeg".to_string();
    }
    if url.contains(".m4a") {
        return "audio/mp4".to_string();
    }
    if url.contains(".wav") {
        return "audio/wav".to_string();
    }
    if url.contains(".ogg") {
        return "audio/ogg".to_string();
    }
    if url.contains(".wmv") {
        return "video/x-ms-wmv".to_string();
    }

    "".to_string()
}

/*
* Convert time string to seconds
* 01:02 = 62 seconds
* Thanks to Glenn Bennett!
*/
pub fn time_to_seconds(time_string: &str) -> i32 {
    let parts = time_string.split(':').collect::<Vec<&str>>();

    match parts.len() - 1 {
        1 => {
            let minutes = parts[0].parse::<i32>().unwrap_or(0);
            let seconds = parts[1].parse::<i32>().unwrap_or(0);
            minutes * 60 + seconds
        }
        2 => {
            let hours = parts[0].parse::<i32>().unwrap_or(0);
            let minutes = parts[1].parse::<i32>().unwrap_or(0);
            let seconds = parts[2].parse::<i32>().unwrap_or(0);
            hours * 3600 + minutes * 60 + seconds
        }
        _ => time_string.parse::<i32>().unwrap_or(30 * 60),
    }
}

////////////////


const CATEGORY_LOOKUP: &[&str] = &[
    "", "arts", "books", "design", "fashion", "beauty", "food", "performing", "visual", "business",
    "careers", "entrepreneurship", "investing", "management", "marketing", "nonprofit", "comedy",
    "interviews", "improv", "standup", "education", "courses", "howto", "language", "learning",
    "selfimprovement", "fiction", "drama", "history", "health", "fitness", "alternative", "medicine",
    "mental", "nutrition", "sexuality", "kids", "family", "parenting", "pets", "animals", "stories",
    "leisure", "animation", "manga", "automotive", "aviation", "crafts", "games", "hobbies", "home",
    "garden", "videogames", "music", "commentary", "news", "daily", "entertainment", "government",
    "politics", "buddhism", "christianity", "hinduism", "islam", "judaism", "religion", "spirituality",
    "science", "astronomy", "chemistry", "earth", "life", "mathematics", "natural", "nature", "physics",
    "social", "society", "culture", "documentary", "personal", "journals", "philosophy", "places",
    "travel", "relationships", "sports", "baseball", "basketball", "cricket", "fantasy", "football",
    "golf", "hockey", "rugby", "running", "soccer", "swimming", "tennis", "volleyball", "wilderness",
    "wrestling", "technology", "truecrime", "tv", "film", "aftershows", "reviews", "climate", "weather",
    "tabletop", "role-playing", "cryptocurrency",
];

pub fn md5_hex_from_parts(parts: &[&str]) -> String {
    let mut ctx = md5::Context::new();
    for part in parts {
        ctx.consume(part.trim().as_bytes());
    }
    format!("{:x}", ctx.compute())
}

pub fn parse_itunes_duration(raw: &str) -> i32 {
    if let Ok(seconds) = raw.parse::<i32>() {
        truncate_int(seconds)
    } else {
        time_to_seconds(raw)
    }
}


pub fn build_category_ids(raw: &[String]) -> Vec<i32> {
    let mut cats: Vec<String> = raw
        .iter()
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect();

    if cats.contains(&"video".to_string()) && cats.contains(&"games".to_string()) {
        cats.push("videogames".to_string());
    }
    if cats.contains(&"true".to_string()) && cats.contains(&"crime".to_string()) {
        cats.push("truecrime".to_string());
    }
    if cats.contains(&"after".to_string()) && cats.contains(&"shows".to_string()) {
        cats.push("aftershows".to_string());
    }
    if cats.contains(&"self".to_string()) && cats.contains(&"improvement".to_string()) {
        cats.push("selfimprovement".to_string());
    }
    if cats.contains(&"how".to_string()) && cats.contains(&"to".to_string()) {
        cats.push("howto".to_string());
    }

    let mut ids = vec![0_i32; 11];
    let mut seen = 0;
    for name in cats {
        // Match JS: stop after the first 8 valid categories
        if seen >= 8 {
            break;
        }
        let normalized = name.replace(' ', "").replace('-', "");
        if let Some(idx) = CATEGORY_LOOKUP.iter().position(|v| v == &normalized) {
            if idx > 0 {
                seen += 1;
                ids[seen] = idx as i32;
            }
        }
    }
    ids
}

pub fn truncate_str(s: &str, max_len: usize) -> String {
    // Walk chars so we do not split multi-byte characters when truncating
    let mut out = String::new();
    for (idx, ch) in s.trim().chars().enumerate() {
        if idx >= max_len {
            break;
        }
        out.push(ch);
    }
    out
}

pub fn truncate_preserve(s: &str, max_len: usize) -> String {
    // Match JS substring-style behaviour without breaking UTF-8 boundaries
    let mut out = String::new();
    for (idx, ch) in s.chars().enumerate() {
        if idx >= max_len {
            break;
        }
        out.push(ch);
    }
    out
}

pub fn transcript_type_from_mime(mime: &str) -> i32 {
    let lower = mime.to_ascii_lowercase();
    if lower.contains("json") {
        1
    } else if lower.contains("srt") {
        2
    } else if lower.contains("vtt") {
        3
    } else {
        0
    }
}

pub fn parse_f64_or_zero(raw: &str) -> f64 {
    raw.trim().parse::<f64>().unwrap_or(0.0)
}

pub fn build_value_block(state: &ParserState) -> Option<String> {
    if state.value_recipients.is_empty() {
        return None;
    }

    let mut recips = state.value_recipients.clone();
    recips.truncate(100);

    let destinations: Vec<JsonValue> = recips
        .into_iter()
        .map(|r: ValueRecipient| {
            let mut obj = serde_json::Map::new();
            obj.insert("name".into(), JsonValue::from(r.name));
            obj.insert("type".into(), JsonValue::from(r.recipient_type));
            obj.insert("address".into(), JsonValue::from(r.address));
            obj.insert("split".into(), JsonValue::from(r.split));
            if r.fee {
                obj.insert("fee".into(), JsonValue::from(true));
            }
            if let Some(k) = r.custom_key {
                obj.insert("customKey".into(), JsonValue::from(k));
            }
            if let Some(v) = r.custom_value {
                obj.insert("customValue".into(), JsonValue::from(v));
            }
            JsonValue::Object(obj)
        })
        .collect();

    let mut model: HashMap<&str, &str> = HashMap::new();
    model.insert("type", &state.value_model_type);
    model.insert("method", &state.value_model_method);

    if !state.value_model_suggested.is_empty() {
        model.insert("suggested", &state.value_model_suggested);
    }

    let value_block = json!({
        "model": model,
        "destinations": destinations,
    });

    Some(canonical_json_string(&value_block))
}

pub fn map_value_type(raw: &str) -> i32 {
    match raw.trim().to_ascii_lowercase().as_str() {
        "lightning" => 0,
        "hbd" => 1,
        "bitcoin" => 2,
        _ => 0,
    }
}

fn canonicalize_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(map) => {
            let mut keys: Vec<_> = map.keys().cloned().collect();
            keys.sort_unstable();

            let mut out = serde_json::Map::with_capacity(map.len());
            for key in keys {
                if let Some(v) = map.get(&key) {
                    out.insert(key, canonicalize_json(v));
                }
            }
            JsonValue::Object(out)
        }
        JsonValue::Array(items) => JsonValue::Array(items.iter().map(canonicalize_json).collect()),
        _ => value.clone(),
    }
}

fn canonical_json_string(value: &JsonValue) -> String {
    canonicalize_json(value).to_string()
}


