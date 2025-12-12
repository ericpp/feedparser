use std::collections::HashMap;

use chrono::DateTime;
use md5;
use serde_json::{json, Value as JsonValue};

use crate::parser_state::{ParserState, ValueRecipient};

pub fn md5_hex_from_parts(parts: &[&str]) -> String {
    let mut ctx = md5::Context::new();
    for part in parts {
        ctx.consume(part.trim().as_bytes());
    }
    format!("{:x}", ctx.compute())
}

pub fn parse_pub_date_to_unix(raw: &str) -> Option<i64> {
    let t = raw.trim();
    if t.is_empty() {
        return None;
    }
    if let Ok(num) = t.parse::<i64>() {
        return Some(num);
    }
    DateTime::parse_from_rfc2822(t)
        .or_else(|_| DateTime::parse_from_rfc3339(t))
        .or_else(|_| DateTime::parse_from_str(t, "%a, %d %b %Y %H:%M:%S %z"))
        .or_else(|_| DateTime::parse_from_str(t, "%a, %d %b %Y %H:%M:%S GMT"))
        .map(|dt| dt.timestamp())
        .ok()
}

pub fn time_to_seconds(time_string: &str) -> i32 {
    let parts: Vec<&str> = time_string.split(':').collect();

    match parts.len() {
        2 => match (parts[0].parse::<i32>(), parts[1].parse::<i32>()) {
            (Ok(minutes), Ok(secs)) => {
                minutes * 60 + secs
            }
            _ => 0,
        },
        3 => match (parts[0].parse::<i32>(), parts[1].parse::<i32>(), parts[2].parse::<i32>()) {
            (Ok(hours), Ok(minutes), Ok(secs)) => {
                hours * 3600 + minutes * 60 + secs
            }
            _ => 0,
        },
        _ => 0,
    }
}

pub fn truncate_int(number: i32) -> i32 {
    number.clamp(-2147483647, 2147483647)
}

pub fn parse_itunes_duration(raw: &str) -> i32 {
    if let Ok(seconds) = raw.parse::<i32>() {
        truncate_int(seconds)
    } else {
        time_to_seconds(raw)
    }
}

pub fn guess_enclosure_type(url: &str) -> String {
    let lower = url.to_ascii_lowercase();
    if lower.ends_with(".mp3") || lower.ends_with(".mpeg") {
        "audio/mpeg".to_string()
    } else if lower.ends_with(".m4a") || lower.ends_with(".mp4") {
        "audio/mp4".to_string()
    } else if lower.ends_with(".ogg") || lower.ends_with(".oga") {
        "audio/ogg".to_string()
    } else if lower.ends_with(".wav") {
        "audio/wav".to_string()
    } else if lower.ends_with(".webm") {
        "audio/webm".to_string()
    } else if lower.ends_with(".flac") {
        "audio/flac".to_string()
    } else {
        "audio/mpeg".to_string()
    }
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

pub fn build_category_ids(raw: &[String]) -> Vec<i32> {
    // index 0 unused to preserve positional mapping
    let mut ids = vec![0_i32; 11];
    let mut seen = 0;
    for name in raw {
        if seen >= 10 {
            break;
        }
        let code = match name.trim().to_ascii_lowercase().as_str() {
            "technology" => 102,
            "video" => 48,
            "games" => 52,
            _ => 0,
        };
        if code > 0 {
            seen += 1;
            ids[seen] = code;
        }
    }
    ids
}

pub fn truncate_str(s: &str, max_len: usize) -> String {
    let mut out = s.trim().to_string();
    if out.len() > max_len {
        out.truncate(max_len);
    }
    out
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


