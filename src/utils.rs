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

pub fn parse_itunes_duration(raw: &str) -> i32 {
    let t = raw.trim();
    if t.is_empty() {
        return 0;
    }
    let parts: Vec<&str> = t.split(':').collect();
    if parts.len() == 3 {
        let h = parts[0].parse::<i32>().unwrap_or(0);
        let m = parts[1].parse::<i32>().unwrap_or(0);
        let s = parts[2].parse::<i32>().unwrap_or(0);
        return h * 3600 + m * 60 + s;
    }
    if parts.len() == 2 {
        let m = parts[0].parse::<i32>().unwrap_or(0);
        let s = parts[1].parse::<i32>().unwrap_or(0);
        return m * 60 + s;
    }
    t.parse::<i32>().unwrap_or(0)
}

pub fn parse_numeric_token(raw: &str) -> i32 {
    let digits: String = raw.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        0
    } else {
        digits.parse::<i32>().unwrap_or(0)
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
    if pubdates.len() < 2 {
        return 9;
    }
    let min = *pubdates.iter().min().unwrap_or(&0);
    let max = *pubdates.iter().max().unwrap_or(&0);
    let span = max.saturating_sub(min);
    let avg = span / (pubdates.len().saturating_sub(1) as i64).max(1);

    let day = 86_400;
    if avg <= 5 * day {
        1
    } else if avg <= 10 * day {
        2
    } else if avg <= 20 * day {
        3
    } else if avg <= 40 * day {
        4
    } else if avg <= 70 * day {
        5
    } else if avg <= 100 * day {
        6
    } else if avg <= 200 * day {
        7
    } else if avg <= 400 * day {
        8
    } else {
        9
    }
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

pub fn generate_item_id(guid: &str, enclosure_url: &str, feed_id: Option<i64>) -> String {
    let composite = format!(
        "{}|{}|{}",
        feed_id.unwrap_or(0),
        guid.trim(),
        enclosure_url.trim()
    );
    format!("{:x}", md5::compute(composite.as_bytes()))
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
            obj.insert("fee".into(), JsonValue::from(r.fee));
            if let Some(k) = r.custom_key {
                obj.insert("customKey".into(), JsonValue::from(k));
            }
            if let Some(v) = r.custom_value {
                obj.insert("customValue".into(), JsonValue::from(v));
            }
            JsonValue::Object(obj)
        })
        .collect();

    let value_block = json!({
        "model": {
            "type": state.value_model_type,
            "method": state.value_model_method,
            "suggested": state.value_model_suggested,
        },
        "destinations": destinations,
    });

    Some(value_block.to_string())
}

pub fn map_value_type(raw: &str) -> i32 {
    match raw.trim().to_ascii_lowercase().as_str() {
        "lightning" => 0,
        "hbd" => 1,
        "bitcoin" => 2,
        _ => 0,
    }
}


