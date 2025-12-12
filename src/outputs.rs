use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Serialize, Deserialize};
use serde_json::Value as JsonValue;

use crate::{parser_state::ParserState, OUTPUT_SUBDIR, GLOBAL_COUNTER};
use crate::utils;

fn get_output_dir() -> PathBuf {
    OUTPUT_SUBDIR
        .get()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("outputs"))
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SqlInsert {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<JsonValue>,
    pub feed_id: Option<i64>,
}

fn write_record(record: &SqlInsert, table_for_name: &str) {
    // Ensure directory exists
    let out_dir = get_output_dir();
    if let Err(e) = fs::create_dir_all(&out_dir) {
        eprintln!("Failed to create outputs directory '{}': {}", out_dir.display(), e);
    }

    // Compute counter (1-based) and build filename
    let counter_val = GLOBAL_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    let fid_for_name = record
        .feed_id
        .map(|v| v.to_string())
        .unwrap_or_else(|| "NULL".to_string());
    let file_name = format!("{}_{}_{}.json", counter_val, table_for_name, fid_for_name);
    let file_path = out_dir.join(file_name);

    match serde_json::to_string(record) {
        Ok(serialized) => {
            if let Err(e) = fs::write(&file_path, serialized) {
                eprintln!("Failed to write {}: {}", file_path.display(), e);
            }
        }
        Err(e) => {
            eprintln!("Failed to serialize record for {}: {}", table_for_name, e);
        }
    }
}

pub fn write_newsfeeds(state: &ParserState, feed_id: Option<i64>) {
    let final_image = if state.channel_image.is_empty() && !state.channel_itunes_image.is_empty() {
        state.channel_itunes_image.clone()
    } else {
        state.channel_image.clone()
    };

    let final_description = if !state.channel_itunes_summary.trim().is_empty() {
        state.channel_itunes_summary.trim().to_string()
    } else {
        state.channel_description.trim().to_string()
    };

    let mut final_owner = state.channel_podcast_owner.trim().to_string();
    if final_owner.is_empty() && state.channel_podcast_locked == 1 {
        final_owner = state.channel_itunes_owner_email.trim().to_string();
    }
    let final_owner = final_owner.trim().to_string();

    let final_title = state.channel_title.trim().to_string();
    let final_language = state.channel_language.trim().to_string();

    let chash = utils::md5_hex_from_parts(&[
        &final_title,
        state.channel_link.trim(),
        &final_language,
        state.channel_generator.trim(),
        state.channel_itunes_author.trim(),
        state.channel_itunes_owner_name.trim(),
        state.channel_itunes_owner_email.trim(),
    ]);

    let podcast_chapters_hash = format!("{:x}", state.item_hash.clone().compute());
    let update_frequency = utils::calculate_update_frequency(&state.item_pubdates);

    let record = SqlInsert {
        table: "newsfeeds".to_string(),
        columns: vec![
            "id".to_string(),
            "title".to_string(),
            "url".to_string(),
            "content".to_string(),
            "language".to_string(),
            "generator".to_string(),
            "itunes_author".to_string(),
            "itunes_owner_name".to_string(),
            "itunes_owner_email".to_string(),
            "itunes_type".to_string(),
            "itunes_new_feed_url".to_string(),
            "explicit".to_string(),
            "image".to_string(),
            "itunes_image".to_string(),
            "podcast_locked".to_string(),
            "podcast_owner".to_string(),
            "artwork_url_600".to_string(),
            "item_count".to_string(),
            "newest_item_pubdate".to_string(),
            "oldest_item_pubdate".to_string(),
            "chash".to_string(),
            "podcast_chapters".to_string(),
            "update_frequency".to_string(),
            "itunes_id".to_string(),
        ],
        values: vec![
            match feed_id { Some(v) => JsonValue::from(v), None => JsonValue::Null },
            JsonValue::from(final_title),
            JsonValue::from(state.channel_link.trim().to_string()),
            JsonValue::from(final_description),
            JsonValue::from(final_language),
            JsonValue::from(state.channel_generator.trim().to_string()),
            JsonValue::from(state.channel_itunes_author.trim().to_string()),
            JsonValue::from(state.channel_itunes_owner_name.trim().to_string()),
            JsonValue::from(state.channel_itunes_owner_email.trim().to_string()),
            JsonValue::from(state.channel_itunes_type.trim().to_string()),
            JsonValue::from(state.channel_itunes_new_feed_url.trim().to_string()),
            JsonValue::from(state.channel_explicit),
            JsonValue::from(final_image.trim().to_string()),
            JsonValue::from(state.channel_itunes_image.trim().to_string()),
            JsonValue::from(state.channel_podcast_locked),
            JsonValue::from(final_owner),
            JsonValue::from(state.channel_itunes_image.trim().to_string()),
            JsonValue::from(state.item_count),
            match state.newest_item_pubdate { Some(v) => JsonValue::from(v), None => JsonValue::Null },
            match state.oldest_item_pubdate { Some(v) => JsonValue::from(v), None => JsonValue::Null },
            JsonValue::from(chash),
            JsonValue::from(podcast_chapters_hash),
            JsonValue::from(update_frequency),
            JsonValue::from(0), // itunes_id default
        ],
        feed_id,
    };
    write_record(&record, "newsfeeds");
}

pub fn write_nfitems(state: &ParserState, feed_id: Option<i64>) {
    // Prefer the most specific artwork available, fall back to channel-level art
    let final_item_image = if !state.item_image.is_empty() {
        state.item_image.clone()
    } else if !state.itunes_image.is_empty() {
        state.itunes_image.clone()
    } else if !state.channel_image.is_empty() {
        state.channel_image.clone()
    } else if !state.channel_itunes_image.is_empty() {
        state.channel_itunes_image.clone()
    } else {
        String::new()
    };

    // Preserve trailing whitespace to match JS output, but drop leading space
    let final_title = if !state.itunes_title.is_empty() {
        state.itunes_title.clone() // itunes:title not trimmed in partytime
    } else {
        state.title.trim().to_string()
    };

    let final_description = if !state.content.is_empty() {
        state.content.trim().to_string()
    } else if !state.itunes_summary.is_empty() {
        state.itunes_summary.trim().to_string()
    } else if !state.content_encoded.is_empty() {
        state.content_encoded.trim().to_string()
    } else if !state.description.is_empty() {
        state.description.trim().to_string()
    } else {
        String::new()
    };

    let itunes_season = state.itunes_season.trim().parse::<i32>().ok();
    let itunes_episode = state.itunes_episode.trim().parse::<i32>().ok();

    let duration_secs = utils::parse_itunes_duration(state.itunes_duration.trim());

    let final_enclosure_type = state.enclosure_type.trim().to_string();
    let final_enclosure_url = utils::truncate_str(&state.enclosure_url.trim().to_string(), 738);
    let enclosure_length = state.enclosure_length.trim().parse::<i64>().ok();

    let final_guid = utils::truncate_str(&state.guid.trim().to_string(), 740);

    let pub_date_ts = utils::parse_pub_date_to_unix(state.pub_date.trim()).unwrap_or_else(|| {
        state
            .pub_date
            .trim()
            .parse::<i64>()
            .unwrap_or_else(|_| state.pub_date.trim().to_string().parse().unwrap_or(0))
    });

    // timeadded should reflect when we processed the item, not 0
    let time_added = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let record = SqlInsert {
        table: "nfitems".to_string(),
        columns: vec![
            "feedid".to_string(),
            "title".to_string(),
            "link".to_string(),
            "description".to_string(),
            "timestamp".to_string(),
            "itunes_image".to_string(),
            "image".to_string(),
            "guid".to_string(),
            "itunes_duration".to_string(),
            "itunes_episode".to_string(),
            "itunes_season".to_string(),
            "itunes_episode_type".to_string(),
            "itunes_explicit".to_string(),
            "enclosure_url".to_string(),
            "enclosure_length".to_string(),
            "enclosure_type".to_string(),
            "timeadded".to_string(),
            "purge".to_string(),
        ],
        values: vec![
            match feed_id { Some(v) => JsonValue::from(v), None => JsonValue::Null },
            JsonValue::from(final_title),
            JsonValue::from(state.link.trim().to_string()),
            JsonValue::from(final_description),
            JsonValue::from(pub_date_ts),
            JsonValue::from(state.itunes_image.trim().to_string()),
            JsonValue::from(final_item_image.trim().to_string()),
            JsonValue::from(final_guid),
            JsonValue::from(duration_secs),
            JsonValue::from(itunes_episode),
            JsonValue::from(itunes_season),
            JsonValue::from(state.itunes_episode_type.trim().to_string()),
            JsonValue::from(state.itunes_explicit),
            JsonValue::from(final_enclosure_url),
            JsonValue::from(enclosure_length),
            JsonValue::from(final_enclosure_type),
            JsonValue::from(time_added),
            JsonValue::from(0), // purge default
        ],
        feed_id,
    };
    write_record(&record, "nfitems");
}

pub fn write_nfguids(state: &ParserState, feed_id: Option<i64>) {
    let guid = state.channel_podcast_guid.trim();
    if guid.is_empty() {
        return;
    }

    let record = SqlInsert {
        table: "nfguids".to_string(),
        columns: vec!["feedid".to_string(), "guid".to_string()],
        values: vec![
            match feed_id { Some(v) => JsonValue::from(v), None => JsonValue::Null },
            JsonValue::from(guid.to_string()),
        ],
        feed_id,
    };

    write_record(&record, "nfguids");
}

pub fn write_pubsub(state: &ParserState, feed_id: Option<i64>) {
    if state.pubsub_hub_url.trim().is_empty() && state.pubsub_self_url.trim().is_empty() {
        return;
    }

    let record = SqlInsert {
        table: "pubsub".to_string(),
        columns: vec![
            "feedid".to_string(),
            "hub_url".to_string(),
            "self_url".to_string(),
        ],
        values: vec![
            match feed_id { Some(v) => JsonValue::from(v), None => JsonValue::Null },
            JsonValue::from(state.pubsub_hub_url.trim().to_string()),
            JsonValue::from(state.pubsub_self_url.trim().to_string()),
        ],
        feed_id,
    };

    write_record(&record, "pubsub");
}

pub fn write_nffunding(state: &ParserState, feed_id: Option<i64>) {
    if state.channel_podcast_funding_url.trim().is_empty() {
        return;
    }

    let record = SqlInsert {
        table: "nffunding".to_string(),
        columns: vec![
            "feedid".to_string(),
            "url".to_string(),
            "message".to_string(),
        ],
        values: vec![
            match feed_id { Some(v) => JsonValue::from(v), None => JsonValue::Null },
            JsonValue::from(state.channel_podcast_funding_url.trim().to_string()),
            JsonValue::from(state.channel_podcast_funding_text.trim().to_string()),
        ],
        feed_id,
    };

    write_record(&record, "nffunding");
}

pub fn write_nfcategories(state: &ParserState, feed_id: Option<i64>) {
    if state.channel_categories_raw.is_empty() {
        return;
    }

    let mut raw = state.channel_categories_raw.clone();
    raw.dedup();
    let cat_ids = utils::build_category_ids(&raw);

    if cat_ids.iter().skip(1).all(|v| *v == 0) {
        return;
    }

    let record = SqlInsert {
        table: "nfcategories".to_string(),
        columns: vec![
            "feedid".to_string(),
            "catid1".to_string(),
            "catid2".to_string(),
            "catid3".to_string(),
            "catid4".to_string(),
            "catid5".to_string(),
            "catid6".to_string(),
            "catid7".to_string(),
            "catid8".to_string(),
            "catid9".to_string(),
            "catid10".to_string(),
        ],
        values: vec![
            match feed_id { Some(v) => JsonValue::from(v), None => JsonValue::Null },
            JsonValue::from(cat_ids[1]),
            JsonValue::from(cat_ids[2]),
            JsonValue::from(cat_ids[3]),
            JsonValue::from(cat_ids[4]),
            JsonValue::from(cat_ids[5]),
            JsonValue::from(cat_ids[6]),
            JsonValue::from(cat_ids[7]),
            JsonValue::from(cat_ids[8]),
            JsonValue::from(cat_ids[9]),
            JsonValue::from(cat_ids[10]),
        ],
        feed_id,
    };

    write_record(&record, "nfcategories");
}

pub fn write_nfitem_transcript(state: &ParserState, feed_id: Option<i64>) {
    if state.current_transcript_url.trim().is_empty() {
        return;
    }

    let item_id = format!("{}_{}", feed_id.unwrap_or(0), state.item_count + 1);

    let record = SqlInsert {
        table: "nfitem_transcripts".to_string(),
        columns: vec![
            "itemid".to_string(),
            "url".to_string(),
            "type".to_string(),
        ],
        values: vec![
            JsonValue::from(item_id),
            JsonValue::from(state.current_transcript_url.trim().to_string()),
            JsonValue::from(state.current_transcript_type.trim().to_string()),
        ],
        feed_id,
    };

    write_record(&record, "nfitem_transcripts");
}

pub fn write_nfitem_chapters(state: &ParserState, feed_id: Option<i64>) {
    if state.current_chapter_url.trim().is_empty() {
        return;
    }

    let item_id = format!("{}_{}", feed_id.unwrap_or(0), state.item_count + 1);

    let record = SqlInsert {
        table: "nfitem_chapters".to_string(),
        columns: vec![
            "itemid".to_string(),
            "url".to_string(),
            "type".to_string(),
        ],
        values: vec![
            JsonValue::from(item_id),
            JsonValue::from(state.current_chapter_url.trim().to_string()),
            JsonValue::from(state.current_chapter_type.trim().to_string()),
        ],
        feed_id,
    };

    write_record(&record, "nfitem_chapters");
}

pub fn write_nfitem_soundbites(state: &ParserState, feed_id: Option<i64>) {
    if state.current_soundbite_start.trim().is_empty() || state.current_soundbite_duration.trim().is_empty() {
        return;
    }

    let item_id = format!("{}_{}", feed_id.unwrap_or(0), state.item_count + 1);

    let record = SqlInsert {
        table: "nfitem_soundbites".to_string(),
        columns: vec![
            "itemid".to_string(),
            "title".to_string(),
            "start_time".to_string(),
            "duration".to_string(),
        ],
        values: vec![
            JsonValue::from(item_id),
            JsonValue::from(state.current_soundbite_title.trim().to_string()),
            JsonValue::from(state.current_soundbite_start.trim().to_string()),
            JsonValue::from(state.current_soundbite_duration.trim().to_string()),
        ],
        feed_id,
    };

    write_record(&record, "nfitem_soundbites");
}

pub fn write_nfitem_persons(state: &ParserState, feed_id: Option<i64>) {
    if state.current_person_name.trim().is_empty() {
        return;
    }
    let item_id = format!("{}_{}", feed_id.unwrap_or(0), state.item_count + 1);

    let record = SqlInsert {
        table: "nfitem_persons".to_string(),
        columns: vec![
            "itemid".to_string(),
            "name".to_string(),
            "role".to_string(),
            "grp".to_string(),
            "img".to_string(),
            "href".to_string(),
        ],
        values: vec![
            JsonValue::from(item_id),
            JsonValue::from(state.current_person_name.trim().to_string()),
            JsonValue::from(state.current_person_role.trim().to_string()),
            JsonValue::from(state.current_person_group.trim().to_string()),
            JsonValue::from(state.current_person_img.trim().to_string()),
            JsonValue::from(state.current_person_href.trim().to_string()),
        ],
        feed_id,
    };

    write_record(&record, "nfitem_persons");
}

pub fn write_nfvalue_from_block(feed_id: Option<i64>, value_type: i32, block: &str) {
    let record = SqlInsert {
        table: "nfvalue".to_string(),
        columns: vec![
            "feedid".to_string(),
            "value_block".to_string(),
            "type".to_string(),
            "createdon".to_string(),
        ],
        values: vec![
            match feed_id { Some(v) => JsonValue::from(v), None => JsonValue::Null },
            JsonValue::from(block.to_string()),
            JsonValue::from(value_type),
            JsonValue::from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
            ),
        ],
        feed_id,
    };

    write_record(&record, "nfvalue");
}

pub fn write_nfitem_value_from_block(
    state: &ParserState,
    feed_id: Option<i64>,
    value_type: i32,
    block: &str,
) {
    let item_id = format!("{}_{}", feed_id.unwrap_or(0), state.item_count + 1);

    let record = SqlInsert {
        table: "nfitem_value".to_string(),
        columns: vec![
            "itemid".to_string(),
            "value_block".to_string(),
            "type".to_string(),
            "createdon".to_string(),
        ],
        values: vec![
            JsonValue::from(item_id),
            JsonValue::from(block.to_string()),
            JsonValue::from(value_type),
            JsonValue::from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
            ),
        ],
        feed_id,
    };

    write_record(&record, "nfitem_value");
}
