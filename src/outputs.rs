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

fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn feed_run_ts(state: &ParserState) -> i64 {
    if state.run_timestamp > 0 {
        state.run_timestamp
    } else {
        now_ts()
    }
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

    let title = utils::clean_string(&state.channel_title);
    let title = utils::truncate_string(&title, 768);

    let link = utils::clean_string(&state.channel_link);

    let description = if !state.channel_itunes_summary.is_empty() {
        state.channel_itunes_summary.clone()
    } else {
        state.channel_description.clone()
    };

    let image = if !state.channel_image.is_empty() {
        utils::sanitize_url(&state.channel_image)
    } else {
        utils::sanitize_url(&state.channel_itunes_image)
    };

    let itunes_new_feed_url = utils::sanitize_url(&state.channel_itunes_new_feed_url);
    let itunes_image = utils::sanitize_url(&state.channel_itunes_image);

    let language = utils::truncate_string(&state.channel_language, 8);
    let item_count = utils::truncate_int(state.item_count);

    let podcast_owner = utils::truncate_string(&state.channel_podcast_owner, 255);

    let current_time = now_ts();
    let past_pub_dates: Vec<i64> = state.item_pubdates.clone()
        .iter()
        .filter(|&pub_date| *pub_date <= current_time)
        .map(|&pub_date| pub_date)
        .collect();
    
    let newest_pub_date: i64 = past_pub_dates.iter().max().copied().unwrap_or(0);
    let oldest_pub_date: i64 = past_pub_dates.iter().min().copied().unwrap_or(0);
    let update_frequency: i32 = utils::calculate_update_frequency(&past_pub_dates);

    let record = SqlInsert {
        table: "newsfeeds".to_string(),
        columns: vec![
            "feed_id".to_string(),
            "type".to_string(),
            "title".to_string(),
            "link".to_string(),
            "description".to_string(),
            "generator".to_string(),
            "itunes_author".to_string(),
            "itunes_owner_name".to_string(),
            "itunes_owner_email".to_string(),
            "itunes_new_feed_url".to_string(),
            "explicit".to_string(),
            "image".to_string(),
            "itunes_image".to_string(),
            "itunes_type".to_string(),
            "language".to_string(),
            "item_count".to_string(),
            "podcast_locked".to_string(),
            "podcast_owner".to_string(),
            "pub_date".to_string(),
            "podcast_guid".to_string(),
            "newest_item_pub_date".to_string(),
            "oldest_item_pub_date".to_string(),
            "update_frequency".to_string(),
        ],
        values: vec![
            match feed_id { Some(v) => JsonValue::from(v), None => JsonValue::Null },
            JsonValue::from(state.feed_type.clone()),
            JsonValue::from(title),
            JsonValue::from(link),
            JsonValue::from(description),
            JsonValue::from(state.channel_generator.clone()),
            JsonValue::from(state.channel_itunes_author.clone()),
            JsonValue::from(state.channel_itunes_owner_name.clone()),
            JsonValue::from(state.channel_itunes_owner_email.clone()),
            JsonValue::from(itunes_new_feed_url),
            JsonValue::from(state.channel_explicit.clone()),
            JsonValue::from(image),
            JsonValue::from(itunes_image),
            JsonValue::from(state.channel_itunes_type.clone()),
            JsonValue::from(language),
            JsonValue::from(item_count),
            JsonValue::from(state.channel_podcast_locked.clone()),
            JsonValue::from(podcast_owner),
            JsonValue::from(state.channel_pub_date.clone()),
            JsonValue::from(state.channel_podcast_guid.clone()),
            JsonValue::from(newest_pub_date),
            JsonValue::from(oldest_pub_date),
            JsonValue::from(update_frequency),
        ],
        feed_id,
    };
    write_record(&record, "newsfeeds");
}

pub fn write_nfitems(state: &ParserState, feed_id: Option<i64>) {
    let title = utils::truncate_string(
        if !state.itunes_title.is_empty() {
            &state.itunes_title
        } else {
            state.title.trim()
        },
        1024,
    );

    let description = if !state.itunes_summary.is_empty() {
        &state.itunes_summary
    } else if !state.content_encoded.is_empty() {
        &state.content_encoded
    } else {
        &state.description
    }.trim();

    let link = utils::sanitize_url(&state.link);

    let guid = utils::truncate_string(
        if !state.guid.is_empty() {
            &state.guid
        } else if !state.enclosure_url.is_empty() && state.enclosure_url.len() > 10 {
            &state.enclosure_url[..state.enclosure_url.len().min(738)]
        } else {
            ""
        },
        740,
    );

    let mut enclosure_url = utils::sanitize_url(&state.enclosure_url);

    if enclosure_url.to_lowercase().contains("&amp;") {
        enclosure_url = enclosure_url.replace("&amp;", "&").to_string();
    }

    let enclosure_length = state.enclosure_length
        .parse::<i64>()
        .ok()
        .filter(|&v| v <= 922337203685477580)
        .unwrap_or(0)
        .min(922337203685477580);

    let enclosure_type = if !state.enclosure_type.is_empty() {
        utils::truncate_string(&state.enclosure_type, 128)
    } else {
        let guessed = utils::guess_enclosure_type(&state.enclosure_url);
        utils::truncate_string(&guessed, 128)
    };

    let itunes_duration = utils::time_to_seconds(&state.itunes_duration);

    let itunes_season = state.itunes_season
        .parse::<i32>()
        .ok()
        .map(utils::truncate_int);

    let itunes_episode = state.itunes_episode
        .parse::<i32>()
        .ok()
        .map(|v| v.min(1000000));

    let image = if !state.itunes_image.is_empty() {
        utils::sanitize_url(&state.itunes_image)
    } else {
        utils::sanitize_url(&state.item_image)
    };




//Set a time in the feed obj to use as the "lastupdate" time
// state.last_update = now_ts();


    let record = SqlInsert {
        table: "nfitems".to_string(),
        columns: vec![
            "title".to_string(),
            "link".to_string(),
            "description".to_string(),
            "guid".to_string(),
            "timestamp".to_string(),
            "enclosure_url".to_string(),
            "enclosure_length".to_string(),
            "enclosure_type".to_string(),
            "itunes_episode".to_string(),
            "itunes_episode_type".to_string(),
            "itunes_explicit".to_string(),
            "itunes_duration".to_string(),
            "image".to_string(),
            "itunes_season".to_string(),
        ],
        values: vec![
            JsonValue::from(title),
            JsonValue::from(link),
            JsonValue::from(description),
            JsonValue::from(guid),
            JsonValue::from(state.pub_date.clone()),
            JsonValue::from(enclosure_url),
            JsonValue::from(enclosure_length),
            JsonValue::from(enclosure_type),
            JsonValue::from(itunes_episode),
            JsonValue::from(state.itunes_episode_type.clone()),
            JsonValue::from(state.itunes_explicit),
            JsonValue::from(itunes_duration),
            JsonValue::from(image),
            JsonValue::from(itunes_season),
        ],
        feed_id,
    };
    write_record(&record, "nfitems");
}

pub fn write_nfguids(state: &ParserState, feed_id: Option<i64>) {
    let guid = &state.channel_podcast_guid;
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
    if state.pubsub_hub_url.is_empty() && state.pubsub_self_url.is_empty() {
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
            JsonValue::from(state.pubsub_hub_url.to_string()),
            JsonValue::from(state.pubsub_self_url.to_string()),
        ],
        feed_id,
    };

    write_record(&record, "pubsub");
}

pub fn write_nffunding(state: &ParserState, feed_id: Option<i64>) {
    if state.channel_podcast_funding_url.is_empty() {
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
            JsonValue::from(state.channel_podcast_funding_url.to_string()),
            JsonValue::from(state.channel_podcast_funding_text.to_string()),
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
            JsonValue::from(raw[1].clone()),
            JsonValue::from(raw[2].clone()),
            JsonValue::from(raw[3].clone()),
            JsonValue::from(raw[4].clone()),
            JsonValue::from(raw[5].clone()),
            JsonValue::from(raw[6].clone()),
            JsonValue::from(raw[7].clone()),
            JsonValue::from(raw[8].clone()),
            JsonValue::from(raw[9].clone()),
            JsonValue::from(raw[10].clone()),
        ],
        feed_id,
    };

    write_record(&record, "nfcategories");
}

pub fn write_nfitem_transcript(state: &ParserState, feed_id: Option<i64>) {
    if state.current_transcript_url.is_empty() {
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
            JsonValue::from(state.current_transcript_url.clone()),
            JsonValue::from(state.current_transcript_type.clone()),
        ],
        feed_id,
    };

    write_record(&record, "nfitem_transcripts");
}

pub fn write_nfitem_chapters(state: &ParserState, feed_id: Option<i64>) {
    if state.current_chapter_url.is_empty() {
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
            JsonValue::from(state.current_chapter_url.clone()),
            JsonValue::from(state.current_chapter_type.clone()),
        ],
        feed_id,
    };

    write_record(&record, "nfitem_chapters");
}

pub fn write_nfitem_soundbites(state: &ParserState, feed_id: Option<i64>) {
    if state.current_soundbite_start.is_empty() || state.current_soundbite_duration.is_empty() {
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
            JsonValue::from(state.current_soundbite_title.clone()),
            JsonValue::from(state.current_soundbite_start.clone()),
            JsonValue::from(state.current_soundbite_duration.clone()),
        ],
        feed_id,
    };

    write_record(&record, "nfitem_soundbites");
}

pub fn write_nfitem_persons(state: &ParserState, feed_id: Option<i64>) {
    if state.current_person_name.is_empty() {
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
            JsonValue::from(state.current_person_name.clone()),
            JsonValue::from(state.current_person_role.clone()),
            JsonValue::from(state.current_person_group.clone()),
            JsonValue::from(state.current_person_img.clone()),
            JsonValue::from(state.current_person_href.clone()),
        ],
        feed_id,
    };

    write_record(&record, "nfitem_persons");
}

pub fn write_nfvalue_from_block(state: &ParserState, feed_id: Option<i64>, value_type: i32, block: &str) {
    let created_on = feed_run_ts(state);

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
            JsonValue::from(created_on),
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
        ],
        values: vec![
            JsonValue::from(item_id),
            JsonValue::from(block.to_string()),
            JsonValue::from(value_type),
        ],
        feed_id,
    };

    write_record(&record, "nfitem_value");
}
