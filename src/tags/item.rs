use crate::{outputs, parser_state::ParserState, utils};

pub fn on_start(state: &mut ParserState) {
    state.in_item = true;
    state.item_has_valid_enclosure = false;
    state.item_written = false;

    state.title.clear();
    state.itunes_title.clear();
    state.link.clear();
    state.description.clear();
    state.itunes_summary.clear();
    state.content_encoded.clear();
    state.pub_date.clear();
    state.guid.clear();

    state.itunes_duration.clear();
    state.itunes_episode.clear();
    state.itunes_season.clear();
    state.itunes_episode_type.clear();
    state.itunes_explicit = 0;

    state.itunes_image.clear();
    state.item_image.clear();
    state.in_item_image = false;

    state.enclosure_url.clear();
    state.enclosure_length.clear();
    state.enclosure_type.clear();

    state.podcast_funding_url.clear();
    state.podcast_funding_text.clear();
    state.in_podcast_funding = false;

    state.in_podcast_transcript = false;
    state.current_transcript_url.clear();
    state.current_transcript_type.clear();

    state.in_podcast_chapters = false;
    state.current_chapter_url.clear();
    state.current_chapter_type.clear();

    state.in_podcast_soundbite = false;
    state.current_soundbite_title.clear();
    state.current_soundbite_start.clear();
    state.current_soundbite_duration.clear();

    state.in_podcast_person = false;
    state.current_person_name.clear();
    state.current_person_role.clear();
    state.current_person_group.clear();
    state.current_person_img.clear();
    state.current_person_href.clear();

    state.in_podcast_value = false;
    state.value_recipients.clear();
    state.value_model_type.clear();
    state.value_model_method.clear();
    state.value_model_suggested.clear();
    state.item_value_pending = None;
    state.item_value_has_lightning = false;
}

pub fn on_end(feed_id: Option<i64>, state: &mut ParserState) {
    if !state.in_item {
        return;
    }

    if !state.item_has_valid_enclosure {
        state.item_value_pending = None;
        state.in_item = false;
        return;
    }

    if state.guid.trim().is_empty() {
        state.guid = state.enclosure_url.clone();
    }
    if state.enclosure_type.trim().is_empty() {
        state.enclosure_type = utils::guess_enclosure_type(&state.enclosure_url);
    }

    outputs::write_nfitems(state, feed_id);

    if let Some((value_type, block)) = state.item_value_pending.take() {
        outputs::write_nfitem_value_from_block(state, feed_id, value_type, &block);
    }

    let pub_date_ts = utils::parse_pub_date_to_unix(state.pub_date.trim())
        .unwrap_or_else(|| state.pub_date.trim().parse::<i64>().unwrap_or(0));
    state.item_pubdates.push(pub_date_ts);
    state.item_count += 1;

    match state.newest_item_pubdate {
        Some(v) if v >= pub_date_ts => {}
        _ => state.newest_item_pubdate = Some(pub_date_ts),
    }
    match state.oldest_item_pubdate {
        Some(v) if v <= pub_date_ts => {}
        _ => state.oldest_item_pubdate = Some(pub_date_ts),
    }

    let hash_title = if !state.itunes_title.trim().is_empty() {
        state.itunes_title.trim()
    } else {
        state.title.trim()
    };
    state.item_hash.consume(hash_title.as_bytes());
    state.item_hash.consume(state.link.trim().as_bytes());
    state
        .item_hash
        .consume(state.enclosure_url.trim().as_bytes());
    state
        .item_hash
        .consume(state.enclosure_type.trim().as_bytes());
    state
        .item_hash
        .consume(state.podcast_funding_url.trim().as_bytes());
    state
        .item_hash
        .consume(state.podcast_funding_text.trim().as_bytes());

    state.in_item = false;
}
