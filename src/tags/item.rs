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
    state.pub_date = 0;
    state.guid.clear();

    state.itunes_duration = 0;
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
    state.in_podcast_alternate_enclosure = false;

    state.podcast_funding_url.clear();
    state.podcast_funding_text.clear();
    state.in_podcast_funding = false;

    state.podcast_transcripts.clear();
    state.podcast_chapters.clear();

    state.in_podcast_soundbite = false;
    state.podcast_soundbites.clear();
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
    state.podcast_values.clear();
    state.value_recipients.clear();
    state.value_model_type.clear();
    state.value_model_method.clear();
    state.value_model_suggested.clear();
    state.item_value_pending = None;
    state.item_value_has_lightning = false;
    state.content.clear();
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

    state.item_pubdates.push(state.pub_date);
    state.item_count += 1;

    match state.newest_item_pubdate {
        Some(v) if v >= state.pub_date => {}
        _ => state.newest_item_pubdate = Some(state.pub_date),
    }
    match state.oldest_item_pubdate {
        Some(v) if v <= state.pub_date => {}
        _ => state.oldest_item_pubdate = Some(state.pub_date),
    }

    state.in_item = false;
}
