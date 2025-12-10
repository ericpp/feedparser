use xml::attribute::OwnedAttribute;

use crate::{outputs, parser_state::ParserState};

pub fn on_start(attributes: &[OwnedAttribute], state: &mut ParserState) {
    if !state.in_item {
        return;
    }

    state.in_podcast_soundbite = true;
    state.current_soundbite_title.clear();
    state.current_soundbite_start.clear();
    state.current_soundbite_duration.clear();

    for attr in attributes {
        match attr.name.local_name.as_str() {
            "startTime" => state.current_soundbite_start = attr.value.clone(),
            "duration" => state.current_soundbite_duration = attr.value.clone(),
            _ => {}
        }
    }
}

pub fn on_text(data: &str, state: &mut ParserState) {
    if state.in_podcast_soundbite {
        state.current_soundbite_title.push_str(data);
    }
}

pub fn on_end(feed_id: Option<i64>, state: &mut ParserState) {
    if state.in_podcast_soundbite {
        state.in_podcast_soundbite = false;
        // Only write soundbite if item has a valid enclosure
        if state.item_has_valid_enclosure {
            state
                .item_hash
                .consume(state.current_soundbite_title.trim().as_bytes());
            state
                .item_hash
                .consume(state.current_soundbite_start.trim().as_bytes());
            state
                .item_hash
                .consume(state.current_soundbite_duration.trim().as_bytes());
            outputs::write_nfitem_soundbites(state, feed_id);
        }
    }
}

