use xml::attribute::OwnedAttribute;

use crate::{outputs, parser_state::ParserState};

pub fn on_start(attributes: &[OwnedAttribute], state: &mut ParserState) {
    if !state.in_item {
        return;
    }

    state.in_podcast_transcript = true;
    state.current_transcript_url.clear();
    state.current_transcript_type.clear();

    for attr in attributes {
        match attr.name.local_name.as_str() {
            "url" => state.current_transcript_url = attr.value.clone(),
            "type" => state.current_transcript_type = attr.value.clone(),
            _ => {}
        }
    }
}

pub fn on_end(feed_id: Option<i64>, state: &mut ParserState) {
    if state.in_podcast_transcript {
        state.in_podcast_transcript = false;
        // Only write transcript if item has a valid enclosure
        if state.item_has_valid_enclosure {
            state
                .item_hash
                .consume(state.current_transcript_url.trim().as_bytes());
            state
                .item_hash
                .consume(state.current_transcript_type.trim().as_bytes());
            outputs::write_nfitem_transcript(state, feed_id);
        }
    }
}

