use xml::attribute::OwnedAttribute;

use crate::{outputs, parser_state::ParserState};

pub fn on_start(attributes: &[OwnedAttribute], state: &mut ParserState) {
    if !state.in_item || state.in_podcast_alternate_enclosure {
        return;
    }

    state.in_podcast_chapters = true;
    state.current_chapter_url.clear();
    state.current_chapter_type.clear();

    for attr in attributes {
        match attr.name.local_name.as_str() {
            "url" => state.current_chapter_url = attr.value.clone(),
            "type" => state.current_chapter_type = attr.value.clone(),
            _ => {}
        }
    }
}

pub fn on_end(feed_id: Option<i64>, state: &mut ParserState) {
    if state.in_podcast_chapters {
        state.in_podcast_chapters = false;
        // Only write chapters if item has a valid enclosure
        if state.item_has_valid_enclosure {
            state
                .item_hash
                .consume(state.current_chapter_url.trim().as_bytes());
            state
                .item_hash
                .consume(state.current_chapter_type.trim().as_bytes());
            outputs::write_nfitem_chapters(state, feed_id);
        }
    }
}

