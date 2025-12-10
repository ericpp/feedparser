use xml::attribute::OwnedAttribute;

use crate::{outputs, parser_state::ParserState};

pub fn on_start(attributes: &[OwnedAttribute], state: &mut ParserState) {
    if !state.in_item {
        return;
    }

    state.in_podcast_person = true;
    state.current_person_name.clear();
    state.current_person_role.clear();
    state.current_person_group.clear();
    state.current_person_img.clear();
    state.current_person_href.clear();

    for attr in attributes {
        match attr.name.local_name.as_str() {
            "role" => state.current_person_role = attr.value.clone(),
            "group" => state.current_person_group = attr.value.clone(),
            "img" => state.current_person_img = attr.value.clone(),
            "href" => state.current_person_href = attr.value.clone(),
            _ => {}
        }
    }
}

pub fn on_text(data: &str, state: &mut ParserState) {
    if state.in_podcast_person {
        state.current_person_name.push_str(data);
    }
}

pub fn on_end(feed_id: Option<i64>, state: &mut ParserState) {
    if state.in_podcast_person {
        state.in_podcast_person = false;
        // Only write person if item has a valid enclosure
        if state.item_has_valid_enclosure {
            state
                .item_hash
                .consume(state.current_person_name.trim().as_bytes());
            state
                .item_hash
                .consume(state.current_person_role.trim().as_bytes());
            state
                .item_hash
                .consume(state.current_person_group.trim().as_bytes());
            state
                .item_hash
                .consume(state.current_person_img.trim().as_bytes());
            state
                .item_hash
                .consume(state.current_person_href.trim().as_bytes());
            outputs::write_nfitem_persons(state, feed_id);
        }
    }
}

