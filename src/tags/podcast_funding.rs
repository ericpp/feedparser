use xml::attribute::OwnedAttribute;

use crate::parser_state::ParserState;

// Detect podcast:funding start; set flag and capture optional url attribute
pub fn on_start(current_element: &str, attributes: &[OwnedAttribute], state: &mut ParserState) {
    if !state.in_item {
        return; // only care about funding within items per prior behavior
    }

    if current_element.starts_with("podcast:funding") {
        state.in_podcast_funding = true;
        if let Some(attr) = attributes.iter().find(|a| a.name.local_name == "url") {
            state.podcast_funding_url = attr.value.clone();
        }
    }
}

pub fn on_text(data: &str, state: &mut ParserState) {
    if state.in_podcast_funding {
        state.podcast_funding_text.push_str(data);
    }
}

pub fn on_end(state: &mut ParserState) {
    if state.in_podcast_funding {
        state.in_podcast_funding = false;
    }
}
