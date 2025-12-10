use xml::attribute::OwnedAttribute;

use crate::parser_state::ParserState;

pub fn on_start(current_element: &str, attributes: &[OwnedAttribute], state: &mut ParserState) {
    if !(state.in_channel && !state.in_item) {
        return;
    }

    // itunes:category text attribute
    if current_element.contains("category") {
        if let Some(attr) = attributes.iter().find(|a| a.name.local_name == "text") {
            let val = attr.value.trim();
            if !val.is_empty() {
                state.channel_categories_raw.push(val.to_string());
            }
        } else {
            state.in_standard_category = true;
        }
    }
}

pub fn on_text(_current_element: &str, data: &str, state: &mut ParserState) {
    if state.in_standard_category {
        let val = data.trim();
        if !val.is_empty() {
            state.channel_categories_raw.push(val.to_string());
        }
    }
}

pub fn on_end(_current_element: &str, state: &mut ParserState) {
    state.in_standard_category = false;
}


