use xml::attribute::OwnedAttribute;

use crate::parser_state::ParserState;

pub fn on_start(_current_element: &str, attributes: &[OwnedAttribute], state: &mut ParserState) {
    if state.in_item {
        let href = attributes.iter().find(|a| a.name.local_name == "href");

        if let Some(href) = href {
            state.link = href.value.clone();
        }
    } else if state.in_channel && state.channel_link.is_empty() {
        let href = attributes.iter().find(|a| a.name.local_name == "href");
        if let Some(href) = href {
            state.channel_link = href.value.clone();
        }
    }
}

pub fn on_text(data: &str, state: &mut ParserState) {
    if state.in_item && state.link.is_empty() {
        state.link.push_str(data);
    } else if state.in_channel && state.channel_link.is_empty() {
        state.channel_link.push_str(data);
    }
}
