use xml::attribute::OwnedAttribute;

use crate::parser_state::ParserState;

pub fn on_start(current_element: &str, attributes: &[OwnedAttribute], state: &mut ParserState) {
    if !state.in_item {
        return;
    }

    if current_element.starts_with("itunes:image") {
        if let Some(attr) = attributes.iter().find(|a| {
            let key = a.name.local_name.as_str();
            key == "href" || key == "url"
        }) {
            state.itunes_image = attr.value.clone();
        }
    }
}
