use xml::attribute::OwnedAttribute;

use crate::parser_state::ParserState;

pub fn on_start(current_element: &str, attributes: &[OwnedAttribute], state: &mut ParserState) {
    // Check if this is an atom:link or a link with rel attribute
    let is_atom_link = current_element == "atom:link" 
        || (current_element == "link" && attributes.iter().any(|a| a.name.local_name == "rel"));

    if !is_atom_link {
        return;
    }

    let mut rel = String::new();
    let mut href = String::new();
    let mut length = String::new();
    let mut link_type = String::new();

    for attr in attributes {
        match attr.name.local_name.as_str() {
            "rel" => rel = attr.value.clone(),
            "href" => href = attr.value.clone(),
            "length" => length = attr.value.clone(),
            "type" => link_type = attr.value.clone(),
            _ => {}
        }
    }

    if state.in_item && rel == "alternate" && !href.is_empty() && state.link.is_empty() {
        state.link = href.clone();
    } else if state.in_channel && !state.in_item && rel == "alternate" && !href.is_empty() && state.channel_link.is_empty() {
        state.channel_link = href.clone();
    }

    // Handle item-level enclosure links
    if state.in_item && rel == "enclosure" && !href.is_empty() {
        // Only use the first enclosure (skip if already set)
        if state.enclosure_url.is_empty() {
            state.enclosure_url = href;
            state.enclosure_length = length;
            state.enclosure_type = link_type;
            let url = state.enclosure_url.trim();
            if url.starts_with("http://") || url.starts_with("https://") {
                state.item_has_valid_enclosure = true;
            }
        }
        return;
    }

    // Handle channel-level PubSub links
    if state.in_channel && !state.in_item {
        if rel == "hub" && !href.is_empty() {
            state.pubsub_hub_url = href;
        } else if rel == "self" && !href.is_empty() {
            state.pubsub_self_url = href;
        }
    }
}

