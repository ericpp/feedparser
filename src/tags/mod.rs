use xml::attribute::OwnedAttribute;

use crate::parser_state::ParserState;

pub mod channel;
pub mod item;
pub mod title;
pub mod link;
pub mod description;
pub mod pub_date;
pub mod image;
pub mod itunes_image;
pub mod podcast_funding;

pub fn dispatch_start(current_element: &str, attributes: &[OwnedAttribute], state: &mut ParserState) {
    // Basic element-based handlers
    match current_element {
        "channel" => channel::on_start(state),
        "item" => item::on_start(state),
        "image" => image::on_start(state),
        _ => {}
    }

    // Namespace-sensitive handlers
    itunes_image::on_start(current_element, attributes, state);
    podcast_funding::on_start(current_element, attributes, state);
}

pub fn dispatch_text(current_element: &str, data: &str, state: &mut ParserState) {
    // Route by current element name
    match current_element {
        "title" => title::on_text(data, state),
        "link" => link::on_text(data, state),
        "description" => description::on_text(data, state),
        "pubDate" => pub_date::on_text(data, state),
        _ => {}
    }

    // Some text handlers depend on context flags rather than element name
    podcast_funding::on_text(data, state);
}

pub fn dispatch_end(current_element: &str, feed_id: Option<i64>, state: &mut ParserState) {
    match current_element {
        "channel" => channel::on_end(feed_id, state),
        "item" => item::on_end(feed_id, state),
        "image" => image::on_end(state),
        "funding" => podcast_funding::on_end(state),
        _ => {}
    }
}
