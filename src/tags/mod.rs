use xml::attribute::OwnedAttribute;

use crate::parser_state::ParserState;

pub mod atom_link;
pub mod category;
pub mod channel;
pub mod content_encoded;
pub mod description;
pub mod enclosure;
pub mod generator;
pub mod guid;
pub mod image;
pub mod item;
pub mod atom_logo;
pub mod itunes_author;
pub mod itunes_duration;
pub mod itunes_episode;
pub mod itunes_episode_type;
pub mod itunes_explicit;
pub mod itunes_image;
pub mod itunes_new_feed_url;
pub mod itunes_owner;
pub mod itunes_season;
pub mod itunes_summary;
pub mod itunes_title;
pub mod itunes_type;
pub mod language;
pub mod link;
pub mod podcast_chapters;
pub mod podcast_funding;
pub mod podcast_guid;
pub mod podcast_locked;
pub mod podcast_person;
pub mod podcast_soundbite;
pub mod podcast_transcript;
pub mod podcast_value;
pub mod pub_date;
pub mod content;
pub mod title;
pub mod url;

pub fn dispatch_start(current_element: &str, attributes: &[OwnedAttribute], state: &mut ParserState) {
    match current_element {
        "channel" => {
            state.feed_type = 0;
            channel::on_start(state);
        }
        "atom:feed" => {
            state.feed_type = 1;
            channel::on_start(state);
        }
        "item" | "atom:entry" => item::on_start(state),
        "link" => link::on_start(current_element, attributes, state),
        "image" => image::on_start(state),
        "itunes:duration" => itunes_duration::on_start(state),
        "itunes:owner" => itunes_owner::on_start(state),
        "podcast:transcript" => podcast_transcript::on_start(attributes, state),
        "podcast:chapters" => podcast_chapters::on_start(attributes, state),
        "podcast:soundbite" => podcast_soundbite::on_start(attributes, state),
        "podcast:person" => podcast_person::on_start(attributes, state),
        "podcast:value" => podcast_value::on_start(attributes, state),
        "podcast:valueRecipient" => podcast_value::on_value_recipient(attributes, state),
        "enclosure" => enclosure::on_start(attributes, state),
        "podcast:alternateEnclosure" => {
            if state.in_item {
                state.in_podcast_alternate_enclosure = true;
            }
        },
        "atom:link" => atom_link::on_start(current_element, attributes, state),
        "itunes:category" => category::on_start(current_element, attributes, state),
        "itunes:image" => itunes_image::on_start(current_element, attributes, state),
        "podcast:funding" => podcast_funding::on_start(current_element, attributes, state),
        "podcast:locked" => podcast_locked::on_start(current_element, attributes, state),
        _ => {}
    }

    // Namespace-sensitive handlers
    // itunes_image::on_start(current_element, attributes, state);
    // podcast_funding::on_start(current_element, attributes, state);
    // podcast_locked::on_start(current_element, attributes, state);
    // category::on_start(current_element, attributes, state);
    // atom_link::on_start(current_element, attributes, state);
}

pub fn dispatch_text(current_element: &str, data: &str, state: &mut ParserState) {
    match current_element {
        "title" => title::on_text(data, state),
        "link" => link::on_text(data, state),
        "description" | "atom:subtitle" => description::on_text(data, state),
        "subtitle" => description::on_text(data, state),
        "summary" => description::on_text(data, state),
        "content:encoded" => content_encoded::on_text(data, state),
        "pubDate" => pub_date::on_text(data, state),
        "atom:updated" | "published" => pub_date::on_text(data, state),
        "language" => language::on_text(data, state),
        "generator" => generator::on_text(data, state),
        "guid" => guid::on_text(data, state),
        "id" => guid::on_text(data, state),
        "itunes:author" => itunes_author::on_text(data, state),
        "itunes:duration" => itunes_duration::on_text(data, state),
        "itunes:episode" => itunes_episode::on_text(data, state),
        "itunes:season" => itunes_season::on_text(data, state),
        "itunes:episodeType" => itunes_episode_type::on_text(data, state),
        "itunes:explicit" => itunes_explicit::on_text(data, state),
        "itunes:image" => itunes_image::on_text(data, state),
        "itunes:summary" => itunes_summary::on_text(data, state),
        "itunes:title" => itunes_title::on_text(data, state),
        "itunes:type" => itunes_type::on_text(data, state),
        "itunes:new-feed-url" => itunes_new_feed_url::on_text(data, state),
        "podcast:guid" => podcast_guid::on_text(data, state),
        "url" => url::on_text(data, state),
        "atom:logo" => atom_logo::on_text(data, state),
        "content" => content::on_text(data, state),
        _ => {}
    }

    // Context-aware handlers
    category::on_text(current_element, data, state);
    podcast_funding::on_text(data, state);
    itunes_owner::on_text(current_element, data, state);
    podcast_locked::on_text(data, state);
    podcast_soundbite::on_text(data, state);
    podcast_person::on_text(data, state);
}

pub fn dispatch_end(current_element: &str, feed_id: Option<i64>, state: &mut ParserState) {
    match current_element {
        "channel" | "atom:feed" => channel::on_end(feed_id, state),
        "item" | "atom:entry" => item::on_end(feed_id, state),
        "image" => image::on_end(state),
        "itunes:owner" => itunes_owner::on_end(state),
        "category" | "itunes:category" => category::on_end(current_element, state),
        "funding" | "podcast:funding" => podcast_funding::on_end(state),
        "podcast:locked" | "locked" => podcast_locked::on_end(state),
        "podcast:transcript" => podcast_transcript::on_end(feed_id, state),
        "podcast:chapters" => podcast_chapters::on_end(feed_id, state),
        "podcast:soundbite" => podcast_soundbite::on_end(feed_id, state),
        "podcast:person" => podcast_person::on_end(feed_id, state),
        "podcast:value" => podcast_value::on_end(feed_id, state),
        "podcast:alternateEnclosure" => state.in_podcast_alternate_enclosure = false,
        _ => {}
    }
}
