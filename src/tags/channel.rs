use crate::outputs;
use crate::parser_state::ParserState;
use md5;

pub fn on_start(state: &mut ParserState) {
    state.in_channel = true;
    state.in_channel_image = false;
    state.channel_title.clear();
    state.channel_link.clear();
    state.channel_description.clear();
    state.channel_itunes_summary.clear();
    state.channel_language.clear();
    state.channel_generator.clear();
    state.channel_itunes_author.clear();
    state.channel_itunes_owner_name.clear();
    state.channel_itunes_owner_email.clear();
    state.channel_itunes_type.clear();
    state.channel_itunes_image.clear();
    state.channel_image.clear();
    state.channel_explicit = 0;
    state.in_channel_itunes_owner = false;
    state.channel_itunes_new_feed_url.clear();
    state.channel_podcast_guid.clear();
    state.channel_podcast_locked = 0;
    state.channel_podcast_owner.clear();
    state.in_channel_podcast_locked = false;
    state.in_channel_podcast_funding = false;
    state.channel_podcast_funding_url.clear();
    state.channel_podcast_funding_text.clear();
    state.pubsub_hub_url.clear();
    state.pubsub_self_url.clear();
    state.channel_categories.clear();
    state.channel_categories_raw.clear();
    state.in_standard_category = false;
    state.channel_value_pending = None;
    state.channel_value_has_lightning = false;
    state.item_pubdates.clear();
    state.item_count = 0;
    state.newest_item_pubdate = None;
    state.oldest_item_pubdate = None;
    state.item_hash = md5::Context::new();
    state.channel_pub_date.clear();
}

pub fn on_end(feed_id: Option<i64>, state: &mut ParserState) {
    if state.in_channel {
        outputs::write_newsfeeds(state, feed_id);
        outputs::write_nfguids(state, feed_id);
        outputs::write_pubsub(state, feed_id);
        outputs::write_nffunding(state, feed_id);
        outputs::write_nfcategories(state, feed_id);
        if let Some((value_type, block)) = state.channel_value_pending.take() {
            outputs::write_nfvalue_from_block(state, feed_id, value_type, &block);
        }
        state.in_channel = false;
    }
}
