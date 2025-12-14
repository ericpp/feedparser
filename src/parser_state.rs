use md5;
use serde::Serialize;

#[derive(Serialize)]
pub struct PodcastTranscript {
    pub url: String,
    pub r#type: String,
}

#[derive(Serialize)]
pub struct PodcastChapter {
    pub url: String,
    pub r#type: String,
}

#[derive(Serialize)]
pub struct PodcastSoundbite {
    pub title: String,
    pub start: String,
    pub duration: String,
}

#[derive(Serialize)]
pub struct PodcastPerson {
    pub name: String,
    pub role: String,
    pub group: String,
    pub img: String,
    pub href: String,
}

#[derive(Serialize, Clone)]
pub struct PodcastValue {
    pub model: PodcastValueModel,
    pub destinations: Vec<PodcastValueRecipient>,
}

#[derive(Serialize, Clone)]
pub struct PodcastValueModel {
    pub r#type: String,
    pub method: String,
    pub suggested: String,
}

#[derive(Serialize, Clone, Default)]
pub struct PodcastValueRecipient {
    pub name: String,
    pub recipient_type: String,
    pub address: String,
    pub split: i32,
    pub fee: bool,
    pub custom_key: Option<String>,
    pub custom_value: Option<String>,
}

pub struct ParserState {
    // Feed-level metadata
    pub feed_type: i32,
    pub run_timestamp: i64,
    pub current_element: String,

    // Channel-level flags
    pub in_channel: bool,

    // Channel-level fields
    pub channel_title: String,
    pub channel_link: String,
    pub channel_description: String,
    pub channel_itunes_summary: String,
    pub channel_language: String,
    pub channel_generator: String,
    pub channel_itunes_author: String,

    pub in_channel_itunes_owner: bool,
    pub channel_itunes_owner_name: String,
    pub channel_itunes_owner_email: String,

    pub channel_itunes_type: String,
    pub channel_itunes_categories: Vec<String>,
    pub channel_itunes_new_feed_url: String,
    pub channel_explicit: i32,
    pub channel_itunes_image: String,

    pub in_channel_image: bool,
    pub channel_image: String,

    pub channel_podcast_guid: String,

    pub in_channel_podcast_locked: bool,
    pub channel_podcast_locked: i32,

    pub channel_podcast_owner: String,

    pub in_channel_podcast_funding: bool,
    pub channel_podcast_funding_url: String,
    pub channel_podcast_funding_text: String,
    pub channel_pubsub_hub_url: String,
    pub channel_pubsub_self_url: String,

    pub in_channel_podcast_value: bool,
    pub channel_podcast_values: Vec<PodcastValue>,
    pub channel_value_recipients: Vec<PodcastValueRecipient>,
    pub channel_value_model_type: String,
    pub channel_value_model_method: String,
    pub channel_value_model_suggested: String,
    pub channel_pub_date: i64,

    // Item-level flags
    pub in_item: bool,
    pub in_podcast_alternate_enclosure: bool,

    // Item-level fields
    pub item_written: bool,
    pub title: String,
    pub itunes_title: String,
    pub link: String,
    pub description: String,
    pub itunes_summary: String,
    pub content_encoded: String,
    pub pub_date: i64,
    pub guid: String,
    pub itunes_duration: i32,
    pub itunes_episode: String,
    pub itunes_season: String,
    pub itunes_episode_type: String,
    pub itunes_explicit: i32,
    pub itunes_image: String,

    pub in_item_image: bool,
    pub item_image: String,

    pub item_has_valid_enclosure: bool,

    pub enclosure_url: String,
    pub enclosure_length: String,
    pub enclosure_type: String,

    pub in_podcast_funding: bool,
    pub podcast_funding_url: String,
    pub podcast_funding_text: String,
    pub podcast_transcripts: Vec<PodcastTranscript>,
    pub podcast_chapters: Vec<PodcastChapter>,

    pub in_podcast_soundbite: bool,
    pub podcast_soundbites: Vec<PodcastSoundbite>,
    pub current_soundbite_title: String,
    pub current_soundbite_start: String,
    pub current_soundbite_duration: String,

    pub in_podcast_person: bool,
    pub podcast_persons: Vec<PodcastPerson>,
    pub current_person_name: String,
    pub current_person_role: String,
    pub current_person_group: String,
    pub current_person_img: String,
    pub current_person_href: String,

    pub item_value_pending: Option<(i32, String)>,
    pub item_value_has_lightning: bool,

    pub in_podcast_value: bool,
    pub podcast_values: Vec<PodcastValue>,
    pub value_recipients: Vec<PodcastValueRecipient>,
    pub value_model_type: String,
    pub value_model_method: String,
    pub value_model_suggested: String,
    pub content: String,

    // Item metrics
    pub item_pubdates: Vec<i64>,
    pub item_count: i32,
    pub newest_item_pubdate: Option<i64>,
    pub oldest_item_pubdate: Option<i64>,
    pub item_hash: md5::Context,
}

impl Default for ParserState {
    fn default() -> Self {
        Self {
            in_channel: false,
            in_channel_image: false,
            in_channel_itunes_owner: false,
            in_channel_podcast_locked: false,
            in_channel_podcast_funding: false,
            feed_type: 0,
            run_timestamp: 0,
            channel_title: String::new(),
            channel_link: String::new(),
            channel_description: String::new(),
            channel_itunes_summary: String::new(),
            channel_language: String::new(),
            channel_generator: String::new(),
            channel_itunes_author: String::new(),
            channel_itunes_owner_name: String::new(),
            channel_itunes_owner_email: String::new(),
            channel_itunes_type: String::new(),
            channel_itunes_new_feed_url: String::new(),
            channel_explicit: 0,
            channel_itunes_image: String::new(),
            channel_image: String::new(),
            channel_podcast_guid: String::new(),
            channel_podcast_locked: 0,
            channel_podcast_owner: String::new(),
            channel_podcast_funding_url: String::new(),
            channel_podcast_funding_text: String::new(),
            channel_pubsub_hub_url: String::new(),
            channel_pubsub_self_url: String::new(),
            channel_itunes_categories: Vec::new(),
            in_channel_podcast_value: false,
            channel_podcast_values: Vec::new(),
            channel_value_recipients: Vec::new(),
            channel_value_model_type: String::new(),
            channel_value_model_method: String::new(),
            channel_value_model_suggested: String::new(),
            channel_pub_date: 0,
            in_item: false,
            item_written: false,
            current_element: String::new(),
            title: String::new(),
            itunes_title: String::new(),
            link: String::new(),
            description: String::new(),
            itunes_summary: String::new(),
            content_encoded: String::new(),
            pub_date: 0,
            guid: String::new(),
            itunes_duration: 0,
            itunes_episode: String::new(),
            itunes_season: String::new(),
            itunes_episode_type: String::new(),
            itunes_explicit: 0,
            itunes_image: String::new(),
            item_image: String::new(),
            in_item_image: false,
            item_has_valid_enclosure: false,
            enclosure_url: String::new(),
            enclosure_length: String::new(),
            enclosure_type: String::new(),
            in_podcast_alternate_enclosure: false,
            podcast_funding_url: String::new(),
            podcast_funding_text: String::new(),
            in_podcast_funding: false,
            podcast_transcripts: Vec::new(),
            podcast_chapters: Vec::new(),
            podcast_soundbites: Vec::new(),
            current_soundbite_title: String::new(),
            current_soundbite_start: String::new(),
            current_soundbite_duration: String::new(),
            in_podcast_soundbite: false,
            podcast_persons: Vec::new(),
            current_person_name: String::new(),
            current_person_role: String::new(),
            current_person_group: String::new(),
            current_person_img: String::new(),
            current_person_href: String::new(),
            in_podcast_person: false,
            item_value_pending: None,
            item_value_has_lightning: false,
            in_podcast_value: false,
            podcast_values: Vec::new(),
            value_recipients: Vec::new(),
            value_model_type: String::new(),
            value_model_method: String::new(),
            value_model_suggested: String::new(),
            item_pubdates: Vec::new(),
            item_count: 0,
            newest_item_pubdate: None,
            oldest_item_pubdate: None,
            item_hash: md5::Context::new(),
            content: String::new(),
        }
    }
}
