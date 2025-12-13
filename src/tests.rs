use super::*;
use std::collections::HashMap;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use chrono::{DateTime, Duration, Utc};
use serde_json::json;

fn unique_temp_dir() -> PathBuf {
    let base = std::env::temp_dir();
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    // Use high-resolution timestamp (nanoseconds) to minimize collision risk
    let dir = base.join(format!("feedparser_test_{}", ts));
    let _ = fs::create_dir_all(&dir);
    dir
}

fn ensure_output_dir() -> PathBuf {
    // Use get_or_init to atomically ensure only one directory is created
    // This prevents race conditions where parallel tests create different directories
    OUTPUT_SUBDIR.get_or_init(|| unique_temp_dir()).clone()
}

fn get_value_from_record(v: &serde_json::Value, col_name: &str) -> Option<serde_json::Value> {
    let columns = v["columns"].as_array()?;
    let values = v["values"].as_array()?;
    let mut targets = vec![col_name.to_string()];
    if col_name == "feed_id" {
        targets.push("id".to_string());
        targets.push("feedid".to_string());
    }
    if col_name == "pub_date" {
        targets.push("timestamp".to_string());
    }
    for (i, col) in columns.iter().enumerate() {
        if let Some(col_name) = col.as_str() {
            if targets.iter().any(|t| t == col_name) {
                return values.get(i).cloned();
            }
        }
    }
    None
}

fn sort_paths_by_numeric_prefix(paths: &mut Vec<PathBuf>) {
    paths.sort_by(|a, b| {
        let an = a.file_name().and_then(|s| s.to_str()).unwrap_or("");
        let bn = b.file_name().and_then(|s| s.to_str()).unwrap_or("");
        let anum = an.split('_').next().and_then(|s| s.parse::<u64>().ok());
        let bnum = bn.split('_').next().and_then(|s| s.parse::<u64>().ok());
        anum.cmp(&bnum).then_with(|| an.cmp(bn))
    });
}

fn read_json_file(path: &Path) -> serde_json::Value {
    let contents = fs::read_to_string(path).expect("read output file");
    serde_json::from_str(&contents).expect("valid JSON output")
}

fn output_files_for(out_dir: &Path, table: &str, feed_id: i64) -> Vec<PathBuf> {
    let suffix = format!("{feed_id}.json");
    let needle = format!("_{table}_");
    fs::read_dir(out_dir)
        .expect("output directory should be readable")
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?.to_owned();
            if name.contains(&needle) && name.ends_with(&suffix) {
                Some(path)
            } else {
                None
            }
        })
        .collect()
}

fn output_records(out_dir: &Path, table: &str, feed_id: i64) -> Vec<serde_json::Value> {
    let mut files = output_files_for(out_dir, table, feed_id);
    sort_paths_by_numeric_prefix(&mut files);
    files.into_iter().map(|p| read_json_file(&p)).collect()
}

fn single_output_record(out_dir: &Path, table: &str, feed_id: i64) -> serde_json::Value {
    let mut records = output_records(out_dir, table, feed_id);
    assert_eq!(
        records.len(),
        1,
        "expected one {table} record for feed {feed_id}"
    );
    records.remove(0)
}

#[test]
fn writes_channel_title_to_newsfeeds_output() {
    // Arrange: ensure outputs directory is set once for all tests in this process
    let out_dir = ensure_output_dir();

    // Synthetic input: 4 header lines followed by minimal RSS with channel title
    let last_modified = "0"; // placeholder
    let etag = "[[NO_ETAG]]";
    let url = "https://example.com/feed.xml";
    let downloaded = "0";
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
<title>My Test Channel</title>
  </channel>
</rss>"#;

    let input = format!(
        "{last}\n{etag}\n{url}\n{dl}\n{xml}\n",
        last = last_modified,
        etag = etag,
        url = url,
        dl = downloaded,
        xml = xml
    );

    let feed_id = 424242_i64;

    // Act: process the synthetic feed synchronously
    process_feed_sync(Cursor::new(input.into_bytes()), "<test>", Some(feed_id));

    // Assert: a newsfeeds JSON file exists with the expected title
    let v = single_output_record(&out_dir, "newsfeeds", feed_id);

    // Basic shape assertions
    assert_eq!(v["table"], "newsfeeds");
    assert_eq!(v["feed_id"], serde_json::json!(424242));

    // Channel title should be present and trimmed
    assert_eq!(
        get_value_from_record(&v, "title"),
        Some(serde_json::json!("My Test Channel"))
    );
}

#[test]
fn writes_channel_link_and_description_cdata() {
    // Arrange
    let out_dir = ensure_output_dir();

    let last_modified = "0";
    let etag = "[[NO_ETAG]]";
    let url = "https://example.com/feed.xml";
    let downloaded = "0";
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
<title>Channel With Links</title>
<link>https://example.com/</link>
<description><![CDATA[ This is a <b>CDATA</b> description. ]]></description>
  </channel>
</rss>"#;

    let input = format!(
        "{last}\n{etag}\n{url}\n{dl}\n{xml}\n",
        last = last_modified,
        etag = etag,
        url = url,
        dl = downloaded,
        xml = xml
    );

    let feed_id = 777001_i64;

    // Act
    process_feed_sync(Cursor::new(input.into_bytes()), "<test>", Some(feed_id));

    // Assert: find the newsfeeds file for this feed_id
    let v = single_output_record(&out_dir, "newsfeeds", feed_id);

    assert_eq!(v["table"], "newsfeeds");
    assert_eq!(
        get_value_from_record(&v, "title"),
        Some(serde_json::json!("Channel With Links"))
    );
    assert_eq!(
        get_value_from_record(&v, "link"),
        Some(serde_json::json!("https://example.com/"))
    );
    assert_eq!(
        get_value_from_record(&v, "description"),
        Some(serde_json::json!("This is a <b>CDATA</b> description."))
    );
}

#[test]
fn writes_item_title_link_description_with_cdata() {
    // Arrange
    let out_dir = ensure_output_dir();

    let last_modified = "0";
    let etag = "[[NO_ETAG]]";
    let url = "https://example.com/feed.xml";
    let downloaded = "0";
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
<title>Channel</title>
<item>
  <title>Episode 1</title>
  <link>https://example.com/ep1</link>
  <description><![CDATA[ Hello & welcome! ]]></description>
  <enclosure url="https://example.com/ep1.mp3" length="1234" type="audio/mpeg"/>
</item>
  </channel>
</rss>"#;

    let input = format!(
        "{last}\n{etag}\n{url}\n{dl}\n{xml}\n",
        last = last_modified,
        etag = etag,
        url = url,
        dl = downloaded,
        xml = xml
    );

    let feed_id = 777002_i64;

    // Act
    process_feed_sync(Cursor::new(input.into_bytes()), "<test>", Some(feed_id));

    // Assert: find the nfitems file for this feed_id
    let v = single_output_record(&out_dir, "nfitems", feed_id);

    assert_eq!(v["table"], "nfitems");
    assert_eq!(v["values"][1], serde_json::json!("Episode 1"));
    assert_eq!(v["values"][2], serde_json::json!("https://example.com/ep1"));
    assert_eq!(v["values"][3], serde_json::json!(" Hello & welcome! "));
}

// Edge case: Empty feed
#[test]
fn test_empty_feed() {
    // Arrange
    let out_dir = ensure_output_dir();

    let feed = r#"1
[[NO_ETAG]]
https://www.ualrpublicradio.org/podcast/arts-letters/rss.xml
1745569945
"#;

    // Act
    let feed_id = 1337_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    // Assert: find the newsfeeds file for this feed_id
    let v = single_output_record(&out_dir, "newsfeeds", feed_id);

    assert_eq!(v["table"], "newsfeeds");
    assert_eq!(v["feed_id"], serde_json::json!(feed_id));
    assert_eq!(get_value_from_record(&v, "title"), Some(serde_json::json!("")));
    assert_eq!(get_value_from_record(&v, "link"), Some(serde_json::json!("")));
    assert_eq!(get_value_from_record(&v, "description"), Some(serde_json::json!("")));
}

// Table: newsfeeds - Complete field coverage
#[test]
fn test_newsfeeds_table() {
    // Arrange
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"
 xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Complete Channel</title>
<link>https://example.com</link>
<description>Full description</description>
<itunes:summary>Channel summary wins</itunes:summary>
<pubDate>Mon, 01 Jan 2024 12:00:00 GMT</pubDate>
<lastBuildDate>Mon, 01 Jan 2024 12:00:01 GMT</lastBuildDate>
<language>en-US</language>
<generator>TestGen 1.0</generator>
<itunes:author>Author Name</itunes:author>
<itunes:owner>
<itunes:name>Owner Name</itunes:name>
<itunes:email>owner@example.com</itunes:email>
</itunes:owner>
<itunes:category text="Technology">
<itunes:category text="Software"/>
</itunes:category>
<itunes:type>episodic</itunes:type>
<itunes:new-feed-url>https://new.example.com/feed.xml</itunes:new-feed-url>
<itunes:explicit>yes</itunes:explicit>
<itunes:image href="https://example.com/itunes.jpg"/>
<image><url>https://example.com/rss.jpg</url></image>
<podcast:locked owner="pod@example.com">yes</podcast:locked>
</channel>
</rss>"#;

    // Act
    let feed_id = 2001_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    // Assert: find the newsfeeds file for this feed_id
    let nf = single_output_record(&out_dir, "newsfeeds", feed_id);

    assert_eq!(get_value_from_record(&nf, "feed_id"), Some(JsonValue::from(feed_id)));
    assert_eq!(get_value_from_record(&nf, "title"), Some(JsonValue::from("Complete Channel")));
    assert_eq!(get_value_from_record(&nf, "link"), Some(JsonValue::from("https://example.com")));
    assert_eq!(get_value_from_record(&nf, "description"), Some(JsonValue::from("Channel summary wins")));
    assert_eq!(get_value_from_record(&nf, "language"), Some(JsonValue::from("en-US")));
    assert_eq!(get_value_from_record(&nf, "itunes_author"), Some(JsonValue::from("Author Name")));
    assert_eq!(get_value_from_record(&nf, "itunes_owner_name"), Some(JsonValue::from("Owner Name")));
    assert_eq!(get_value_from_record(&nf, "explicit"), Some(JsonValue::from(1)));
    assert_eq!(get_value_from_record(&nf, "podcast_locked"), Some(JsonValue::from(1)));
    assert_eq!(get_value_from_record(&nf, "image"), Some(JsonValue::from("https://example.com/rss.jpg")));
    assert_eq!(get_value_from_record(&nf, "artwork_url_600"), Some(JsonValue::from("https://example.com/itunes.jpg")));
}

// Table: newsfeeds - Hashes and item stats
#[test]
fn test_newsfeeds_hashes_and_counts() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"
 xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Hash Channel</title>
<link>https://hash.example.com</link>
<description>Hashing</description>
<language>en</language>
<generator>HashGen</generator>
<itunes:author>Hash Author</itunes:author>
<itunes:owner>
<itunes:name>Hash Owner</itunes:name>
<itunes:email>hash@example.com</itunes:email>
</itunes:owner>
<itunes:explicit>no</itunes:explicit>

<item>
<title>First</title>
<itunes:title>First IT</itunes:title>
<link>https://hash.example.com/1</link>
<pubDate>Mon, 01 Jan 2024 12:00:00 GMT</pubDate>
<guid>g1</guid>
<enclosure url="https://hash.example.com/1.mp3" length="10" type="audio/mpeg"/>
<podcast:funding url="https://fund.example.com/1">Fund1</podcast:funding>
</item>
<item>
<title>Second</title>
<itunes:title>Second IT</itunes:title>
<link>https://hash.example.com/2</link>
<pubDate>Tue, 02 Jan 2024 12:00:00 GMT</pubDate>
<guid>g2</guid>
<enclosure url="https://hash.example.com/2.mp3" length="20" type="audio/mpeg"/>
<podcast:funding url="https://fund.example.com/2">Fund2</podcast:funding>
</item>
</channel>
</rss>"#;

    let feed_id = 20100_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));
    let nf = single_output_record(&out_dir, "newsfeeds", feed_id);

    // Expected chash (channel-level stable fields)
    let expected_chash = utils::md5_hex_from_parts(&[
        "Hash Channel",
        "https://hash.example.com",
        "en",
        "HashGen",
        "Hash Author",
        "Hash Owner",
        "hash@example.com",
    ]);

    // Expected item_content_hash based on item-level fields hashed in order
    let mut h = md5::Context::new();
    // Item 1
    h.consume("First IT".as_bytes());
    h.consume("https://hash.example.com/1".as_bytes());
    h.consume("https://hash.example.com/1.mp3".as_bytes());
    h.consume("audio/mpeg".as_bytes());
    h.consume("https://fund.example.com/1".as_bytes());
    h.consume("Fund1".as_bytes());
    // Item 2
    h.consume("Second IT".as_bytes());
    h.consume("https://hash.example.com/2".as_bytes());
    h.consume("https://hash.example.com/2.mp3".as_bytes());
    h.consume("audio/mpeg".as_bytes());
    h.consume("https://fund.example.com/2".as_bytes());
    h.consume("Fund2".as_bytes());
    let expected_item_hash = format!("{:x}", h.compute());

    // Timestamps
    let newest = utils::parse_pub_date_to_unix("Tue, 02 Jan 2024 12:00:00 GMT").unwrap();
    let oldest = utils::parse_pub_date_to_unix("Mon, 01 Jan 2024 12:00:00 GMT").unwrap();

    assert_eq!(get_value_from_record(&nf, "item_count"), Some(JsonValue::from(2)));
    assert_eq!(get_value_from_record(&nf, "newest_item_pubdate"), Some(JsonValue::from(newest)));
    assert_eq!(get_value_from_record(&nf, "oldest_item_pubdate"), Some(JsonValue::from(oldest)));
    assert_eq!(get_value_from_record(&nf, "chash"), Some(JsonValue::from(expected_chash)));
    assert_eq!(get_value_from_record(&nf, "podcast_chapters"), Some(JsonValue::from(expected_item_hash)));
}

// Table: nfitems - Complete field coverage
#[test]
fn test_nfitems_table() {
    // Arrange
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"
 xmlns:podcast="https://podcastindex.org/namespace/1.0"
 xmlns:content="http://purl.org/rss/1.0/modules/content/">
<channel>
<title>Test Feed</title>
<item>
<title>Complete Episode</title>
<itunes:title>Itunes Episode Title</itunes:title>
<link>https://example.com/ep1</link>
<description>Episode description</description>
<itunes:summary>Itunes summary wins</itunes:summary>
<content:encoded>Content encoded fallback</content:encoded>
<pubDate>Mon, 01 Jan 2024 12:00:00 GMT</pubDate>
<guid>ep-guid</guid>
<itunes:image href="https://example.com/ep.jpg"/>
<itunes:duration>3600</itunes:duration>
<itunes:episode>42</itunes:episode>
<itunes:season>3</itunes:season>
<itunes:episodeType>full</itunes:episodeType>
<itunes:explicit>yes</itunes:explicit>
<enclosure url="https://example.com/ep.mp3" length="12345678" type="audio/mpeg"/>
<podcast:funding url="https://donate.example.com">Support!</podcast:funding>
</item>
</channel>
</rss>"#;

    // Act
    let feed_id = 2002_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    // Assert: find all nfitems files for this feed_id
    let mut items = output_records(&out_dir, "nfitems", feed_id);
    assert_eq!(items.len(), 1);
    let item = items.pop().unwrap();

    assert_eq!(get_value_from_record(&item, "feed_id"), Some(JsonValue::from(feed_id)));
    assert_eq!(get_value_from_record(&item, "title"), Some(JsonValue::from("Itunes Episode Title")));
    assert_eq!(get_value_from_record(&item, "link"), Some(JsonValue::from("https://example.com/ep1")));
    assert_eq!(
        get_value_from_record(&item, "description"),
        Some(JsonValue::from("Content encoded fallback"))
    );
    let expected_pub_date = utils::parse_pub_date_to_unix("Mon, 01 Jan 2024 12:00:00 GMT").unwrap();
    assert_eq!(get_value_from_record(&item, "pub_date"), Some(JsonValue::from(expected_pub_date)));
    assert_eq!(get_value_from_record(&item, "image"), Some(JsonValue::from("https://example.com/ep.jpg")));
    assert_eq!(get_value_from_record(&item, "itunes_episode"), Some(JsonValue::from(42)));
    assert_eq!(get_value_from_record(&item, "itunes_season"), Some(JsonValue::from(3)));
    assert_eq!(get_value_from_record(&item, "itunes_explicit"), Some(JsonValue::from(1)));
    assert_eq!(get_value_from_record(&item, "enclosure_length"), Some(JsonValue::from(12345678)));
}

// Table: nfguids
#[test]
fn test_nfguids_table() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>GUID Test</title>
<podcast:guid>unique-guid-123</podcast:guid>
</channel>
</rss>"#;

    let feed_id = 2003_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let guid = single_output_record(&out_dir, "nfguids", feed_id);

    assert_eq!(get_value_from_record(&guid, "guid"), Some(JsonValue::from("unique-guid-123")));
}

// Table: nffunding
#[test]
fn test_nffunding_table() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Funding Test</title>
<podcast:funding url="https://patreon.com/podcast">Support us!</podcast:funding>
</channel>
</rss>"#;

    let feed_id = 2004_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let funding = single_output_record(&out_dir, "nffunding", feed_id);

    assert_eq!(get_value_from_record(&funding, "url"), Some(JsonValue::from("https://patreon.com/podcast")));
    assert_eq!(get_value_from_record(&funding, "message"), Some(JsonValue::from("Support us!")));
}

// Table: pubsub
#[test]
fn test_pubsub_table() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:atom="http://www.w3.org/2005/Atom">
<channel>
<title>PubSub Test</title>
<atom:link rel="hub" href="https://pubsubhubbub.appspot.com/"/>
<atom:link rel="self" href="https://example.com/feed.xml"/>
</channel>
</rss>"#;

    let feed_id = 2005_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let pubsub = single_output_record(&out_dir, "pubsub", feed_id);

    assert_eq!(get_value_from_record(&pubsub, "hub_url"), Some(JsonValue::from("https://pubsubhubbub.appspot.com/")));
    assert_eq!(get_value_from_record(&pubsub, "self_url"), Some(JsonValue::from("https://example.com/feed.xml")));
}

// Table: nfitem_transcripts - Including type detection
#[test]
fn test_nfitem_transcripts_table() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Transcript Test</title>
<item>
<title>Ep 1</title>
<guid>ep1</guid>
<enclosure url="https://example.com/ep1.mp3" length="123" type="audio/mpeg"/>
<podcast:transcript url="https://example.com/t1.json" type="application/json"/>
</item>
<item>
<title>Ep 2</title>
<guid>ep2</guid>
<enclosure url="https://example.com/ep2.mp3" length="123" type="audio/mpeg"/>
<podcast:transcript url="https://example.com/t2.srt" type="text/srt"/>
</item>
<item>
<title>Ep 3</title>
<guid>ep3</guid>
<enclosure url="https://example.com/ep3.mp3" length="123" type="audio/mpeg"/>
<podcast:transcript url="https://example.com/t3.vtt" type="text/vtt"/>
</item>
</channel>
</rss>"#;

    let feed_id = 2006_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));
    let transcripts = output_records(&out_dir, "nfitem_transcripts", feed_id);

    assert_eq!(transcripts.len(), 3);

    // Type detection: JSON=1, SRT=2, VTT=3
    // Verify each type appears once (order may vary)
    let mut types = Vec::new();
    for transcript in &transcripts {
        if let Some(type_val) = get_value_from_record(transcript, "type") {
            if let Some(t) = type_val.as_i64() {
                types.push(t);
            }
        }
    }

    assert!(types.contains(&1), "Should have JSON type (1)");
    assert!(types.contains(&2), "Should have SRT type (2)");
    assert!(types.contains(&3), "Should have VTT type (3)");
}

// Table: nfitem_chapters
#[test]
fn test_nfitem_chapters_table() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Chapters Test</title>
<item>
<title>Episode</title>
<guid>ep</guid>
<enclosure url="https://example.com/ep.mp3" length="123" type="audio/mpeg"/>
<podcast:chapters url="https://example.com/chapters.json" type="application/json"/>
</item>
</channel>
</rss>"#;

    let feed_id = 2007_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let mut chapters = output_records(&out_dir, "nfitem_chapters", feed_id);

    assert_eq!(chapters.len(), 1);
    let chapter = chapters.pop().unwrap();

    assert_eq!(get_value_from_record(&chapter, "url"), Some(JsonValue::from("https://example.com/chapters.json")));
}

// Table: nfitem_soundbites - Including multiple per item
#[test]
fn test_nfitem_soundbites_table() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Soundbites Test</title>
<item>
<title>Episode</title>
<guid>ep</guid>
<enclosure url="https://example.com/ep.mp3" length="123" type="audio/mpeg"/>
<podcast:soundbite startTime="10" duration="30">Intro</podcast:soundbite>
<podcast:soundbite startTime="100" duration="45">Main topic</podcast:soundbite>
</item>
</channel>
</rss>"#;

    let feed_id = 2008_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let soundbites = output_records(&out_dir, "nfitem_soundbites", feed_id);

    assert_eq!(soundbites.len(), 2);

    assert_eq!(get_value_from_record(&soundbites[0], "title"), Some(JsonValue::from("Intro")));
    assert_eq!(get_value_from_record(&soundbites[0], "start_time"), Some(JsonValue::from(10.0)));
    assert_eq!(get_value_from_record(&soundbites[1], "title"), Some(JsonValue::from("Main topic")));
}

// Table: nfitem_persons - Including multiple per item
#[test]
fn test_nfitem_persons_table() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Persons Test</title>
<item>
<title>Episode</title>
<guid>ep</guid>
<enclosure url="https://example.com/ep.mp3" length="123" type="audio/mpeg"/>
<podcast:person role="host" group="cast" img="https://example.com/host.jpg" href="https://example.com/host">Alice</podcast:person>
<podcast:person role="guest">Bob</podcast:person>
</item>
</channel>
</rss>"#;

    let feed_id = 2009_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let persons = output_records(&out_dir, "nfitem_persons", feed_id);

    assert_eq!(persons.len(), 2);

    assert_eq!(get_value_from_record(&persons[0], "name"), Some(JsonValue::from("Alice")));
    assert_eq!(get_value_from_record(&persons[0], "role"), Some(JsonValue::from("host")));
    assert_eq!(get_value_from_record(&persons[1], "name"), Some(JsonValue::from("Bob")));
    assert_eq!(get_value_from_record(&persons[1], "role"), Some(JsonValue::from("guest")));
}

// Table: nfitem_value
#[test]
fn test_nfitem_value_table() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Item Value Test</title>
<item>
<title>Episode</title>
<guid>ep</guid>
<enclosure url="https://example.com/ep.mp3" length="123" type="audio/mpeg"/>
<podcast:value type="lightning" method="keysend" suggested="0.00000005000">
    <podcast:valueRecipient name="Podcaster" type="node" address="addr123" split="90"/>
    <podcast:valueRecipient name="App" type="node" address="addr456" split="10" fee="true"/>
</podcast:value>
</item>
</channel>
</rss>"#;

    let feed_id = 2010_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let mut values = output_records(&out_dir, "nfitem_value", feed_id);

    assert_eq!(values.len(), 1);
    let value = values.pop().unwrap();

    let value_block_val = get_value_from_record(&value, "value_block").unwrap();
    let value_block_str = value_block_val.as_str().unwrap();
    let value_block: JsonValue = serde_json::from_str(value_block_str).unwrap();
    assert_eq!(value_block["model"]["type"], "lightning");
    assert_eq!(value_block["destinations"].as_array().unwrap().len(), 2);
}

// Table: nfvalue - Including recipient limit and custom fields
#[test]
fn test_nfvalue_table() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Channel Value Test</title>
<podcast:value type="lightning" method="keysend" suggested="0.00000005000">
<podcast:valueRecipient name="Podcaster" type="node" address="addr123" split="95" customKey="key1" customValue="value1"/>
<podcast:valueRecipient name="Hosting" type="node" address="addr789" split="5"/>
</podcast:value>
</channel>
</rss>"#;

    let feed_id = 2011_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let mut values = output_records(&out_dir, "nfvalue", feed_id);

    assert_eq!(values.len(), 1);
    let value = values.pop().unwrap();

    let value_block_val = get_value_from_record(&value, "value_block").unwrap();
    let value_block_str = value_block_val.as_str().unwrap();
    let value_block: JsonValue = serde_json::from_str(value_block_str).unwrap();
    let destinations = value_block["destinations"].as_array().unwrap();
    assert_eq!(destinations[0]["customKey"], "key1");
}

// Edge case: Value block recipient limit (100 cap)
#[test]
fn test_value_recipient_limit() {
    let out_dir = ensure_output_dir();

    let mut recipients = String::new();
    for i in 1..=150 {
        recipients.push_str(&format!(
            r#"<podcast:valueRecipient name="R{}" type="node" address="a{}" split="1"/>"#, i, i
        ));
    }

    let feed = format!(r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Value Limit Test</title>
<podcast:value type="lightning" method="keysend">
{}
</podcast:value>
</channel>
</rss>"#, recipients);

    let feed_id = 2012_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let value = single_output_record(&out_dir, "nfvalue", feed_id);

    let vb_val = get_value_from_record(&value, "value_block").unwrap();
    let vb_str = vb_val.as_str().unwrap();
    let vb: JsonValue = serde_json::from_str(vb_str).unwrap();
    assert_eq!(vb["destinations"].as_array().unwrap().len(), 100); // Capped at 100
}

// Edge case: Image fallback (itunes:image when regular image is empty)
#[test]
fn test_image_fallback() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
<channel>
<title>Image Fallback</title>
<itunes:image href="https://example.com/itunes-only.jpg"/>
<item>
<title>Episode</title>
<guid>ep</guid>
<enclosure url="https://example.com/ep.mp3" length="123" type="audio/mpeg"/>
<itunes:image href="https://example.com/ep-itunes-only.jpg"/>
</item>
</channel>
</rss>"#;

    let feed_id = 2014_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let nf = single_output_record(&out_dir, "newsfeeds", feed_id);
    let nfitems = output_records(&out_dir, "nfitems", feed_id);

    assert_eq!(get_value_from_record(&nf, "image"), Some(JsonValue::from("https://example.com/itunes-only.jpg")));

    assert_eq!(nfitems.len(), 1);
    let item = &nfitems[0];

    assert_eq!(get_value_from_record(item, "image"), Some(JsonValue::from("https://example.com/ep-itunes-only.jpg")));
}

#[test]
fn test_episode_season_parsing() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
<channel>
<title>Parsing Test</title>
<item>
<title>Episode</title>
<guid>ep</guid>
<enclosure url="https://example.com/ep.mp3" length="123" type="audio/mpeg"/>
<itunes:episode>10</itunes:episode>
<itunes:season>02</itunes:season>
</item>
</channel>
</rss>"#;

    let feed_id = 2015_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let mut nfitems = output_records(&out_dir, "nfitems", feed_id);
    assert_eq!(nfitems.len(), 1);
    let item = nfitems.pop().unwrap();

    assert_eq!(get_value_from_record(&item, "itunes_episode"), Some(JsonValue::from(10)));
    assert_eq!(get_value_from_record(&item, "itunes_season"), Some(JsonValue::from(2)));
}

// Edge case: Multiple items
#[test]
fn test_multiple_items() {
    // Arrange
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss>
<channel>
<title>Multi Test</title>
<item><title>Ep 1</title><guid>e1</guid><enclosure url="http://x.com/1.mp3" length="1" type="audio/mpeg"/></item>
<item><title>Ep 2</title><guid>e2</guid><enclosure url="http://x.com/2.mp3" length="1" type="audio/mpeg"/></item>
<item><title>Ep 3</title><guid>e3</guid><enclosure url="http://x.com/3.mp3" length="1" type="audio/mpeg"/></item>
</channel>
</rss>"#;

    // Act
    let feed_id = 2016_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    // Assert: find all nfitems files for this feed_id
    let nfitems = output_records(&out_dir, "nfitems", feed_id);
    assert_eq!(nfitems.len(), 3);
}

// Edge case: itunes:image as text content (channel and item)
#[test]
fn test_itunes_image_text_content() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
<channel>
<title>Image Text</title>
<itunes:image>https://example.com/itunes-text.jpg</itunes:image>
<item>
<title>Episode</title>
<guid>ep</guid>
<enclosure url="https://example.com/ep.mp3" length="123" type="audio/mpeg"/>
<itunes:image>https://example.com/ep-text.jpg</itunes:image>
</item>
</channel>
</rss>"#;

    let feed_id = 2017_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let mut nfitems = output_records(&out_dir, "nfitems", feed_id);
    assert_eq!(nfitems.len(), 1);
    let v = nfitems.pop().unwrap();

    assert_eq!(v["table"], "nfitems");
    assert_eq!(v["feed_id"], serde_json::json!(feed_id));
    assert_eq!(
        get_value_from_record(&v, "image"),
        Some(JsonValue::from("https://example.com/ep-text.jpg"))
    );

}

// Lightning podcast:value should be chosen when multiple exist (channel-level)
#[test]
fn test_channel_value_lightning_priority() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Value Priority</title>
<podcast:value type="bitcoin" method="custom">
<podcast:valueRecipient name="BTC" type="node" address="btc" split="100"/>
</podcast:value>
<podcast:value type="lightning" method="keysend">
<podcast:valueRecipient name="LN" type="node" address="ln" split="100"/>
</podcast:value>
</channel>
</rss>"#;

    let feed_id = 2018_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let value = single_output_record(&out_dir, "nfvalue", feed_id);

    let vb_val = get_value_from_record(&value, "value_block").unwrap();
    let vb: JsonValue = serde_json::from_str(vb_val.as_str().unwrap()).unwrap();
    assert_eq!(vb["model"]["type"], "lightning");
    assert_eq!(vb["destinations"][0]["name"], "LN");
}

// fee="yes" should be treated as true in podcast:valueRecipient
#[test]
fn test_value_fee_yes() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Fee Yes</title>
<item>
<title>Episode</title>
<guid>ep</guid>
<enclosure url="https://example.com/ep.mp3" length="123" type="audio/mpeg"/>
<podcast:value type="lightning" method="keysend">
    <podcast:valueRecipient name="App" type="node" address="addr" split="100" fee="yes"/>
</podcast:value>
</item>
</channel>
</rss>"#;

    let feed_id = 2019_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let value = single_output_record(&out_dir, "nfitem_value", feed_id);

    let vb_val = get_value_from_record(&value, "value_block").unwrap();
    let vb: JsonValue = serde_json::from_str(vb_val.as_str().unwrap()).unwrap();
    assert_eq!(vb["destinations"][0]["fee"], true);
}

// Items without a valid enclosure should be skipped entirely (including transcripts/value)
#[test]
fn test_skip_items_without_enclosure() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>No Enclosure</title>
<item>
<title>Missing Enclosure</title>
<guid>ep1</guid>
<podcast:transcript url="https://example.com/t1.json" type="application/json"/>
<podcast:value type="lightning" method="keysend">
    <podcast:valueRecipient name="App" type="node" address="addr" split="100"/>
</podcast:value>
</item>
</channel>
</rss>"#;

    let feed_id = 2020_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let nfitems = output_records(&out_dir, "nfitems", feed_id);
    let transcripts = output_records(&out_dir, "nfitem_transcripts", feed_id);
    let values = output_records(&out_dir, "nfitem_value", feed_id);

    assert_eq!(nfitems.len(), 0);
    assert_eq!(transcripts.len(), 0);
    assert_eq!(values.len(), 0);
}

// Atom <link rel="enclosure"> should be treated as enclosure
#[test]
fn test_atom_enclosure() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:atom="http://www.w3.org/2005/Atom">
<channel>
<title>Atom Enclosure</title>
<item>
<title>Episode</title>
<guid>ep</guid>
<atom:link rel="enclosure" href="https://example.com/ep.mp3" length="555" type="audio/mpeg"/>
</item>
</channel>
</rss>"#;

    let feed_id = 2026_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let mut nfitems = output_records(&out_dir, "nfitems", feed_id);
    assert_eq!(nfitems.len(), 1);
    let item = nfitems.pop().unwrap();

    assert_eq!(get_value_from_record(&item, "enclosure_url"), Some(JsonValue::from("https://example.com/ep.mp3")));
    assert_eq!(get_value_from_record(&item, "enclosure_length"), Some(JsonValue::from(555)));
    assert_eq!(get_value_from_record(&item, "enclosure_type"), Some(JsonValue::from("audio/mpeg")));
}

// itunes:explicit boolean true/false should be honored for channel and item
#[test]
fn test_itunes_explicit_boolean() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
<channel>
<title>Explicit Bool</title>
<itunes:explicit>true</itunes:explicit>
<item>
<title>Episode</title>
<guid>ep</guid>
<itunes:explicit>false</itunes:explicit>
<enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
</item>
</channel>
</rss>"#;

    let feed_id = 2027_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let nf = single_output_record(&out_dir, "newsfeeds", feed_id);
    let nfitems = output_records(&out_dir, "nfitems", feed_id);

    assert_eq!(get_value_from_record(&nf, "explicit"), Some(JsonValue::from(1)));

    assert_eq!(nfitems.len(), 1);
    let item = &nfitems[0];

    assert_eq!(get_value_from_record(item, "itunes_explicit"), Some(JsonValue::from(0)));
}

// Soundbite and person fields should be truncated to Partytime limits
#[test]
fn test_soundbite_and_person_truncation() {
    let out_dir = ensure_output_dir();

    let long_title = "x".repeat(600);
    let long_name = "n".repeat(200);
    let long_role = "R".repeat(200);
    let long_group = "G".repeat(200);
    let long_img = format!("https://example.com/{}.jpg", "i".repeat(800));
    let long_href = format!("https://example.com/{}", "h".repeat(800));

    let feed = format!(r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Truncate</title>
<item>
<title>Episode</title>
<guid>ep</guid>
<enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
<podcast:soundbite startTime="0" duration="1">{long_title}</podcast:soundbite>
<podcast:person role="{long_role}" group="{long_group}" img="{long_img}" href="{long_href}">{long_name}</podcast:person>
</item>
</channel>
</rss>"#);

    process_feed_sync(Cursor::new(feed), "test.xml", Some(2028));

    let soundbites = output_records(&out_dir, "nfitem_soundbites", 2028);
    let persons = output_records(&out_dir, "nfitem_persons", 2028);

    assert_eq!(soundbites.len(), 1);
    let sb_title = get_value_from_record(&soundbites[0], "title")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(sb_title.len(), 500);

    assert_eq!(persons.len(), 1);
    let name = get_value_from_record(&persons[0], "name")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let role = get_value_from_record(&persons[0], "role")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let group = get_value_from_record(&persons[0], "grp")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let img = get_value_from_record(&persons[0], "img")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let href = get_value_from_record(&persons[0], "href")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();

    assert_eq!(name.len(), 128);
    assert_eq!(role.len(), 128);
    assert_eq!(group.len(), 128);
    assert_eq!(img.len(), 768);
    assert_eq!(href.len(), 768);
}

// Category mapping should emit nfcategories with correct IDs and compounds
#[test]
fn test_category_mapping_catmap() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
<channel>
<title>Categories</title>
<itunes:category text="Technology"/>
<itunes:category text="Video"/>
<itunes:category text="Games"/>
<item>
<title>Ep</title>
<guid>g1</guid>
<enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
</item>
</channel>
</rss>"#;

    let feed_id = 2021_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let catmap = single_output_record(&out_dir, "nfcategories", feed_id);

    assert_eq!(get_value_from_record(&catmap, "catid1"), Some(JsonValue::from(102)));
    assert_eq!(get_value_from_record(&catmap, "catid2"), Some(JsonValue::from(48)));
    assert_eq!(get_value_from_record(&catmap, "catid3"), Some(JsonValue::from(52)));
}

// First enclosure wins and type is guessed when missing
#[test]
fn test_enclosure_first_and_type_guess() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss>
<channel>
<title>Enclosures</title>
<item>
<title>Episode</title>
<guid>ep</guid>
<enclosure url="https://example.com/first.mp3" length="123"/>
<enclosure url="https://example.com/second.ogg" length="999" type="audio/ogg"/>
</item>
</channel>
</rss>"#;

    let feed_id = 2022_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let mut nfitems = output_records(&out_dir, "nfitems", feed_id);
    assert_eq!(nfitems.len(), 1);
    let item = nfitems.pop().unwrap();

    assert_eq!(get_value_from_record(&item, "enclosure_url"), Some(JsonValue::from("https://example.com/first.mp3")));
    assert_eq!(get_value_from_record(&item, "enclosure_type"), Some(JsonValue::from("audio/mpeg")));
    assert_eq!(get_value_from_record(&item, "enclosure_length"), Some(JsonValue::from(123)));
}

// Truncation/clamp behavior should mirror partytime.js limits
#[test]
fn test_partytime_truncation_and_clamps() {
    let out_dir = ensure_output_dir();

    let long_title = "T".repeat(1500);
    let long_guid = "G".repeat(900);
    let long_type = "audio/verylongtype".repeat(20);
    let long_owner = "owner@example.com-".repeat(20);

    let feed = format!(
        r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"
 xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Clamp Feed</title>
<language>abcdefghijklmnop</language>
<podcast:locked owner="{owner}">true</podcast:locked>
<item>
<title>{title}</title>
<guid>{guid}</guid>
<enclosure url="https://example.com/audio.mp3" length="9999999999999999999" type="{etype}"/>
<itunes:episode>9999999999</itunes:episode>
</item>
</channel>
</rss>"#,
        owner = long_owner,
        title = long_title,
        guid = long_guid,
        etype = long_type
    );

    let feed_id = 30305_i64;

    process_feed_sync(Cursor::new(feed), "clamp.xml", Some(feed_id));

    let item = single_output_record(&out_dir, "nfitems", feed_id);
    let news = single_output_record(&out_dir, "newsfeeds", feed_id);

    let title = get_value_from_record(&item, "title")
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap();
    assert_eq!(title.len(), 1024);
    let guid = get_value_from_record(&item, "guid")
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap();
    assert_eq!(guid.len(), 740);
    let enclosure_type = get_value_from_record(&item, "enclosure_type")
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap();
    assert_eq!(enclosure_type.len(), 128);
    assert_eq!(get_value_from_record(&item, "enclosure_length"), Some(JsonValue::from(0)));
    assert_eq!(
        get_value_from_record(&item, "itunes_episode"),
        Some(JsonValue::from(1_000_000))
    );

    assert_eq!(
        get_value_from_record(&news, "language"),
        Some(JsonValue::from("abcdefgh"))
    );
    let owner_val = get_value_from_record(&news, "podcast_owner")
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap();
    assert_eq!(owner_val.len(), 255);
}

// itunes:duration should normalize to seconds (including mm:ss format)
#[test]
fn test_duration_normalization() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
<channel>
<title>Duration</title>
<item>
<title>Episode</title>
<guid>ep</guid>
<itunes:duration>01:02</itunes:duration>
<enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
</item>
</channel>
</rss>"#;

    process_feed_sync(Cursor::new(feed), "test.xml", Some(2023));

    let nfitems = output_records(&out_dir, "nfitems", 2023);
    assert_eq!(nfitems.len(), 1);
    assert_eq!(get_value_from_record(&nfitems[0], "itunes_duration"), Some(JsonValue::from(62)));
}

// Value type should be mapped to numeric codes (HBD=1, bitcoin=2, lightning=0)
#[test]
fn test_value_type_mapping() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Value Types</title>
<podcast:value type="bitcoin" method="custom">
<podcast:valueRecipient name="BTC" type="node" address="addr" split="100"/>
</podcast:value>
<item>
<title>Episode</title>
<guid>ep</guid>
<enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
<podcast:value type="HBD" method="keysend">
    <podcast:valueRecipient name="App" type="node" address="addr" split="100"/>
</podcast:value>
</item>
</channel>
</rss>"#;

    let feed_id = 2024_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let channel_value = single_output_record(&out_dir, "nfvalue", feed_id);
    let item_value = single_output_record(&out_dir, "nfitem_value", feed_id);

    assert_eq!(get_value_from_record(&channel_value, "type"), Some(JsonValue::from(2)));
    assert_eq!(get_value_from_record(&item_value, "type"), Some(JsonValue::from(1)));
}

// podcast:locked should fall back to itunes owner email when owner is missing
#[test]
fn test_locked_owner_email_fallback() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd" xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Owner Fallback</title>
<itunes:owner>
<itunes:email>owner@example.com</itunes:email>
</itunes:owner>
<podcast:locked>yes</podcast:locked>
<item>
<title>Episode</title>
<guid>ep</guid>
<enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
</item>
</channel>
</rss>"#;

    let feed_id = 2025_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let nf = single_output_record(&out_dir, "newsfeeds", feed_id);

    assert_eq!(get_value_from_record(&nf, "podcast_locked"), Some(JsonValue::from(1)));
    assert_eq!(get_value_from_record(&nf, "podcast_owner"), Some(JsonValue::from("owner@example.com")));
}

// Update frequency should reflect recent publishing cadence and pub dates should be epoch seconds
#[test]
fn test_update_frequency_and_epoch_pubdates() {
    let out_dir = ensure_output_dir();

    let now = Utc::now();
    let recent = now - Duration::days(1);
    let rfc_now = DateTime::<Utc>::from(now).to_rfc2822();
    let rfc_recent = DateTime::<Utc>::from(recent).to_rfc2822();

    let feed = format!(r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss>
<channel>
<title>UpdateFreq</title>
<item>
<title>Ep1</title>
<guid>g1</guid>
<pubDate>{}</pubDate>
<enclosure url="https://example.com/ep1.mp3" length="10" type="audio/mpeg"/>
</item>
<item>
<title>Ep2</title>
<guid>g2</guid>
<pubDate>{}</pubDate>
<enclosure url="https://example.com/ep2.mp3" length="20" type="audio/mpeg"/>
</item>
</channel>
</rss>"#, rfc_now, rfc_recent);

    let feed_id = 30303_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let nf = single_output_record(&out_dir, "newsfeeds", feed_id);

    assert_eq!(get_value_from_record(&nf, "item_count"), Some(JsonValue::from(2)));
    assert_eq!(
        get_value_from_record(&nf, "newest_item_pubdate"),
        Some(JsonValue::from(now.timestamp()))
    );
    assert_eq!(
        get_value_from_record(&nf, "oldest_item_pubdate"),
        Some(JsonValue::from(recent.timestamp()))
    );
    // Two items within 5 days -> frequency bucket 1
    assert_eq!(get_value_from_record(&nf, "update_frequency"), Some(JsonValue::from(1)));

    // Verify nfitems pub_date fields are numeric epoch seconds
    let nfitems = output_records(&out_dir, "nfitems", feed_id);
    assert_eq!(nfitems.len(), 2);
    for item in nfitems {
        let pub_date_val = get_value_from_record(&item, "pub_date");
        assert!(pub_date_val.is_some(), "pub_date column present");
        assert!(
            pub_date_val.unwrap().is_number(),
            "pub_date should be numeric epoch seconds"
        );
    }
}

// When guid is missing, enclosure URL should be used as the GUID
#[test]
fn test_guid_fallback_to_enclosure() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss>
<channel>
<title>GuidFallback</title>
<item>
<title>Episode</title>
<enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
</item>
</channel>
</rss>"#;

    let feed_id = 30304_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let mut nfitems = output_records(&out_dir, "nfitems", feed_id);
    assert_eq!(nfitems.len(), 1);
    let item = nfitems.pop().unwrap();

    assert_eq!(
        get_value_from_record(&item, "guid"),
        Some(JsonValue::from("https://example.com/ep.mp3"))
    );
}

// Channel-level metadata like generator, itunes:type and itunes:new-feed-url should be preserved
#[test]
fn test_newsfeeds_itunes_metadata_fields() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
<channel>
<title>Meta Channel</title>
<generator>GenX/1.2</generator>
<itunes:type>trailer</itunes:type>
<itunes:new-feed-url>https://example.com/new.xml</itunes:new-feed-url>
<itunes:owner><itunes:email>meta@example.com</itunes:email></itunes:owner>
<itunes:image href="https://example.com/meta.jpg"/>
<item>
<title>Ep</title>
<guid>g-meta</guid>
<enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
</item>
</channel>
</rss>"#;

    let feed_id = 33001_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let nf = single_output_record(&out_dir, "newsfeeds", feed_id);

    assert_eq!(get_value_from_record(&nf, "generator"), Some(JsonValue::from("GenX/1.2")));
    assert_eq!(get_value_from_record(&nf, "itunes_type"), Some(JsonValue::from("trailer")));
    assert_eq!(
        get_value_from_record(&nf, "itunes_new_feed_url"),
        Some(JsonValue::from("https://example.com/new.xml"))
    );
}

// podcast:locked owner attribute should populate podcast_owner when provided
#[test]
fn test_locked_owner_attribute() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd" xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Locked Owner</title>
<itunes:owner><itunes:email>fallback@example.com</itunes:email></itunes:owner>
<podcast:locked owner="owner@example.com">yes</podcast:locked>
<item>
<title>Ep</title>
<guid>g-lock</guid>
<enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
</item>
</channel>
</rss>"#;

    let feed_id = 33002_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let nf = single_output_record(&out_dir, "newsfeeds", feed_id);

    assert_eq!(get_value_from_record(&nf, "podcast_locked"), Some(JsonValue::from(1)));
    assert_eq!(
        get_value_from_record(&nf, "podcast_owner"),
        Some(JsonValue::from("owner@example.com"))
    );
}

// itunes:episodeType should be emitted for items
#[test]
fn test_itunes_episode_type_output() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
<channel>
<title>EpisodeType</title>
<item>
<title>Ep</title>
<guid>g-type</guid>
<itunes:episodeType>bonus</itunes:episodeType>
<enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
</item>
</channel>
</rss>"#;

    let feed_id = 33003_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let mut nfitems = output_records(&out_dir, "nfitems", feed_id);
    assert_eq!(nfitems.len(), 1);
    let item = nfitems.pop().unwrap();

    assert_eq!(
        get_value_from_record(&item, "itunes_episode_type"),
        Some(JsonValue::from("bonus"))
    );
}

// Item-level podcast:value should prefer lightning when multiple blocks exist
#[test]
fn test_item_value_lightning_priority() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Item Value Priority</title>
<item>
<title>Ep</title>
<guid>g-val</guid>
<enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
<podcast:value type="bitcoin" method="keysend">
    <podcast:valueRecipient name="BTC" type="node" address="addr-btc" split="100"/>
</podcast:value>
<podcast:value type="lightning" method="keysend">
    <podcast:valueRecipient name="LN" type="node" address="addr-ln" split="100"/>
</podcast:value>
</item>
</channel>
</rss>"#;

    let feed_id = 33004_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let value = single_output_record(&out_dir, "nfitem_value", feed_id);

    // Lightning maps to type code 0 and should replace the earlier bitcoin block
    assert_eq!(get_value_from_record(&value, "type"), Some(JsonValue::from(0)));
    let vb_val = get_value_from_record(&value, "value_block").unwrap();
    let vb: JsonValue = serde_json::from_str(vb_val.as_str().unwrap()).unwrap();
    assert_eq!(vb["model"]["type"], "lightning");
    assert_eq!(vb["destinations"][0]["name"], "LN");
}

// content:encoded should be used as a fallback when description/summary are missing
#[test]
fn test_content_encoded_fallback_description() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss xmlns:content="http://purl.org/rss/1.0/modules/content/">
<channel>
<title>Content Encoded</title>
<item>
<title>Ep</title>
<guid>g-content</guid>
<content:encoded><![CDATA[<p>Rich description</p>]]></content:encoded>
<enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
</item>
</channel>
</rss>"#;

    process_feed_sync(Cursor::new(feed), "test.xml", Some(33005));

    let mut items = output_records(&out_dir, "nfitems", 33005);
    assert_eq!(items.len(), 1);
    let item = items.pop().unwrap();

    assert_eq!(
        get_value_from_record(&item, "description"),
        Some(JsonValue::from("<p>Rich description</p>"))
    );
}

// End-to-end check that all Partytime tables are emitted as JSON files
#[test]
fn test_partytime_feature_parity_end_to_end() {
    let out_dir = ensure_output_dir();
    let feed_id = Some(404040_i64);

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0"?>
<rss version="2.0"
xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"
xmlns:podcast="https://podcastindex.org/namespace/1.0"
xmlns:atom="http://www.w3.org/2005/Atom"
xmlns:content="http://purl.org/rss/1.0/modules/content/">
<channel>
<title>Partytime Show</title>
<link>https://example.com/show</link>
<description>Fallback channel description</description>
<itunes:summary>Itunes channel summary</itunes:summary>
<language>en-US</language>
<generator>PartyGen</generator>
<itunes:author>Party Author</itunes:author>
<itunes:owner><itunes:name>Owner One</itunes:name><itunes:email>owner@example.com</itunes:email></itunes:owner>
<itunes:type>serial</itunes:type>
<itunes:new-feed-url>https://example.com/newfeed.xml</itunes:new-feed-url>
<itunes:explicit>yes</itunes:explicit>
<image><url>https://example.com/rss.jpg</url></image>
<itunes:image href="https://example.com/itunes.jpg"/>
<podcast:guid>party-guid</podcast:guid>
<podcast:locked owner="lock@example.com">true</podcast:locked>
<podcast:funding url="https://example.com/support">Support us</podcast:funding>
<atom:link rel="hub" href="https://hub.example.com"/>
<atom:link rel="self" href="https://example.com/feed.xml"/>
<category>Technology</category>
<itunes:category text="Technology"/>
<podcast:value type="bitcoin" method="keysend" suggested="5">
  <podcast:valueRecipient name="Alice" type="node" address="alice" split="90"/>
  <podcast:valueRecipient name="Bob" type="node" address="bob" split="10" fee="true"/>
</podcast:value>
<item>
  <title>Item Title</title>
  <itunes:title>Item Itunes Title</itunes:title>
  <link>https://example.com/episode</link>
  <description>Item desc</description>
  <content:encoded><![CDATA[<p>HTML desc</p>]]></content:encoded>
  <itunes:summary>Itunes item summary</itunes:summary>
  <pubDate>Tue, 02 Jan 2024 03:04:05 +0000</pubDate>
  <guid>guid-party</guid>
  <itunes:image href="https://example.com/item.jpg"/>
  <enclosure url="https://cdn.example.com/episode.mp3" length="321" type="audio/mpeg"/>
  <itunes:duration>1:05</itunes:duration>
  <itunes:episode>2</itunes:episode>
  <itunes:season>1</itunes:season>
  <itunes:episodeType>bonus</itunes:episodeType>
  <itunes:explicit>no</itunes:explicit>
  <podcast:funding url="https://example.com/ep-support">Episode support</podcast:funding>
  <podcast:transcript url="https://example.com/ep.vtt" type="text/vtt"/>
  <podcast:chapters url="https://example.com/chapters.json" type="application/json"/>
  <podcast:soundbite startTime="10" duration="15">Clip</podcast:soundbite>
  <podcast:person role="host" group="cast" img="https://example.com/host.jpg" href="https://example.com/host">Host Name</podcast:person>
  <podcast:value type="HBD" method="keysend" suggested="1">
<podcast:valueRecipient name="Carol" type="node" address="carol" split="100"/>
  </podcast:value>
</item>
</channel>
</rss>"#;

    process_feed_sync(Cursor::new(feed), "partytime.xml", feed_id);

    let mut tables: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
    for entry in fs::read_dir(&out_dir).expect("output directory should be readable").flatten() {
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
            if name.ends_with("404040.json") {
                let contents = fs::read_to_string(&path).expect("read output file");
                let record: serde_json::Value =
                    serde_json::from_str(&contents).expect("valid JSON output");
                let table = record["table"].as_str().unwrap_or("").to_string();
                tables.entry(table).or_default().push(record);
            }
        }
    }

    let expected_tables = [
        "newsfeeds",
        "nfitems",
        "nfguids",
        "pubsub",
        "nffunding",
        "nfcategories",
        "nfitem_transcripts",
        "nfitem_chapters",
        "nfitem_soundbites",
        "nfitem_persons",
        "nfitem_value",
        "nfvalue",
    ];
    for table in expected_tables {
        assert!(tables.contains_key(table), "missing table {}", table);
        assert_eq!(tables[table].len(), 1, "expected one record for {}", table);
    }

    let get_value = |obj: &serde_json::Value, col_name: &str| -> Option<serde_json::Value> {
        let columns = obj["columns"].as_array()?;
        let values = obj["values"].as_array()?;
        for (i, col) in columns.iter().enumerate() {
            if col.as_str()? == col_name {
                return values.get(i).cloned();
            }
        }
        None
    };

    let news = &tables["newsfeeds"][0];
    assert_eq!(get_value(news, "title"), Some(json!("Partytime Show")));
    assert_eq!(
        get_value(news, "description"),
        Some(json!("Itunes channel summary"))
    );
    assert_eq!(get_value(news, "itunes_author"), Some(json!("Party Author")));
    assert_eq!(
        get_value(news, "itunes_owner_email"),
        Some(json!("owner@example.com"))
    );
    assert_eq!(get_value(news, "itunes_type"), Some(json!("serial")));
    assert_eq!(get_value(news, "podcast_locked"), Some(json!(1)));
    assert_eq!(
        get_value(news, "podcast_owner"),
        Some(json!("lock@example.com"))
    );
    assert_eq!(
        get_value(news, "itunes_image"),
        Some(json!("https://example.com/itunes.jpg"))
    );
    assert_eq!(
        get_value(news, "image"),
        Some(json!("https://example.com/rss.jpg"))
    );
    assert_eq!(get_value(news, "item_count"), Some(json!(1)));
    assert_eq!(get_value(news, "update_frequency"), Some(json!(9)));
    let podcast_chapters_hash = get_value(news, "podcast_chapters")
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap();
    assert_eq!(podcast_chapters_hash.len(), 32);

    let nfcategories = &tables["nfcategories"][0];
    let expected_cat_ids = utils::build_category_ids(&vec!["Technology".to_string()]);
    assert_eq!(
        get_value(nfcategories, "catid1"),
        Some(json!(expected_cat_ids[1]))
    );

    let nfguids = &tables["nfguids"][0];
    assert_eq!(get_value(nfguids, "guid"), Some(json!("party-guid")));

    let nffunding = &tables["nffunding"][0];
    assert_eq!(
        get_value(nffunding, "url"),
        Some(json!("https://example.com/support"))
    );
    assert_eq!(
        get_value(nffunding, "message"),
        Some(json!("Support us"))
    );

    let pubsub = &tables["pubsub"][0];
    assert_eq!(
        get_value(pubsub, "hub_url"),
        Some(json!("https://hub.example.com"))
    );
    assert_eq!(
        get_value(pubsub, "self_url"),
        Some(json!("https://example.com/feed.xml"))
    );

    let expected_pub_ts =
        utils::parse_pub_date_to_unix("Tue, 02 Jan 2024 03:04:05 +0000").unwrap();
    // let expected_item_id =
    //     utils::generate_item_id("guid-party", "https://cdn.example.com/episode.mp3", feed_id);
    let expected_item_id = "404040_1";

    let nfitem = &tables["nfitems"][0];
    assert_eq!(
        get_value(nfitem, "title"),
        Some(json!("Item Itunes Title"))
    );
    assert_eq!(
        get_value(nfitem, "description"),
        Some(json!("<p>HTML desc</p>"))
    );
    assert_eq!(
        get_value(nfitem, "pub_date"),
        Some(json!(expected_pub_ts))
    );
    assert_eq!(get_value(nfitem, "guid"), Some(json!("guid-party")));
    assert_eq!(get_value(nfitem, "itunes_duration"), Some(json!(65)));
    assert_eq!(get_value(nfitem, "itunes_episode"), Some(json!(2)));
    assert_eq!(get_value(nfitem, "itunes_season"), Some(json!(1)));
    assert_eq!(
        get_value(nfitem, "itunes_episode_type"),
        Some(json!("bonus"))
    );
    assert_eq!(get_value(nfitem, "itunes_explicit"), Some(json!(0)));
    assert_eq!(
        get_value(nfitem, "enclosure_url"),
        Some(json!("https://cdn.example.com/episode.mp3"))
    );
    assert_eq!(get_value(nfitem, "enclosure_length"), Some(json!(321)));
    assert_eq!(
        get_value(nfitem, "enclosure_type"),
        Some(json!("audio/mpeg"))
    );

    let transcripts = &tables["nfitem_transcripts"][0];
    assert_eq!(
        get_value(transcripts, "itemid"),
        Some(json!(expected_item_id))
    );
    assert_eq!(
        get_value(transcripts, "url"),
        Some(json!("https://example.com/ep.vtt"))
    );
    assert_eq!(get_value(transcripts, "type"), Some(json!(3)));

    let chapters = &tables["nfitem_chapters"][0];
    assert_eq!(
        get_value(chapters, "itemid"),
        Some(json!(expected_item_id))
    );
    assert_eq!(
        get_value(chapters, "url"),
        Some(json!("https://example.com/chapters.json"))
    );
    assert_eq!(get_value(chapters, "type"), Some(json!(0)));

    let soundbite = &tables["nfitem_soundbites"][0];
    assert_eq!(
        get_value(soundbite, "itemid"),
        Some(json!(expected_item_id))
    );
    assert_eq!(get_value(soundbite, "title"), Some(json!("Clip")));
    assert_eq!(get_value(soundbite, "start_time"), Some(json!(10.0)));
    assert_eq!(get_value(soundbite, "duration"), Some(json!(15.0)));

    let person = &tables["nfitem_persons"][0];
    assert_eq!(
        get_value(person, "itemid"),
        Some(json!(expected_item_id))
    );
    assert_eq!(get_value(person, "name"), Some(json!("Host Name")));
    assert_eq!(get_value(person, "role"), Some(json!("host")));
    assert_eq!(get_value(person, "grp"), Some(json!("cast")));
    assert_eq!(
        get_value(person, "img"),
        Some(json!("https://example.com/host.jpg"))
    );
    assert_eq!(
        get_value(person, "href"),
        Some(json!("https://example.com/host"))
    );

    let item_value = &tables["nfitem_value"][0];
    assert_eq!(
        get_value(item_value, "itemid"),
        Some(json!(expected_item_id))
    );
    assert_eq!(get_value(item_value, "type"), Some(json!(1)));
    let vb_val = get_value(item_value, "value_block").unwrap();
    let vb: JsonValue = serde_json::from_str(vb_val.as_str().unwrap()).unwrap();
    assert_eq!(vb["model"]["type"], "HBD");
    assert_eq!(vb["destinations"][0]["name"], "Carol");

    let channel_value = &tables["nfvalue"][0];
    assert_eq!(get_value(channel_value, "type"), Some(json!(2)));
    let cv_val = get_value(channel_value, "value_block").unwrap();
    let cv: JsonValue = serde_json::from_str(cv_val.as_str().unwrap()).unwrap();
    assert_eq!(cv["model"]["type"], "bitcoin");
    assert_eq!(cv["destinations"].as_array().unwrap().len(), 2);
    assert_eq!(cv["destinations"][1]["fee"], true);
}

// Atom feeds should mirror Partytime behavior (alternate links, enclosures, pubsub)
#[test]
fn test_atom_feed_support() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/atom.xml
1700000001
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Atom Cast</title>
  <subtitle>Atom Description</subtitle>
  <link rel="alternate" href="https://example.com/atom"/>
  <link rel="hub" href="https://pubsubhubbub.appspot.com/"/>
  <link rel="self" href="https://example.com/atom.xml"/>
  <logo>https://example.com/logo.png</logo>
  <entry>
<id>tag:example.com,2024:1</id>
<title>Atom Episode</title>
<updated>2024-01-01T00:00:00Z</updated>
<summary>Atom entry summary.</summary>
<link rel="alternate" href="https://example.com/atom/1"/>
<link rel="enclosure" href="https://example.com/audio.mp3" length="1234" type="audio/mpeg"/>
  </entry>
</feed>"#;

    let feed_id = 55001_i64;
    process_feed_sync(Cursor::new(feed), "atom.xml", Some(feed_id));

    let nf = single_output_record(&out_dir, "newsfeeds", feed_id);
    let nfitems = output_records(&out_dir, "nfitems", feed_id);
    let pubsub = single_output_record(&out_dir, "pubsub", feed_id);

    assert_eq!(get_value_from_record(&nf, "link"), Some(json!("https://example.com/atom")));
    assert_eq!(get_value_from_record(&nf, "description"), Some(json!("Atom Description")));
    assert_eq!(get_value_from_record(&nf, "image"), Some(json!("https://example.com/logo.png")));

    assert_eq!(nfitems.len(), 1);
    let item = &nfitems[0];

    assert_eq!(get_value_from_record(item, "link"), Some(json!("https://example.com/atom/1")));
    assert_eq!(get_value_from_record(item, "pub_date"), Some(json!(1704067200)));
    assert_eq!(get_value_from_record(item, "enclosure_url"), Some(json!("https://example.com/audio.mp3")));
    assert_eq!(get_value_from_record(item, "enclosure_length"), Some(json!(1234)));
    assert_eq!(get_value_from_record(item, "enclosure_type"), Some(json!("audio/mpeg")));
    assert_eq!(get_value_from_record(item, "description"), Some(json!("Atom entry summary.")));

    assert_eq!(get_value_from_record(&pubsub, "hub_url"), Some(json!("https://pubsubhubbub.appspot.com/")));
    assert_eq!(get_value_from_record(&pubsub, "self_url"), Some(json!("https://example.com/atom.xml")));
}

#[test]
fn test_preserve_spaces_in_itunes_title() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"
 xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Title with trailing spaces</title>
<item>
<title><![CDATA[Ep ]]></title>
<itunes:title><![CDATA[Ep ]]></itunes:title>
<guid>g-content</guid>
<enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
</item>
<item>
<title><![CDATA[Ep2 ]]></title>
<guid>g-content</guid>
<enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
</item>
</channel>
</rss>"#;

    let feed_id = 33006_i64;
    process_feed_sync(Cursor::new(feed), "test.xml", Some(feed_id));

    let mut nfitems = output_records(&out_dir, "nfitems", feed_id);
    assert_eq!(nfitems.len(), 2);
    let item1 = nfitems.remove(0);
    let item2 = nfitems.remove(0);

    assert_eq!(
        get_value_from_record(&item2, "title"),
        Some(JsonValue::from("Ep2"))
    );

    assert_eq!(
        get_value_from_record(&item1, "title"),
        Some(JsonValue::from("Ep "))
    );
}

#[test]
fn test_description_precedence() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"
     xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"
     xmlns:content="http://purl.org/rss/1.0/modules/content/">

  <channel>
    <title>Example Feed</title>

    <!-- Case 1: iTunes summary (common in podcast feeds) -->
    <item>
      <title>Episode with iTunes Summary</title>
      <itunes:summary>This is the iTunes summary description for the podcast episode.</itunes:summary>
      <guid>case-1</guid>
      <enclosure url="https://example.com/case1.mp3" length="1" type="audio/mpeg"/>
    </item>

    <!-- Case 2: content:encoded within description (WordPress/blog feeds) -->
    <item>
      <title>Blog Post with Content Encoded</title>
      <description>
        <content:encoded><![CDATA[
          <p>This is the full HTML content of the blog post with formatting.</p>
          <p>It can contain multiple paragraphs and rich content.</p>
        ]]></content:encoded>
      </description>
      <enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
   </item>

    <!-- Case 3: Simple description field -->
    <item>
      <title>Simple Description</title>
      <description>This is a plain text description of the item.</description>
      <enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
    </item>

    <!-- Case 4: content field as array (parsed from Atom feeds) -->
    <item>
      <title>Atom-style Content</title>
      <content type="html">
        <![CDATA[<p>First content element</p>]]>
      </content>
      <content type="text">
        Second content element (would be ignored)
      </content>
      <enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
    </item>

    <!-- Case 5: content with #text property (from XML parsing) -->
    <item>
      <title>Content with Text Node</title>
      <content type="html">
        This text becomes the #text property when parsed
      </content>
      <enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
    </item>

    <!-- Case 6: Multiple sources - iTunes takes priority -->
    <item>
      <title>Multiple Description Sources</title>
      <itunes:summary>iTunes summary (wins)</itunes:summary>
      <description>Regular description (ignored)</description>
      <content:encoded>Content encoded (ignored)</content:encoded>
      <enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
    </item>

    <!-- Case 7: No description at all -->
    <item>
      <title>No Description</title>
      <!-- Will result in empty string -->
      <enclosure url="https://example.com/ep.mp3" length="1" type="audio/mpeg"/>
    </item>

  </channel>
</rss>"#;

    process_feed_sync(Cursor::new(feed), "test.xml", Some(33007));

    let nfitems_files = output_records(&out_dir, "nfitems", 33007);
    assert_eq!(nfitems_files.len(), 7);

    assert_eq!(
        get_value_from_record(&nfitems_files[0], "description"),
        Some(JsonValue::from(""))
    );
    assert_eq!(get_value_from_record(&nfitems_files[1], "description"), Some(JsonValue::from(
        "<p>This is the full HTML content of the blog post with formatting.</p>\n          <p>It can contain multiple paragraphs and rich content.</p>"
    )));

    assert_eq!(get_value_from_record(&nfitems_files[2], "description"), Some(JsonValue::from("This is a plain text description of the item.")));
    assert_eq!(get_value_from_record(&nfitems_files[3], "description"), Some(JsonValue::from("<p>First content element</p>")));
    assert_eq!(get_value_from_record(&nfitems_files[4], "description"), Some(JsonValue::from("This text becomes the #text property when parsed")));
    assert_eq!(
        get_value_from_record(&nfitems_files[5], "description"),
        Some(JsonValue::from("Content encoded (ignored)"))
    );
    assert_eq!(get_value_from_record(&nfitems_files[6], "description"), Some(JsonValue::from("")));
}

#[test]
fn test_alternate_enclosures() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"
    xmlns:podcast="https://podcastindex.org/namespace/1.0"
    xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"
    xmlns:content="http://purl.org/rss/1.0/modules/content/">

  <channel>
    <title>Example Feed</title>
    <item>
      <itunes:episodeType>bonus</itunes:episodeType>
      <enclosure url="https://feeds.fountain.fm/40huHEEF6JMPGYctMuUI/items/WxMQ7HpjU1XJpgUbo1Fm/files/AUDIO---DEFAULT---a757e3df-28f7-4b38-a704-e0e7780c70f9.mp3" length="2397614" type="audio/mpeg"/>
      <itunes:duration>83</itunes:duration>
      <podcast:chapters url="https://feeds.fountain.fm/40huHEEF6JMPGYctMuUI/items/WxMQ7HpjU1XJpgUbo1Fm/files/AUDIO---CHAPTERS---DEFAULT---PODCAST.json" type="application/json+chapters"/>
      <podcast:transcript url="https://feeds.fountain.fm/40huHEEF6JMPGYctMuUI/items/WxMQ7HpjU1XJpgUbo1Fm/files/AUDIO---TRANSCRIPT---DEFAULT---SRT.srt" type="application/x-subrip" rel="captions"/>
      <podcast:alternateEnclosure type="audio/mpeg" length="78858122" title="Bonus Episode" paywall="L402" auth="NOSTR">
        <itunes:duration>3269</itunes:duration>
        <podcast:chapters url="https://feeds.fountain.fm/40huHEEF6JMPGYctMuUI/items/WxMQ7HpjU1XJpgUbo1Fm/files/AUDIO---CHAPTERS---PAID---PODCAST.json" type="application/json+chapters"/>
        <podcast:transcript url="https://feeds.fountain.fm/40huHEEF6JMPGYctMuUI/items/WxMQ7HpjU1XJpgUbo1Fm/files/AUDIO---TRANSCRIPT---PAID---SRT.srt" type="application/x-subrip" rel="captions"/>
        <podcast:source uri="https://feeds.fountain.fm/40huHEEF6JMPGYctMuUI/items/WxMQ7HpjU1XJpgUbo1Fm/files/AUDIO---PAID---91408357-3379-407d-a8b3-85b3cc2d3349.mp3"/>
      </podcast:alternateEnclosure>
      <guid isPermaLink="false">0bf8aeaf-9f2b-4008-812d-e9389e4639f7</guid>
      <pubDate>Thu, 24 Jul 2025 19:35:57 GMT</pubDate>
      <title>Bonus 01: Living in the Shadow of Bitcoin</title>
      <description>&lt;p&gt;A viral anti-Bitcoin video spreads fear through emotional storytelling and slick sound design. But what's the real story behind the drama, and why does it matter?&lt;/p&gt;</description>
      <itunes:explicit>false</itunes:explicit>
      <itunes:image href="https://feeds.fountain.fm/40huHEEF6JMPGYctMuUI/items/WxMQ7HpjU1XJpgUbo1Fm/files/CHAPTER_ART---DEFAULT---e32b13c4-9bdf-4102-9ff0-4c4fc72462a9.jpg"/>
    </item>
  </channel>
</rss>"#;

    process_feed_sync(Cursor::new(feed), "test.xml", Some(33008));

    let nfitems_files = output_records(&out_dir, "nfitems", 33008);
    assert_eq!(nfitems_files.len(), 1);

    assert_eq!(get_value_from_record(&nfitems_files[0], "itunes_duration"), Some(JsonValue::from(83)));
    // assert_eq!(get_value_from_record(&nfitems_files[1], "duration"), Some(JsonValue::from(3269)));
}

#[test]
fn test_ignore_duplicate_channel_tags() {
    let out_dir = ensure_output_dir();

    let feed = r#"1700000000
[[NO_ETAG]]
https://example.com/feed.xml
1700000001
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"
 xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Channel Title</title>
<generator>Channel Generator</generator>
<generator>Another Channel Generator</generator>
<link>http://example.com/link-channel</link>
<link>http://example.com/another-link-channel</link>
<description>Channel Description</description>
<description>Another Channel Description</description>
<itunes:author>Itunes Channel Author</itunes:author>
<itunes:author>Another Itunes Channel Author</itunes:author>
<itunes:new-feed-url>http://example.com/new-feed-url</itunes:new-feed-url>
<itunes:new-feed-url>http://example.com/another-new-feed-url</itunes:new-feed-url>
</channel>
</rss>"#;

    process_feed_sync(Cursor::new(feed), "test.xml", Some(33009));

    let nf = single_output_record(&out_dir, "newsfeeds", 33009);
    println!("{:?}", nf);
    assert_eq!(get_value_from_record(&nf, "generator"), Some(JsonValue::from("Channel Generator")));
    assert_eq!(get_value_from_record(&nf, "link"), Some(JsonValue::from("http://example.com/link-channel")));
    assert_eq!(get_value_from_record(&nf, "description"), Some(JsonValue::from("Channel Description")));
    assert_eq!(get_value_from_record(&nf, "itunes_author"), Some(JsonValue::from("Itunes Channel Author")));
    assert_eq!(get_value_from_record(&nf, "itunes_new_feed_url"), Some(JsonValue::from("http://example.com/new-feed-url")));
}