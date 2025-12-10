use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::io::Cursor;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::path::{PathBuf};
use std::time::{SystemTime, UNIX_EPOCH, Instant};
use serde::{Serialize, Deserialize};
use serde_json::Value as JsonValue;
use xml::reader::{EventReader, XmlEvent};
use xml::name::OwnedName;

mod parser_state;
mod tags;
mod outputs;
mod utils;
use parser_state::ParserState;

// Global counter initialized to zero at program start
pub(crate) static GLOBAL_COUNTER: AtomicUsize = AtomicUsize::new(0);
// Per-run output subfolder based on startup UNIX timestamp
pub(crate) static OUTPUT_SUBDIR: OnceLock<PathBuf> = OnceLock::new();

#[derive(Serialize, Deserialize, Debug)]
pub struct SqlInsert {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<JsonValue>,
    pub feed_id: Option<i64>,
}

#[tokio::main]
async fn main() {
    // Track total runtime for the entire program
    let program_start = Instant::now();
    // Establish a stable per-run timestamped subfolder under outputs
    let startup_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let subfolder = PathBuf::from("outputs").join(startup_unix.to_string());
    if let Err(e) = fs::create_dir_all(&subfolder) {
        eprintln!("Failed to create outputs subfolder '{}': {}", subfolder.display(), e);
    }
    let _ = OUTPUT_SUBDIR.set(subfolder);

    // Find all XML input files in the inputs directory
    let feeds_dir = "inputs";
    let entries = match fs::read_dir(feeds_dir) {
        Ok(it) => it,
        Err(e) => {
            eprintln!("Unable to read directory '{}': {}", feeds_dir, e);
            return;
        }
    };

    //Process each XML input file in parallel (asynchronously)
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                eprintln!("Error reading a directory entry: {}", e);
                continue;
            }
        };

        let path = entry.path();
        let file_name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("<unknown>")
            .to_string();

        // Only process regular files with .xml or .txt extension
        let is_file = entry
            .file_type()
            .map(|t| t.is_file())
            .unwrap_or(false);
        let ext_ok = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| {
                let e = e.to_ascii_lowercase();
                e == "xml" || e == "txt"
            })
            .unwrap_or(false);

        if !is_file || !ext_ok {
            continue;
        }

        // Try to parse feed_id from file name pattern: [feed id]_[http response code].txt
        let feed_id: Option<i64> = {
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("");
            let mut parts = stem.splitn(2, '_');
            let id_part = parts.next().unwrap_or("");
            match id_part.parse::<i64>() {
                Ok(v) => Some(v),
                Err(_) => None,
            }
        };

        match File::open(&path) {
            Ok(file) => {
                let reader = BufReader::new(file);
                // Measure processing time per file
                let start = Instant::now();
                // Run feed processing asynchronously
                process_feed(reader, file_name.clone(), feed_id).await;
                println!("Processed {} in {:?}", file_name, start.elapsed());
            }
            Err(e) => {
                eprintln!("Unable to open file '{}': {}", path.display(), e);
                continue;
            }
        }
    }

    // Print total runtime just before exiting
    println!("Total runtime: {:?}", program_start.elapsed());
}

// Synchronous parser implementation (unchanged logic)
fn process_feed_sync<R: Read>(reader: R, _source_name: &str, feed_id: Option<i64>) {
    // Wrap in a BufReader so we can read header lines and then pass the same reader to the XML parser
    let mut buf_reader = BufReader::new(reader);

    // New input format header (first 4 lines before the XML):
    // 1) unix timestamp of Last-Modified
    // 2) e-tag header (or [[NO_ETAG]])
    // 3) XML feed URL
    // 4) unix timestamp of when the XML was downloaded
    // 5..end) the XML document

    fn read_line_trim<R: Read>(r: &mut BufReader<R>) -> Option<String> {
        let mut line = String::new();
        match r.read_line(&mut line) {
            Ok(0) => None, // EOF
            Ok(_) => Some(line.trim_end_matches(['\r', '\n']).to_string()),
            Err(_) => None,
        }
    }

    let last_modified_str = read_line_trim(&mut buf_reader);
    let etag_str = read_line_trim(&mut buf_reader);
    let feed_url_str = read_line_trim(&mut buf_reader);
    let downloaded_str = read_line_trim(&mut buf_reader);

    // Parse optional metadata (currently not used in SQL output; reserved for future use)
    let _last_modified_unix: Option<i64> = last_modified_str
        .as_deref()
        .and_then(|s| s.parse::<i64>().ok());
    let _etag_opt: Option<String> = etag_str.as_deref().and_then(|s| {
        if s == "[[NO_ETAG]]" || s.is_empty() {
            None
        } else {
            Some(s.to_string())
        }
    });
    let _feed_url_opt: Option<String> = feed_url_str.filter(|s| !s.is_empty());
    let _downloaded_unix: Option<i64> = downloaded_str
        .as_deref()
        .and_then(|s| s.parse::<i64>().ok());

    // After headers, read the remaining payload to determine if XML content exists
    let mut xml_bytes: Vec<u8> = Vec::new();
    if let Err(e) = buf_reader.read_to_end(&mut xml_bytes) {
        eprintln!("Failed to read XML payload after headers: {}", e);
        return;
    }

    // Check if payload is empty or whitespace-only
    let has_non_whitespace = xml_bytes
        .iter()
        .any(|b| !matches!(b, b' ' | b'\t' | b'\r' | b'\n'));

    if !has_non_whitespace {
        // XML payload is empty or whitespace-only: write a single channel-level record with blank fields
        let record = SqlInsert {
            table: "newsfeeds".to_string(),
            columns: vec![
                "feed_id".to_string(),
                "title".to_string(),
                "link".to_string(),
                "description".to_string(),
            ],
            values: vec![
                match feed_id { Some(v) => JsonValue::from(v), None => JsonValue::Null },
                JsonValue::from(String::new()),
                JsonValue::from(String::new()),
                JsonValue::from(String::new()),
            ],
            feed_id,
        };

        // Use per-run outputs subfolder established at startup
        let out_dir: PathBuf = OUTPUT_SUBDIR
            .get()
            .cloned()
            .unwrap_or_else(|| PathBuf::from("outputs"));
        if let Err(e) = fs::create_dir_all(&out_dir) {
            eprintln!("Failed to create outputs directory '{}': {}", out_dir.display(), e);
        }

        // Compute counter (1-based) and build filename
        let counter_val = GLOBAL_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
        let fid_for_name = feed_id
            .map(|v| v.to_string())
            .unwrap_or_else(|| "NULL".to_string());
        let file_name = format!("{}_{}_{}.json", counter_val, "newsfeeds", fid_for_name);
        let file_path = out_dir.join(file_name);

        match serde_json::to_string(&record) {
            Ok(serialized) => {
                if let Err(e) = fs::write(&file_path, serialized) {
                    eprintln!("Failed to write {}: {}", file_path.display(), e);
                } else {
                    println!(
                        "Empty XML payload detected for '{}'; wrote blank newsfeeds record to {}",
                        _source_name,
                        file_path.display()
                    );
                }
            }
            Err(e) => {
                eprintln!("Failed to serialize blank newsfeeds record: {}", e);
            }
        }

        // Nothing else to do for this file
        return;
    }

    // Create an XML parser from the buffered payload
    let cursor = Cursor::new(xml_bytes);
    let parser = EventReader::new(cursor);

    // Parser state holds all flags and accumulators used by handlers
    let mut state = ParserState::default();

    fn get_prefixed_name(name: &OwnedName) -> String {
        let prefix = name.prefix.clone();
        let local_name = name.local_name.clone();

        if (matches!(prefix.as_deref(), Some("itunes"))
            || matches!(name.namespace.as_deref(), Some("http://www.itunes.com/dtds/podcast-1.0.dtd"))
        ) {
            format!("itunes:{}", local_name)
        } else if (matches!(prefix.as_deref(), Some("podcast"))
            || matches!(name.namespace.as_deref(), Some("https://podcastindex.org/namespace/1.0"))
            || matches!(name.namespace.as_deref(), Some("http://podcastindex.org/namespace/1.0"))
        ) {
            format!("podcast:{}", local_name)
        } else if prefix.is_some() {
            format!("{}:{}", prefix.unwrap(), local_name)
        } else {
            local_name
        }
    }

    // Parse the XML document
    for event in parser {
        match event {
            //A tag is opened.
            Ok(XmlEvent::StartElement { name, attributes, .. }) => {
                state.current_element = get_prefixed_name(&name);
                let current = state.current_element.clone();
                #[cfg(test)]
                {
                    println!("start: {} attrs {}", current, attributes.len());
                    for attr in &attributes {
                        println!(" attr {}={}", attr.name.local_name, attr.value);
                    }
                }
                tags::dispatch_start(&current, &attributes, &mut state);
            }

            //Text is found.
            Ok(XmlEvent::Characters(data)) => {
                let current = state.current_element.clone();
                tags::dispatch_text(&current, &data, &mut state);
            }

            // CDATA is also textual content — treat it the same as Characters
            Ok(XmlEvent::CData(data)) => {
                let current = state.current_element.clone();
                tags::dispatch_text(&current, &data, &mut state);
            }

            //A tag is closed.
            Ok(XmlEvent::EndElement { name }) => {
                state.current_element = get_prefixed_name(&name);
                let current = state.current_element.clone();
                tags::dispatch_end(&current, feed_id, &mut state);
            }

            //An error occurred.
            Err(e) => {
                eprintln!("Error parsing XML: {}", e);
                break;
            }
            _ => {}
        }
    }
}

// Public async wrapper that executes the synchronous parser on a blocking thread
async fn process_feed<R>(reader: R, source_name: String, feed_id: Option<i64>)
where
    R: Read + Send + 'static,
{
    // Ignore the JoinError here but log if it occurs
    let source_for_task = source_name.clone();
    if let Err(e) = tokio::task::spawn_blocking(move || {
        process_feed_sync(reader, &source_for_task, feed_id);
    })
        .await
    {
        eprintln!("Error in async processing for '{}': {}", source_name, e);
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs;
    use std::io::Cursor;
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

        let feed_id = Some(424242_i64);

        // Act: process the synthetic feed synchronously
        process_feed_sync(Cursor::new(input.into_bytes()), "<test>", feed_id);

        // Assert: a newsfeeds JSON file exists with the expected title
        let entries = fs::read_dir(&out_dir)
            .expect("output directory should be readable");
        let mut found_path: Option<PathBuf> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_newsfeeds_") && name.ends_with("424242.json") {
                    found_path = Some(path);
                    break;
                }
            }
        }

        let file_path = found_path.expect("should have written a newsfeeds output file");
        let contents = fs::read_to_string(&file_path)
            .expect("should be able to read newsfeeds file");
        let v: serde_json::Value = serde_json::from_str(&contents)
            .expect("valid JSON in newsfeeds file");

        // Basic shape assertions
        assert_eq!(v["table"], "newsfeeds");
        assert_eq!(v["columns"][1], "title");
        assert_eq!(v["feed_id"], serde_json::json!(424242));

        // Channel title should be the second value (index 1), trimmed
        assert_eq!(v["values"][1], serde_json::json!("My Test Channel"));
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

        let feed_id = Some(777001_i64);

        // Act
        process_feed_sync(Cursor::new(input.into_bytes()), "<test>", feed_id);

        // Assert: find the newsfeeds file for this feed_id
        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut found_path: Option<PathBuf> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_newsfeeds_") && name.ends_with("777001.json") {
                    found_path = Some(path);
                    break;
                }
            }
        }

        let file_path = found_path.expect("should have written a newsfeeds output file");
        let contents = fs::read_to_string(&file_path).expect("read newsfeeds file");
        let v: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        assert_eq!(v["table"], "newsfeeds");
        // title
        assert_eq!(v["values"][1], serde_json::json!("Channel With Links"));
        // link
        assert_eq!(v["values"][2], serde_json::json!("https://example.com/"));
        // description (trimmed)
        assert_eq!(v["values"][3], serde_json::json!("This is a <b>CDATA</b> description."));
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

        let feed_id = Some(777002_i64);

        // Act
        process_feed_sync(Cursor::new(input.into_bytes()), "<test>", feed_id);

        // Assert: find the nfitems file for this feed_id
        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut found_path: Option<PathBuf> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitems_") && name.ends_with("777002.json") {
                    found_path = Some(path);
                    break;
                }
            }
        }

        let file_path = found_path.expect("should have written an nfitems output file");
        let contents = fs::read_to_string(&file_path).expect("read nfitems file");
        let v: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

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
        process_feed_sync(Cursor::new(feed), "test.xml", Some(1337));

        // Assert: find the newsfeeds file for this feed_id
        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut found_path: Option<PathBuf> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_newsfeeds_") && name.ends_with("1337.json") {
                    found_path = Some(path);
                    break;
                }
            }
        }

        let file_path = found_path.expect("should have written a newsfeeds output file");
        let contents = fs::read_to_string(&file_path).expect("read newsfeeds file");
        let v: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        assert_eq!(v["table"], "newsfeeds");
        assert_eq!(v["feed_id"], serde_json::json!(1337));
        assert_eq!(v["values"][1], serde_json::json!("")); // title
        assert_eq!(v["values"][2], serde_json::json!("")); // link
        assert_eq!(v["values"][3], serde_json::json!("")); // description
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
        process_feed_sync(Cursor::new(feed), "test.xml", Some(2001));

        // Assert: find the newsfeeds file for this feed_id
        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut found_path: Option<PathBuf> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_newsfeeds_") && name.ends_with("2001.json") {
                    found_path = Some(path);
                    break;
                }
            }
        }

        let file_path = found_path.expect("should have written a newsfeeds output file");
        let contents = fs::read_to_string(&file_path).expect("read newsfeeds file");
        let nf: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        // Helper to get value by column name
        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = nf["columns"].as_array()?;
            let values = nf["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_value("feed_id"), Some(JsonValue::from(2001)));
        assert_eq!(get_value("title"), Some(JsonValue::from("Complete Channel")));
        assert_eq!(get_value("link"), Some(JsonValue::from("https://example.com")));
        assert_eq!(get_value("description"), Some(JsonValue::from("Channel summary wins")));
        assert_eq!(get_value("language"), Some(JsonValue::from("en-US")));
        assert_eq!(get_value("itunes_author"), Some(JsonValue::from("Author Name")));
        assert_eq!(get_value("itunes_owner_name"), Some(JsonValue::from("Owner Name")));
        assert_eq!(get_value("explicit"), Some(JsonValue::from(1)));
        assert_eq!(get_value("podcast_locked"), Some(JsonValue::from(1)));
        assert_eq!(get_value("image"), Some(JsonValue::from("https://example.com/rss.jpg")));
        assert_eq!(get_value("artwork_url_600"), Some(JsonValue::from("https://example.com/itunes.jpg")));
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(20100));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut found_path: Option<PathBuf> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_newsfeeds_") && name.ends_with("20100.json") {
                    found_path = Some(path);
                    break;
                }
            }
        }

        let file_path = found_path.expect("should have written a newsfeeds output file");
        let contents = fs::read_to_string(&file_path).expect("read newsfeeds file");
        let nf: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = nf["columns"].as_array()?;
            let values = nf["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

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

        assert_eq!(get_value("item_count"), Some(JsonValue::from(2)));
        assert_eq!(get_value("newest_item_pubdate"), Some(JsonValue::from(newest)));
        assert_eq!(get_value("oldest_item_pubdate"), Some(JsonValue::from(oldest)));
        assert_eq!(get_value("chash"), Some(JsonValue::from(expected_chash)));
        assert_eq!(get_value("podcast_chapters"), Some(JsonValue::from(expected_item_hash)));
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
        process_feed_sync(Cursor::new(feed), "test.xml", Some(2002));

        // Assert: find all nfitems files for this feed_id
        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut nfitems_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitems_") && name.ends_with("2002.json") {
                    nfitems_files.push(path);
                }
            }
        }

        assert_eq!(nfitems_files.len(), 1);
        let file_path = &nfitems_files[0];
        let contents = fs::read_to_string(file_path).expect("read nfitems file");
        let item: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = item["columns"].as_array()?;
            let values = item["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_value("feed_id"), Some(JsonValue::from(2002)));
        assert_eq!(get_value("title"), Some(JsonValue::from("Itunes Episode Title")));
        assert_eq!(get_value("link"), Some(JsonValue::from("https://example.com/ep1")));
        assert_eq!(get_value("description"), Some(JsonValue::from("Itunes summary wins")));
        let expected_pub_date = utils::parse_pub_date_to_unix("Mon, 01 Jan 2024 12:00:00 GMT").unwrap();
        assert_eq!(get_value("pub_date"), Some(JsonValue::from(expected_pub_date)));
        assert_eq!(get_value("itunes_image"), Some(JsonValue::from("https://example.com/ep.jpg")));
        assert_eq!(get_value("podcast_funding_url"), Some(JsonValue::from("https://donate.example.com")));
        assert_eq!(get_value("podcast_funding_text"), Some(JsonValue::from("Support!")));
        assert_eq!(get_value("itunes_episode"), Some(JsonValue::from(42)));
        assert_eq!(get_value("itunes_season"), Some(JsonValue::from(3)));
        assert_eq!(get_value("itunes_explicit"), Some(JsonValue::from(1)));
        assert_eq!(get_value("enclosure_length"), Some(JsonValue::from(12345678)));
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2003));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut found_path: Option<PathBuf> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfguids_") && name.ends_with("2003.json") {
                    found_path = Some(path);
                    break;
                }
            }
        }

        let file_path = found_path.expect("should have written a nfguids output file");
        let contents = fs::read_to_string(&file_path).expect("read nfguids file");
        let guid: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = guid["columns"].as_array()?;
            let values = guid["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_value("guid"), Some(JsonValue::from("unique-guid-123")));
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2004));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut found_path: Option<PathBuf> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nffunding_") && name.ends_with("2004.json") {
                    found_path = Some(path);
                    break;
                }
            }
        }

        let file_path = found_path.expect("should have written a nffunding output file");
        let contents = fs::read_to_string(&file_path).expect("read nffunding file");
        let funding: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = funding["columns"].as_array()?;
            let values = funding["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_value("url"), Some(JsonValue::from("https://patreon.com/podcast")));
        assert_eq!(get_value("message"), Some(JsonValue::from("Support us!")));
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2005));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut found_path: Option<PathBuf> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_pubsub_") && name.ends_with("2005.json") {
                    found_path = Some(path);
                    break;
                }
            }
        }

        let file_path = found_path.expect("should have written a pubsub output file");
        let contents = fs::read_to_string(&file_path).expect("read pubsub file");
        let pubsub: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = pubsub["columns"].as_array()?;
            let values = pubsub["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_value("hub_url"), Some(JsonValue::from("https://pubsubhubbub.appspot.com/")));
        assert_eq!(get_value("self_url"), Some(JsonValue::from("https://example.com/feed.xml")));
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2006));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut transcripts_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitem_transcripts_") && name.ends_with("2006.json") {
                    transcripts_files.push(path);
                }
            }
        }

        assert_eq!(transcripts_files.len(), 3);

        // Type detection: JSON=1, SRT=2, VTT=3
        // Verify each type appears once (order may vary)
        let mut types = Vec::new();
        for file_path in &transcripts_files {
            let contents = fs::read_to_string(file_path).expect("read transcript file");
            let transcript: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");
            
            let get_value = |col_name: &str| -> Option<serde_json::Value> {
                let columns = transcript["columns"].as_array()?;
                let values = transcript["values"].as_array()?;
                for (i, col) in columns.iter().enumerate() {
                    if col.as_str()? == col_name {
                        return values.get(i).cloned();
                    }
                }
                None
            };
            
            if let Some(type_val) = get_value("type") {
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2007));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut chapters_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitem_chapters_") && name.ends_with("2007.json") {
                    chapters_files.push(path);
                }
            }
        }

        assert_eq!(chapters_files.len(), 1);
        let file_path = &chapters_files[0];
        let contents = fs::read_to_string(file_path).expect("read chapters file");
        let chapter: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = chapter["columns"].as_array()?;
            let values = chapter["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_value("url"), Some(JsonValue::from("https://example.com/chapters.json")));
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2008));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut soundbites_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitem_soundbites_") && name.ends_with("2008.json") {
                    soundbites_files.push(path);
                }
            }
        }

        soundbites_files.sort();
        assert_eq!(soundbites_files.len(), 2);

        let mut soundbites = Vec::new();
        for file_path in &soundbites_files {
            let contents = fs::read_to_string(file_path).expect("read soundbite file");
            let soundbite: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");
            soundbites.push(soundbite);
        }

        let get_value = |sb: &serde_json::Value, col_name: &str| -> Option<serde_json::Value> {
            let columns = sb["columns"].as_array()?;
            let values = sb["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_value(&soundbites[0], "title"), Some(JsonValue::from("Intro")));
        assert_eq!(get_value(&soundbites[0], "start_time"), Some(JsonValue::from("10")));
        assert_eq!(get_value(&soundbites[1], "title"), Some(JsonValue::from("Main topic")));
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2009));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut persons_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitem_persons_") && name.ends_with("2009.json") {
                    persons_files.push(path);
                }
            }
        }

        persons_files.sort();
        assert_eq!(persons_files.len(), 2);

        let mut persons = Vec::new();
        for file_path in &persons_files {
            let contents = fs::read_to_string(file_path).expect("read person file");
            let person: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");
            persons.push(person);
        }

        let get_value = |p: &serde_json::Value, col_name: &str| -> Option<serde_json::Value> {
            let columns = p["columns"].as_array()?;
            let values = p["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_value(&persons[0], "name"), Some(JsonValue::from("Alice")));
        assert_eq!(get_value(&persons[0], "role"), Some(JsonValue::from("host")));
        assert_eq!(get_value(&persons[1], "name"), Some(JsonValue::from("Bob")));
        assert_eq!(get_value(&persons[1], "role"), Some(JsonValue::from("guest")));
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2010));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut values_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitem_value_") && name.ends_with("2010.json") {
                    values_files.push(path);
                }
            }
        }

        assert_eq!(values_files.len(), 1);
        let file_path = &values_files[0];
        let contents = fs::read_to_string(file_path).expect("read nfitem_value file");
        let value: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = value["columns"].as_array()?;
            let values = value["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        let value_block_val = get_value("value_block").unwrap();
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2011));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut values_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfvalue_") && name.ends_with("2011.json") {
                    values_files.push(path);
                }
            }
        }

        assert_eq!(values_files.len(), 1);
        let file_path = &values_files[0];
        let contents = fs::read_to_string(file_path).expect("read nfvalue file");
        let value: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = value["columns"].as_array()?;
            let values = value["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        let value_block_val = get_value("value_block").unwrap();
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2012));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut found_path: Option<PathBuf> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfvalue_") && name.ends_with("2012.json") {
                    found_path = Some(path);
                    break;
                }
            }
        }

        let file_path = found_path.expect("should have written a nfvalue output file");
        let contents = fs::read_to_string(&file_path).expect("read nfvalue file");
        let value: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = value["columns"].as_array()?;
            let values = value["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        let vb_val = get_value("value_block").unwrap();
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2014));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut newsfeeds_path: Option<PathBuf> = None;
        let mut nfitems_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_newsfeeds_") && name.ends_with("2014.json") {
                    newsfeeds_path = Some(path);
                } else if name.contains("_nfitems_") && name.ends_with("2014.json") {
                    nfitems_files.push(path);
                }
            }
        }

        let nf_file = newsfeeds_path.expect("should have newsfeeds file");
        let nf_contents = fs::read_to_string(&nf_file).expect("read newsfeeds file");
        let nf: serde_json::Value = serde_json::from_str(&nf_contents).expect("valid JSON");

        let get_nf_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = nf["columns"].as_array()?;
            let values = nf["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_nf_value("image"), Some(JsonValue::from("https://example.com/itunes-only.jpg")));

        assert_eq!(nfitems_files.len(), 1);
        let item_contents = fs::read_to_string(&nfitems_files[0]).expect("read nfitems file");
        let item: serde_json::Value = serde_json::from_str(&item_contents).expect("valid JSON");

        let get_item_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = item["columns"].as_array()?;
            let values = item["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_item_value("image"), Some(JsonValue::from("https://example.com/ep-itunes-only.jpg")));
    }

    // Edge case: Episode/season parsing (extract numbers from strings)
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
    <itunes:episode>E10</itunes:episode>
    <itunes:season>S02</itunes:season>
</item>
</channel>
</rss>"#;

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2015));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut nfitems_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitems_") && name.ends_with("2015.json") {
                    nfitems_files.push(path);
                }
            }
        }

        assert_eq!(nfitems_files.len(), 1);
        let item_contents = fs::read_to_string(&nfitems_files[0]).expect("read nfitems file");
        let item: serde_json::Value = serde_json::from_str(&item_contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = item["columns"].as_array()?;
            let values = item["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_value("itunes_episode"), Some(JsonValue::from(10)));
        assert_eq!(get_value("itunes_season"), Some(JsonValue::from(2)));
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
        process_feed_sync(Cursor::new(feed), "test.xml", Some(2016));

        // Assert: find all nfitems files for this feed_id
        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut nfitems_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitems_") && name.ends_with("2016.json") {
                    nfitems_files.push(path);
                }
            }
        }

        assert_eq!(nfitems_files.len(), 3);
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2017));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut nfitems_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitems_") && name.ends_with("2017.json") {
                    nfitems_files.push(path);
                }
            }
        }

        assert_eq!(nfitems_files.len(), 1);
        let file_path = &nfitems_files[0];
        let contents = fs::read_to_string(&file_path).expect("read nfitems file");
        let v: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        assert_eq!(v["table"], "nfitems");
        assert_eq!(v["feed_id"], serde_json::json!(2017));
        assert_eq!(v["values"][5], serde_json::json!("https://example.com/ep-text.jpg")); // itunes_image

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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2018));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut values_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfvalue_") && name.ends_with("2018.json") {
                    values_files.push(path);
                }
            }
        }

        assert_eq!(values_files.len(), 1);
        let file_path = &values_files[0];
        let contents = fs::read_to_string(file_path).expect("read nfvalue file");
        let value: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = value["columns"].as_array()?;
            let values = value["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        let vb_val = get_value("value_block").unwrap();
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2019));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut values_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitem_value_") && name.ends_with("2019.json") {
                    values_files.push(path);
                }
            }
        }

        assert_eq!(values_files.len(), 1);
        let file_path = &values_files[0];
        let contents = fs::read_to_string(file_path).expect("read nfitem_value file");
        let value: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = value["columns"].as_array()?;
            let values = value["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        let vb_val = get_value("value_block").unwrap();
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2020));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut nfitems_count = 0;
        let mut transcripts_count = 0;
        let mut values_count = 0;

        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitems_") && name.ends_with("2020.json") {
                    nfitems_count += 1;
                } else if name.contains("_nfitem_transcripts_") && name.ends_with("2020.json") {
                    transcripts_count += 1;
                } else if name.contains("_nfitem_value_") && name.ends_with("2020.json") {
                    values_count += 1;
                }
            }
        }

        assert_eq!(nfitems_count, 0);
        assert_eq!(transcripts_count, 0);
        assert_eq!(values_count, 0);
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2026));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut nfitems_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitems_") && name.ends_with("2026.json") {
                    nfitems_files.push(path);
                }
            }
        }

        assert_eq!(nfitems_files.len(), 1);
        let file_path = &nfitems_files[0];
        let contents = fs::read_to_string(file_path).expect("read nfitems file");
        let item: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = item["columns"].as_array()?;
            let values = item["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_value("enclosure_url"), Some(JsonValue::from("https://example.com/ep.mp3")));
        assert_eq!(get_value("enclosure_length"), Some(JsonValue::from(555)));
        assert_eq!(get_value("enclosure_type"), Some(JsonValue::from("audio/mpeg")));
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2027));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut newsfeeds_path: Option<PathBuf> = None;
        let mut nfitems_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_newsfeeds_") && name.ends_with("2027.json") {
                    newsfeeds_path = Some(path);
                } else if name.contains("_nfitems_") && name.ends_with("2027.json") {
                    nfitems_files.push(path);
                }
            }
        }

        let nf_file = newsfeeds_path.expect("should have newsfeeds file");
        let nf_contents = fs::read_to_string(&nf_file).expect("read newsfeeds file");
        let nf: serde_json::Value = serde_json::from_str(&nf_contents).expect("valid JSON");

        let get_nf_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = nf["columns"].as_array()?;
            let values = nf["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_nf_value("explicit"), Some(JsonValue::from(1)));

        assert_eq!(nfitems_files.len(), 1);
        let item_contents = fs::read_to_string(&nfitems_files[0]).expect("read nfitems file");
        let item: serde_json::Value = serde_json::from_str(&item_contents).expect("valid JSON");

        let get_item_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = item["columns"].as_array()?;
            let values = item["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_item_value("itunes_explicit"), Some(JsonValue::from(0)));
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

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut soundbites_files = Vec::new();
        let mut persons_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitem_soundbites_") && name.ends_with("2028.json") {
                    soundbites_files.push(path);
                } else if name.contains("_nfitem_persons_") && name.ends_with("2028.json") {
                    persons_files.push(path);
                }
            }
        }

        assert_eq!(soundbites_files.len(), 1);
        let sb_contents = fs::read_to_string(&soundbites_files[0]).expect("read soundbite file");
        let sb: serde_json::Value = serde_json::from_str(&sb_contents).expect("valid JSON");
        let get_sb_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = sb["columns"].as_array()?;
            let values = sb["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };
        let sb_title = get_sb_value("title").unwrap().as_str().unwrap().to_string();
        assert_eq!(sb_title.len(), 500);

        assert_eq!(persons_files.len(), 1);
        let person_contents = fs::read_to_string(&persons_files[0]).expect("read person file");
        let person: serde_json::Value = serde_json::from_str(&person_contents).expect("valid JSON");
        let get_person_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = person["columns"].as_array()?;
            let values = person["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };
        let name = get_person_value("name").unwrap().as_str().unwrap().to_string();
        let role = get_person_value("role").unwrap().as_str().unwrap().to_string();
        let group = get_person_value("grp").unwrap().as_str().unwrap().to_string();
        let img = get_person_value("img").unwrap().as_str().unwrap().to_string();
        let href = get_person_value("href").unwrap().as_str().unwrap().to_string();

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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2021));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut found_path: Option<PathBuf> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfcategories_") && name.ends_with("2021.json") {
                    found_path = Some(path);
                    break;
                }
            }
        }

        let file_path = found_path.expect("should have written a nfcategories output file");
        let contents = fs::read_to_string(&file_path).expect("read nfcategories file");
        let catmap: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = catmap["columns"].as_array()?;
            let values = catmap["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_value("catid1"), Some(JsonValue::from(102)));
        assert_eq!(get_value("catid2"), Some(JsonValue::from(48)));
        assert_eq!(get_value("catid3"), Some(JsonValue::from(52)));
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2022));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut nfitems_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitems_") && name.ends_with("2022.json") {
                    nfitems_files.push(path);
                }
            }
        }

        assert_eq!(nfitems_files.len(), 1);
        let file_path = &nfitems_files[0];
        let contents = fs::read_to_string(file_path).expect("read nfitems file");
        let item: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = item["columns"].as_array()?;
            let values = item["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_value("enclosure_url"), Some(JsonValue::from("https://example.com/first.mp3")));
        assert_eq!(get_value("enclosure_type"), Some(JsonValue::from("audio/mpeg")));
        assert_eq!(get_value("enclosure_length"), Some(JsonValue::from(123)));
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

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut nfitems_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitems_") && name.ends_with("2023.json") {
                    nfitems_files.push(path);
                }
            }
        }

        assert_eq!(nfitems_files.len(), 1);
        let file_path = &nfitems_files[0];
        let contents = fs::read_to_string(file_path).expect("read nfitems file");
        let item: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = item["columns"].as_array()?;
            let values = item["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_value("itunes_duration"), Some(JsonValue::from(62)));
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2024));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut channel_values_files = Vec::new();
        let mut item_values_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfvalue_") && name.ends_with("2024.json") {
                    channel_values_files.push(path);
                } else if name.contains("_nfitem_value_") && name.ends_with("2024.json") {
                    item_values_files.push(path);
                }
            }
        }

        assert_eq!(channel_values_files.len(), 1);
        let channel_value_contents = fs::read_to_string(&channel_values_files[0]).expect("read nfvalue file");
        let channel_value: serde_json::Value = serde_json::from_str(&channel_value_contents).expect("valid JSON");
        let get_channel_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = channel_value["columns"].as_array()?;
            let values = channel_value["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };
        assert_eq!(get_channel_value("type"), Some(JsonValue::from(2)));

        assert_eq!(item_values_files.len(), 1);
        let item_value_contents = fs::read_to_string(&item_values_files[0]).expect("read nfitem_value file");
        let item_value: serde_json::Value = serde_json::from_str(&item_value_contents).expect("valid JSON");
        let get_item_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = item_value["columns"].as_array()?;
            let values = item_value["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };
        assert_eq!(get_item_value("type"), Some(JsonValue::from(1)));
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(2025));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut found_path: Option<PathBuf> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_newsfeeds_") && name.ends_with("2025.json") {
                    found_path = Some(path);
                    break;
                }
            }
        }

        let file_path = found_path.expect("should have written a newsfeeds output file");
        let contents = fs::read_to_string(&file_path).expect("read newsfeeds file");
        let nf: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = nf["columns"].as_array()?;
            let values = nf["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_value("podcast_locked"), Some(JsonValue::from(1)));
        assert_eq!(get_value("podcast_owner"), Some(JsonValue::from("owner@example.com")));
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(30303));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut newsfeeds_path: Option<PathBuf> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_newsfeeds_") && name.ends_with("30303.json") {
                    newsfeeds_path = Some(path);
                    break;
                }
            }
        }

        let nf_path = newsfeeds_path.expect("should have newsfeeds file");
        let nf_contents = fs::read_to_string(&nf_path).expect("read newsfeeds file");
        let nf: serde_json::Value = serde_json::from_str(&nf_contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = nf["columns"].as_array()?;
            let values = nf["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_value("item_count"), Some(JsonValue::from(2)));
        assert_eq!(
            get_value("newest_item_pubdate"),
            Some(JsonValue::from(now.timestamp()))
        );
        assert_eq!(
            get_value("oldest_item_pubdate"),
            Some(JsonValue::from(recent.timestamp()))
        );
        // Two items within 5 days -> frequency bucket 1
        assert_eq!(get_value("update_frequency"), Some(JsonValue::from(1)));

        // Verify nfitems pub_date fields are numeric epoch seconds
        let mut nfitems_files = Vec::new();
        for entry in fs::read_dir(&out_dir).expect("output directory should be readable").flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitems_") && name.ends_with("30303.json") {
                    nfitems_files.push(path);
                }
            }
        }
        assert_eq!(nfitems_files.len(), 2);
        for file_path in nfitems_files {
            let contents = fs::read_to_string(&file_path).expect("read nfitems file");
            let item: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");
            let columns = item["columns"].as_array().expect("columns array");
            let values = item["values"].as_array().expect("values array");
            let mut pub_date_val = None;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str() == Some("pub_date") {
                    pub_date_val = values.get(i);
                    break;
                }
            }
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(30304));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut nfitems_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitems_") && name.ends_with("30304.json") {
                    nfitems_files.push(path);
                }
            }
        }

        assert_eq!(nfitems_files.len(), 1);
        let contents = fs::read_to_string(&nfitems_files[0]).expect("read nfitems file");
        let item: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = item["columns"].as_array()?;
            let values = item["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(
            get_value("guid"),
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(33001));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut found_path: Option<PathBuf> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_newsfeeds_") && name.ends_with("33001.json") {
                    found_path = Some(path);
                    break;
                }
            }
        }

        let nf_path = found_path.expect("should have written a newsfeeds output file");
        let contents = fs::read_to_string(&nf_path).expect("read newsfeeds file");
        let nf: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = nf["columns"].as_array()?;
            let values = nf["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_value("generator"), Some(JsonValue::from("GenX/1.2")));
        assert_eq!(get_value("itunes_type"), Some(JsonValue::from("trailer")));
        assert_eq!(
            get_value("itunes_new_feed_url"),
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(33002));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut found_path: Option<PathBuf> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_newsfeeds_") && name.ends_with("33002.json") {
                    found_path = Some(path);
                    break;
                }
            }
        }

        let nf_path = found_path.expect("should have written a newsfeeds output file");
        let contents = fs::read_to_string(&nf_path).expect("read newsfeeds file");
        let nf: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = nf["columns"].as_array()?;
            let values = nf["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(get_value("podcast_locked"), Some(JsonValue::from(1)));
        assert_eq!(
            get_value("podcast_owner"),
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(33003));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut nfitems_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitems_") && name.ends_with("33003.json") {
                    nfitems_files.push(path);
                }
            }
        }

        assert_eq!(nfitems_files.len(), 1);
        let contents = fs::read_to_string(&nfitems_files[0]).expect("read nfitems file");
        let item: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = item["columns"].as_array()?;
            let values = item["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(
            get_value("itunes_episode_type"),
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

        process_feed_sync(Cursor::new(feed), "test.xml", Some(33004));

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut values_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitem_value_") && name.ends_with("33004.json") {
                    values_files.push(path);
                }
            }
        }

        assert_eq!(values_files.len(), 1);
        let contents = fs::read_to_string(&values_files[0]).expect("read nfitem_value file");
        let value: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = value["columns"].as_array()?;
            let values = value["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        // Lightning maps to type code 0 and should replace the earlier bitcoin block
        assert_eq!(get_value("type"), Some(JsonValue::from(0)));
        let vb_val = get_value("value_block").unwrap();
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

        let entries = fs::read_dir(&out_dir).expect("output directory should be readable");
        let mut nfitems_files = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.contains("_nfitems_") && name.ends_with("33005.json") {
                    nfitems_files.push(path);
                }
            }
        }

        assert_eq!(nfitems_files.len(), 1);
        let contents = fs::read_to_string(&nfitems_files[0]).expect("read nfitems file");
        let item: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        let get_value = |col_name: &str| -> Option<serde_json::Value> {
            let columns = item["columns"].as_array()?;
            let values = item["values"].as_array()?;
            for (i, col) in columns.iter().enumerate() {
                if col.as_str()? == col_name {
                    return values.get(i).cloned();
                }
            }
            None
        };

        assert_eq!(
            get_value("description"),
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
        let expected_item_id =
            utils::generate_item_id("guid-party", "https://cdn.example.com/episode.mp3", feed_id);

        let nfitem = &tables["nfitems"][0];
        assert_eq!(
            get_value(nfitem, "title"),
            Some(json!("Item Itunes Title"))
        );
        assert_eq!(
            get_value(nfitem, "description"),
            Some(json!("Itunes item summary"))
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
        assert_eq!(
            get_value(nfitem, "podcast_funding_url"),
            Some(json!("https://example.com/ep-support"))
        );
        assert_eq!(
            get_value(nfitem, "podcast_funding_text"),
            Some(json!("Episode support"))
        );

        let transcripts = &tables["nfitem_transcripts"][0];
        assert_eq!(
            get_value(transcripts, "itemid"),
            Some(json!(expected_item_id.clone()))
        );
        assert_eq!(
            get_value(transcripts, "url"),
            Some(json!("https://example.com/ep.vtt"))
        );
        assert_eq!(get_value(transcripts, "type"), Some(json!(3)));

        let chapters = &tables["nfitem_chapters"][0];
        assert_eq!(
            get_value(chapters, "itemid"),
            Some(json!(expected_item_id.clone()))
        );
        assert_eq!(
            get_value(chapters, "url"),
            Some(json!("https://example.com/chapters.json"))
        );
        assert_eq!(get_value(chapters, "type"), Some(json!(0)));

        let soundbite = &tables["nfitem_soundbites"][0];
        assert_eq!(
            get_value(soundbite, "itemid"),
            Some(json!(expected_item_id.clone()))
        );
        assert_eq!(get_value(soundbite, "title"), Some(json!("Clip")));
        assert_eq!(get_value(soundbite, "start_time"), Some(json!("10")));
        assert_eq!(get_value(soundbite, "duration"), Some(json!("15")));

        let person = &tables["nfitem_persons"][0];
        assert_eq!(
            get_value(person, "itemid"),
            Some(json!(expected_item_id.clone()))
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
            Some(json!(expected_item_id.clone()))
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
}