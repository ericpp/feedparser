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
    use std::fs;
    use std::io::Cursor;
    use std::time::{SystemTime, UNIX_EPOCH};

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
<rss>
<channel>
<title>Complete Channel</title>
<link>https://example.com</link>
<description>Full description</description>
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
        let v: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        assert_eq!(v["table"], "newsfeeds");
        assert_eq!(v["feed_id"], serde_json::json!(2001));
        assert_eq!(v["values"][1], serde_json::json!("Complete Channel")); // title
        assert_eq!(v["values"][2], serde_json::json!("https://example.com")); // link
        assert_eq!(v["values"][3], serde_json::json!("Full description")); // description
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
     xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Test Feed</title>
<item>
    <title>Complete Episode</title>
    <itunes:title>Itunes Episode Title</itunes:title>
    <link>https://example.com/ep1</link>
    <description>Episode description</description>
    <pubDate>Mon, 01 Jan 2024 12:00:00 GMT</pubDate>
    <itunes:image href="https://example.com/ep.jpg"/>
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
        let contents = fs::read_to_string(&file_path).expect("read nfitems file");
        let v: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        assert_eq!(v["table"], "nfitems");
        assert_eq!(v["feed_id"], serde_json::json!(2002));
        assert_eq!(v["values"][1], serde_json::json!("Complete Episode")); // title
        assert_eq!(v["values"][2], serde_json::json!("https://example.com/ep1")); // link
        assert_eq!(v["values"][3], serde_json::json!("Episode description")); // description
        assert_eq!(v["values"][4], serde_json::json!("Mon, 01 Jan 2024 12:00:00 GMT")); // pub_date
        assert_eq!(v["values"][5], serde_json::json!("https://example.com/ep.jpg")); // itunes_image
        // Note: podcast_funding_url and podcast_funding_text would be additional fields beyond the basic ones
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


}