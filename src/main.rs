use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::io::Cursor;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::path::{PathBuf};
use std::time::{SystemTime, UNIX_EPOCH, Instant};
use serde::Serialize;
use serde_json::Value as JsonValue;
use xml::reader::{EventReader, XmlEvent};

// Global counter initialized to zero at program start
static GLOBAL_COUNTER: AtomicUsize = AtomicUsize::new(0);
// Per-run output subfolder based on startup UNIX timestamp
static OUTPUT_SUBDIR: OnceLock<PathBuf> = OnceLock::new();

#[derive(Serialize)]
pub struct SqlInsert {
    table: String,
    columns: Vec<String>,
    values: Vec<JsonValue>,
    feed_id: Option<i64>,
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

    // Variables to track extracted data
    // Channel-level
    let mut in_channel = false;
    let mut in_channel_image = false; // avoid picking up <image><title>/<link> as channel title/link
    let mut channel_title = String::new();
    let mut channel_link = String::new();
    let mut channel_description = String::new();

    // Item-level
    let mut in_item = false;
    let mut current_element = String::new();
    let mut title = String::new();
    let mut link = String::new();
    let mut description = String::new();
    let mut pub_date = String::new();
    let mut itunes_image = String::new();
    let mut podcast_funding_url = String::new();
    let mut podcast_funding_text = String::new();
    let mut in_podcast_funding = false; // track when inside <podcast:funding>

    // Parse the XML document
    for event in parser {
        match event {
            //A tag is opened.
            Ok(XmlEvent::StartElement { name, attributes, .. }) => {
                current_element = name.local_name.clone();
                if current_element == "channel" {
                    in_channel = true;
                    // Reset channel-level fields when a new channel starts
                    channel_title.clear();
                    channel_link.clear();
                    channel_description.clear();
                    in_channel_image = false;
                }

                // Track when entering an <image> inside the channel (but not inside items)
                if in_channel && !in_item && name.local_name == "image" {
                    in_channel_image = true;
                }
                if current_element == "item" {
                    in_item = true;
                    // Clear previous data for new item
                    title.clear();
                    link.clear();
                    description.clear();
                    pub_date.clear();
                    itunes_image.clear();
                    podcast_funding_url.clear();
                    podcast_funding_text.clear();
                    in_podcast_funding = false;
                }

                // Handle itunes:image which is a self-closing tag with an href (or url) attribute
                // We only capture it when inside an <item>
                if in_item {
                    let is_itunes_image = name.local_name == "image"
                        && (matches!(name.prefix.as_deref(), Some("itunes"))
                        || matches!(
                                name.namespace.as_deref(),
                                Some("http://www.itunes.com/dtds/podcast-1.0.dtd")
                            ));

                    if is_itunes_image {
                        // Find the href or url attribute
                        if let Some(attr) = attributes.iter().find(|a| {
                            let key = a.name.local_name.as_str();
                            key == "href" || key == "url"
                        }) {
                            itunes_image = attr.value.clone();
                        }
                    }

                    // Detect podcast:funding start
                    let is_podcast_funding = name.local_name == "funding"
                        && (matches!(name.prefix.as_deref(), Some("podcast"))
                        || matches!(
                                name.namespace.as_deref(),
                                Some("https://podcastindex.org/namespace/1.0")
                            )
                        || matches!(
                                name.namespace.as_deref(),
                                Some("http://podcastindex.org/namespace/1.0")
                            ));

                    if is_podcast_funding {
                        in_podcast_funding = true;
                        // capture optional url attribute
                        if let Some(attr) = attributes.iter().find(|a| a.name.local_name == "url") {
                            podcast_funding_url = attr.value.clone();
                        }
                    }
                }
            }

            //Text is found.
            Ok(XmlEvent::Characters(data)) => {
                if in_item {
                    match current_element.as_str() {
                        "title" => title.push_str(&data),
                        "link" => link.push_str(&data),
                        "description" => description.push_str(&data),
                        "pubDate" => pub_date.push_str(&data),
                        _ => {
                            if in_podcast_funding {
                                podcast_funding_text.push_str(&data);
                            }
                        }
                    }
                } else if in_channel && !in_channel_image {
                    // Capture top-level channel fields (outside <item> and not inside <image>)
                    match current_element.as_str() {
                        "title" => channel_title.push_str(&data),
                        "link" => channel_link.push_str(&data),
                        "description" => channel_description.push_str(&data),
                        _ => {}
                    }
                }
            }

            //A tag is closed.
            Ok(XmlEvent::EndElement { name }) => {
                // Close channel <image> scope
                if name.local_name == "image" && in_channel_image {
                    in_channel_image = false;
                }
                // When closing channel, emit one INSERT-equivalent record for newsfeeds table
                if name.local_name == "channel" && in_channel {
                    in_channel = false;
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
                            JsonValue::from(channel_title.trim().to_string()),
                            JsonValue::from(channel_link.trim().to_string()),
                            JsonValue::from(channel_description.trim().to_string()),
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
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to serialize record for newsfeeds: {}", e);
                        }
                    }
                }
                // Close podcast:funding scope if needed
                if name.local_name == "funding" && in_podcast_funding {
                    in_podcast_funding = false;
                }

                // When closing item, emit one INSERT-equivalent record for nfitems table
                if name.local_name == "item" {
                    in_item = false;

                    // Build the INSERT-equivalent record for the current item
                    let record = SqlInsert {
                        table: "nfitems".to_string(),
                        columns: vec![
                            "feed_id".to_string(),
                            "title".to_string(),
                            "link".to_string(),
                            "description".to_string(),
                            "pub_date".to_string(),
                            "itunes_image".to_string(),
                            "podcast_funding_url".to_string(),
                            "podcast_funding_text".to_string(),
                        ],
                        values: vec![
                            match feed_id { Some(v) => JsonValue::from(v), None => JsonValue::Null },
                            JsonValue::from(title.clone()),
                            JsonValue::from(link.clone()),
                            JsonValue::from(description.clone()),
                            JsonValue::from(pub_date.clone()),
                            JsonValue::from(itunes_image.clone()),
                            JsonValue::from(podcast_funding_url.clone()),
                            JsonValue::from(podcast_funding_text.trim().to_string()),
                        ],
                        feed_id,
                    };

                    // Use a per-run outputs subfolder established at startup
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
                    let file_name = format!("{}_{}_{}.json", counter_val, "nfitems", fid_for_name);
                    let file_path = out_dir.join(file_name);

                    match serde_json::to_string(&record) {
                        Ok(serialized) => {
                            if let Err(e) = fs::write(&file_path, serialized) {
                                eprintln!("Failed to write {}: {}", file_path.display(), e);
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to serialize record for nfitems: {}", e);
                        }
                    }
                }
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