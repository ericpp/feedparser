use crate::parser_state::ParserState;

pub fn on_start(state: &mut ParserState) {
    if state.in_item {
        state.itunes_duration.clear();
        println!("itunes_duration: start: {}", state.enclosure_url);
    }
}

pub fn on_text(data: &str, state: &mut ParserState) {
    if state.in_item {
        println!("itunes_duration: text: '{}'", data);
        state.itunes_duration = data.to_string();
    }
}
