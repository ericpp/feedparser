use crate::parser_state::ParserState;

pub fn on_start(state: &mut ParserState) {
    if state.in_item && !state.in_podcast_alternate_enclosure {
        state.itunes_duration.clear();
    }
}

pub fn on_text(data: &str, state: &mut ParserState) {
    if state.in_item && !state.in_podcast_alternate_enclosure {
        state.itunes_duration = data.to_string();
    }
}
