use crate::parser_state::ParserState;

pub fn on_text(data: &str, state: &mut ParserState) {
    if state.in_item {
        state.itunes_duration.push_str(data);
    }
}
