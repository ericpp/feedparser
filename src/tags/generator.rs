use crate::parser_state::ParserState;

pub fn on_text(data: &str, state: &mut ParserState) {
    if state.in_channel && !state.in_item && state.channel_generator.is_empty() {
        state.channel_generator.push_str(data);
    }
}