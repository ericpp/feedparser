use crate::parser_state::ParserState;

pub fn on_text(data: &str, state: &mut ParserState) {
    if state.in_item {
        state.pub_date.push_str(data);
    } else if state.in_channel && state.pub_date.is_empty() {
        state.channel_pub_date.push_str(data);
    }
}