use crate::parser_state::ParserState;

pub fn on_start(state: &mut ParserState) {
    if state.in_channel && !state.in_item {
        state.in_channel_itunes_owner = true;
        state.channel_itunes_owner_name.clear();
        state.channel_itunes_owner_email.clear();
    }
}

pub fn on_text(current_element: &str, data: &str, state: &mut ParserState) {
    if state.in_channel_itunes_owner {
        match current_element {
            "name" | "itunes:name" => state.channel_itunes_owner_name.push_str(data),
            "email" | "itunes:email" => state.channel_itunes_owner_email.push_str(data),
            _ => {}
        }
    }
}

pub fn on_end(state: &mut ParserState) {
    if state.in_channel_itunes_owner {
        state.in_channel_itunes_owner = false;
    }
}
