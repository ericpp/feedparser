use xml::attribute::OwnedAttribute;

use crate::{
    parser_state::{ParserState, ValueRecipient},
    utils,
};

pub fn on_start(attributes: &[OwnedAttribute], state: &mut ParserState) {
    if !(state.in_channel || state.in_item) {
        return;
    }

    state.in_podcast_value = true;
    state.value_recipients.clear();
    state.value_model_type = attributes
        .iter()
        .find(|a| a.name.local_name == "type")
        .map(|a| a.value.clone())
        .unwrap_or_default();
    state.value_model_method = attributes
        .iter()
        .find(|a| a.name.local_name == "method")
        .map(|a| a.value.clone())
        .unwrap_or_default();
    state.value_model_suggested = attributes
        .iter()
        .find(|a| a.name.local_name == "suggested")
        .map(|a| a.value.clone())
        .unwrap_or_default();
}

pub fn on_value_recipient(attributes: &[OwnedAttribute], state: &mut ParserState) {
    if !state.in_podcast_value {
        return;
    }

    if state.value_recipients.len() >= 100 {
        return; // enforce cap
    }

    let mut vr = ValueRecipient::default();
    for attr in attributes {
        match attr.name.local_name.as_str() {
            "name" => vr.name = attr.value.clone(),
            "type" => vr.recipient_type = attr.value.clone(),
            "address" => vr.address = attr.value.clone(),
            "split" => vr.split = attr.value.parse::<i32>().unwrap_or(0),
            "fee" => {
                let val = attr.value.to_ascii_lowercase();
                vr.fee = matches!(val.as_str(), "true" | "yes" | "1");
            }
            "customKey" => vr.custom_key = Some(attr.value.clone()),
            "customValue" => vr.custom_value = Some(attr.value.clone()),
            _ => {}
        }
    }

    state.value_recipients.push(vr);
}

pub fn on_end(_feed_id: Option<i64>, state: &mut ParserState) {
    if state.in_podcast_value && !state.value_recipients.is_empty() {
        if let Some(block) = utils::build_value_block(state) {
            let type_code = utils::map_value_type(&state.value_model_type);
            if state.in_item && state.item_has_valid_enclosure {
                if !state.item_value_has_lightning || type_code == 0 || state.item_value_pending.is_none() {
                    state.item_value_pending = Some((type_code, block));
                    if type_code == 0 {
                        state.item_value_has_lightning = true;
                    }
                }
            } else if state.in_channel {
                if !state.channel_value_has_lightning || type_code == 0 || state.channel_value_pending.is_none() {
                    state.channel_value_pending = Some((type_code, block));
                    if type_code == 0 {
                        state.channel_value_has_lightning = true;
                    }
                }
            }
        }
    }

    state.in_podcast_value = false;
    state.value_recipients.clear();
    state.value_model_type.clear();
    state.value_model_method.clear();
    state.value_model_suggested.clear();
}