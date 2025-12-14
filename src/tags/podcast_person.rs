use xml::attribute::OwnedAttribute;

use crate::{parser_state::ParserState, parser_state::PodcastPerson};

pub fn on_start(attributes: &[OwnedAttribute], state: &mut ParserState) {
    if !state.in_item {
        return;
    }

    state.in_podcast_person = true;
    state.current_person_name.clear();
    state.current_person_role.clear();
    state.current_person_group.clear();
    state.current_person_img.clear();
    state.current_person_href.clear();

    for attr in attributes {
        match attr.name.local_name.as_str() {
            "role" => state.current_person_role = attr.value.clone(),
            "group" => state.current_person_group = attr.value.clone(),
            "img" => state.current_person_img = attr.value.clone(),
            "href" => state.current_person_href = attr.value.clone(),
            _ => {}
        }
    }
}

pub fn on_text(data: &str, state: &mut ParserState) {
    if state.in_podcast_person {
        state.current_person_name.push_str(data);
    }
}

pub fn on_end(_feed_id: Option<i64>, state: &mut ParserState) {
    if state.in_podcast_person {
        state.in_podcast_person = false;

        state.podcast_persons.push(PodcastPerson {
            name: state.current_person_name.clone(),
            role: state.current_person_role.clone(),
            group: state.current_person_group.clone(),
            img: state.current_person_img.clone(),
            href: state.current_person_href.clone(),
        });
    }
}

