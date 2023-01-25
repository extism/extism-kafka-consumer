use extism_pdk::*;
use serde::{Serialize, Deserialize};

extern "C" {
    fn consumer_commit();
}

#[derive(Serialize)]
struct ConfigMap {
    #[serde(rename(serialize = "group.id"))]
    group_id: String,
    // #[serde(rename(serialize = "enable.auto.commit"))]
    // auto_commit: String,
}

#[plugin_fn]
pub fn config_map(_: ()) -> FnResult<Json<ConfigMap>> {
    let map = ConfigMap {
        group_id: "weather_group".to_string(),
        //auto_commit: "true".to_string(),
    };
    Ok(Json(map))
}

#[derive(Serialize)]
struct Poll {
    time_ms: i32,
}

#[plugin_fn]
pub fn poll(_: ()) -> FnResult<Json<Poll>> {
    let poll = Poll { time_ms: 100 };
    Ok(Json(poll))
}

#[derive(Serialize)]
struct Subscribe {
    topics: Vec<String>,
}

#[plugin_fn]
pub fn subscribe_topics(_: ()) -> FnResult<Json<Subscribe>> {
    let subscribe = Subscribe {
        topics: vec!["weather".into()],
    };
    Ok(Json(subscribe))
}

#[derive(Deserialize)]
struct Header {
    key: String,
    value: String,
}

#[derive(Deserialize)]
struct TopicPartition {
    topic: Option<String>,
    partition: i32,
    offset: i64,
    metadata: Option<String>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct Message {
    topic_partition: TopicPartition,
    value: String,
    key: String,
    timestamp: String,
    headers: Option<Vec<Header>>,
}


#[plugin_fn]
pub fn on_message(Json(msg): Json<Message>) -> FnResult<String> {
    unsafe { consumer_commit() };
    let offset = msg.topic_partition.offset;
    let divs = (offset % 3 == 0, offset % 5 == 0);

    match divs {
        (true, true) => Ok("fizzbuzz".into()),
        (true, false) => Ok("fizz".into()),
        (false, true) => Ok("buzz".into()),
        _ => Ok(offset.to_string())
    }
}
