use pb::sf::substreams::sink::pubsub::v1::{Attribute, Message, Publish};
use pb::sf::substreams::v1::Clock;

mod pb;

#[substreams::handlers::map]
fn map_clocks(clock: Clock) -> Result<Publish, substreams::errors::Error> {
    let publish = Publish {
        messages: vec![Message {
            data: clock.number.to_be_bytes().to_vec(),
            attributes: vec![Attribute {
                key: "timestamp".to_string(),
                value: "test".to_string(),
            }],
        }],
    };

    Ok(publish)
}
