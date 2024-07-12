use std::time::Duration;

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use http::Uri;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_websockets::{ClientBuilder, Message};
use tracing::{error, info};

use crate::order_book::{parse_data, OrderBookUpdate};

#[derive(Debug, Deserialize)]
pub struct OrderBookData {
    timestamp: String,
    // microtimestamp: String,
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "event")]
#[serde(rename_all = "snake_case")]
pub enum BitstampMessage {
    #[serde(rename = "bts:subscription_succeeded")]
    BtsSubscriptionSucceeded { channel: String },
    #[serde(rename = "data")]
    Data { data: OrderBookData },
}

pub struct BitstampExchangeSource {
    currency_pair: String,
}

impl BitstampExchangeSource {
    pub fn new(currency_pair: String) -> BitstampExchangeSource {
        BitstampExchangeSource { currency_pair }
    }

    pub fn begin(self, sender: mpsc::Sender<OrderBookUpdate>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                match self.connect(sender.clone()).await {
                    Err(e) => error!("Worker task errored, retrying: {}", e),
                    Ok(_) => error!("Worker task exited, reconnecting"),
                };

                sleep(Duration::from_secs(2)).await;
            }
        })
    }

    pub async fn connect(&self, sender: mpsc::Sender<OrderBookUpdate>) -> Result<()> {
        let uri = Uri::from_static("wss://ws.bitstamp.net");
        let (mut client, _) = ClientBuilder::from_uri(uri).connect().await?;

        let connect_msg = format!(
            r#"
        {{
            "event": "bts:subscribe",
            "data": {{
                "channel": "order_book_{}"
            }}
        }}
        "#,
            self.currency_pair
        );

        client.send(Message::text(connect_msg)).await?;

        while let Some(res) = timeout(Duration::from_secs(15), client.next()).await? {
            if let Some(text) = res?.as_text() {
                match serde_json::from_str::<BitstampMessage>(text)? {
                    BitstampMessage::BtsSubscriptionSucceeded { channel } => {
                        info!("Subscribed to {}", channel)
                    }
                    BitstampMessage::Data { data } => {
                        info!("Received order book data, timestamp: {}", data.timestamp);
                        let update = parse_data(data.bids, data.asks, "bitstamp")?;
                        sender.send(update).await?;
                    }
                }
            }
        }

        Result::Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_connect() {
        let (sender, mut receiver) = mpsc::channel(64);
        let bitstamp = BitstampExchangeSource {
            currency_pair: "btcusdt".to_owned(),
        };

        let _handle = bitstamp.begin(sender);

        let mut received_updates = Vec::new();

        for _ in 0..5 {
            if let Some(update) = receiver.recv().await {
                received_updates.push(update);
            } else {
                break;
            }
        }

        assert!(!received_updates.is_empty());
    }
}
