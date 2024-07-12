use std::str::FromStr;
use std::time::Duration;

use anyhow::Result;
use futures_util::StreamExt;
use http::Uri;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_websockets::ClientBuilder;
use tracing::{error, info};

use crate::order_book::{parse_data, OrderBookUpdate};

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct BinanceMsg {
    last_update_id: u64,
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
}

pub struct BinanceExchangeSource {
    currency_pair: String,
}

impl BinanceExchangeSource {
    pub fn new(currency_pair: String) -> BinanceExchangeSource {
        BinanceExchangeSource { currency_pair }
    }

    fn url(&self) -> String {
        format!(
            "wss://stream.binance.com:9443/ws/{}@depth20@100ms",
            self.currency_pair
        )
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
        let uri = Uri::from_str(&self.url())?;
        let (mut client, _) = ClientBuilder::from_uri(uri).connect().await?;

        while let Some(res) = timeout(Duration::from_secs(15), client.next()).await? {
            if let Some(text) = res?.as_text() {
                let msg = serde_json::from_str::<BinanceMsg>(text)?;
                info!("Received order book data, update_id: {}", msg.last_update_id);
                let update = parse_data(msg.bids, msg.asks, "binance")?;
                sender.send(update).await?;
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
        let bitstamp = BinanceExchangeSource {
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
