mod order_book;
mod exchanges;

use std::env;

use exchanges::binance::BinanceExchangeSource;
use exchanges::bitstamp::BitstampExchangeSource;
use order_book::MergedOrderBook;
use tokio::sync::mpsc;
use tracing::info;


#[tokio::main]
async fn main() {
    // let subscriber = tracing_subscriber::FmtSubscriber::new();
    // tracing::subscriber::set_global_default(subscriber).expect("Cannot set tracing subscriber");

    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: {} <currency_pair>", args[0]);
        std::process::exit(1);
    }

    let currency_pair = args[1].clone();

    let (sender, mut receiver) = mpsc::channel(64);

    let binance = BinanceExchangeSource::new(currency_pair.clone());
    binance.begin(sender.clone());

    let bitstamp = BitstampExchangeSource::new(currency_pair.clone());
    bitstamp.begin(sender);

    let mut order_book = MergedOrderBook::new(10);

    loop {
        let msg = receiver.recv().await;

        if let Some(update) = msg {
            info!("Received order book update for: {}", update.exchange);

            let mut result = order_book.update(update);
            result.normalize(2, 4, 3);

            let json = serde_json::to_string_pretty(&result).unwrap();

            println!("{}", json);
        }
    }
}

