use rust_decimal::Decimal;
use serde::Serialize;
use std::{collections::HashMap, str::FromStr};
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Bid {
    pub price: Decimal,
    pub amount: Decimal,
}

#[derive(Debug, Clone)]
pub struct Ask {
    pub price: Decimal,
    pub amount: Decimal,
}

#[derive(Debug, Clone)]
pub struct OrderBookUpdate {
    pub exchange: String,
    pub bids: Vec<Bid>,
    pub asks: Vec<Ask>,
}

#[derive(Serialize)]
pub struct ExchangeAsk {
    exchange: String,
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    price: Decimal,
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    amount: Decimal
}

#[derive(Serialize)]
pub struct ExchangeBid {
    exchange: String,
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    price: Decimal,
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    amount: Decimal
}

#[derive(Serialize)]
pub struct UpdateResult {
    pub asks: Vec<ExchangeAsk>,
    pub bids: Vec<ExchangeBid>,
    pub spread: Option<Decimal>,
}

impl UpdateResult {
    pub fn normalize(
        &mut self,
        price_decimal_places: u32,
        amount_decimal_places: u32,
        spread_decimal_places: u32,
    ) {
        fn normalize_decimal(value: Decimal, decimal_places: u32) -> Decimal {
            value.round_dp(decimal_places)
        }

        for ask in &mut self.asks {
            ask.price = normalize_decimal(ask.price, price_decimal_places);
            ask.amount = normalize_decimal(ask.amount, amount_decimal_places);
        }

        for bid in &mut self.bids {
            bid.price = normalize_decimal(bid.price, price_decimal_places);
            bid.amount = normalize_decimal(bid.amount, amount_decimal_places);
        }

        if let Some(spread) = &mut self.spread {
            *spread = normalize_decimal(*spread, spread_decimal_places);
        }
    }
}

#[derive(Debug)]
pub struct MergedOrderBook {
    latest_bids: HashMap<String, Vec<Bid>>,
    latest_asks: HashMap<String, Vec<Ask>>,
    top_n: usize,
}

impl MergedOrderBook {
    pub fn new(top_n: usize) -> Self {
        MergedOrderBook {
            latest_bids: HashMap::new(),
            latest_asks: HashMap::new(),
            top_n,
        }
    }

    // Updates the latest bid snapshot of the exchange and returns top 10 combined bids
    // Assumes bids are already sorted and in correct order
    fn update_bids(&mut self, exchange: &str, bids: Vec<Bid>) -> Vec<ExchangeBid> {
        self.latest_bids.insert(exchange.to_string(), bids);

        let mut all_bids: Vec<ExchangeBid> = Vec::new();

        for (exchange, bids) in &mut self.latest_bids {
            let list: Vec<ExchangeBid> = bids
                .into_iter()
                .map(|b| ExchangeBid { exchange: exchange.clone(), price: b.price, amount: b.amount } )
                .collect();
            all_bids.extend(list.into_iter().take(self.top_n));
        }

        all_bids.sort_by(|a, b| b.price.cmp(&a.price));

        all_bids.into_iter().take(self.top_n).collect()
    }

    // Updates the latest ask snapshot of the exchange and returns top 10 combined asks
    // Assumes asks are already sorted and in correct order
    fn update_asks(&mut self, exchange: &str, asks: Vec<Ask>) -> Vec<ExchangeAsk> {
        self.latest_asks.insert(exchange.to_string(), asks);

        let mut all_asks: Vec<ExchangeAsk> = Vec::new();

        for (exchange, asks) in &mut self.latest_asks {
            let list: Vec<ExchangeAsk> = asks
                .into_iter()
                .map(|b| ExchangeAsk { exchange: exchange.clone(), price: b.price, amount: b.amount })
                .collect();
            all_asks.extend(list.into_iter().take(self.top_n));
        }

        all_asks.sort_by(|a, b| a.price.cmp(&b.price));

        all_asks.into_iter().take(self.top_n).collect()
    }

    pub fn update(&mut self, update: OrderBookUpdate) -> UpdateResult {
        let top_asks = self.update_asks(&update.exchange, update.asks);
        let top_bids = self.update_bids(&update.exchange, update.bids);

        let spread: Option<Decimal> = top_asks.first().and_then(|ask| {
            top_bids
                .first()
                .map(|bid| ask.price - bid.price)
        });

        UpdateResult {
            asks: top_asks,
            bids: top_bids,
            spread,
        }
    }
}

pub fn parse_data(bids: Vec<(String, String)>, asks: Vec<(String, String)>, exchange: &str) -> Result<OrderBookUpdate> {
    let bids = bids
        .into_iter()
        .map(|(bid_value, amount)| -> Result<Bid> {
            Ok(Bid {
                price: Decimal::from_str(&bid_value)?,
                amount: Decimal::from_str(&amount)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let asks = asks
        .into_iter()
        .map(|(ask_value, amount)| -> Result<Ask> {
            Ok(Ask {
                price: Decimal::from_str(&ask_value)?,
                amount: Decimal::from_str(&amount)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(OrderBookUpdate {
        exchange: exchange.to_string(),
        bids,
        asks,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_update_bids() {
        let mut merged_book = MergedOrderBook::new(3);

        let update = OrderBookUpdate {
            exchange: "binance".to_owned(),
            bids: vec![
                Bid {
                    price: dec!(100.5),
                    amount: dec!(1.5),
                },
                Bid {
                    price: dec!(100.0),
                    amount: dec!(2.0),
                },
                Bid {
                    price: dec!(99.5),
                    amount: dec!(1.0),
                },
            ],
            asks: vec![],
        };

        let result = merged_book.update_bids(&update.exchange, update.bids);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].exchange, "binance");
        assert_eq!(result[0].price, dec!(100.5));
        assert_eq!(result[0].amount, dec!(1.5));
        assert_eq!(result[1].exchange, "binance");
        assert_eq!(result[1].price, dec!(100.0));
        assert_eq!(result[1].amount, dec!(2.0));
        assert_eq!(result[2].exchange, "binance");
        assert_eq!(result[2].price, dec!(99.5));
        assert_eq!(result[2].amount, dec!(1.0));

        let update2 = OrderBookUpdate {
            exchange: "kraken".to_owned(),
            bids: vec![
                Bid {
                    price: dec!(101.0),
                    amount: dec!(1.0),
                },
                Bid {
                    price: dec!(100.2),
                    amount: dec!(2.5),
                },
            ],
            asks: vec![],
        };

        let result2 = merged_book.update_bids(&update2.exchange, update2.bids);

        assert_eq!(result2.len(), 3);
        assert_eq!(result2[0].exchange, "kraken");
        assert_eq!(result2[0].price, dec!(101.0));
        assert_eq!(result2[0].amount, dec!(1.0));
        assert_eq!(result2[1].exchange, "binance");
        assert_eq!(result2[1].price, dec!(100.5));
        assert_eq!(result2[1].amount, dec!(1.5));
        assert_eq!(result2[2].exchange, "kraken");
        assert_eq!(result2[2].price, dec!(100.2));
        assert_eq!(result2[2].amount, dec!(2.5));
    }

    #[test]
    fn test_update_asks() {
        let mut merged_book = MergedOrderBook::new(3);

        let update = OrderBookUpdate {
            exchange: "binance".to_owned(),
            bids: vec![],
            asks: vec![
                Ask {
                    price: dec!(101.0),
                    amount: dec!(1.5),
                },
                Ask {
                    price: dec!(101.5),
                    amount: dec!(2.0),
                },
                Ask {
                    price: dec!(102.0),
                    amount: dec!(1.0),
                },
            ],
        };

        let result = merged_book.update_asks(&update.exchange, update.asks);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].exchange, "binance");
        assert_eq!(result[0].price, dec!(101.0));
        assert_eq!(result[0].amount, dec!(1.5));
        assert_eq!(result[1].exchange, "binance");
        assert_eq!(result[1].price, dec!(101.5));
        assert_eq!(result[1].amount, dec!(2.0));
        assert_eq!(result[2].exchange, "binance");
        assert_eq!(result[2].price, dec!(102.0));
        assert_eq!(result[2].amount, dec!(1.0));

        let update2 = OrderBookUpdate {
            exchange: "kraken".to_owned(),
            bids: vec![],
            asks: vec![
                Ask {
                    price: dec!(100.5),
                    amount: dec!(1.0),
                },
                Ask {
                    price: dec!(101.2),
                    amount: dec!(2.5),
                },
            ],
        };

        let result2 = merged_book.update_asks(&update2.exchange, update2.asks);

        assert_eq!(result2.len(), 3);
        assert_eq!(result2[0].exchange, "kraken");
        assert_eq!(result2[0].price, dec!(100.5));
        assert_eq!(result2[0].amount, dec!(1.0));
        assert_eq!(result2[1].exchange, "binance");
        assert_eq!(result2[1].price, dec!(101.0));
        assert_eq!(result2[1].amount, dec!(1.5));
        assert_eq!(result2[2].exchange, "kraken");
        assert_eq!(result2[2].price, dec!(101.2));
        assert_eq!(result2[2].amount, dec!(2.5));
    }
}
