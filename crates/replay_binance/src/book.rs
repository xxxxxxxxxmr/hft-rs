use std::collections::{BTreeMap, VecDeque};

use serde::Serialize;

const EPS: f64 = 1e-9;

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventKind {
    Snapshot { qty: f64 },
    New { qty: f64 },
    Fill { qty: f64, remaining: f64 },
    Underflow { missing_qty: f64 },
}

#[derive(Debug, Serialize)]
pub struct InferredEvent {
    pub sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<u64>,
    pub side: Side,
    pub price_ticks: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<u64>,
    pub kind: EventKind,
}

impl InferredEvent {
    fn snapshot(
        sequence: u64,
        timestamp_ms: Option<u64>,
        side: Side,
        price_ticks: u64,
        order_id: u64,
        qty: f64,
    ) -> Self {
        Self {
            sequence,
            timestamp_ms,
            side,
            price_ticks,
            order_id: Some(order_id),
            kind: EventKind::Snapshot { qty },
        }
    }

    fn new(
        sequence: u64,
        timestamp_ms: Option<u64>,
        side: Side,
        price_ticks: u64,
        order_id: u64,
        qty: f64,
    ) -> Self {
        Self {
            sequence,
            timestamp_ms,
            side,
            price_ticks,
            order_id: Some(order_id),
            kind: EventKind::New { qty },
        }
    }

    fn fill(
        sequence: u64,
        timestamp_ms: Option<u64>,
        side: Side,
        price_ticks: u64,
        order_id: u64,
        fill_qty: f64,
        remaining: f64,
    ) -> Self {
        Self {
            sequence,
            timestamp_ms,
            side,
            price_ticks,
            order_id: Some(order_id),
            kind: EventKind::Fill {
                qty: fill_qty,
                remaining,
            },
        }
    }

    fn underflow(
        sequence: u64,
        timestamp_ms: Option<u64>,
        side: Side,
        price_ticks: u64,
        missing_qty: f64,
    ) -> Self {
        Self {
            sequence,
            timestamp_ms,
            side,
            price_ticks,
            order_id: None,
            kind: EventKind::Underflow { missing_qty },
        }
    }
}

pub struct L3Book {
    bids: BookSide,
    asks: BookSide,
}

impl L3Book {
    pub fn new() -> Self {
        Self {
            bids: BookSide::new(),
            asks: BookSide::new(),
        }
    }

    pub fn seed_from_snapshot(
        &mut self,
        side: Side,
        entries: &[(u64, f64)],
        sequence: u64,
        timestamp_ms: Option<u64>,
    ) -> Vec<InferredEvent> {
        self.side_mut(side)
            .seed(entries, sequence, timestamp_ms, side)
    }

    pub fn apply_updates(
        &mut self,
        side: Side,
        updates: &[(u64, f64)],
        sequence: u64,
        timestamp_ms: Option<u64>,
    ) -> Vec<InferredEvent> {
        self.side_mut(side)
            .apply(updates, sequence, timestamp_ms, side)
    }

    fn side_mut(&mut self, side: Side) -> &mut BookSide {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }
}

struct BookSide {
    levels: BTreeMap<u64, PriceLevel>,
}

impl BookSide {
    fn new() -> Self {
        Self {
            levels: BTreeMap::new(),
        }
    }

    fn seed(
        &mut self,
        entries: &[(u64, f64)],
        sequence: u64,
        timestamp_ms: Option<u64>,
        side: Side,
    ) -> Vec<InferredEvent> {
        let mut out = Vec::new();
        for &(price_ticks, qty) in entries {
            if qty <= EPS {
                continue;
            }
            let mut level = PriceLevel::new(price_ticks);
            let order_id = level.push_order(qty);
            out.push(InferredEvent::snapshot(
                sequence,
                timestamp_ms,
                side,
                price_ticks,
                order_id,
                qty,
            ));
            self.levels.insert(price_ticks, level);
        }
        out
    }

    fn apply(
        &mut self,
        updates: &[(u64, f64)],
        sequence: u64,
        timestamp_ms: Option<u64>,
        side: Side,
    ) -> Vec<InferredEvent> {
        let mut out = Vec::new();
        for &(price_ticks, new_total) in updates {
            let mut remove_level = false;
            if let Some(level) = self.levels.get_mut(&price_ticks) {
                let old_total = level.total_qty;
                if approx_equal(old_total, new_total) {
                    // nothing to do
                } else if new_total > old_total {
                    let diff = new_total - old_total;
                    if diff > EPS {
                        let order_id = level.push_order(diff);
                        out.push(InferredEvent::new(
                            sequence,
                            timestamp_ms,
                            side,
                            price_ticks,
                            order_id,
                            diff,
                        ));
                    }
                } else {
                    let diff = (old_total - new_total).max(0.0);
                    if diff > EPS {
                        let fills = level.consume(diff, sequence, timestamp_ms, side);
                        out.extend(fills);
                    }
                    if level.total_qty <= EPS || new_total <= EPS {
                        remove_level = true;
                    }
                }
            } else if new_total > EPS {
                let mut level = PriceLevel::new(price_ticks);
                let order_id = level.push_order(new_total);
                out.push(InferredEvent::new(
                    sequence,
                    timestamp_ms,
                    side,
                    price_ticks,
                    order_id,
                    new_total,
                ));
                self.levels.insert(price_ticks, level);
            }

            if remove_level {
                self.levels.remove(&price_ticks);
            }
        }
        out
    }
}

struct PriceLevel {
    price_ticks: u64,
    queue: VecDeque<OrderSlice>,
    next_counter: u32,
    total_qty: f64,
}

impl PriceLevel {
    fn new(price_ticks: u64) -> Self {
        Self {
            price_ticks,
            queue: VecDeque::new(),
            next_counter: 1,
            total_qty: 0.0,
        }
    }

    fn push_order(&mut self, qty: f64) -> u64 {
        let order_id = self.make_order_id();
        self.queue.push_back(OrderSlice { id: order_id, qty });
        self.total_qty += qty;
        order_id
    }

    fn consume(
        &mut self,
        mut requested: f64,
        sequence: u64,
        timestamp_ms: Option<u64>,
        side: Side,
    ) -> Vec<InferredEvent> {
        let mut events = Vec::new();
        while requested > EPS {
            match self.queue.front_mut() {
                Some(front) if front.qty > requested + EPS => {
                    front.qty -= requested;
                    self.total_qty -= requested;
                    let order_id = front.id;
                    events.push(InferredEvent::fill(
                        sequence,
                        timestamp_ms,
                        side,
                        self.price_ticks,
                        order_id,
                        requested,
                        front.qty,
                    ));
                    requested = 0.0;
                }
                Some(front) => {
                    let consumed = front.qty;
                    requested -= consumed;
                    self.total_qty -= consumed;
                    let order_id = front.id;
                    events.push(InferredEvent::fill(
                        sequence,
                        timestamp_ms,
                        side,
                        self.price_ticks,
                        order_id,
                        consumed,
                        0.0,
                    ));
                    self.queue.pop_front();
                }
                None => {
                    if requested > EPS {
                        events.push(InferredEvent::underflow(
                            sequence,
                            timestamp_ms,
                            side,
                            self.price_ticks,
                            requested,
                        ));
                        requested = 0.0;
                    }
                }
            }
        }
        self.clamp_totals();
        events
    }

    fn clamp_totals(&mut self) {
        if self.total_qty < EPS {
            self.total_qty = 0.0;
        }
    }

    fn make_order_id(&mut self) -> u64 {
        let id = ((self.price_ticks as u128) << 32) | (self.next_counter as u128);
        self.next_counter = self.next_counter.wrapping_add(1);
        id as u64
    }
}

struct OrderSlice {
    id: u64,
    qty: f64,
}

fn approx_equal(a: f64, b: f64) -> bool {
    (a - b).abs() <= EPS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_orders_and_fifo_consumption() {
        let mut book = L3Book::new();
        let mut events = book.seed_from_snapshot(Side::Bid, &[(1000, 1.0)], 1, None);
        assert_eq!(events.len(), 1);
        let first_id = events[0].order_id.unwrap();

        events = book.apply_updates(Side::Bid, &[(1000, 1.5)], 2, None);
        assert_eq!(events.len(), 1);
        let second_id = events[0].order_id.unwrap();
        assert_ne!(first_id, second_id);
        if let EventKind::New { qty } = events[0].kind {
            assert!((qty - 0.5).abs() < 1e-9);
        } else {
            panic!("expected new event");
        }

        events = book.apply_updates(Side::Bid, &[(1000, 0.4)], 3, None);
        assert_eq!(events.len(), 2);
        match events[0].kind {
            EventKind::Fill { qty, remaining } => {
                assert!((qty - 1.0).abs() < 1e-9);
                assert!((remaining).abs() < 1e-9);
                assert_eq!(events[0].order_id.unwrap(), first_id);
            }
            _ => panic!("expected fill"),
        }
        match events[1].kind {
            EventKind::Fill { qty, remaining } => {
                assert!((qty - 0.1).abs() < 1e-9);
                assert!((remaining - 0.4).abs() < 1e-9);
                assert_eq!(events[1].order_id.unwrap(), second_id);
            }
            _ => panic!("expected fill"),
        }
    }

    #[test]
    fn removing_level_emits_fills() {
        let mut book = L3Book::new();
        let events = book.seed_from_snapshot(Side::Ask, &[(2000, 0.8)], 1, None);
        let order_id = events[0].order_id.unwrap();

        let events = book.apply_updates(Side::Ask, &[(2000, 0.0)], 2, None);
        assert_eq!(events.len(), 1);
        match events[0].kind {
            EventKind::Fill { qty, remaining } => {
                assert!((qty - 0.8).abs() < 1e-9);
                assert_eq!(remaining, 0.0);
                assert_eq!(events[0].order_id.unwrap(), order_id);
            }
            _ => panic!("expected fill"),
        }
    }

    #[test]
    fn underflow_event_is_emitted_when_data_inconsistent() {
        let mut book = L3Book::new();
        book.seed_from_snapshot(Side::Bid, &[(4000, 0.2)], 1, None);

        let events = book.apply_updates(Side::Bid, &[(4000, -0.1)], 2, None);
        assert_eq!(events.len(), 2);
        let mut saw_underflow = false;
        for ev in events {
            if let EventKind::Underflow { missing_qty } = ev.kind {
                assert!((missing_qty - 0.1).abs() < 1e-9);
                saw_underflow = true;
            }
        }
        assert!(saw_underflow);
    }
}
