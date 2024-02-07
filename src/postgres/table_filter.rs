use dashmap::DashMap;
use futures::Stream;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::sync::{Arc, Weak};
use tokio::sync::mpsc;

use super::types::OperationMessage;

type FilterFunction = Box<dyn Fn(&OperationMessage) -> bool + Send + Sync + 'static>;

/// Fan out communication to multiple consumers in memory
/// Consumers register a filter function and receive a stream of messages that match their filter.
/// When a message is received, it is passed to all filter functions that match the predicate
/// Predicate checking is done in parallel
/// Ordering is not ensured between consumers of the same message, however, messages are guaranteed to be delivered in order between all consumers
struct TableFilter {
    filter_map: Arc<DashMap<String, Vec<FilterCallback>>>,
}

struct FilterCallback {
    function: FilterFunction,
    sender: mpsc::UnboundedSender<OperationMessage>,
}

impl FilterCallback {
    pub fn new<F: Fn(&OperationMessage) -> bool + Send + Sync + 'static>(
        function: F,
        sender: mpsc::UnboundedSender<OperationMessage>,
    ) -> Self {
        Self {
            function: Box::new(function),
            sender,
        }
    }
}

struct FilterReciever {
    table_name: String,
    receiver: mpsc::UnboundedReceiver<OperationMessage>,
    index: usize,
    table_ref: Weak<DashMap<String, Vec<FilterCallback>>>,
}

impl Drop for FilterReciever {
    fn drop(&mut self) {
        if let Some(table) = self.table_ref.upgrade() {
            // This has to be in it's own block because if we hold a reference to a key
            // it will deadlock when we try to remove key.
            let is_empty = {
                let mut filters = table.get_mut(&self.table_name).unwrap();
                filters.remove(self.index);
                filters.is_empty()
            };

            if is_empty {
                table.remove(&self.table_name);
            }
        }
    }
}

impl Stream for FilterReciever {
    type Item = OperationMessage;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl TableFilter {
    pub fn new() -> Self {
        Self {
            filter_map: DashMap::new().into(),
        }
    }

    pub fn add_filter<F: Fn(&OperationMessage) -> bool + Send + Sync + 'static>(
        &self,
        table_name: &str,
        filter: F,
    ) -> impl Stream<Item = OperationMessage> {
        let mut filters = self.filter_map.entry(table_name.to_string()).or_default();
        let (sender, receiver) = mpsc::unbounded_channel();
        let callback = FilterCallback::new(filter, sender);
        filters.push(callback);

        FilterReciever {
            table_name: table_name.to_string(),
            receiver,
            index: filters.len() - 1,
            table_ref: Arc::downgrade(&self.filter_map),
        }
    }

    pub fn filter_message(&self, message: &OperationMessage) {
        let Some(filters) = self.filter_map.get(&message.table) else {
            return;
        };

        filters.par_iter().for_each(|filter| {
            if (filter.function)(message) {
                let _ = filter.sender.send(message.clone());
            }
        });
    }
}
