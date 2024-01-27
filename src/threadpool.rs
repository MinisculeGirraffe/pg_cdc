use std::{
    future::Future,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    pin::Pin,
    thread::{self},
};
use tokio::sync::oneshot;

#[must_use]
#[derive(Debug)]
pub struct TokioRayonHandle<T> {
    rx: oneshot::Receiver<thread::Result<T>>,
}

impl<T> TokioRayonHandle<T>
where
    T: Send + 'static,
{
    #[allow(dead_code)]
    pub fn spawn<F: FnOnce() -> T + Send + 'static>(func: F) -> TokioRayonHandle<T> {
        let (tx, rx) = oneshot::channel();

        rayon::spawn(move || {
            let _ = tx.send(catch_unwind(AssertUnwindSafe(func)));
        });

        TokioRayonHandle { rx }
    }
}

impl<T> Future for TokioRayonHandle<T> {
    type Output = T;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let rx = Pin::new(&mut self.rx);
        rx.poll(cx).map(|result| {
            result
                .expect("Unreachable error: Tokio channel closed")
                .unwrap_or_else(|err| resume_unwind(err))
        })
    }
}
