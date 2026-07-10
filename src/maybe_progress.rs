use std::{borrow::Cow, sync::Arc};

use indicatif::{ProgressBar, ProgressBarIter, ProgressStyle};
use tokio::sync::{MappedMutexGuard, Mutex, MutexGuard};

use crate::Context;

#[derive(Clone)]
pub(crate) struct MaybeProgressBar {
    pub(crate) real: Arc<Mutex<Option<ProgressBar>>>,
    pub(crate) inner: Arc<Mutex<InnerMaybeProgressBar>>,
    pub(crate) context: Context,
}

pub(crate) struct InnerMaybeProgressBar {
    pub(crate) style: Option<ProgressStyle>,
    pub(crate) prefix: Option<Cow<'static, str>>,
    pub(crate) length: Option<u64>,
}

impl MaybeProgressBar {
    pub(crate) fn new(context: Context) -> MaybeProgressBar {
        MaybeProgressBar {
            real: Arc::new(Mutex::new(None)),
            inner: Arc::new(Mutex::new(InnerMaybeProgressBar {
                style: None,
                prefix: None,
                length: None,
            })),
            context,
        }
    }

    pub(crate) async fn set_style(&self, style: ProgressStyle) {
        if let Some(real) = &*self.real.lock().await {
            real.set_style(style);
        } else {
            self.inner.lock().await.style = Some(style);
        }
    }

    pub(crate) async fn set_prefix(&self, prefix: impl Into<Cow<'static, str>>) {
        if let Some(real) = &*self.real.lock().await {
            real.set_prefix(prefix);
        } else {
            self.inner.lock().await.prefix = Some(prefix.into());
        }
    }

    pub(crate) async fn set_length(&self, length: u64) {
        if let Some(real) = &*self.real.lock().await {
            real.set_length(length);
        } else {
            self.inner.lock().await.length = Some(length);
        }
    }

    pub(crate) async fn realize(&self) -> MappedMutexGuard<'_, ProgressBar> {
        let mut real = self.real.lock().await;
        if real.is_none() {
            let new = self.context.progress.add(ProgressBar::new(1));
            let mut contents = self.inner.lock().await;
            if let Some(style) = contents.style.take() {
                new.set_style(style);
            }
            if let Some(prefix) = contents.prefix.take() {
                new.set_prefix(prefix);
            }
            if let Some(length) = contents.length.take() {
                new.set_length(length);
            }
            *real = Some(new);
        }
        MutexGuard::map(real, |m| m.as_mut().expect("must be set"))
    }

    pub(crate) async fn set_message(&self, msg: impl Into<Cow<'static, str>>) {
        self.realize().await.set_message(msg);
    }

    pub(crate) async fn set_position(&self, position: u64) {
        self.realize().await.set_position(position);
    }

    pub(crate) async fn finish_and_clear(&self) {
        if let Some(real) = &*self.real.lock().await {
            real.finish_and_clear();
        }
    }

    pub(crate) async fn finish_with_message(&self, msg: impl Into<Cow<'static, str>>) {
        if let Some(real) = &*self.real.lock().await {
            real.finish_with_message(msg);
        }
    }

    pub async fn wrap_async_read<R: tokio::io::AsyncRead + Unpin>(
        &self,
        read: R,
    ) -> ProgressBarIter<R> {
        self.realize().await.wrap_async_read(read)
    }
}
