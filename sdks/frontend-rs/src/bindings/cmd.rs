use std::time::Duration;

use bytes::Bytes;
use jni::objects::GlobalRef;
use model::error::EsError;

use super::tracing::Tracer;
use crate::{Frontend, Stream};

pub enum Command<'a> {
    CreateStream {
        front_end: &'a mut Frontend,
        replica: u8,
        ack_count: u8,
        retention: Duration,
        future: GlobalRef,
    },
    OpenStream {
        front_end: &'a mut Frontend,
        stream_id: u64,
        epoch: u64,
        future: GlobalRef,
    },
    StartOffset {
        stream: &'a mut Stream,
        future: GlobalRef,
    },
    NextOffset {
        stream: &'a mut Stream,
        future: GlobalRef,
    },

    Append {
        stream: &'a mut Stream,
        buf: Bytes,
        future: GlobalRef,
        tracer: Tracer,
    },

    Read {
        stream: &'a mut Stream,
        start_offset: i64,
        end_offset: i64,
        batch_max_bytes: i32,
        future: GlobalRef,
        tracer: Tracer,
    },

    Trim {
        stream: &'a mut Stream,
        new_start_offset: i64,
        future: GlobalRef,
    },

    Delete {
        stream: &'a mut Stream,
        future: GlobalRef,
    },

    CloseStream {
        stream: &'a mut Stream,
        future: GlobalRef,
    },
}

pub enum CallbackCommand {
    Append {
        future: GlobalRef,
        base_offset: i64,
        tracer: Tracer,
    },
    Read {
        future: GlobalRef,
        buffers: Vec<Bytes>,
        tracer: Tracer,
    },
    CreateStream {
        future: GlobalRef,
        stream_id: i64,
    },
    OpenStream {
        future: GlobalRef,
        ptr: i64,
    },
    StartOffset {
        future: GlobalRef,
        offset: i64,
    },
    NextOffset {
        future: GlobalRef,
        offset: i64,
    },
    Trim {
        future: GlobalRef,
    },
    Delete {
        future: GlobalRef,
    },
    CloseStream {
        future: GlobalRef,
    },
    ClientError {
        future: GlobalRef,
        err: EsError,
    },
}
