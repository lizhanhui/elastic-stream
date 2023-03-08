use bytes::Bytes;
use flatbuffers::{FlatBufferBuilder, Verifiable, WIPOffset};

use slog::Logger;

/// Verifies that a buffer of bytes contains a rpc request and returns it.
pub(crate) fn root_as_rpc_request<'a, R>(buf: &'a [u8]) -> Result<R, flatbuffers::InvalidFlatbuffer>
where
    R: flatbuffers::Follow<'a, Inner = R> + 'a + Verifiable,
{
    flatbuffers::root::<R>(buf)
}

/// Finish the response builder and returns the finished response frame header.
/// Use a generic type to support different response types,
/// and ensure the finished_data is called after the builder.finish().
pub(crate) fn finish_response_builder<R>(
    builder: &mut FlatBufferBuilder,
    response_offset: WIPOffset<R>,
) -> Option<Bytes> {
    builder.finish(response_offset, None);
    Some(Bytes::copy_from_slice(builder.finished_data()))
}
