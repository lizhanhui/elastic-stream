// automatically generated by the FlatBuffers compiler, do not modify

// @generated

use core::cmp::Ordering;
use core::mem;

extern crate flatbuffers;
use self::flatbuffers::{EndianScalar, Follow};

#[allow(unused_imports, dead_code)]
pub mod header {

    use core::cmp::Ordering;
    use core::mem;

    extern crate flatbuffers;
    use self::flatbuffers::{EndianScalar, Follow};

    #[deprecated(
        since = "2.0.0",
        note = "Use associated constants instead. This will no longer be generated in 2021."
    )]
    pub const ENUM_MIN_CODE: i16 = 0;
    #[deprecated(
        since = "2.0.0",
        note = "Use associated constants instead. This will no longer be generated in 2021."
    )]
    pub const ENUM_MAX_CODE: i16 = 2;
    #[deprecated(
        since = "2.0.0",
        note = "Use associated constants instead. This will no longer be generated in 2021."
    )]
    #[allow(non_camel_case_types)]
    pub const ENUM_VALUES_CODE: [Code; 3] = [Code::OK, Code::InsufficientStorage, Code::NotLeader];

    /// Status of the response
    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
    #[repr(transparent)]
    pub struct Code(pub i16);
    #[allow(non_upper_case_globals)]
    impl Code {
        /// Requests are properly served
        pub const OK: Self = Self(0);
        /// One of the replica-group member failed due to insufficient storage.
        pub const InsufficientStorage: Self = Self(1);
        /// Publishing requests are routed to data nodes that is not the leader of the
        /// writable range. Clients should renew their metadata cache on receiving response
        /// with this status.
        pub const NotLeader: Self = Self(2);

        pub const ENUM_MIN: i16 = 0;
        pub const ENUM_MAX: i16 = 2;
        pub const ENUM_VALUES: &'static [Self] =
            &[Self::OK, Self::InsufficientStorage, Self::NotLeader];
        /// Returns the variant's name or "" if unknown.
        pub fn variant_name(self) -> Option<&'static str> {
            match self {
                Self::OK => Some("OK"),
                Self::InsufficientStorage => Some("InsufficientStorage"),
                Self::NotLeader => Some("NotLeader"),
                _ => None,
            }
        }
    }
    impl core::fmt::Debug for Code {
        fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
            if let Some(name) = self.variant_name() {
                f.write_str(name)
            } else {
                f.write_fmt(format_args!("<UNKNOWN {:?}>", self.0))
            }
        }
    }
    impl<'a> flatbuffers::Follow<'a> for Code {
        type Inner = Self;
        #[inline]
        unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
            let b = flatbuffers::read_scalar_at::<i16>(buf, loc);
            Self(b)
        }
    }

    impl flatbuffers::Push for Code {
        type Output = Code;
        #[inline]
        unsafe fn push(&self, dst: &mut [u8], _written_len: usize) {
            flatbuffers::emplace_scalar::<i16>(dst, self.0);
        }
    }

    impl flatbuffers::EndianScalar for Code {
        type Scalar = i16;
        #[inline]
        fn to_little_endian(self) -> i16 {
            self.0.to_le()
        }
        #[inline]
        #[allow(clippy::wrong_self_convention)]
        fn from_little_endian(v: i16) -> Self {
            let b = i16::from_le(v);
            Self(b)
        }
    }

    impl<'a> flatbuffers::Verifiable for Code {
        #[inline]
        fn run_verifier(
            v: &mut flatbuffers::Verifier,
            pos: usize,
        ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
            use self::flatbuffers::Verifiable;
            i16::run_verifier(v, pos)
        }
    }

    impl flatbuffers::SimpleToVerifyInSlice for Code {}
    pub enum RecordMetadataOffset {}
    #[derive(Copy, Clone, PartialEq)]

    /// The metadata for a record that has been acknowledged by the server
    pub struct RecordMetadata<'a> {
        pub _tab: flatbuffers::Table<'a>,
    }

    impl<'a> flatbuffers::Follow<'a> for RecordMetadata<'a> {
        type Inner = RecordMetadata<'a>;
        #[inline]
        unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
            Self {
                _tab: flatbuffers::Table::new(buf, loc),
            }
        }
    }

    impl<'a> RecordMetadata<'a> {
        pub const VT_OFFSET: flatbuffers::VOffsetT = 4;
        pub const VT_PARTITION: flatbuffers::VOffsetT = 6;
        pub const VT_SERIALIZED_KEY_SIZE: flatbuffers::VOffsetT = 8;
        pub const VT_SERIALIZED_VALUE_SIZE: flatbuffers::VOffsetT = 10;
        pub const VT_TIMESTAMP: flatbuffers::VOffsetT = 12;
        pub const VT_TOPIC: flatbuffers::VOffsetT = 14;

        #[inline]
        pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
            RecordMetadata { _tab: table }
        }
        #[allow(unused_mut)]
        pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
            _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
            args: &'args RecordMetadataArgs<'args>,
        ) -> flatbuffers::WIPOffset<RecordMetadata<'bldr>> {
            let mut builder = RecordMetadataBuilder::new(_fbb);
            builder.add_timestamp(args.timestamp);
            builder.add_offset(args.offset);
            if let Some(x) = args.topic {
                builder.add_topic(x);
            }
            builder.add_serialized_value_size(args.serialized_value_size);
            builder.add_serialized_key_size(args.serialized_key_size);
            builder.add_partition(args.partition);
            builder.finish()
        }

        /// Logical offset of the record in the topic/partition.
        #[inline]
        pub fn offset(&self) -> i64 {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<i64>(RecordMetadata::VT_OFFSET, Some(0))
                    .unwrap()
            }
        }
        /// The partition the record was sent to
        #[inline]
        pub fn partition(&self) -> i32 {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<i32>(RecordMetadata::VT_PARTITION, Some(0))
                    .unwrap()
            }
        }
        /// The size of the serialized, uncompressed key in bytes.
        #[inline]
        pub fn serialized_key_size(&self) -> i32 {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<i32>(RecordMetadata::VT_SERIALIZED_KEY_SIZE, Some(0))
                    .unwrap()
            }
        }
        /// The size of the serialized, uncompressed value in bytes.
        #[inline]
        pub fn serialized_value_size(&self) -> i32 {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<i32>(RecordMetadata::VT_SERIALIZED_VALUE_SIZE, Some(0))
                    .unwrap()
            }
        }
        /// The timestamp of the record in the topic/partition.
        #[inline]
        pub fn timestamp(&self) -> i64 {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<i64>(RecordMetadata::VT_TIMESTAMP, Some(0))
                    .unwrap()
            }
        }
        /// The topic the record was appended to
        #[inline]
        pub fn topic(&self) -> Option<&'a str> {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<flatbuffers::ForwardsUOffset<&str>>(RecordMetadata::VT_TOPIC, None)
            }
        }
    }

    impl flatbuffers::Verifiable for RecordMetadata<'_> {
        #[inline]
        fn run_verifier(
            v: &mut flatbuffers::Verifier,
            pos: usize,
        ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
            use self::flatbuffers::Verifiable;
            v.visit_table(pos)?
                .visit_field::<i64>("offset", Self::VT_OFFSET, false)?
                .visit_field::<i32>("partition", Self::VT_PARTITION, false)?
                .visit_field::<i32>("serialized_key_size", Self::VT_SERIALIZED_KEY_SIZE, false)?
                .visit_field::<i32>(
                    "serialized_value_size",
                    Self::VT_SERIALIZED_VALUE_SIZE,
                    false,
                )?
                .visit_field::<i64>("timestamp", Self::VT_TIMESTAMP, false)?
                .visit_field::<flatbuffers::ForwardsUOffset<&str>>("topic", Self::VT_TOPIC, false)?
                .finish();
            Ok(())
        }
    }
    pub struct RecordMetadataArgs<'a> {
        pub offset: i64,
        pub partition: i32,
        pub serialized_key_size: i32,
        pub serialized_value_size: i32,
        pub timestamp: i64,
        pub topic: Option<flatbuffers::WIPOffset<&'a str>>,
    }
    impl<'a> Default for RecordMetadataArgs<'a> {
        #[inline]
        fn default() -> Self {
            RecordMetadataArgs {
                offset: 0,
                partition: 0,
                serialized_key_size: 0,
                serialized_value_size: 0,
                timestamp: 0,
                topic: None,
            }
        }
    }

    pub struct RecordMetadataBuilder<'a: 'b, 'b> {
        fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
        start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
    }
    impl<'a: 'b, 'b> RecordMetadataBuilder<'a, 'b> {
        #[inline]
        pub fn add_offset(&mut self, offset: i64) {
            self.fbb_
                .push_slot::<i64>(RecordMetadata::VT_OFFSET, offset, 0);
        }
        #[inline]
        pub fn add_partition(&mut self, partition: i32) {
            self.fbb_
                .push_slot::<i32>(RecordMetadata::VT_PARTITION, partition, 0);
        }
        #[inline]
        pub fn add_serialized_key_size(&mut self, serialized_key_size: i32) {
            self.fbb_.push_slot::<i32>(
                RecordMetadata::VT_SERIALIZED_KEY_SIZE,
                serialized_key_size,
                0,
            );
        }
        #[inline]
        pub fn add_serialized_value_size(&mut self, serialized_value_size: i32) {
            self.fbb_.push_slot::<i32>(
                RecordMetadata::VT_SERIALIZED_VALUE_SIZE,
                serialized_value_size,
                0,
            );
        }
        #[inline]
        pub fn add_timestamp(&mut self, timestamp: i64) {
            self.fbb_
                .push_slot::<i64>(RecordMetadata::VT_TIMESTAMP, timestamp, 0);
        }
        #[inline]
        pub fn add_topic(&mut self, topic: flatbuffers::WIPOffset<&'b str>) {
            self.fbb_
                .push_slot_always::<flatbuffers::WIPOffset<_>>(RecordMetadata::VT_TOPIC, topic);
        }
        #[inline]
        pub fn new(
            _fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>,
        ) -> RecordMetadataBuilder<'a, 'b> {
            let start = _fbb.start_table();
            RecordMetadataBuilder {
                fbb_: _fbb,
                start_: start,
            }
        }
        #[inline]
        pub fn finish(self) -> flatbuffers::WIPOffset<RecordMetadata<'a>> {
            let o = self.fbb_.end_table(self.start_);
            flatbuffers::WIPOffset::new(o.value())
        }
    }

    impl core::fmt::Debug for RecordMetadata<'_> {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            let mut ds = f.debug_struct("RecordMetadata");
            ds.field("offset", &self.offset());
            ds.field("partition", &self.partition());
            ds.field("serialized_key_size", &self.serialized_key_size());
            ds.field("serialized_value_size", &self.serialized_value_size());
            ds.field("timestamp", &self.timestamp());
            ds.field("topic", &self.topic());
            ds.finish()
        }
    }
    pub enum NodeOffset {}
    #[derive(Copy, Clone, PartialEq)]

    /// Address of a data node
    pub struct Node<'a> {
        pub _tab: flatbuffers::Table<'a>,
    }

    impl<'a> flatbuffers::Follow<'a> for Node<'a> {
        type Inner = Node<'a>;
        #[inline]
        unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
            Self {
                _tab: flatbuffers::Table::new(buf, loc),
            }
        }
    }

    impl<'a> Node<'a> {
        pub const VT_HOST: flatbuffers::VOffsetT = 4;
        pub const VT_PORT: flatbuffers::VOffsetT = 6;

        #[inline]
        pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
            Node { _tab: table }
        }
        #[allow(unused_mut)]
        pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
            _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
            args: &'args NodeArgs<'args>,
        ) -> flatbuffers::WIPOffset<Node<'bldr>> {
            let mut builder = NodeBuilder::new(_fbb);
            if let Some(x) = args.host {
                builder.add_host(x);
            }
            builder.add_port(args.port);
            builder.finish()
        }

        #[inline]
        pub fn host(&self) -> Option<&'a str> {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<flatbuffers::ForwardsUOffset<&str>>(Node::VT_HOST, None)
            }
        }
        #[inline]
        pub fn port(&self) -> u16 {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe { self._tab.get::<u16>(Node::VT_PORT, Some(0)).unwrap() }
        }
    }

    impl flatbuffers::Verifiable for Node<'_> {
        #[inline]
        fn run_verifier(
            v: &mut flatbuffers::Verifier,
            pos: usize,
        ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
            use self::flatbuffers::Verifiable;
            v.visit_table(pos)?
                .visit_field::<flatbuffers::ForwardsUOffset<&str>>("host", Self::VT_HOST, false)?
                .visit_field::<u16>("port", Self::VT_PORT, false)?
                .finish();
            Ok(())
        }
    }
    pub struct NodeArgs<'a> {
        pub host: Option<flatbuffers::WIPOffset<&'a str>>,
        pub port: u16,
    }
    impl<'a> Default for NodeArgs<'a> {
        #[inline]
        fn default() -> Self {
            NodeArgs {
                host: None,
                port: 0,
            }
        }
    }

    pub struct NodeBuilder<'a: 'b, 'b> {
        fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
        start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
    }
    impl<'a: 'b, 'b> NodeBuilder<'a, 'b> {
        #[inline]
        pub fn add_host(&mut self, host: flatbuffers::WIPOffset<&'b str>) {
            self.fbb_
                .push_slot_always::<flatbuffers::WIPOffset<_>>(Node::VT_HOST, host);
        }
        #[inline]
        pub fn add_port(&mut self, port: u16) {
            self.fbb_.push_slot::<u16>(Node::VT_PORT, port, 0);
        }
        #[inline]
        pub fn new(_fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>) -> NodeBuilder<'a, 'b> {
            let start = _fbb.start_table();
            NodeBuilder {
                fbb_: _fbb,
                start_: start,
            }
        }
        #[inline]
        pub fn finish(self) -> flatbuffers::WIPOffset<Node<'a>> {
            let o = self.fbb_.end_table(self.start_);
            flatbuffers::WIPOffset::new(o.value())
        }
    }

    impl core::fmt::Debug for Node<'_> {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            let mut ds = f.debug_struct("Node");
            ds.field("host", &self.host());
            ds.field("port", &self.port());
            ds.finish()
        }
    }
    pub enum StatusOffset {}
    #[derive(Copy, Clone, PartialEq)]

    pub struct Status<'a> {
        pub _tab: flatbuffers::Table<'a>,
    }

    impl<'a> flatbuffers::Follow<'a> for Status<'a> {
        type Inner = Status<'a>;
        #[inline]
        unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
            Self {
                _tab: flatbuffers::Table::new(buf, loc),
            }
        }
    }

    impl<'a> Status<'a> {
        pub const VT_CODE: flatbuffers::VOffsetT = 4;
        pub const VT_MESSAGE: flatbuffers::VOffsetT = 6;
        pub const VT_NODES: flatbuffers::VOffsetT = 8;

        #[inline]
        pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
            Status { _tab: table }
        }
        #[allow(unused_mut)]
        pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
            _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
            args: &'args StatusArgs<'args>,
        ) -> flatbuffers::WIPOffset<Status<'bldr>> {
            let mut builder = StatusBuilder::new(_fbb);
            if let Some(x) = args.nodes {
                builder.add_nodes(x);
            }
            if let Some(x) = args.message {
                builder.add_message(x);
            }
            builder.add_code(args.code);
            builder.finish()
        }

        /// Status code
        #[inline]
        pub fn code(&self) -> Code {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<Code>(Status::VT_CODE, Some(Code::OK))
                    .unwrap()
            }
        }
        /// Human readable text for the code.
        #[inline]
        pub fn message(&self) -> Option<&'a str> {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<flatbuffers::ForwardsUOffset<&str>>(Status::VT_MESSAGE, None)
            }
        }
        /// If status were errors, `nodes` are the list of nodes that reports such errors.
        #[inline]
        pub fn nodes(
            &self,
        ) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Node<'a>>>> {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab.get::<flatbuffers::ForwardsUOffset<
                    flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Node>>,
                >>(Status::VT_NODES, None)
            }
        }
    }

    impl flatbuffers::Verifiable for Status<'_> {
        #[inline]
        fn run_verifier(
            v: &mut flatbuffers::Verifier,
            pos: usize,
        ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
            use self::flatbuffers::Verifiable;
            v.visit_table(pos)?
                .visit_field::<Code>("code", Self::VT_CODE, false)?
                .visit_field::<flatbuffers::ForwardsUOffset<&str>>(
                    "message",
                    Self::VT_MESSAGE,
                    false,
                )?
                .visit_field::<flatbuffers::ForwardsUOffset<
                    flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<Node>>,
                >>("nodes", Self::VT_NODES, false)?
                .finish();
            Ok(())
        }
    }
    pub struct StatusArgs<'a> {
        pub code: Code,
        pub message: Option<flatbuffers::WIPOffset<&'a str>>,
        pub nodes: Option<
            flatbuffers::WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Node<'a>>>>,
        >,
    }
    impl<'a> Default for StatusArgs<'a> {
        #[inline]
        fn default() -> Self {
            StatusArgs {
                code: Code::OK,
                message: None,
                nodes: None,
            }
        }
    }

    pub struct StatusBuilder<'a: 'b, 'b> {
        fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
        start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
    }
    impl<'a: 'b, 'b> StatusBuilder<'a, 'b> {
        #[inline]
        pub fn add_code(&mut self, code: Code) {
            self.fbb_.push_slot::<Code>(Status::VT_CODE, code, Code::OK);
        }
        #[inline]
        pub fn add_message(&mut self, message: flatbuffers::WIPOffset<&'b str>) {
            self.fbb_
                .push_slot_always::<flatbuffers::WIPOffset<_>>(Status::VT_MESSAGE, message);
        }
        #[inline]
        pub fn add_nodes(
            &mut self,
            nodes: flatbuffers::WIPOffset<
                flatbuffers::Vector<'b, flatbuffers::ForwardsUOffset<Node<'b>>>,
            >,
        ) {
            self.fbb_
                .push_slot_always::<flatbuffers::WIPOffset<_>>(Status::VT_NODES, nodes);
        }
        #[inline]
        pub fn new(_fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>) -> StatusBuilder<'a, 'b> {
            let start = _fbb.start_table();
            StatusBuilder {
                fbb_: _fbb,
                start_: start,
            }
        }
        #[inline]
        pub fn finish(self) -> flatbuffers::WIPOffset<Status<'a>> {
            let o = self.fbb_.end_table(self.start_);
            flatbuffers::WIPOffset::new(o.value())
        }
    }

    impl core::fmt::Debug for Status<'_> {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            let mut ds = f.debug_struct("Status");
            ds.field("code", &self.code());
            ds.field("message", &self.message());
            ds.field("nodes", &self.nodes());
            ds.finish()
        }
    }
    pub enum PublishRecordResponseHeaderOffset {}
    #[derive(Copy, Clone, PartialEq)]

    pub struct PublishRecordResponseHeader<'a> {
        pub _tab: flatbuffers::Table<'a>,
    }

    impl<'a> flatbuffers::Follow<'a> for PublishRecordResponseHeader<'a> {
        type Inner = PublishRecordResponseHeader<'a>;
        #[inline]
        unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
            Self {
                _tab: flatbuffers::Table::new(buf, loc),
            }
        }
    }

    impl<'a> PublishRecordResponseHeader<'a> {
        pub const VT_STATUS: flatbuffers::VOffsetT = 4;
        pub const VT_METADATA: flatbuffers::VOffsetT = 6;

        #[inline]
        pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
            PublishRecordResponseHeader { _tab: table }
        }
        #[allow(unused_mut)]
        pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
            _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
            args: &'args PublishRecordResponseHeaderArgs<'args>,
        ) -> flatbuffers::WIPOffset<PublishRecordResponseHeader<'bldr>> {
            let mut builder = PublishRecordResponseHeaderBuilder::new(_fbb);
            if let Some(x) = args.metadata {
                builder.add_metadata(x);
            }
            if let Some(x) = args.status {
                builder.add_status(x);
            }
            builder.finish()
        }

        #[inline]
        pub fn status(&self) -> Option<Status<'a>> {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab.get::<flatbuffers::ForwardsUOffset<Status>>(
                    PublishRecordResponseHeader::VT_STATUS,
                    None,
                )
            }
        }
        #[inline]
        pub fn metadata(&self) -> Option<RecordMetadata<'a>> {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<flatbuffers::ForwardsUOffset<RecordMetadata>>(
                        PublishRecordResponseHeader::VT_METADATA,
                        None,
                    )
            }
        }
    }

    impl flatbuffers::Verifiable for PublishRecordResponseHeader<'_> {
        #[inline]
        fn run_verifier(
            v: &mut flatbuffers::Verifier,
            pos: usize,
        ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
            use self::flatbuffers::Verifiable;
            v.visit_table(pos)?
                .visit_field::<flatbuffers::ForwardsUOffset<Status>>(
                    "status",
                    Self::VT_STATUS,
                    false,
                )?
                .visit_field::<flatbuffers::ForwardsUOffset<RecordMetadata>>(
                    "metadata",
                    Self::VT_METADATA,
                    false,
                )?
                .finish();
            Ok(())
        }
    }
    pub struct PublishRecordResponseHeaderArgs<'a> {
        pub status: Option<flatbuffers::WIPOffset<Status<'a>>>,
        pub metadata: Option<flatbuffers::WIPOffset<RecordMetadata<'a>>>,
    }
    impl<'a> Default for PublishRecordResponseHeaderArgs<'a> {
        #[inline]
        fn default() -> Self {
            PublishRecordResponseHeaderArgs {
                status: None,
                metadata: None,
            }
        }
    }

    pub struct PublishRecordResponseHeaderBuilder<'a: 'b, 'b> {
        fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
        start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
    }
    impl<'a: 'b, 'b> PublishRecordResponseHeaderBuilder<'a, 'b> {
        #[inline]
        pub fn add_status(&mut self, status: flatbuffers::WIPOffset<Status<'b>>) {
            self.fbb_
                .push_slot_always::<flatbuffers::WIPOffset<Status>>(
                    PublishRecordResponseHeader::VT_STATUS,
                    status,
                );
        }
        #[inline]
        pub fn add_metadata(&mut self, metadata: flatbuffers::WIPOffset<RecordMetadata<'b>>) {
            self.fbb_
                .push_slot_always::<flatbuffers::WIPOffset<RecordMetadata>>(
                    PublishRecordResponseHeader::VT_METADATA,
                    metadata,
                );
        }
        #[inline]
        pub fn new(
            _fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>,
        ) -> PublishRecordResponseHeaderBuilder<'a, 'b> {
            let start = _fbb.start_table();
            PublishRecordResponseHeaderBuilder {
                fbb_: _fbb,
                start_: start,
            }
        }
        #[inline]
        pub fn finish(self) -> flatbuffers::WIPOffset<PublishRecordResponseHeader<'a>> {
            let o = self.fbb_.end_table(self.start_);
            flatbuffers::WIPOffset::new(o.value())
        }
    }

    impl core::fmt::Debug for PublishRecordResponseHeader<'_> {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            let mut ds = f.debug_struct("PublishRecordResponseHeader");
            ds.field("status", &self.status());
            ds.field("metadata", &self.metadata());
            ds.finish()
        }
    }
    pub enum HeartbeatOffset {}
    #[derive(Copy, Clone, PartialEq)]

    pub struct Heartbeat<'a> {
        pub _tab: flatbuffers::Table<'a>,
    }

    impl<'a> flatbuffers::Follow<'a> for Heartbeat<'a> {
        type Inner = Heartbeat<'a>;
        #[inline]
        unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
            Self {
                _tab: flatbuffers::Table::new(buf, loc),
            }
        }
    }

    impl<'a> Heartbeat<'a> {
        pub const VT_CLIENT_ID: flatbuffers::VOffsetT = 4;

        #[inline]
        pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
            Heartbeat { _tab: table }
        }
        #[allow(unused_mut)]
        pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
            _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
            args: &'args HeartbeatArgs<'args>,
        ) -> flatbuffers::WIPOffset<Heartbeat<'bldr>> {
            let mut builder = HeartbeatBuilder::new(_fbb);
            if let Some(x) = args.client_id {
                builder.add_client_id(x);
            }
            builder.finish()
        }

        #[inline]
        pub fn client_id(&self) -> Option<&'a str> {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<flatbuffers::ForwardsUOffset<&str>>(Heartbeat::VT_CLIENT_ID, None)
            }
        }
    }

    impl flatbuffers::Verifiable for Heartbeat<'_> {
        #[inline]
        fn run_verifier(
            v: &mut flatbuffers::Verifier,
            pos: usize,
        ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
            use self::flatbuffers::Verifiable;
            v.visit_table(pos)?
                .visit_field::<flatbuffers::ForwardsUOffset<&str>>(
                    "client_id",
                    Self::VT_CLIENT_ID,
                    false,
                )?
                .finish();
            Ok(())
        }
    }
    pub struct HeartbeatArgs<'a> {
        pub client_id: Option<flatbuffers::WIPOffset<&'a str>>,
    }
    impl<'a> Default for HeartbeatArgs<'a> {
        #[inline]
        fn default() -> Self {
            HeartbeatArgs { client_id: None }
        }
    }

    pub struct HeartbeatBuilder<'a: 'b, 'b> {
        fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
        start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
    }
    impl<'a: 'b, 'b> HeartbeatBuilder<'a, 'b> {
        #[inline]
        pub fn add_client_id(&mut self, client_id: flatbuffers::WIPOffset<&'b str>) {
            self.fbb_
                .push_slot_always::<flatbuffers::WIPOffset<_>>(Heartbeat::VT_CLIENT_ID, client_id);
        }
        #[inline]
        pub fn new(_fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>) -> HeartbeatBuilder<'a, 'b> {
            let start = _fbb.start_table();
            HeartbeatBuilder {
                fbb_: _fbb,
                start_: start,
            }
        }
        #[inline]
        pub fn finish(self) -> flatbuffers::WIPOffset<Heartbeat<'a>> {
            let o = self.fbb_.end_table(self.start_);
            flatbuffers::WIPOffset::new(o.value())
        }
    }

    impl core::fmt::Debug for Heartbeat<'_> {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            let mut ds = f.debug_struct("Heartbeat");
            ds.field("client_id", &self.client_id());
            ds.finish()
        }
    }
    pub enum ListRangeOffset {}
    #[derive(Copy, Clone, PartialEq)]

    pub struct ListRange<'a> {
        pub _tab: flatbuffers::Table<'a>,
    }

    impl<'a> flatbuffers::Follow<'a> for ListRange<'a> {
        type Inner = ListRange<'a>;
        #[inline]
        unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
            Self {
                _tab: flatbuffers::Table::new(buf, loc),
            }
        }
    }

    impl<'a> ListRange<'a> {
        pub const VT_PARTITION_ID: flatbuffers::VOffsetT = 4;

        #[inline]
        pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
            ListRange { _tab: table }
        }
        #[allow(unused_mut)]
        pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
            _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
            args: &'args ListRangeArgs,
        ) -> flatbuffers::WIPOffset<ListRange<'bldr>> {
            let mut builder = ListRangeBuilder::new(_fbb);
            builder.add_partition_id(args.partition_id);
            builder.finish()
        }

        #[inline]
        pub fn partition_id(&self) -> i64 {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<i64>(ListRange::VT_PARTITION_ID, Some(0))
                    .unwrap()
            }
        }
    }

    impl flatbuffers::Verifiable for ListRange<'_> {
        #[inline]
        fn run_verifier(
            v: &mut flatbuffers::Verifier,
            pos: usize,
        ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
            use self::flatbuffers::Verifiable;
            v.visit_table(pos)?
                .visit_field::<i64>("partition_id", Self::VT_PARTITION_ID, false)?
                .finish();
            Ok(())
        }
    }
    pub struct ListRangeArgs {
        pub partition_id: i64,
    }
    impl<'a> Default for ListRangeArgs {
        #[inline]
        fn default() -> Self {
            ListRangeArgs { partition_id: 0 }
        }
    }

    pub struct ListRangeBuilder<'a: 'b, 'b> {
        fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
        start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
    }
    impl<'a: 'b, 'b> ListRangeBuilder<'a, 'b> {
        #[inline]
        pub fn add_partition_id(&mut self, partition_id: i64) {
            self.fbb_
                .push_slot::<i64>(ListRange::VT_PARTITION_ID, partition_id, 0);
        }
        #[inline]
        pub fn new(_fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>) -> ListRangeBuilder<'a, 'b> {
            let start = _fbb.start_table();
            ListRangeBuilder {
                fbb_: _fbb,
                start_: start,
            }
        }
        #[inline]
        pub fn finish(self) -> flatbuffers::WIPOffset<ListRange<'a>> {
            let o = self.fbb_.end_table(self.start_);
            flatbuffers::WIPOffset::new(o.value())
        }
    }

    impl core::fmt::Debug for ListRange<'_> {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            let mut ds = f.debug_struct("ListRange");
            ds.field("partition_id", &self.partition_id());
            ds.finish()
        }
    }
} // pub mod Header
