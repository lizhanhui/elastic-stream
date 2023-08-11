use bytes::Bytes;
use jni::objects::{
    GlobalRef, JByteBuffer, JClass, JMethodID, JObject, JString, JValue, JValueGen,
};
use jni::sys::{jint, jlong, JNINativeInterface_, JNI_VERSION_1_8};
use jni::{JNIEnv, JavaVM};
use log::{error, info, trace};
use minitrace::future::FutureExt;
use minitrace::Span;
use model::error::EsError;
use std::alloc::Layout;
use std::cell::{OnceCell, RefCell};
use std::ffi::c_void;
use std::slice;
use std::sync::Arc;
use std::time::Duration;

use crate::{Frontend, Stopwatch, Stream, StreamOptions};
use crossbeam::channel::{unbounded, Sender};

use super::cmd::{CallbackCommand, Command};
use super::tracing::{Tracer, TracingService};

static mut TX: OnceCell<mpsc::UnboundedSender<Command>> = OnceCell::new();
static mut CALLBACK_TX: OnceCell<Sender<CallbackCommand>> = OnceCell::new();
static mut STREAM_CLASS_CACHE: OnceCell<GlobalRef> = OnceCell::new();
static mut STREAM_CTOR_CACHE: OnceCell<JMethodID> = OnceCell::new();
static mut JLONG_CLASS_CACHE: OnceCell<GlobalRef> = OnceCell::new();
static mut JLONG_CTOR_CACHE: OnceCell<JMethodID> = OnceCell::new();
static mut VOID_CLASS_CACHE: OnceCell<GlobalRef> = OnceCell::new();
static mut VOID_CTOR_CACHE: OnceCell<JMethodID> = OnceCell::new();
static mut FUTURE_COMPLETE_CACHE: OnceCell<JMethodID> = OnceCell::new();
static mut TRACING_SERVICE: OnceCell<TracingService> = OnceCell::new();
static mut ES_EXCEPTION_CLASS_CACHE: OnceCell<GlobalRef> = OnceCell::new();
static mut ES_EXCEPTION_CTOR_CACHE: OnceCell<JMethodID> = OnceCell::new();

thread_local! {
    static JAVA_VM: RefCell<Option<Arc<JavaVM>>> = RefCell::new(None);
    static JENV: RefCell<Option<*mut jni::sys::JNIEnv>> = RefCell::new(None);
}

async fn process_command(cmd: Command<'_>) {
    match cmd {
        Command::CreateStream {
            front_end,
            replica,
            ack_count,
            retention,
            future,
        } => {
            process_create_stream_command(front_end, replica, ack_count, retention, future).await;
        }
        Command::OpenStream {
            front_end,
            stream_id,
            epoch,
            future,
        } => {
            process_open_stream_command(front_end, stream_id, epoch, future).await;
        }
        Command::StartOffset { stream, future } => {
            process_start_offset_command(stream, future).await;
        }
        Command::NextOffset { stream, future } => {
            process_next_offset_command(stream, future).await;
        }
        Command::Append {
            stream,
            buf,
            future,
            tracer,
        } => {
            process_append_command(stream, buf, future, tracer).await;
        }
        Command::Read {
            stream,
            start_offset,
            end_offset,
            batch_max_bytes,
            future,
            tracer,
        } => {
            process_read_command(
                stream,
                start_offset,
                end_offset,
                batch_max_bytes,
                future,
                tracer,
            )
            .await;
        }
        Command::CloseStream { stream, future } => {
            process_close_stream_command(stream, future).await;
        }
    }
}
async fn process_close_stream_command(stream: &Stream, future: GlobalRef) {
    trace!("Start processing close command");
    let result = stream.close().await;
    match result {
        Ok(_) => {
            let tx = unsafe { CALLBACK_TX.get() }.unwrap();
            let _ = tx.send(CallbackCommand::CloseStream { future });
            // complete_future_with_void(future);
        }
        Err(err) => {
            // complete_future_with_error(future, err);
            let tx = unsafe { CALLBACK_TX.get() }.unwrap();
            let _ = tx.send(CallbackCommand::ClientError { future, err });
        }
    };
    trace!("Close command finished");
}

async fn process_append_command(stream: &Stream, buf: Bytes, future: GlobalRef, tracer: Tracer) {
    trace!("Start processing append command");
    let result = stream
        .append(buf)
        .in_span(tracer.get_child_span("stream.append()"))
        .await;
    match result {
        Ok(result) => {
            let base_offset = result.base_offset;
            // complete_future_with_jlong(future, base_offset);
            let tx = unsafe { CALLBACK_TX.get() }.unwrap();
            let _ = tx.send(CallbackCommand::Append {
                future,
                base_offset,
                tracer,
            });
        }
        Err(err) => {
            // complete_future_with_error(future, err);
            let tx = unsafe { CALLBACK_TX.get() }.unwrap();
            let _ = tx.send(CallbackCommand::ClientError { future, err });
        }
    };
    trace!("Append command finished");
}

async fn process_read_command(
    stream: &Stream,
    start_offset: i64,
    end_offset: i64,
    batch_max_bytes: i32,
    future: GlobalRef,
    tracer: Tracer,
) {
    trace!("Start processing read command");
    let result = stream
        .read(start_offset, end_offset, batch_max_bytes)
        .in_span(tracer.get_child_span("stream.read()"))
        .await;
    match result {
        Ok(buffers) => {
            let tx = unsafe { CALLBACK_TX.get() }.unwrap();
            let _ = tx.send(CallbackCommand::Read {
                future,
                buffers,
                tracer,
            });
            // complete_future_with_byte_array(future, buffers);
        }
        Err(err) => {
            // complete_future_with_error(future, err);
            let tx = unsafe { CALLBACK_TX.get() }.unwrap();
            let _ = tx.send(CallbackCommand::ClientError { future, err });
        }
    };
    trace!("Read command finished");
}

async fn process_start_offset_command(stream: &Stream, future: GlobalRef) {
    trace!("Start processing start_offset command");
    let result = stream.start_offset().await;
    match result {
        Ok(offset) => {
            let tx = unsafe { CALLBACK_TX.get() }.unwrap();
            let _ = tx.send(CallbackCommand::StartOffset { future, offset });
            // complete_future_with_jlong(future, offset);
        }
        Err(err) => {
            // complete_future_with_error(future, err);
            let tx = unsafe { CALLBACK_TX.get() }.unwrap();
            let _ = tx.send(CallbackCommand::ClientError { future, err });
        }
    };
    trace!("Start_offset command finished");
}

async fn process_next_offset_command(stream: &Stream, future: GlobalRef) {
    trace!("Start processing next_offset command");
    let result = stream.next_offset().await;
    match result {
        Ok(offset) => {
            let tx = unsafe { CALLBACK_TX.get() }.unwrap();
            let _ = tx.send(CallbackCommand::NextOffset { future, offset });
            // complete_future_with_jlong(future, offset);
        }
        Err(err) => {
            // complete_future_with_error(future, err);
            let tx = unsafe { CALLBACK_TX.get() }.unwrap();
            let _ = tx.send(CallbackCommand::ClientError { future, err });
        }
    };
    trace!("Next_offset command finished");
}

async fn process_open_stream_command(
    front_end: &Frontend,
    stream_id: u64,
    epoch: u64,
    future: GlobalRef,
) {
    trace!("Start processing open_stream command");
    let result = front_end.open(stream_id, epoch).await;
    match result {
        Ok(stream) => {
            let ptr = Box::into_raw(Box::new(stream)) as jlong;
            // complete_future_with_stream(future, ptr);
            let tx = unsafe { CALLBACK_TX.get() }.unwrap();
            let _ = tx.send(CallbackCommand::OpenStream { future, ptr });
        }
        Err(err) => {
            // complete_future_with_error(future, err);
            let tx = unsafe { CALLBACK_TX.get() }.unwrap();
            let _ = tx.send(CallbackCommand::ClientError { future, err });
        }
    };
    trace!("Open_stream command finished");
}

async fn process_create_stream_command(
    front_end: &Frontend,
    replica: u8,
    ack_count: u8,
    retention: Duration,
    future: GlobalRef,
) {
    trace!("Start processing create_stream command");
    let options = StreamOptions {
        replica,
        ack: ack_count,
        retention,
    };
    let result = front_end.create(options).await;
    match result {
        Ok(stream_id) => {
            // complete_future_with_jlong(future, stream_id as i64);
            let tx = unsafe { CALLBACK_TX.get() }.unwrap();
            let _ = tx.send(CallbackCommand::CreateStream {
                future,
                stream_id: stream_id as i64,
            });
        }
        Err(err) => {
            // complete_future_with_error(future, err);
            let tx = unsafe { CALLBACK_TX.get() }.unwrap();
            let _ = tx.send(CallbackCommand::ClientError { future, err });
        }
    };
    trace!("Create_stream command finished");
}
/// # Safety
///
/// This function could be only called by java vm when onload this lib.
#[no_mangle]
pub extern "system" fn JNI_OnLoad(vm: JavaVM, _: *mut c_void) -> jint {
    crate::init_log();
    let java_vm = Arc::new(vm);
    let (tx, mut rx) = flume::unbounded();
    let (callback_tx, callback_rx) = unbounded();
    let _ = unsafe { TRACING_SERVICE.set(TracingService::new(Duration::from_millis(1))) };
    // # Safety
    // Set OnceCell, expected to be safe.
    if unsafe { TX.set(tx) }.is_err() {
        error!("Failed to set command channel sender");
    }
    if unsafe { CALLBACK_TX.set(callback_tx) }.is_err() {
        error!("Failed to set callback channel sender");
    }

    let mut env = java_vm.get_env().unwrap();
    let stream_path = "com/automq/elasticstream/client/jni/Stream";
    let void_path = "java/lang/Void";
    let jlong_path = "java/lang/Long";
    let completable_future_path = "java/util/concurrent/CompletableFuture";
    let stream_class = env.find_class(stream_path).unwrap();
    let stream_class: GlobalRef = env.new_global_ref(stream_class).unwrap();
    let stream_ctor = env.get_method_id(stream_path, "<init>", "(J)V").unwrap();

    let void_class = env.find_class(void_path).unwrap();
    let void_class: GlobalRef = env.new_global_ref(void_class).unwrap();
    let void_ctor = env.get_method_id(void_path, "<init>", "()V").unwrap();
    let jlong_class = env.find_class(jlong_path).unwrap();
    let jlong_class: GlobalRef = env.new_global_ref(jlong_class).unwrap();
    let jlong_ctor = env.get_method_id(jlong_path, "<init>", "(J)V").unwrap();
    let future_complete_method = env
        .get_method_id(completable_future_path, "complete", "(Ljava/lang/Object;)Z")
        .unwrap();

    let es_exception_path = "com/automq/elasticstream/client/api/ElasticStreamClientException";
    let es_exception_class = env.find_class(es_exception_path).unwrap();
    let es_exception_class = env.new_global_ref(es_exception_class).unwrap();
    let es_exception_ctor = env
        .get_method_id(es_exception_path, "<init>", "(ILjava/lang/String;)V")
        .unwrap();

    unsafe { STREAM_CLASS_CACHE.set(stream_class).unwrap() };
    unsafe { STREAM_CTOR_CACHE.set(stream_ctor).unwrap() };
    unsafe { VOID_CLASS_CACHE.set(void_class).unwrap() };
    unsafe { VOID_CTOR_CACHE.set(void_ctor).unwrap() };
    unsafe { JLONG_CLASS_CACHE.set(jlong_class).unwrap() };
    unsafe { JLONG_CTOR_CACHE.set(jlong_ctor).unwrap() };
    unsafe { FUTURE_COMPLETE_CACHE.set(future_complete_method).unwrap() };
    unsafe { ES_EXCEPTION_CLASS_CACHE.set(es_exception_class).unwrap() };
    unsafe { ES_EXCEPTION_CTOR_CACHE.set(es_exception_ctor).unwrap() };

    // Callback thread number is between [2, 4].
    let cpu_sum = std::cmp::max(std::cmp::min(num_cpus::get(), 4), 2);

    (0..cpu_sum).for_each(|i| {
        let java_vm_clone = java_vm.clone();
        let callback_rx_clone = callback_rx.clone();
        let _ = std::thread::Builder::new()
            .name(format!("CallBackThread-{}", i))
            .spawn(move || {
                JENV.with(|cell| {
                    if let Ok(env) = java_vm_clone.attach_current_thread_as_daemon() {
                        *cell.borrow_mut() = Some(env.get_raw());
                    } else {
                        error!("Failed to attach current thread as daemon");
                    }
                });
                loop {
                    match callback_rx_clone.recv() {
                        Ok(command) => match command {
                            CallbackCommand::Append {
                                future,
                                base_offset,
                                tracer,
                            } => {
                                let _stopwatch =
                                    Stopwatch::new("Append#callback", Duration::from_millis(1));
                                let _guard = tracer.set_local_parent();
                                complete_future_with_jlong(future, base_offset);
                            }
                            CallbackCommand::Read {
                                future,
                                buffers,
                                tracer,
                            } => {
                                let _stopwatch =
                                    Stopwatch::new("Read#callback", Duration::from_millis(1));
                                let _guard = tracer.set_local_parent();
                                complete_future_with_direct_byte_buffer(future, buffers);
                            }
                            CallbackCommand::CreateStream { future, stream_id } => {
                                complete_future_with_jlong(future, stream_id);
                            }
                            CallbackCommand::OpenStream { future, ptr } => {
                                complete_future_with_stream(future, ptr);
                            }
                            CallbackCommand::StartOffset { future, offset } => {
                                complete_future_with_jlong(future, offset);
                            }
                            CallbackCommand::NextOffset { future, offset } => {
                                complete_future_with_jlong(future, offset);
                            }
                            CallbackCommand::CloseStream { future } => {
                                complete_future_with_void(future);
                            }
                            CallbackCommand::ClientError { future, err } => {
                                complete_future_with_error(future, err);
                            }
                        },
                        Err(_) => {
                            info!("Callback channel is dropped");
                            break;
                        }
                    }
                }
            });
    });
    let _ = std::thread::Builder::new()
        .name("Runtime".to_string())
        .spawn(move || {
            trace!("JNI Runtime thread started");
            let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                trace!("JNI tokio-uring runtime started");
                loop {
                    match rx.recv_async().await {
                        Ok(cmd) => {
                            trace!("JNI tokio-uring receive command");
                            monoio::spawn(async move { process_command(cmd).await });
                        }
                        Err(_e) => {
                            info!("JNI command channel is dropped");
                            break;
                        }
                    }
                }
            });
        });
    JNI_VERSION_1_8
}

/// # Safety
///
/// This function could be only called by java vm when unload this lib.
#[no_mangle]
pub extern "system" fn JNI_OnUnload(_: JavaVM, _: *mut c_void) {
    unsafe { TX.take() };
}

// Frontend

#[no_mangle]
pub extern "system" fn Java_com_automq_elasticstream_client_jni_Frontend_getFrontend(
    mut env: JNIEnv,
    _class: JClass,
    access_point: JString,
) -> jlong {
    if let Ok(access_point) = env.get_string(&access_point) {
        let access_point: String = access_point.into();
        let result = Frontend::new(&access_point);
        match result {
            Ok(frontend) => Box::into_raw(Box::new(frontend)) as jlong,
            Err(err) => {
                throw_exception(&mut env, &err.to_string());
                0
            }
        }
    } else {
        info!("Failed to construct GetFrontend command. Ignore a GetFrontend request");
        0
    }
}

/// # Safety
///
/// Expose `C` API to Java
#[no_mangle]
pub unsafe extern "system" fn Java_com_automq_elasticstream_client_jni_Frontend_freeFrontend(
    mut _env: JNIEnv,
    _class: JClass,
    ptr: *mut Frontend,
) {
    // Take ownership of the pointer by wrapping it with a Box
    let _ = unsafe { Box::from_raw(ptr) };
}

/// # Safety
///
/// Expose `C` API to Java to free memory that is allocated in JNI
#[no_mangle]
pub unsafe extern "system" fn Java_com_automq_elasticstream_client_jni_Frontend_allocateDirect<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass,
    size: i32,
) -> JByteBuffer<'local> {
    // Safety: we use `alignment = 1` so it is power of 2.
    let layout = unsafe { Layout::from_size_align_unchecked(size as usize, 1) };

    // Safety: Java caller guarantee that the DirectByteBuffer is allocated by JNI when fetch records
    // and the backing memory segment is NOT deallocated before.
    let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
    env.new_direct_byte_buffer(ptr, size as usize).unwrap()
}

/// # Safety
///
/// Expose `C` API to Java to free memory that is allocated in JNI
#[no_mangle]
pub unsafe extern "system" fn Java_com_automq_elasticstream_client_jni_Frontend_freeMemory(
    mut _env: JNIEnv,
    _class: JClass,
    ptr: *mut u8,
    size: i32,
) {
    // Safety: we use `alignment = 1` so it is power of 2.
    let layout = unsafe { Layout::from_size_align_unchecked(size as usize, 1) };

    // Safety: Java caller guarantee that the DirectByteBuffer is allocated by JNI when fetch records
    // and the backing memory segment is NOT deallocated before.
    unsafe { std::alloc::dealloc(ptr, layout) };
}

/// # Safety
///
/// Expose `C` API to Java
#[no_mangle]
pub unsafe extern "system" fn Java_com_automq_elasticstream_client_jni_Frontend_create(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut Frontend,
    replica: jint,
    ack: jint,
    retention_millis: jlong,
    future: JObject,
) {
    let command = env.new_global_ref(future).map(|future| {
        let front_end = unsafe { &mut *ptr };
        Command::CreateStream {
            front_end,
            replica: replica as u8,
            ack_count: ack as u8,
            retention: Duration::from_millis(retention_millis as u64),
            future,
        }
    });
    if let Ok(command) = command {
        if let Some(tx) = unsafe { TX.get() } {
            if let Err(_e) = tx.send(command) {
                error!("Failed to dispatch CreateStream command to tokio-uring runtime");
                throw_exception(
                    &mut env,
                    "Failed to dispatch CreateStream command to tokio-uring runtime",
                );
            }
        } else {
            info!("JNI command channel was dropped. Ignore a CreateStream request");
            throw_exception(
                &mut env,
                "JNI command channel was dropped. Ignore a CreateStream request",
            );
        }
    } else {
        info!("Failed to construct CreateStream command");
        throw_exception(&mut env, "Failed to construct CreateStream command");
    };
}

/// # Safety
///
/// Expose `C` API to Java
#[no_mangle]
pub unsafe extern "system" fn Java_com_automq_elasticstream_client_jni_Frontend_open(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut Frontend,
    id: jlong,
    epoch: jlong,
    future: JObject,
) {
    debug_assert!(id >= 0, "Stream ID should be non-negative");
    let command = env.new_global_ref(future).map(|future| {
        let front_end = unsafe { &mut *ptr };
        Command::OpenStream {
            front_end,
            stream_id: id as u64,
            epoch: epoch as u64,
            future,
        }
    });
    if let Ok(command) = command {
        if let Some(tx) = unsafe { TX.get() } {
            if let Err(_e) = tx.send(command) {
                error!("Failed to dispatch OpenStream command to tokio-uring runtime");
                throw_exception(
                    &mut env,
                    "Failed to dispatch OpenStream command to tokio-uring runtime",
                );
            }
        } else {
            info!("JNI command channel was dropped. Ignore an OpenStream request");
            throw_exception(
                &mut env,
                "JNI command channel was dropped. Ignore an OpenStream request",
            );
        }
    } else {
        info!("Failed to construct OpenStream command");
        throw_exception(&mut env, "Failed to construct OpenStream command");
    }
}

/// # Safety
///
/// Expose `C` API to Java
#[no_mangle]
pub unsafe extern "system" fn Java_com_automq_elasticstream_client_jni_Stream_freeStream(
    _env: JNIEnv,
    _class: JClass,
    ptr: *mut Stream,
) {
    // Take ownership of the pointer by wrapping it with a Box
    let _ = Box::from_raw(ptr);
}

/// # Safety
///
/// Expose `C` API to Java
#[no_mangle]
pub unsafe extern "system" fn Java_com_automq_elasticstream_client_jni_Stream_asyncClose(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut Stream,
    future: JObject,
) {
    trace!("Started jni_Stream_asyncClose");
    let command = env.new_global_ref(future).map(|future| {
        let stream = unsafe { &mut *ptr };
        Command::CloseStream { stream, future }
    });
    if let Ok(command) = command {
        if let Some(tx) = unsafe { TX.get() } {
            if let Err(_e) = tx.send(command) {
                error!("Failed to dispatch CloseStream command to tokio-uring runtime");
                throw_exception(
                    &mut env,
                    "Failed to dispatch CloseStream command to tokio-uring runtime",
                );
            } else {
                trace!("Dispatched the CloseStream command to tokio-uring runtime");
            }
        } else {
            info!("JNI command channel was dropped. Ignore a CloseStream request");
            throw_exception(
                &mut env,
                "JNI command channel was dropped. Ignore a CloseStream request",
            );
        }
    } else {
        info!("Failed to construct CloseStream command.");
        throw_exception(&mut env, "Failed to construct CloseStream command.");
    }
}

/// # Safety
///
/// Expose `C` API to Java
#[no_mangle]
pub unsafe extern "system" fn Java_com_automq_elasticstream_client_jni_Stream_startOffset(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut Stream,
    future: JObject,
) {
    trace!("Started jni_Stream_startOffset");
    let command = env.new_global_ref(future).map(|future| {
        let stream = unsafe { &mut *ptr };
        Command::StartOffset { stream, future }
    });
    if let Ok(command) = command {
        if let Some(tx) = unsafe { TX.get() } {
            if let Err(_e) = tx.send(command) {
                error!("Failed to dispatch StartOffset command to tokio-uring runtime");
                throw_exception(
                    &mut env,
                    "Failed to dispatch StartOffset command to tokio-uring runtime",
                );
            } else {
                trace!("Dispatched the StartOffset command to tokio-uring runtime");
            }
        } else {
            info!("JNI command channel was dropped. Ignore a StartOffset request");
            throw_exception(
                &mut env,
                "JNI command channel was dropped. Ignore a StartOffset request",
            );
        }
    } else {
        info!("Failed to construct StartOffset command.");
        throw_exception(&mut env, "Failed to construct StartOffset command.");
    }
}

/// # Safety
///
/// Expose `C` API to Java
#[no_mangle]
pub unsafe extern "system" fn Java_com_automq_elasticstream_client_jni_Stream_nextOffset(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut Stream,
    future: JObject,
) {
    trace!("Started jni_Stream_nextOffset");
    let command = env.new_global_ref(future).map(|future| {
        let stream = unsafe { &mut *ptr };
        Command::NextOffset { stream, future }
    });
    if let Ok(command) = command {
        if let Some(tx) = unsafe { TX.get() } {
            if let Err(_e) = tx.send(command) {
                error!("Failed to dispatch NextOffset command to tokio-uring runtime");
                throw_exception(
                    &mut env,
                    "Failed to dispatch NextOffset command to tokio-uring runtime",
                );
            } else {
                trace!("Dispatched the NextOffset command to tokio-uring runtime");
            }
        } else {
            info!("JNI command channel was dropped. Ignore a NextOffset request");
            throw_exception(
                &mut env,
                "JNI command channel was dropped. Ignore a NextOffset request",
            );
        }
    } else {
        info!("Failed to construct NextOffset command");
        throw_exception(&mut env, "Failed to construct NextOffset command");
    }
}

/// # Safety
///
/// Expose `C` API to Java
#[no_mangle]
pub unsafe extern "system" fn Java_com_automq_elasticstream_client_jni_Stream_append(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut Stream,
    data: JObject,
    future: JObject,
) {
    let tracing_service = TRACING_SERVICE.get().unwrap();
    let tracer = tracing_service.new_tracer("root#append");
    let _guard = tracer.set_local_parent();
    trace!("Started jni_Stream_append");
    let buf = {
        let _span = tracer.get_child_span_with_local_parent("env.get_direct_buffer_address()");
        env.get_direct_buffer_address((&data).into())
    };
    let len = {
        let _span = tracer.get_child_span_with_local_parent("env.get_direct_buffer_capacity()");
        env.get_direct_buffer_capacity((&data).into())
    };
    let future = {
        let _span = tracer.get_child_span_with_local_parent("env.new_global_ref()");
        env.new_global_ref(future)
    };
    let command: Result<Command, ()> = match (buf, len, future) {
        (Ok(buf), Ok(len), Ok(future)) => {
            let _span = tracer.get_child_span_with_local_parent("Create an append command");
            let stream = &mut *ptr;
            // # Safety
            // Java caller guarantees that `buf` will remain valid until `Future#complete` is called.
            // As a result, we can safely treat the slice as 'static.
            let slice = unsafe { slice::from_raw_parts(buf, len) };
            let buf = Bytes::from_static(slice);
            Ok(Command::Append {
                stream,
                buf,
                future,
                tracer,
            })
        }
        _ => Err(()),
    };
    if let Ok(command) = command {
        if let Some(tx) = TX.get() {
            if let Err(_e) = tx.send(command) {
                error!("Failed to dispatch Append command to tokio-uring runtime");
                throw_exception(
                    &mut env,
                    "Failed to dispatch Append command to tokio-uring runtime",
                );
            } else {
                trace!("Dispatched the Append command to tokio-uring runtime");
            }
        } else {
            info!("JNI command channel was dropped. Ignore an Append request");
            throw_exception(
                &mut env,
                "JNI command channel was dropped. Ignore an Append request",
            );
        }
    } else {
        info!("Failed to construct Append command");
        throw_exception(&mut env, "Failed to construct Append command");
    }
    trace!("Finished jni_Stream_append");
}

/// # Safety
///
/// Expose `C` API to Java
#[no_mangle]
pub unsafe extern "system" fn Java_com_automq_elasticstream_client_jni_Stream_read(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut Stream,
    start_offset: jlong,
    end_offset: jlong,
    batch_max_bytes: jint,
    future: JObject,
) {
    let tracing_service = TRACING_SERVICE.get().unwrap();
    let tracer = tracing_service.new_tracer("root#read");
    let _guard = tracer.set_local_parent();
    trace!("Started jni_Stream_read");
    let command = {
        let _span = tracer.get_child_span_with_local_parent("env.new_global_ref()");
        env.new_global_ref(future).map(|future| {
            let stream = unsafe { &mut *ptr };
            Command::Read {
                stream,
                start_offset,
                end_offset,
                batch_max_bytes,
                future,
                tracer,
            }
        })
    };
    if let Ok(command) = command {
        if let Some(tx) = unsafe { TX.get() } {
            if let Err(_e) = tx.send(command) {
                error!("Failed to dispatch Read command to tokio-uring runtime");
                throw_exception(
                    &mut env,
                    "Failed to dispatch Read command to tokio-uring runtime",
                );
            } else {
                trace!("Dispatched the Read command to tokio-uring runtime");
            }
        } else {
            info!("JNI command channel was dropped. Ignore a Read request");
            throw_exception(
                &mut env,
                "JNI command channel was dropped. Ignore a Read request",
            );
        }
    } else {
        info!("Failed to construct Read command");
        throw_exception(&mut env, "Failed to construct Read command");
    }
}

#[minitrace::trace("call_future_complete_method()")]
fn call_future_complete_method(mut env: JNIEnv, future: GlobalRef, obj: JObject) {
    let obj = env.auto_local(obj);
    let s = JValueGen::from(&obj);
    let _stopwatch = Stopwatch::new("Future#complete", Duration::from_millis(1));
    let method = unsafe { FUTURE_COMPLETE_CACHE.get() }.unwrap();
    if unsafe {
        env.call_method_unchecked(
            future,
            method,
            jni::signature::ReturnType::Primitive(jni::signature::Primitive::Boolean),
            &[s.as_jni()],
        )
    }
    .is_err()
    {
        panic!("Failed to call future complete method");
    }
}

fn call_future_complete_exceptionally_method(env: &mut JNIEnv, future: GlobalRef, err: EsError) {
    let obj = unsafe {
        let error_message = match env.new_string(err.message) {
            Ok(s) => s,
            Err(_) => panic!("Failed to create error message"),
        };
        env.new_object_unchecked(
            ES_EXCEPTION_CLASS_CACHE.get().unwrap(),
            *ES_EXCEPTION_CTOR_CACHE.get().unwrap(),
            &[
                JValue::Int(err.code.0 as i32).as_jni(),
                JValue::Object(error_message.as_ref()).as_jni(),
            ],
        )
    };
    if let Ok(obj) = obj {
        let s = JValueGen::from(obj);
        let _stopwatch = Stopwatch::new("Future#completeExceptionally", Duration::from_millis(1));
        if env
            .call_method(
                future,
                "completeExceptionally",
                "(Ljava/lang/Throwable;)Z",
                &[s.borrow()],
            )
            .is_err()
        {
            panic!("Failed to call future completeExceptionally method");
        }
    } else {
        panic!("Failed to create exception object");
    }
}

fn get_thread_local_jenv(cell: &RefCell<Option<*mut *const JNINativeInterface_>>) -> JNIEnv {
    let env_ptr = cell
        .borrow()
        .expect("Couldn't get raw ptr of thread local jenv");
    unsafe { JNIEnv::from_raw(env_ptr).expect("Couldn't create a JNIEnv from raw pointer") }
}

fn throw_exception(env: &mut JNIEnv, msg: &str) {
    let exception_path =
        "com/automq/elasticstream/client/api/ElasticStreamClientException$Internal";
    let _ = env.exception_clear();
    if env.throw_new(exception_path, msg).is_err() {
        panic!("Failed to throw new exception");
    }
}

fn complete_future_with_stream(future: GlobalRef, ptr: i64) {
    JENV.with(|cell| {
        let mut env = get_thread_local_jenv(cell);
        let stream_class = unsafe { STREAM_CLASS_CACHE.get() };
        let stream_ctor = unsafe { STREAM_CTOR_CACHE.get() };
        if let (Some(stream_class), Some(stream_ctor)) = (stream_class, stream_ctor) {
            if let Ok(obj) = unsafe {
                env.new_object_unchecked(
                    stream_class,
                    *stream_ctor,
                    &[jni::objects::JValue::Long(ptr).as_jni()],
                )
            } {
                call_future_complete_method(env, future, obj);
            } else {
                panic!("Couldn't create Stream object");
            }
        } else {
            panic!("Couldn't find Stream class");
        }
    });
}

#[minitrace::trace("complete_future_with_jlong()")]
fn complete_future_with_jlong(future: GlobalRef, value: i64) {
    JENV.with(|cell| {
        let mut env = get_thread_local_jenv(cell);
        let long_class = unsafe { JLONG_CLASS_CACHE.get() };
        let long_ctor = unsafe { JLONG_CTOR_CACHE.get() };
        if let (Some(long_class), Some(long_ctor)) = (long_class, long_ctor) {
            if let Ok(obj) = unsafe {
                {
                    let _span = Span::enter_with_local_parent("env.new_object_unchecked()");
                    env.new_object_unchecked(
                        long_class,
                        *long_ctor,
                        &[jni::objects::JValue::Long(value).as_jni()],
                    )
                }
            } {
                call_future_complete_method(env, future, obj);
            } else {
                panic!("Failed to create Long object");
            }
        } else {
            panic!("Failed to find Long object");
        }
    });
}

fn complete_future_with_void(future: GlobalRef) {
    JENV.with(|cell| {
        let mut env = get_thread_local_jenv(cell);
        let void_class = unsafe { VOID_CLASS_CACHE.get() };
        let void_ctor = unsafe { VOID_CTOR_CACHE.get() };
        if let (Some(void_class), Some(void_ctor)) = (void_class, void_ctor) {
            if let Ok(obj) = unsafe { env.new_object_unchecked(void_class, *void_ctor, &[]) } {
                call_future_complete_method(env, future, obj);
            } else {
                panic!("Failed to create Void object");
            }
        } else {
            panic!("Failed to find Void class");
        }
    });
}

#[allow(unused)]
fn complete_future_with_byte_array(future: GlobalRef, buffers: Vec<Bytes>) {
    let total: usize = buffers.iter().map(|buf| buf.len()).sum();
    JENV.with(|cell| {
        let env = get_thread_local_jenv(cell);
        let byte_array = env.new_byte_array(total as i32);
        let mut p: usize = 0;
        if let Ok(byte_array) = byte_array {
            {
                buffers.iter().for_each(|buf| {
                    let slice = buf.as_ref();
                    let slice: &[i8] = unsafe {
                        std::slice::from_raw_parts(slice.as_ptr() as *const i8, slice.len())
                    };
                    let _ = env.set_byte_array_region(&byte_array, p as i32, slice);
                    p += buf.len();
                });
            }
            call_future_complete_method(env, future, JObject::from(byte_array));
        } else {
            complete_future_with_error(future, EsError::unexpected("Failed to create byte array"));
            error!("Failed to create byte array");
        }
    });
}

fn complete_future_with_error(future: GlobalRef, err: EsError) {
    JENV.with(|cell| {
        let mut env = get_thread_local_jenv(cell);
        call_future_complete_exceptionally_method(&mut env, future, err);
    });
}

#[minitrace::trace("complete_future_with_direct_byte_buffer()")]
fn complete_future_with_direct_byte_buffer(future: GlobalRef, buffers: Vec<Bytes>) {
    // Copy buffers to `DirectByteBuffer`
    let total = buffers.iter().map(|buf| buf.len()).sum();
    if let Ok(layout) = Layout::from_size_align(total, 1) {
        // # Safety
        // It should always be safe to allocate memory with alignment of 1 unless the system
        // is running out of memory.
        let ptr = unsafe { std::alloc::alloc(layout) };
        let mut p = 0;
        buffers.iter().for_each(|buf| {
            // # Safety
            // We are copying slices from store to continuous memory for DirectByteBuffer. This
            // is definitely a non-overlapping copy and thus safe.
            //
            // Note DirectByteBuffer is responsible of returning the allocated memory.
            unsafe { std::ptr::copy_nonoverlapping(buf.as_ptr(), ptr.add(p), buf.len()) };
            p += buf.len();
        });
        JENV.with(|cell| {
            let mut env = get_thread_local_jenv(cell);
            // # Safety
            // Standard JNI call.
            if let Ok(obj) = unsafe { env.new_direct_byte_buffer(ptr, total) } {
                call_future_complete_method(env, future, JObject::from(obj));
            } else {
                complete_future_with_error(
                    future,
                    EsError::unexpected("Failed to create a new direct_byte_buffer"),
                );
                error!("Failed to create a new direct_byte_buffer");
            }
        });
    } else {
        complete_future_with_error(future, EsError::unexpected("Bad alignment"));
        error!("Bad alignment");
    }
}
