use codec::frame::Frame;

pub trait Notifier {
    fn on_notification(&self, frame: Frame) -> Frame;
}

pub struct UnsupportedNotifier {}

impl Notifier for UnsupportedNotifier {
    fn on_notification(&self, frame: Frame) -> Frame {
        let mut response = Frame::new(frame.operation_code);
        response.flag_response();
        response.flag_unsupported();
        response
    }
}
