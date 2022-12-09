use std::{fmt::Display, future::Future, iter::Enumerate};

pub trait Service<Request>: Clone {
    type Response;

    type Error: Display;

    type Future<'cx>: Future<Output = Result<Self::Response, Self::Error>>
    where
        Self: 'cx;

    /// Process the request and return the reponse asynchronously.
    fn call(&mut self, req: Request) -> Self::Future<'_>;
}

pub trait Layer<S> {
    type Service;

    fn layer(&self, service: S) -> Self::Service;
}

pub struct ServiceList<S>
where
    S: IntoIterator,
{
    inner: Enumerate<S::IntoIter>,
}

type DefaultServiceList<S> = ServiceList<Vec<S>>;

pub struct ServiceBuilder<L> {
    layer: L,
}

#[derive(Default)]
pub struct Identity {
    _p: (),
}

impl Identity {
    pub fn new() -> Self {
        Self { _p: () }
    }
}

impl<S> Layer<S> for Identity {
    type Service = S;

    fn layer(&self, inner: S) -> Self::Service {
        inner
    }
}

impl ServiceBuilder<Identity> {
    pub fn new() -> Self {
        Self {
            layer: Identity::new(),
        }
    }
}

impl Default for ServiceBuilder<Identity> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Stack<I, O> {
    pub(crate) inner: I,
    pub(crate) outer: O,
}

impl<I, O> Stack<I, O> {
    pub fn new(inner: I, outer: O) -> Self {
        Self { inner, outer }
    }
}

impl<S, I, O> Layer<S> for Stack<I, O>
where
    I: Layer<S>,
    O: Layer<I::Service>,
{
    type Service = O::Service;

    fn layer(&self, service: S) -> Self::Service {
        let inner = self.inner.layer(service);
        self.outer.layer(inner)
    }
}

impl<L> ServiceBuilder<L> {
    pub fn layer<T>(self, s: T) -> ServiceBuilder<Stack<T, L>> {
        ServiceBuilder {
            layer: Stack::new(s, self.layer),
        }
    }

    pub fn service<S>(&self, s: S) -> L::Service
    where
        L: Layer<S>,
    {
        self.layer.layer(s)
    }
}

#[cfg(test)]
mod tests {

    #[derive(Clone, Debug, Copy)]
    struct LogService {}

    use std::pin::Pin;

    use thiserror::Error;

    use crate::{Service, ServiceBuilder};

    #[derive(Error, Debug)]
    enum TestError {
        #[error("Sample")]
        Item1,
    }

    #[derive(Debug)]
    struct TestRequest {}

    #[derive(Debug, PartialEq)]
    struct TestResposne {
        val: usize,
    }

    impl<TestRequest> crate::Service<TestRequest> for LogService {
        type Response = TestResposne;

        type Error = TestError;

        type Future<'cx> =
            Pin<Box<dyn std::future::Future<Output = Result<Self::Response, TestError>>>>;

        fn call(&mut self, _req: TestRequest) -> Self::Future<'_> {
            let fut = futures::future::ready(Ok::<_, TestError>(TestResposne { val: 1 }));
            Box::pin(fut)
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct FixService<S> {
        inner: S,
    }

    // impl<R, S> FixService<S>
    // where
    //     S: crate::Service<R>,
    // {
    //     fn new(inner: S) -> Self {
    //         inner
    //     }
    // }

    // impl<TestRequest> crate::Service<TestRequest> for FixService {
    //     type Response = TestResposne;

    //     type Error = TestError;

    //     type Future<'cx> =
    //         Pin<Box<dyn std::future::Future<Output = Result<Self::Response, TestError>>>>;

    //     fn call(&mut self, req: TestRequest) -> Self::Future<'_> {
    //         todo!()
    //     }
    // }

    #[monoio_macros::test]
    async fn test_service_builder() {
        let svc = LogService {};
        let req = TestRequest {};
        let mut s = ServiceBuilder::new().service(svc);
        assert_eq!(TestResposne { val: 1 }, s.call(req).await.unwrap());
    }
}
