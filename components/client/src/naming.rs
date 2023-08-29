use std::{
    io::Result,
    net::{SocketAddr, ToSocketAddrs},
};

const DNS_PREFIX: &str = "dns:";
const IPV4_PREFIX: &str = "ipv4:";
const IPV6_PREFIX: &str = "ipv6:";

pub(crate) struct Naming {
    name: String,
}

impl Naming {
    #[allow(dead_code)]
    pub(crate) fn new(name: String) -> Self {
        Self { name }
    }

    fn parse_csv(csv: &str, endpoints: &mut Vec<SocketAddr>) -> Result<()> {
        let mut remain = csv;
        loop {
            match remain.find(',') {
                Some(pos) => {
                    let (host_port, remain_) = remain.split_at(pos);
                    for socket_addr in host_port.trim().to_socket_addrs()? {
                        endpoints.push(socket_addr);
                    }
                    if let Some(r) = remain_.strip_prefix(',') {
                        remain = r;
                    }
                }

                None => {
                    if !remain.is_empty() {
                        for socket_addr in remain.trim().to_socket_addrs()? {
                            endpoints.push(socket_addr);
                        }
                    }
                    break;
                }
            }
        }
        Ok(())
    }
}

pub(crate) struct NamingIter {
    endpoints: Vec<SocketAddr>,
}

impl NamingIter {
    pub(crate) fn new(endpoints: Vec<SocketAddr>) -> Self {
        Self { endpoints }
    }
}

impl Iterator for NamingIter {
    type Item = SocketAddr;

    fn next(&mut self) -> Option<Self::Item> {
        self.endpoints.pop()
    }
}

impl ToSocketAddrs for Naming {
    type Iter = NamingIter;

    fn to_socket_addrs(&self) -> Result<Self::Iter> {
        let endpoints = vec![];
        let mut iter = NamingIter::new(endpoints);
        if self.name.starts_with(DNS_PREFIX) {
            let (_prefix, host_port) = self.name.as_str().split_at(DNS_PREFIX.len());
            for socket_addr in host_port.to_socket_addrs()? {
                iter.endpoints.push(socket_addr);
            }
        } else if self.name.starts_with(IPV4_PREFIX) {
            let (_prefix, csv) = self.name.as_str().split_at(IPV4_PREFIX.len());
            Self::parse_csv(csv, &mut iter.endpoints)?;
        } else if self.name.starts_with(IPV6_PREFIX) {
            let (_prefix, csv) = self.name.as_str().split_at(IPV6_PREFIX.len());
            Self::parse_csv(csv, &mut iter.endpoints)?;
        } else {
            Self::parse_csv(&self.name, &mut iter.endpoints)?;
        }
        Ok(iter)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs},
    };

    #[test]
    fn test_dns_scheme() -> Result<(), Box<dyn Error>> {
        let naming = super::Naming::new("dns:www.baidu.com:80".to_owned());
        let addrs = naming.to_socket_addrs()?.collect::<Vec<_>>();
        assert!(!addrs.is_empty());
        Ok(())
    }

    #[test]
    fn test_ipv4_scheme() -> Result<(), Box<dyn Error>> {
        let naming = super::Naming::new("ipv4:10.0.0.1:80".to_owned());
        let addrs = naming.to_socket_addrs()?.collect::<Vec<_>>();
        assert_eq!(
            addrs,
            vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 80)]
        );

        let naming = super::Naming::new("ipv4:10.0.0.1:80,10.0.0.2:80,10.0.0.3:80".to_owned());
        let mut addrs = naming.to_socket_addrs()?.collect::<Vec<_>>();
        addrs.sort();
        assert_eq!(
            addrs,
            vec![
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 80),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 80),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)), 80),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_default_scheme() -> Result<(), Box<dyn Error>> {
        let naming = super::Naming::new("www.baidu.com:80".to_owned());
        let mut addrs = naming.to_socket_addrs()?.collect::<Vec<_>>();
        addrs.sort();

        let mut expected = "www.baidu.com:80".to_socket_addrs()?.collect::<Vec<_>>();
        expected.sort();
        assert_eq!(addrs, expected);

        let naming = super::Naming::new("10.0.0.1:80".to_owned());
        let addrs = naming.to_socket_addrs()?.collect::<Vec<_>>();
        assert_eq!(
            addrs,
            vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 80)]
        );

        let naming = super::Naming::new("10.0.0.1:80,10.0.0.2:80,10.0.0.3:80".to_owned());
        let mut addrs = naming.to_socket_addrs()?.collect::<Vec<_>>();
        addrs.sort();
        assert_eq!(
            addrs,
            vec![
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 80),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 80),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)), 80),
            ]
        );
        Ok(())
    }
}
