use std::{
    net::{SocketAddr, ToSocketAddrs},
    str::FromStr,
};

use crate::error::ClientError;

#[derive(Debug)]
pub struct Endpoints {
    pub(super) addrs: Vec<SocketAddr>,
    index: usize,
}

impl Endpoints {
    fn new(addrs: Vec<SocketAddr>) -> Self {
        Self {
            addrs: addrs,
            index: 0,
        }
    }

    pub(crate) fn get(&mut self) -> Option<&SocketAddr> {
        if self.addrs.is_empty() {
            return None;
        }

        let addr = &self.addrs[self.index];
        if self.addrs.len() == self.index + 1 {
            self.index = 0;
        } else {
            self.index += 1;
        }
        Some(addr)
    }
}

const DNS_PREFIX: &'static str = "dns:";
const IPV4_PREFIX: &'static str = "ipv4:";
const IPV6_PREFIX: &'static str = "ipv6:";

impl FromStr for Endpoints {
    type Err = ClientError;

    /// Name resolution roughly conforms to the following URI scheme: https://github.com/grpc/grpc/blob/master/doc/naming.md
    ///
    /// Typical valid addresses are:
    /// - `dns:host:port`
    /// - `ipv4:address:port[,address:port,...]`
    /// - `ipv6:address:port[,address:port,...]`
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with(DNS_PREFIX) {
            let host_port = &s[DNS_PREFIX.len()..];
            let addrs: Vec<_> = host_port
                .to_socket_addrs()
                .map_err(|_e| ClientError::BadAddress)?
                .collect();
            return Ok(Self::new(addrs));
        }

        if s.starts_with(IPV4_PREFIX) {
            let ip_port_list = &s[IPV4_PREFIX.len()..];
            let addrs: Vec<_> = ip_port_list
                .split(',')
                .map_while(|addr| match addr.trim().to_socket_addrs() {
                    Ok(mut addrs) => addrs.nth(0),
                    Err(_e) => None,
                })
                .collect();
            return Ok(Self::new(addrs));
        }

        if s.starts_with(IPV6_PREFIX) {
            let ip_port_list = &s[IPV6_PREFIX.len()..];
            let addrs: Vec<_> = ip_port_list
                .split(',')
                .map_while(|addr| match addr.trim().to_socket_addrs() {
                    Ok(mut addrs) => addrs.nth(0),
                    Err(_e) => None,
                })
                .collect();
            return Ok(Self::new(addrs));
        }

        unreachable!()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_dns() -> Result<(), ClientError> {
        let connect_string = "dns:www.baidu.com:80";
        let endpoints = Endpoints::from_str(connect_string)?;
        assert_eq!(false, endpoints.addrs.is_empty());
        Ok(())
    }

    #[test]
    fn test_bad_address() -> Result<(), ClientError> {
        let connect_string = "dns:non-existing-domain-name.io:9876";
        match Endpoints::from_str(connect_string) {
            Ok(_) => {
                panic!("Should have reported bad address");
            }
            Err(e) => {
                assert_eq!(ClientError::BadAddress, e);
            }
        }
        Ok(())
    }

    #[test]
    fn test_ipv4() -> Result<(), ClientError> {
        let ip1 = "104.193.88.123";
        let ip2 = "104.193.88.77";
        let connect_string = format!("ipv4:{}:443,{}:80", ip1, ip2);

        let endpoints = Endpoints::from_str(&connect_string)?;

        assert_eq!(2, endpoints.addrs.len());
        assert_eq!(ip1, endpoints.addrs[0].ip().to_string());
        assert_eq!(ip2, endpoints.addrs[1].ip().to_string());
        Ok(())
    }

    #[test]
    fn test_get_none() -> Result<(), ClientError> {
        let mut endpoints = Endpoints::from_str("ipv4:")?;
        let addr = endpoints.get();
        assert_eq!(None, addr);
        Ok(())
    }

    #[test]
    fn test_get() -> Result<(), ClientError> {
        let host = "8.8.8.8";
        let connect_string = format!("ipv4:{}:53", host);
        let mut endpoints = Endpoints::from_str(&connect_string)?;
        let addr = endpoints.get();
        assert_eq!(host, addr.ok_or(ClientError::BadAddress)?.ip().to_string());
        assert_eq!(host, addr.ok_or(ClientError::BadAddress)?.ip().to_string());
        Ok(())
    }
}
