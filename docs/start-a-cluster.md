# Start a Elastic Stream Cluster

## Overview

Starting an Elastic Stream cluster requires that each PD node knows each other and every Range Server knows at least one PD node. So we need to configure the addresses when starting up the cluster.

In this tutorial, we will start a Elastic Stream cluster with 3 machines. We will use the following IP addresses as an example:
- node1: 10.0.0.1
- node2: 10.0.0.2
- node3: 10.0.0.3

Note: Each PD node needs 3 ports to communicate with each other, and each Range Server needs 1 to serve requests.
In this example, we will use 12378, 12379, 12380 and 10911. You can change the ports to any other ports you like. But make sure these ports are accessible by other machines.

## Step 1: Install PD and Range Server

On each machine, download deb files from [latest release](https://github.com/AutoMQ/elastic-stream/releases/latest). And install them with the following command:

```shell
# Please change the file name to match the version you downloaded and the architecture of your machine.
dpkg -i pd_x.x.x_amd64.deb
dpkg -i range-server_x.x.x_amd64.deb
```

## Step 2: Start PD

On node1, start PD with the following command:

```shell
ip1=10.0.0.1;
ip2=10.0.0.2;
ip3=10.0.0.3;
/usr/local/bin/pd \
    --name pd1 \
    --data-dir /tmp/pd1 \
    --pd-addr $ip1:12378 \
    --advertise-pd-addr $ip1:12378 \
    --client-urls http://$ip1:12379 \
    --advertise-client-urls http://$ip1:12379 \
    --peer-urls http://$ip1:12380 \
    --advertise-peer-urls http://$ip1:12380 \
    --initial-cluster pd1=http://$ip1:12380,pd2=http://$ip2:12380,pd3=http://$ip3:12380
```

On node2:

```shell
ip1=10.0.0.1;
ip2=10.0.0.2;
ip3=10.0.0.3;
/usr/local/bin/pd \
    --name pd2 \
    --data-dir /tmp/pd2 \
    --pd-addr $ip2:12378 \
    --advertise-pd-addr $ip2:12378 \
    --client-urls http://$ip2:12379 \
    --advertise-client-urls http://$ip2:12379 \
    --peer-urls http://$ip2:12380 \
    --advertise-peer-urls http://$ip2:12380 \
    --initial-cluster pd1=http://$ip1:12380,pd2=http://$ip2:12380,pd3=http://$ip3:12380
```

On node3:

```shell
ip1=10.0.0.1;
ip2=10.0.0.2;
ip3=10.0.0.3;
/usr/local/bin/pd \
    --name pd3 \
    --data-dir /tmp/pd3 \
    --pd-addr $ip3:12378 \
    --advertise-pd-addr $ip3:12378 \
    --client-urls http://$ip3:12379 \
    --advertise-client-urls http://$ip3:12379 \
    --peer-urls http://$ip3:12380 \
    --advertise-peer-urls http://$ip3:12380 \
    --initial-cluster pd1=http://$ip1:12380,pd2=http://$ip2:12380,pd3=http://$ip3:12380
```

To check whether the PD is started successfully, you can use the following command:

```shell
ip1=10.0.0.1;
ip2=10.0.0.2;
ip3=10.0.0.3;
echo -e '\x1dclose\x0d' | telnet $ip1 12378
echo -e '\x1dclose\x0d' | telnet $ip2 12378
echo -e '\x1dclose\x0d' | telnet $ip3 12378
```

If exit code is 0, then the PD is started successfully.

## Step 3: Start Range Server

On node1, start Range Server with the following command:

```shell
ip1=10.0.0.1;
ip2=10.0.0.2;
ip3=10.0.0.3;
/usr/local/bin/range-server start \
    --store-path /tmp/rs1 \
    --addr $ip1:10911 \
    --advertise-addr $ip1:10911 \
    --pd $ip1:12378 \
    --config /etc/range-server/range-server.yaml \
    --log /etc/range-server/range-server-log.yaml
```

On node2:

```shell
ip1=10.0.0.1;
ip2=10.0.0.2;
ip3=10.0.0.3;
/usr/local/bin/range-server start \
    --store-path /tmp/rs2 \
    --addr $ip2:10911 \
    --advertise-addr $ip2:10911 \
    --pd $ip2:12378 \
    --config /etc/range-server/range-server.yaml \
    --log /etc/range-server/range-server-log.yaml
```

On node3:

```shell
ip1=10.0.0.1;
ip2=10.0.0.2;
ip3=10.0.0.3;
/usr/local/bin/range-server start \
    --store-path /tmp/rs3 \
    --addr $ip3:10911 \
    --advertise-addr $ip3:10911 \
    --pd $ip3:12378 \
    --config /etc/range-server/range-server.yaml \
    --log /etc/range-server/range-server-log.yaml
```

To check whether the Range Server is started successfully, you can use the following command:

```shell
ip1=10.0.0.1;
ip2=10.0.0.2;
ip3=10.0.0.3;
echo -e '\x1dclose\x0d' | telnet $ip1 10911
echo -e '\x1dclose\x0d' | telnet $ip2 10911
echo -e '\x1dclose\x0d' | telnet $ip3 10911
```

If exit code is 0, then the Range Server is started successfully.
