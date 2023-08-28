---
sidebar_position: 2
---

# Trace

The tracing system allows developers to visualize io flows to locate the problem.

Elastic Stream uses OpenTracing, an open standard designed for distributed tracing and supports OpenTelemetry as tracing backends:

## OpenTelemetry

To enable the OpenTelemetry trace exporter:

```yaml
observation:
  trace:
    enable: false
    endpoint: "http://localhost:4317"
```

### Configuration

#### enable

Required, Default="false"

Enable trace exporter.

```yaml
observation:
  trace:
    enable: true
```

#### endpoint

Required, Default="http://localhost:4317", Format="`http://<host>:<port>`"

Address of the OpenTelemetry Collector to send spans to.

```yaml
observation:
  trace:
    endpoint: "http://localhost:4317"
```

#### protocol

Optional, Default="grpc"

This instructs the exporter to send spans to the OpenTelemetry Collector using which protocol.

Currently, only gRPC with plain text is supported.

```yaml
observation:
  trace:
    protocol: "grpc"
```

#### timeout

Optional, Default="3"

Max waiting time for exporting spans to the OpenTelemetry Collector, unit is seconds.

```yaml
observation:
  trace:
    timeout: 3
```