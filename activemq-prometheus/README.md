<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# ActiveMQ Prometheus Metrics

## Activation

The metrics web application ships with the broker but is not loaded by default.

1. In `conf/jetty-spring.properties`, uncomment `jettyExtraXmlFiles=jetty-webapp-prometheus.xml`.
2. Restart the broker.

The endpoint uses the existing Jetty management listener, TLS configuration,
IP allowlist, and JAAS realm. Access follows the console's catch-all `/*` rule
in `conf/jetty/jetty-security.xml`, which allows the `users` and `admins` roles.
To restrict it further, add a `/metrics/*` rule there.

The endpoint scrapes every JMX-enabled broker registered in the same JVM. It
uses each broker's own MBean name, so a custom `jmxDomainName` works.

## Endpoints

Two endpoints because brokers with many destinations might produce large responses:
- `GET /metrics`: broker-level metrics only.
- `GET /metrics?per_object=true`: broker-level metrics plus one series per destination.

`per_object` must be `true` or `false`, given once. Any other value, or a
repeated parameter, returns `400 Bad Request`.

### Destination types

The destination types reported by `per_object=true` are configured with the
`destinationTypes` init parameter: a comma separated list of `queue`, `topic`,
`temp-queue`, `temp-topic`. The default is `queue,topic`; temporary destinations
are short lived and usually not worth a time series. Set it on the web
application in `conf/jetty/jetty-webapp-prometheus.xml`:

```xml
<Call name="setInitParameter">
  <Arg>destinationTypes</Arg>
  <Arg>queue,topic,temp-queue,temp-topic</Arg>
</Call>
```

An unknown value fails deployment of the web application.

## Metrics

### Broker metrics (`activemq_broker_*`)

| Metric | Type | Description |
|--------|------|-------------|
| `current_connections` | gauge | Current number of connections |
| `connections_total` | counter | Total connections since last start |
| `messages_enqueued_total` | counter | Total messages enqueued since last start |
| `messages_dequeued_total` | counter | Total messages dequeued since last start |
| `consumers` | gauge | Current number of consumers |
| `producers` | gauge | Current number of producers |
| `messages` | gauge | Current number of messages across all destinations |
| `memory_percent_usage` | gauge | Percent of memory limit used |
| `memory_limit_bytes` | gauge | Memory limit in bytes |
| `store_percent_usage` | gauge | Percent of store limit used |
| `store_limit_bytes` | gauge | Store limit in bytes |
| `temp_percent_usage` | gauge | Percent of temp limit used |
| `temp_limit_bytes` | gauge | Temp limit in bytes |
| `uptime_milliseconds` | gauge | Broker uptime in milliseconds |
| `queues` | gauge | Number of queues on the broker |
| `topics` | gauge | Number of topics on the broker |
| `job_scheduler_store_percent_usage` | gauge | Percent of job scheduler store limit used |
| `job_scheduler_store_limit_bytes` | gauge | Job scheduler store limit in bytes |

### Destination metrics (`activemq_destination_*`)

Returned only when `?per_object=true` is set. Every series carries the labels
`broker`, `destination_type` (`queue`, `topic`, `temp-queue` or `temp-topic`)
and `destination`, so one metric family covers all destination types:

```
activemq_destination_messages{broker="localhost",destination_type="queue",destination="orders"} 12.0
```

| Metric | Type | Description |
|--------|------|-------------|
| `messages` | gauge | Number of messages in destination |
| `enqueued_total` | counter | Total messages enqueued since last start |
| `dequeued_total` | counter | Total messages dequeued since last start |
| `dispatched_total` | counter | Total messages dispatched since last start |
| `messages_inflight` | gauge | Messages dispatched but not acknowledged |
| `expired_total` | counter | Total messages expired since last start |
| `consumers` | gauge | Number of consumers |
| `producers` | gauge | Number of producers |
| `memory_percent_usage` | gauge | Percent of destination memory limit used |
| `memory_limit_bytes` | gauge | Memory limit for destination in bytes |
| `memory_usage_bytes` | gauge | Memory used by destination in bytes |
| `store_message_size_bytes` | gauge | Store message size in bytes |
| `average_enqueue_time_milliseconds` | gauge | Average time (since last start) messages waited before dispatch |

## Prometheus configuration

Example yaml configuration for running a Prometheus scraper on the same machine as the broker
```yaml
scrape_configs:
  - job_name: activemq
    metrics_path: /metrics
    params:
      per_object: ['true']  # omit for broker-only
    basic_auth:
      username: admin
      password: admin
    static_configs:
      - targets: ['localhost:8161']
```
