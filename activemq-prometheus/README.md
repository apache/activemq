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

1. Uncomment the `Prometheus Metrics Web Application` block from `conf/jetty/jetty-webapps.xml`.
2. Restart the broker

The endpoint uses the existing Jetty management listener, TLS configuration,
IP allowlist, and JAAS realm. Its path is restricted to the `users` and
`admins` roles but can be changed in `conf/jetty/jetty-security.xml`.

## Endpoints

Two endpoints because brokers with many destinations might produce large responses:
- `GET /metrics`: broker-level metrics only.
- `GET /metrics?per_object=true`: per-destination (queues, topics, temporary queues and topics) and broker-level metrics

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

### Destination metrics (`activemq_queue_*` / `activemq_topic_*` / `activemq_tempqueue_*` / `activemq_temptopic_*`)

Returned only when `?per_object=true` is set.

Each destination type is reported as its own metric family.
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
