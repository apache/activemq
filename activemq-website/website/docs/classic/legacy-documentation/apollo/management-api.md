---
title: "Apollo 1.7.1 Management API"
slug: /management-api
---

        /\* <![CDATA[ */ table.reference tr th { text-align:left; } tr.even { background-color: #eee; } table.reference tr td { text-align:left; padding: .4em; vertical-align:top; font-size:90%; } table.reference tr td p { margin: 0 0 .5em 0; } td.methods { font-weight:bold; } /* ]]> */  

Apollo 1.7.1 Management API
===========================

*   [Overview](#overview)
*   *   [Authentication](#authentication)
    *   [JSON Representation](#json-representation)
    *   [Response Codes](#response-codes)
    *   [Working with Tabular Results](#working-with-tabular-results)
*   [Route Reference](#route-reference)
*   *   [Broker Management](#broker-management)
    *   [Virtual Host Management](#virtual-host-management)
    *   [Connection Management](#connection-management)
    *   [Broker Configuration Management](#broker-configuration-management)
    *   [Session Management](#session-management)

Overview
--------

Apollo's REST API runs on port 61680 or via SSL on port 61681. If your running an Apollo broker on your local machine, you could access the API at the following HTTP URLs:

    http://localhost:61680
    https://localhost:61681

For all of the rest of this document, we will be leaving off that part, since it is the same for every API call.

### Authentication

The broker requires all requests against the management API to supply user credentials which have administration privileges.

The user credentials can be supplied using via HTTP basic authentication. Example:

    $ curl -u "admin:password" http://localhost:61680/broker

### JSON Representation

The API routes are intended to be access programmatically as JSON services but they also provide an HTML representation so that the API services can easily be browsed using a standard web browser.

You must either set the HTTP `Accept` header to `application/json` or append `.json` to the URL to get the JSON representation of the data. Example:

    $ curl -H "Accept: application/json" -u "admin:password" \
    http://localhost:61680/broker
    
    $ curl -u "admin:password" http://localhost:61680/broker.json

### Response Codes

You can use the HTTP response code to determine if your request succeeded or failed. Expect the following:

*   _`200`_: If a GET, PUT, or DELETE request succeeds.
*   _`303`_: If a POST request succeeds.
*   _`304`_: If the resource cannot be modified at the current time.
*   _`400`_: The request could not be understood by the server due to malformed syntax. The client SHOULD NOT repeat the request without modifications.
*   _`404`_: If the resource cannot be found.
*   _`401`_: If the user does not have access to the resource.
*   _`50x`_: If an internal server error occurs while processing the request.

If your get a _`40x`_ or _`50x`_ response code, the response message will contain a document describing the failure. For example, if we try to use invalid query syntax you would get:

    $ curl -i -u "admin:password" `echo 'http://localhost:61680/broker/virtual-hosts/apollo-01/queues.json?f=id&q=foo(id=="foo")'`
    HTTP/1.1 400 Bad Request
    Content-Type: application/json
    Transfer-Encoding: chunked
    
    {"message":"Unable to find function (method): \"foo(java.lang.Boolean)\" in any user-defined function handlers or the default function handler"}

### Working with Tabular Results

Many of the resource routes provided by the broker implement a selectable paged tabular interface. A good example of such a resource route is the connections list. It's route is:

    /broker/connections

Example:

    $ curl -u "admin:password" \
    'http://localhost:61680/broker/connections.json'

Results in a Data Page :

    
    {
      "page":0,
      "page_size":100,
      "total_pages":1,
      "total_rows":2,
      "headers":[
        "*"
      ],
      "rows":[
        {
          "id":"4",
          "state":"STARTED",
          "state_since":1306848325102,
          "read_counter":103,
          "write_counter":239110628,
          "connector":"tcp",
          "protocol":"stomp",
          "remote_address":"/127.0.0.1:61775",
          "protocol_version":"1.0",
          "user":"admin",
          "waiting_on":"client request",
          "subscription_count":1
        },
        {
          "id":"5",
          "state":"STARTED",
          "state_since":1306848325102,
          "read_counter":227739229,
          "write_counter":113,
          "connector:"tcp",
          "protocol":"stomp",
          "remote_address":"/127.0.0.1:61776",
          "protocol_version":"1.0",
          "user":"admin",
          "waiting_on":"blocked sending to: org.apache.activemq.apollo.broker.Queue$$anon$1@13765e9b",
          "subscription_count":0
        }
      ]
    }
    

The `*` header field means the record was selected. To narrow down the selected fields you can add multiple `f` query parameters to pick the fields you want to retrieve.

Example:

    $ curl -u "admin:password" \
    'http://localhost:61680/broker/connections.json?f=id&f=read_counter'

Results in :

    
    {
      "page":0,
      "page_size":100,
      "total_pages":1,
      "total_rows":2,
      "headers":[
        "id",
        "read_counter"
      ],
      "rows":[
        [
          "7",
          110733109
        ],
        [
          "6",
          103
        ]
      ]
    }
    

If you want to narrow down the records which get selected, you can set a `q` query parameter to SQL 92 style where clause which uses the record's fields to filter down the selected records.

For example to only view local connection, you would want to use a where clause like `remote_address LIKE "/127.0.0.01:%"` which to execute with `curl` you would run:

    curl -u "admin:password" \
    'http://localhost:61680/broker/connections.json?q=remote_address%20LIKE%20"/127.0.0.1:%"'

The records are paged. The default page size is 100, so only the first 100 records will be displayed. If you want to view subsequent results, you must set the `p` query parameter to the page you wish to access. You can change the page size by setting the `ps` query parameter.

The results are typically return in the order in which the records were original created. If you want to retrieve them in a different order, use the `o` query parameter to define the order in which they are returned. Example:

    curl -u "admin:password" \
    'http://localhost:61680/broker/connections.json?o=write_counter%20DESC&o=id'

Route Reference
---------------

### Broker Management

Route

On Success

`POST`

`/broker/action/shutdown.json`

`303`

Shuts down the JVM.

`GET`

`/broker.json`

`200 [BrokerStatusDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/BrokerStatusDTO.html)`

General information about the broker, JVM, and OS status. Example:

`curl -u "admin:password" http://localhost:61680/broker.json`

    
    {
      "id": "default",
      "state": "STARTED",
      "state_since": 1314573353753,
      "version": "1.7.1",
      "jvm_metrics": {
        "heap_memory": {
          "used": 22165160,
          "alloc": 162201600,
          "max": 954466304
        },
        "non_heap_memory": {
          "used": 44522128,
          "alloc": 45944832,
          "max": 136314880
        },
        "classes_loaded": 5776,
        "classes_unloaded": 0,
        "threads_current": 21,
        "threads_peak": 22,
        "os_arch": "x86_64",
        "os_name": "Mac OS X 10.7",
        "os_memory_total": 8589934592,
        "os_memory_free": 3339231232,
        "os_swap_total": 0,
        "os_swap_free": 2147483648,
        "os_fd_open": 111,
        "os_fd_max": 10240,
        "os_load_average": 0.99267578125,
        "os_cpu_time": 5630000000,
        "os_processors": 4,
        "runtime_name": "86739@chirino-mbp.local",
        "jvm_name": "Java HotSpot(TM) 64-Bit Server VM 1.6.0_26 (Apple Inc.)",
        "uptime": 37761,
        "start_time": 1314573351000
      },
      "current_time": 1314573388707,
      "virtual_hosts": ["default"],
      "connectors": ["tls", "tcp"],
      "connection_counter": 0
    }
    

You can the `?connections=true` query parameter to have a list of connections included n the result.

`GET`

`/broker/queue-metrics.json`

`200 [AggregateDestMetricsDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/AggregateDestMetricsDTO.html)`

Aggregates the messaging metrics for all the queue destinations. Example:

`curl -u "admin:password" http://localhost:61680/broker/queue-metrics.json`

    
    {
      "current_time": 1314573698381,
      "enqueue_item_counter": 57343,
      "enqueue_size_counter": 63128582,
      "enqueue_ts": 1314573698478,
      "dequeue_item_counter": 55121,
      "dequeue_size_counter": 60681503,
      "dequeue_ts": 1314573698478,
      "producer_counter": 22,
      "consumer_counter": 16,
      "producer_count": 22,
      "consumer_count": 16,
      "expired_item_counter": 0,
      "expired_size_counter": 0,
      "expired_ts": 1314573695015,
      "nack_item_counter": 0,
      "nack_size_counter": 0,
      "nack_ts": 1314573695015,
      "queue_size": 2447079,
      "queue_items": 2222,
      "swapped_in_size_max": 5963776,
      "swapped_in_size": 5278341,
      "swapped_in_items": 4794,
      "swapping_in_size": 0,
      "swapping_out_size": 0,
      "swap_out_item_counter": 60,
      "swap_out_size_counter": 65931,
      "swap_in_item_counter": 60,
      "swap_in_size_counter": 65931,
      "objects": 10
    }
    

`GET`

`/broker/topic-metrics.json`

`200 [AggregateDestMetricsDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/AggregateDestMetricsDTO.html)`

Aggregates the messaging metrics for all the topic destinations

`GET`

`/broker/dsub-metrics.json`

`200 [AggregateDestMetricsDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/AggregateDestMetricsDTO.html)`

Aggregates the messaging metrics for all the durable subscription destinations

`GET`

`/broker/dest-metrics.json`

`200 [AggregateDestMetricsDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/AggregateDestMetricsDTO.html)`

Aggregates the messaging metrics for all the destinations

### Virtual Host Management

Route

On Success

`GET`

`/broker/virtual-hosts.json`

`200 [DataPageDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/DataPageDTO.html) paging [VirtualHostStatusDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/VirtualHostStatusDTO.html)

`

Provides tabular access to all the virtual hosts.

`GET`

`/broker/virtual-hosts/{host}.json`

`200 [VirtualHostStatusDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/VirtualHostStatusDTO.html)`

General details about the `{host}` virtual host. Example:

`curl -u "admin:password" http://localhost:61680/broker/virtual-hosts/default.json`

    
    {
      "id": "default",
      "state": "STARTED",
      "state_since": 1314573798989,
      "topics": [],
      "queues": ["example-4", "example-7", "example-6", "example-0", 
                 "example-3", "example-8", "example-1", "example-5", 
                 "example-9", "example-2"],
      "dsubs": [],
      "store": true,
      "host_names": ["default", "localhost", "127.0.0.1"]
    }
    

`GET`

`/broker/virtual-hosts/{host}/queue-metrics.json`

`200 [AggregateDestMetricsDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/AggregateDestMetricsDTO.html)`

Aggregates the messaging metrics for all the queue destinations on the `{host}` virtual host.

`GET`

`/broker/virtual-hosts/{host}/topic-metrics.json`

`200 [AggregateDestMetricsDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/AggregateDestMetricsDTO.html)`

Aggregates the messaging metrics for all the topic destinations on the `{host}` virtual host.

`GET`

`/broker/virtual-hosts/{host}/dsub-metrics.json`

`200 [AggregateDestMetricsDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/AggregateDestMetricsDTO.html)`

Aggregates the messaging metrics for all the durable subscription destinations on the `{host}` virtual host.

`GET`

`/broker/virtual-hosts/{host}/dest-metrics.json`

`200 [AggregateDestMetricsDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/AggregateDestMetricsDTO.html)`

Aggregates the messaging metrics for all the destinations on the `{host}` virtual host.

`GET`

`/broker/virtual-hosts/{host}/store.json`

`200 [StoreStatusDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/StoreStatusDTO.html) sub class (depends on the store implementation)`

Gets metrics about the status of the message store used by the `{host}` virtual host. Example:

`curl -u "admin:password" http://localhost:61680/broker/virtual-hosts/default/store.json`

    
    {
      "@class": "bdb_store_status",
      "id": "bdb store at /Users/chirino/opt/default/data",
      "state": "STARTED",
      "state_since": 1314573798939,
      "canceled_message_counter": 0,
      "flushed_message_counter": 581034,
      "canceled_enqueue_counter": 0,
      "flushed_enqueue_counter": 581034,
      "message_load_latency": {
        "count": 0,
        "total": 0,
        "max": 0,
        "min": 0
      },
      "flush_latency": {
        "count": 0,
        "total": 0,
        "max": 0,
        "min": 0
      },
      "pending_stores": 0,
      "message_load_batch_size": {
        "count": 0,
        "total": 0,
        "max": -2147483648,
        "min": 2147483647
      }
    }
    

`GET`

`/broker/virtual-hosts/{host}/queues.json`

`200 [DataPageDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/DataPageDTO.html) paging [QueueStatusDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/QueueStatusDTO.html)

`

Provides tabular access to all queue destinations on the `{host}` virtual host.

`GET`

`/broker/virtual-hosts/{host}/queues/{dest}.json`

`200 [QueueStatusDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/QueueStatusDTO.html)`

Gets metrics and details about the `{dest}` queue on the `{host}` virtual host. Example:

You can the `?producers=true` query parameter to have a list of producers included n the result. You can the `?consumers=true` query parameter to have a list of consumers included n the result.

`curl -u "admin:password" http://localhost:61680/broker/virtual-hosts/default/queues/example-5.json?producers=true&consumers=true`

    
    {
      "id": "example-5",
      "state": "STARTED",
      "state_since": 1314573814532,
      "config": {
        "other": []
      },
      "binding": {
        "@class": "queue_destination",
        "path": ["example-5"]
      },
      "metrics": {
        "current_time": 1314574273342,
        "enqueue_item_counter": 878638,
        "enqueue_size_counter": 975430031,
        "enqueue_ts": 1314574273335,
        "dequeue_item_counter": 878522,
        "dequeue_size_counter": 975301271,
        "dequeue_ts": 1314574273328,
        "producer_counter": 4,
        "consumer_counter": 4,
        "producer_count": 2,
        "consumer_count": 2,
        "expired_item_counter": 0,
        "expired_size_counter": 0,
        "expired_ts": 1314573814488,
        "nack_item_counter": 118,
        "nack_size_counter": 131098,
        "nack_ts": 1314573928240,
        "queue_size": 128760,
        "queue_items": 116,
        "swapped_in_size_max": 688128,
        "swapped_in_size": 688200,
        "swapped_in_items": 620,
        "swapping_in_size": 0,
        "swapping_out_size": 0,
        "swap_out_item_counter": 605,
        "swap_out_size_counter": 671279,
        "swap_in_item_counter": 605,
        "swap_in_size_counter": 671279
      },
      "entries": [],
      "producers": [{
            "kind": "connection",
            "id": "43",
            "label": "/127.0.0.1:51632",
            "enqueue_item_counter": 12168,
            "enqueue_size_counter": 13495374,
            "enqueue_ts": 1314574273328
          },
          {
            "kind": "connection",
            "id": "76",
            "label": "/127.0.0.1:51665",
            "enqueue_item_counter": 13663,
            "enqueue_size_counter": 15154824,
            "enqueue_ts": 1314574273336
          }],
      "consumers": [{
            "kind": "connection",
            "id": "65",
            "label": "/127.0.0.1:51654",
            "enqueue_item_counter": 12678,
            "enqueue_size_counter": 14060805,
            "enqueue_ts": 1314574273248,
            "position": 878615,
            "acquired_count": 60,
            "acquired_size": 66600,
            "total_ack_count": 12618,
            "total_nack_count": 0,
            "waiting_on": "ack"
          },
          {
            "kind": "connection",
            "id": "75",
            "label": "/127.0.0.1:51664",
            "enqueue_item_counter": 13397,
            "enqueue_size_counter": 14860500,
            "enqueue_ts": 1314574273248,
            "position": 878639,
            "acquired_count": 56,
            "acquired_size": 62160,
            "total_ack_count": 13341,
            "total_nack_count": 0,
            "waiting_on": "producer"
          }]
    }
    

`DELETE`

`/broker/virtual-hosts/{host}/queues/{dest}.json`

`200`

Deletes the `{dest}` queue on the `{host}` virtual host.

`PUT`

`/broker/virtual-hosts/{host}/queues/{dest}.json`

`200`

Creates the `{dest}` queue on the `{host}` virtual host.

`GET`

`/broker/virtual-hosts/{host}/topics.json`

`200 [DataPageDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/DataPageDTO.html) paging [TopicStatusDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/TopicStatusDTO.html)

`

Provides tabular access to all topic destinations on the `{host}` virtual host.

`GET`

`/broker/virtual-hosts/{host}/topics/{dest}.json`

`200 [TopicStatusDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/TopicStatusDTO.html)`

Gets metrics and details about the `{dest}` topic on the `{host}` virtual host. Example:

You can the `?producers=true` query parameter to have a list of producers included n the result. You can the `?consumers=true` query parameter to have a list of consumers included n the result.

`curl -u "admin:password" http://localhost:61680/broker/virtual-hosts/default/topics/example-2.json?producers=true&consumers=true`

    
    {
      "id": "example-2",
      "state": "STARTED",
      "state_since": 1314574398831,
      "config": {
        "other": []
      },
      "producers": [{
        "kind": "connection",
        "id": "78",
        "label": "/127.0.0.1:51668",
        "enqueue_item_counter": 1133674,
        "enqueue_size_counter": 1259289609,
        "enqueue_ts": 1314574422110
      }],
      "consumers": [{
        "kind": "connection",
        "id": "86",
        "label": "/127.0.0.1:51677",
        "enqueue_item_counter": 566836,
        "enqueue_size_counter": 629643696,
        "enqueue_ts": 1314574422110
      },
      {
        "kind": "connection",
        "id": "82",
        "label": "/127.0.0.1:51673",
        "enqueue_item_counter": 566838,
        "enqueue_size_counter": 629645913,
        "enqueue_ts": 1314574422110
      }],
      "dsubs": [],
      "metrics": {
        "current_time": 0,
        "enqueue_item_counter": 1133674,
        "enqueue_size_counter": 1259289609,
        "enqueue_ts": 1314574422110,
        "dequeue_item_counter": 1133674,
        "dequeue_size_counter": 1259289609,
        "dequeue_ts": 1314574422110,
        "producer_counter": 1,
        "consumer_counter": 2,
        "producer_count": 1,
        "consumer_count": 2,
        "expired_item_counter": 0,
        "expired_size_counter": 0,
        "expired_ts": 0,
        "nack_item_counter": 0,
        "nack_size_counter": 0,
        "nack_ts": 0,
        "queue_size": 0,
        "queue_items": 0,
        "swapped_in_size_max": 0,
        "swapped_in_size": 0,
        "swapped_in_items": 0,
        "swapping_in_size": 0,
        "swapping_out_size": 0,
        "swap_out_item_counter": 0,
        "swap_out_size_counter": 0,
        "swap_in_item_counter": 0,
        "swap_in_size_counter": 0
      }
    }
    

`DELETE`

`/broker/virtual-hosts/{host}/topics/{dest}.json`

`200`

Deletes the `{dest}` topic on the `{host}` virtual host. Returns a 304 (Not Modified) if the topic is being used by any clients.

`PUT`

`/broker/virtual-hosts/{host}/topics/{dest}.json`

`200`

Creates the `{dest}` topic on the `{host}` virtual host.

`GET`

`/broker/virtual-hosts/{host}/topic-queues/{dest}/{queue}.json`

`200 [QueueStatusDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/QueueStatusDTO.html)`

Gets metrics and details about `{queue}` consumer queue which is being used to spool messages being sent to the `{dest}` topic on the `{host}` virtual host.

`GET`

`/broker/virtual-hosts/{host}/dsubs.json`

`200 [DataPageDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/DataPageDTO.html) paging [QueueStatusDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/QueueStatusDTO.html)

`

Provides tabular access to all durable subscription destinations on the `{host}` virtual host.

`GET`

`/broker/virtual-hosts/{host}/dsubs/{dest}.json`

`200 [QueueStatusDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/QueueStatusDTO.html)`

Gets metrics and details about the `{dest}` durable subscription on the `{host}` virtual host.

### Connection Management

Route

On Success

`GET`

`/broker/connectors.json`

`200 [DataPageDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/DataPageDTO.html) paging [ServiceStatusDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/ServiceStatusDTO.html)

`

Provides tabular access to all connectors on the broker.

`GET`

`/broker/connectors/{connector}.json`

`200 [ServiceStatusDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/ServiceStatusDTO.html)`

Gets details about the `{connector}`. Example:

`curl -u "admin:password" http://localhost:61680/broker/connectors/tcp.json`

    
    {
      "id": "tcp",
      "state": "STARTED",
      "state_since": 1314573798510,
      "local_address": "/0.0.0.0:61613",
      "protocol": "any",
      "connection_counter": 100,
      "connected": 12
    }
    

You can the `?connections=true` query parameter to have a list of connections included n the result.

`POST`

`/broker/connectors/{connector}/action/stop.json`

`303`

Stops the `{connector}`.

`POST`

`/broker/connectors/{connector}/action/start.json`

`303`

Starts the `{connector}`.

`GET`

`/broker/connection-metrics.json`

`200 [AggregateConnectionMetricsDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/AggregateConnectionMetricsDTO.html)`

Aggregates metrics about the connections.

`GET`

`/broker/connections.json`

`200 [DataPageDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/DataPageDTO.html) paging [ConnectionStatusDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/ConnectionStatusDTO.html)

`

Provides tabular access to all connections on the broker.

`GET`

`/broker/connections/{connection}.json`

`200 [ConnectionStatusDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/ConnectionStatusDTO.html)`

Gets details about the `{connection}`. Example:

`curl -u "admin:password" http://localhost:61680/broker/connections/108.json`

    
    {
      "id": "108",
      "state": "STARTED",
      "state_since": 1314574636579,
      "last_read_size": 0,
      "last_write_size": 32973,
      "read_counter": 106,
      "write_counter": 632923992,
      "connector: "tcp",
      "protocol": "stomp",
      "protocol_version": "1.0",
      "remote_address": "/127.0.0.1:51733",
      "local_address": "/127.0.0.1:61613",
      "user": "admin",
      "waiting_on": "client request",
      "subscription_count": 1
    }
    

`DELETE`

`/broker/connections/{connection}.json`

`200`

Deletes the `{connection}`.

`POST`

`/broker/connections/{connection}/action/delete.json`

`303`

Deletes the `{connection}`.

### Broker Configuration Management

Route

On Success

`GET`

`/config/runtime.json`

`200 [BrokerDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/BrokerDTO.html)`

The runtime configuration the broker using. This version of the configuration has all property placeholders resolved and then password fields are cleared.

`GET`

`/config/runtime.json`

`200 [BrokerDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/BrokerDTO.html)`

The runtime configuration the broker using. This version of the configuration has all property placeholders resolved and then password fields are cleared.

`GET`

`/config/files.json`

`200 string array`

A listing of the configuration files.

`GET`

`/config/files/{file}`

`200 bytes`

Downloads the contents of the config file.

`POST`

`/config/files/{file}`

`303`

Updates the contents of the config file. You can either post the raw bytes, or post a url encoded form with a field `config` set to the contents of the file. In the latter case, the content type is expected to be `application/x-www-form-urlencoded`

### Session Management

Route

On Success

`GET`

`/broker/whoami.json`

`200 [PrincipalDTO](http://activemq.apache.org/apollo/versions/1.7.1/website/documentation/api/apollo-dto/org/apache/activemq/apollo/dto/PrincipalDTO.html) array`

Lists the principals associated with your username. Example: curl -u “admin:password” http://localhost:61680/broker/whoami.json

    
    [{
      "name": "admin",
      "kind": "org.apache.activemq.jaas.UserPrincipal"
    }, {
      "name": "admins",
      "kind": "org.apache.activemq.jaas.GroupPrincipal"
    }]
    

`GET`

`/broker/signin.json`

`200 true`

Log you in using the `username` and `password` query parameters. creates a cookie based session.

`POST`

`/broker/signin.json`

`200 true`

Signs you in using the `username` and `password` form parameters. Creates a cookie based session.

`GET`

`/broker/signout.json`

`200 true`

Signs you off by invalidating your session.