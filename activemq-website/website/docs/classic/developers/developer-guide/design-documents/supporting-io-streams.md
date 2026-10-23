---
title: "Supporting IO Streams"
slug: /supporting-io-streams
---


It'd be great to offer kick ass support for streaming files over ActiveMQ of any arbitrary size. The basic idea is to fragment the stream into multiple messages and send/receive those over JMS.

There are a few issues to consider...

### Use casess

1.  many producers writing and only 1 consumer. Each consumer ideally should completely process 1 stream at once (IO streams are often blocking IO, so the client thread can't do much with other streams while its reading 1).
2.  if a consumer gets 1Gb through a 10Gb file and dies, we need to re-deliver the messages to another consumer.

### Goal

Our goal should be

*   for each consumer to process a single stream in one go before trying to process another.

### Possible Failure Conditions

*   Consumer could die in the middle of reading a stream. Recovery options:
    1.  Trash the rest of the stream
    2.  Restart delivery of the stream to the next consumer
    3.  Continue delivery of the steam to the next consumer at the point of failure.
*   Producer could die in the middle of writing a stream. We may need to detect his failure. We could
    1.  Send the stream in a transaction. The stream is not sent to a consumer until it's fully received by the broker. Down side: consumer has high latency before being able to receive the message.
    2.  Consumer timeout: if a message is not receive soon enough the consumer assumes the producer is dead. (What if he's not??)
*   Consumer could start to receive in the middle of a stream. Condition could happen:
    1.  if another consumer assumed the broker was dead (but it was not).
    2.  if a non stream consumer acidentally removed messages, or messages were sent to the DLQ due to consumer rollbacks.

### Implementation Issues

*   we can use message groups to ensure the same consumer processes all messages for a given stream - but unfortunately message groups does not prevent a consumer being overloaded with more than one message group at a time - maybe thats a new feature we can add?
*   avoid the broker running out of RAM - so spool to disk (or throttle the producer) if the only consumer is working on a different stream.
    *   If messages sent using transactions, major chanages need may need to be made to how we do transaction management and message journaling. Currently, all messages in a in progress transaction are held in memory until it commits (Synchronization callbacks are holding on the the messages). The journal currently holds on to all transacted messages in it's journal + memory until commit, it does now allow the journal to rollover messages that are part of transaction that is in progess.
*   given that the entire stream must be processed in one go, we can only ACK the entire stream of messages - so we need to disable pre-fetch?
    *   Prefetch disabling is not needed as the consumers send back a special kind of ack which only temporarily expands the prefetch window.

