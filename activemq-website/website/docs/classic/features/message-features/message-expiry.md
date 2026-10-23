---
title: "Message Expiry"
slug: /message-expiry
---

Apache ActiveMQ handles message expiry in two parts:

1.  **The client sets the expiration time when it sends the message.**
2.  **The broker checks that expiration time and discards expired messages.**

When a JMS producer sets a time-to-live (TTL), ActiveMQ converts that TTL into an absolute `JMSExpiration` value when the message is sent. A TTL of `0` means the message never expires.

At the broker, expired messages can be removed in several places, including when a message arrives, while messages are being held for dispatch, and during periodic destination scans. Consumers may also filter expired messages before delivery.

## Client-side expiry

In JMS, message expiration is controlled by the producer:

*   [`MessageProducer.setTimeToLive(long)`](how-do-i-set-the-message-expiration)
*   [`MessageProducer.send(...)`](how-do-i-set-the-message-expiration)

ActiveMQ stores the TTL as an absolute expiration timestamp on the message. That timestamp is based on the sending machine's clock.

## Broker-side expiry

The broker checks the stored expiration timestamp against its own clock and removes expired messages as they are encountered. Because of that, expiry is not purely a client-side concept: the broker still has to enforce it.

For that reason, the clocks on all machines involved — producers, brokers, and consumers — should be kept in sync. If the machines are not time-synchronized, a message may appear to expire early or late depending on which clock is ahead.

## Consumer expiry checks

The ActiveMQ client also performs expiry checks before delivering messages to application code. This helps prevent expired messages from being consumed accidentally.

## Related documentation

*   [How do I set the message expiration](how-do-i-set-the-message-expiration)
*   [Message Redelivery and DLQ Handling](message-redelivery-and-dlq-handling)
*   [Slow Consumer Handling](slow-consumer-handling)

