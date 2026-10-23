---
title: "java.io.InterruptedIOException"
slug: /javaiointerruptedioexception
---

The activemq client will throw an [java.io](http://java.io).InterruptedIOException if  the calling thread has been interrupted while the transport (ResponseCorrelator) is waiting for a response.

Thread interruption while waiting for a response is treated as an IOException and reported to the transport listener. For an ActiveMQConnection, this will force an async close of the transport, closing the underlying socket.

Because commands are not idempotent and with an interruption, it is not possible to know if the broker received the command, the only safe course of action is to close the connection.

If the thread has been interrupted before a request is attempted, the interrupted state is cleared and reset after the request has completed. This allows shutdown operations to complete in the interrupted state.

