---
title: "C integration scenarios"
slug: /c-integration-scenarios
---


One way and RPC with C services
-------------------------------

Its common for the C services to be the back end services. The 2 common use cases are

*   we send a message one-way to a C service
*   we send a message to a C-service and wait for the result, then reply back to the originator the result

### Making one-ways reliable

If we assume that the C-service is fairly atomic, it works or it doesn't and does not partially work, then for one-way handling we just need some code to...

1.  consume a message from some destination
2.  invoke the C service and wait until the service completes
3.  acknowledge the message

If invoking the C service fails (such that we know it wasn't invoked), we could automatically retry a certain number of times before we acknowledge the message.

The problem is if the above code were to be killed before the message is acknowledged, we'd invoke the service again.

To get around this we could persist that we have invoked the service, so that if we are killed we would not invoke the service again but put the message on some dead letter queue for manual reconciliation.

1.  consume a message from some destination
2.  have we seen this message before - if so put on a dead letter queue for manual reconciliation
3.  persist that we have processed this message
4.  invoke the C service and wait until the service completes
5.  acknowledge the message

Another approach could be for the C service to say whether or not it has successfully processed the message before. This just pushes the problem inside the C code requiring that it persists when things are invoked and when things complete so that it can know when duplicate messages are delivered.

### making RPC reliable

This scenario is as above but rather than just acknowledge the inbound message we wish to send a reply and acknowledge the inbound message. So this could be regarded as a small JMS transaction.

1.  consume a message from some destination
2.  have we seen this message before - if so put on a dead letter queue for manual reconciliation
3.  persist that we have processed this message
4.  invoke the C service and wait until the service completes
5.  send the reply with results from the C service
6.  commit the JMS transaction

Again if the C service is capable of knowing if it has seen the message before then we can avoiid step 3.

### connectivity to C

We can link C into a Java process and invoke it directly via JNI. Another option is to wrap the C code as an Apache module and perform a HTTP POST to invoke a C service and extract the results of the service.

