# Jakarta Messaging 3.1: asynchronous send design doc

Date: Jan 5th 2025

The purpose of this document is to discuss the design of implementing asynchronous send (with CompletionListener) that is JMS 2.0 / Jakarta Messaging 3.1 compliant.

## **Background**

Here is the [link](https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#asynchronous-send) to the official Jakarta Messaging 3.1 spec (Section 7.3).

ActiveMQ as it is, already sends messages asynchronously by default (except persistent messages outside of a transaction boundary) and supports non-JMS compliance interface `AsyncCallback` for client applications to specify the callback to trigger once the server sends the acknowledgement.

## **Implementation Requirements**

I highlight the requirements defined in [official Jakarta Messaging 3.1 spec](https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#asynchronous-send) Please see the [appendix](#summary-of-jakarta-messaging-3.1-requirements) for more details.

## **Overall approach**

There was a long discussion on the original [PR](https://github.com/apache/activemq/pull/1364/commits). The consensus was to follow the spec 100%. But to capture the rationale behind, I will outline the two approaches that were initially considered.

**Approach 1: Make CompletionListener behave the same as AsyncCallback**

The idea is to wrap CompletionListener with a AsyncCallback. I.E every CompletionListener passed by client application is transformed into a AsyncCallback under the hood. Also users are already familiar with the async message behaviour (a message is default sent as async unless it’s a persistent message sent outside of a transacted boundary).

Pros:

- Simple to implement.

Cons:

- It doesn’t follow the spec and as a matter of fact, is very different. (All the other design decisions mentioned below are not met)

This approach is not recommended because new users of ActiveMQ 6 will probably expect it to stick to the spec 100% and it will be hard to explain to them why we are sticking with the old implementation.

**\[Recommended\] Approach 2: Stick to the spec 100%, and deprecate AsyncCallback in ActiveMQ 7**

We will reference the code flow of AsyncCallback but will support all the behaviour outlined in the specs. It is up to the users which behaviour they choose. I.E:

1. User configures always use async or doesn’t send persistent message outside of a transaction boundary: no change
2. User uses AsyncCallback as the callback for the async send: no change, it is not Jakarta Messaging Compliant
3. User uses the new API (supports jakarta.jms.CompletionListener): it will stick to Jakarta Messaging 3.1 spec outlined 100%

So in ActiveMQ 6, there are two ways to pass callback to async send. That will cause a lot of effort to maintain both going forward and it will be confusing. Hence, at the release that launches this feature, it will also mark the AsyncCallback as deprecated. And in ActiveMQ 7 it will be removed.

## **CompletionListener unable to close, commit, rollback session, context and producer**

This corresponds to the behaviour outlined in [7.3.4](https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#close-commit-or-rollback).

**\[Recommended\] Approach 1: Use ThreadLocal variable and wrapper CompletionListener**

In this approach, it  will use a ThreadLocal\<Boolean\> flag on the session, context and producer to mark if the current thread is inside a CompletionListener callback. The reason why it is ThreadLocal is because a session (it also applies to JMSContext because it wraps a session under the hood) can be shared by multiple consumers and producers. If we have the thread that executes the CompletionListener (has to be a separate thread required by [7.3.8](https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#use-of-the-completionlistener-by-the-jakarta-messaging-provider)) sets a simple shared boolean flag on the session, then the application thread will also be affected (unable to close, commit and rollback session while the CompletionListener is running), which violates the spec.

At the same time, we are using a wrapper CompletionListener to do pre & post processing before invoking the user-provided CompletionListener and specified the wrapper as the callback for the async send. Therefore, we can set and unset the ThreadLocal flags before invoking user-provided CompletionListener.

Here is sample code flow that illustrates the point:

It wraps the user-provided CompletionListener (named `completionListener` in the screenshot below) into a `wrapperCompletionListener` which is later passed to the connection and the transport layer’s `asyncRequest` method.  
```java
private ThreadLocal<Boolean> inCompletionListenerCallback = new ThreadLocal<>();
```
```java
CompletionListener wrapperCompletionListener = new CompletionListener() {
    @Override
    public void onCompletion(Message message) {
        try {
            originalMessage.setMessageAccessible(true);
            inCompletionListenerCallback.set(true);
            producerInCompletionListenerCallback.set(true);
            // Invoke application provided completionListener
            completionListener.onCompletion(message);
        } catch (Exception e) {
            // invoke onException if the exception can't be thrown in the thread that calls the send
            // per Jakarta Messaging 3.1 spec section 7.3.2
            completionListener.onException(message, e);
        } finally {
            inCompletionListenerCallback.set(false);
            producerInCompletionListenerCallback.set(false);
            numIncompleteAsyncSend.doDecrement();
            producerNumIncompleteSend.doDecrement();
            isAsyncSendCompleted.countDown();
        }
    }

    @Override
    public void onException(Message message, Exception e) {
        try {
            originalMessage.setMessageAccessible(true);
            inCompletionListenerCallback.set(true);
            producerInCompletionListenerCallback.set(true);
            completionListener.onException(message, e);
        } finally {
            inCompletionListenerCallback.set(false);
            producerInCompletionListenerCallback.set(false);
            numIncompleteAsyncSend.doDecrement();
            producerNumIncompleteSend.doDecrement();
            isAsyncSendCompleted.countDown();
        }
    }
};
```

As you can see, before invoking the user-provided CompletionListener, it sets the `inCompletionListenerCallback` (an instance attribute ThreadLocal\<Boolean\> on the `Session` object) to true.

Then in the `close` method of the session, check if the flag is set to true before closing.

```java
    private void checkIsSessionUseAllowed(String errorMsg) {
        if (inCompletionListenerCallback.get() != null && inCompletionListenerCallback.get()) {
            throw new IllegalStateRuntimeException(errorMsg);
        }
    }
```

Hence if the user-provided CompletionListener tries to call close() on the session inside a CompletionListener, the `inCompletionListenerCallback` will be true in that transport thread and it will throw an exception. In contrast, in the application thread, since `inCompletionListenerCallback` is false, the application thread can still close the session even while the transport thread is processing the user-provided CompletionListener.

When the user-provided CompletionListener completes, `inCompletionListenerCallback` will be set to false by the wrapper.

The same idea applies to transacted session, producer and connection.

## **Making message object inaccessible**

This corresponds to the behaviour outlined in [7.3.9](https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#restrictions-on-the-use-of-the-message-object) and [7.3.6](https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-headers). Basically, before the async send of the message is completed, it will not be accessible (not readable and not writable) by the client application.

We extend the idea of the wrapper CompletionListener stated above and add a flag in the ActiveMQMessage object, `messageAccessible` if the flag is true. The client application will not be able to read or modify message attributes.

```java
public class ActiveMQMessage extends Message implements org.apache.activemq.Message, ScheduledMessage {
    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.ACTIVEMQ_MESSAGE;
    public static final String DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY = "dlqDeliveryFailureCause";
    public static final String BROKER_PATH_PROPERTY = "JMSActiveMQBrokerPath";

    private static final Map<String, PropertySetter> JMS_PROPERTY_SETERS = new HashMap<String, PropertySetter>();

    protected transient Callback acknowledgeCallback;

    private volatile boolean messageAccessible = true;
```

I.E at every setter and getter of the Message object  
```java
    private void checkMessageAccessible() throws JMSException {
        if (!messageAccessible) {
            throw new JMSException(
                    "Can not access and mutate message, the message is sent asynchronously and its completion listener has not been invoked");
        }
    }
```

When a message is sent, currently by default, the session is making a copy (`isCopyMessageOnSend` is default true). Hence we need to make sure we set the flag on the original message reference, and we set its `messageAccessible` to false right before it is sent asynchronously in a connection method. After the send is complete, the wrapper CompletionListener will set the `messageAccessible` attribute of the message reference back to true.

There is a small decision to make here. Because users can configure `isCopyMessageOnSend`, so in theory they can set it to false. I.E no copy of the message and marshal that message object into the wire. There are two options here:

**1\. \[Recommended\] Approach 1: Enforce message copy if CompletionListener is specified:**

Even though the OpenWire marshaller will ignore the field we just added, i.e on the broker server end, the `messageAccessible` attribute will always be true regardless. It is more safe if we enforce message copy, such that in the future when we change the marshaller logic, it will not “lock” the message on the broker server end by accident.

**2\. Approach 2: Leave it as it is and honour `isCopyMessageOnSend`:**

If `CopyMessageOnSend` is false, then we don’t make a copy and set the message to be inaccessible and let the marshaller encode it (the field will be ignored anyway since we are not changing the marshaller logic).

## **Messages ordering**

This corresponds to [7.3.3](https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-order-2) and [7.3.8](https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#use-of-the-completionlistener-by-the-jakarta-messaging-provider). Basically it needs to meet two cases of messaging ordering requirements:

1. The order of asynchronous sends to a particular destination from a producer has to be the same as the order of execution of its corresponding CompletionListener.
2. The order of asynchronous sends across a set of destinations from the same producer has to be the same as the order or execution of its corresponding CompletionListener.

Requirement \#1 is already satisfied because the broker acknowledges messages in the same order as the messages were sent within the same destination. The client doesn’t need any additional logic to handle that.

To satisfy requirement \#2, there are a few approaches.

**\[Recommended\] Approach 1: Application thread send async message one at a time**

This is the most straightforward option, sending async messages one at a time in the application thread. It basically makes async send behave like sync send. CompletionListener still needs to be executed on another thread (meeting the threading restriction 7.3.7), in this case the transport thread.   
Pros:

- It is very simple to implement (also makes implementing other features simpler, see the next design decision below)

Cons:

- It sort of defies the purpose of async send because the application thread needs to wait before the send is completed (can lead to performance issues). Even though it doesn’t violate the spec.

This approach is recommended due to its simplicity. Will follow up with a plan to optimize such behaviour.

**Approach 2: Application thread submits the async send to a work queue, a single background thread will dequeue a message from the queue, send it asynchronously and wait for it to finish, before dequeuing the next.**

This is similar to approach 1, the async message is sent one at a time. The difference being instead of doing it in the application thread, the `Session` object will have a `SingleThreadExecutor` to handle sending of the messages on a background thread. The advantage of doing that is the application thread is not blocked by sending an async message, it simply enqueues the message to the work queue (ExecutorService) then proceeds. Allowing it to batch send a lot of async messages at a time without blocking.

Pros:

- It allows batching async messages without blocking. Potentially increase throughput of messages.

Cons:

- It makes producer flow control hard (even though for AsyncCallback case, there is producer flow control mechanism. We can reference that design).
- It is more complex and let’s say if there’s an exception thrown by the send operation. The exception is thrown by the background thread, not the application thread. Making exception handling harder.

This approach is not recommended because of exception handling and making the code more complex for initial implementation of the feature.

**Approach 3: Change the transport thread logic to enforce ordering**

The reason why ordering across destinations is not honoured is because the transport is using a map to correlate request and response. I.E there’s no ordering enforcement. For this approach, we can add an array or a new data structure to keep track of when the request is made to prevent out of order execution of its CompletionListener.

Pros:

- It allows batching async messages without blocking. Potentially increase throughput of messages.
- It makes the session code more clean and removes the need of ExecutorService / one additional thread in the session.

Cons:

- Now the transport is coupled to how the message is sent, this is bad design.
- The change is also a lot fundamental and risky, potentially affecting behaviour of other modes of messaging sending.

This approach is not recommended because of the large code footprint and making the code more complex for initial implementation of the feature.

## **Block until all async sends complete**

This corresponds to the behaviour outlined in [7.3.4](https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#close-commit-or-rollback). “Incomplete sends should be allowed to complete normally unless an error occurs.”

**Approach 1: have a synchronized counter to keep track of the number of incomplete sends. The corresponding close(), commit() and rollback() method will block until that counter reaches 0\.**

It will add a synchronized counter to keep track of the number of incomplete sends. This is similar to a CountDownLatch in which you can wait until it becomes 0 but it exposes an API to count up. A sample implementation of such synchronization data structure is outlined below:  
```java
public class CountdownLock {

    final Object counterMonitor = new Object();
    private final AtomicInteger counter = new AtomicInteger();

    public void doWaitForZero() {
        synchronized(counterMonitor){
            try {
                if (counter.get() > 0) {
                    counterMonitor.wait();
                }
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    public void doDecrement() {
        synchronized(counterMonitor){
            if (counter.decrementAndGet() == 0) {
                counterMonitor.notify();
            }
        }
    }

    public void doIncrement() {
        synchronized(counterMonitor){
            counter.incrementAndGet();
        }
    }
}
```
Then before sending the async message, the application thread will increment the counter by `doIncrement` and in the wrapper CompletionListener, the transport thread will call `doDecrement` on the shared `CountdownLock`.

Before the `close`, `rollback` and `commit` methods are called, the application thread will invoke `doWaitForZero` which blocks until the counter reaches zero.

Pros:

- This is more generic and can handle executing multiple async send at the same time

Cons:

- It is more tricky to implement.

**\[Recommended\]  Approach \#2: Add a synchronized boolean flag to indicates whether there’s an inflight async message**

This will only work if we decide to do async send on the application thread one at a time (as proposed in the recommended approach above).

Pros:

- Very simple to implement

Cons:

- It will not work if we allow client application to keep sending message asynchronously without waiting for the completion of previous async sends (which is outlined in future improvement as performance optimization)

This is recommended because of its simplicity as an initial implementation of the feature.

## **Restrictions on usage in Jakarta EE**

This has already been discussed on [dev mailing list](https://lists.apache.org/thread/5lz9x5xw5gj30mng02gykqjrs4nk67yl). Since there’s no precedent of doing so we will not support this restriction.

## **Future improvement**

We will follow up with removing the constraint that async messages need to be sent one at a time as proposed in [here](#messages-ordering) (to make the initial implementation of the feature simple) while being spec compliant.  This follow up is not a breaking change so it can be shipped as a patch version as performance enhancement.

## 

## **Appendix**

### Summary of Jakarta Messaging 3.1 requirements {#summary-of-jakarta-messaging-3.1-requirements}

1\. Both classic (JMS 1.1 style) and simplified APIs (JMS 2.0/Jakarta Messaging 3.1 style) need to support the use of CompletionListener.

Classic APIs  
```java
void send(Message var1, CompletionListener var2) throws JMSException;
void send(Message var1, int var2, int var3, long var4, CompletionListener var6) throws JMSException;
void send(Destination var1, Message var2, CompletionListener var3) throws JMSException;
void send(Destination var1, Message var2, int var3, int var4, long var5, CompletionListener var7) throws JMSException;
```

Simplified API  
```java
JMSProducer setAsync(CompletionListener var1);
CompletionListener getAsync();
```

2\. The CompletionListener `onCompletion` must not be invoked before the server responds with the acknowledgement. Furthermore, exceptions that occur at the `send` method of the calling thread in client application will be thrown at that thread. If an exception is encountered which cannot be thrown in the thread that is calling the send method then the Jakarta Messaging provider must call the CompletionListener’s onException method.

3\. Message ordering is honored even if a combination of synchronous and asynchronous sends has been performed. The ordering requirement is further defined [here](https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#order-of-message-sends). There are a few cases to consider here:

- The order of asynchronous messages to the same destination from the same producer needs to be honoured.
- The order of asynchronous messages sent by the same producer across different destinations should also be honoured.

4\. Incomplete sends should be allowed to complete normally unless an error occurs. I.E closing the producer, session, connection or commit and  rollback (in local transaction) should be blocked until incomplete send operations have been completed.

5\. A CompletionListener callback method must not call close on its own producer, session (including JMSContext) or connection or call commit or rollback on its own session. Doing so will cause the close, commit or rollback to throw an `IllegalStateException` or `IllegalStateRuntimeException`

6\. An asynchronous send is not permitted in a Jakarta EE web container or Enterprise Beans container. throw a jakarta.jms.JMSException (if allowed by the method) or a jakarta.jms.JMSRuntimeException (if not) when called by an application running in the Jakarta EE web container or Enterprise Beans container. This is **recommended but not required**.

7\. If the send is asynchronous, message header fields and message properties which must be set by the “Jakarta Messaging provider on send” may be accessed on the sending client only after the CompletionListener has been invoked. If the CompletionListener’s onException method is called then the state of these message header fields and properties is undefined.

8\. A Jakarta Messaging provider must not invoke the CompletionListener from the thread that is calling the asynchronous send method. Applications that perform an asynchronous send must conform to the threading restrictions, I.E, session may be used by only one thread at a time.

9\. A Jakarta Messaging provider may throw a `JMSException` if the application attempts to access or modify the Message object after the send method has returned and before the CompletionListener has been invoked. If the Jakarta Messaging provider does not throw an exception then the behavior is undefined.
