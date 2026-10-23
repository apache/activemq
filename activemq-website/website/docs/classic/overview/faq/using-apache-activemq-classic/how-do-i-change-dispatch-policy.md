---
title: "How do I change dispatch policy"
slug: /how-do-i-change-dispatch-policy
---

This is much simpler and more powerful in ActiveMQ 4.0 - you can configure different [Dispatch Policies](dispatch-policies) directly.

For now in ActiveMQ 3.x we have a [PrefetchPolicy configuration](what-is-the-prefetch-limit-for). This allows a certain number of messages to be dispatched to a consumer before they are acknowledged. This feature is to achieve high performance; the higher the value the less likely it is that a client has to wait for messages to arrive before processing the message and so the higher the throughput.

e.g. under heavy load of a busy system you probably want to dispatch 1000 or so messages to a consumer, so as soon as its ready to receive a message, there are a bunch of them in memory waiting to be used - rather than having a slow request-response RPC each time.

However the effect of this is that when you send a small number of messages, they tend to all go to one consumer unless you've lots of messages. Note that this issue can be avoided in 4.x as there is now a configurable DispatchPolicy so that you can enforce, say, round robin dispatch policy to ensure that messages are dispatched fairly to avoid this issue.

In ActiveMQ 3.x or later a work around for this issue is to reconfigure the PrefetchPolicy on the ConnectionFactory/Connection so that the broker only prefetches a single message per consumer; though this will greatly reduce the performance and throughput of the system.

### Configuring the dispatch policy in ActiveMQ 4.x

In AMQ 4.x, the dispatch policy can now be configured on the xml configuration file (ie. activemq.xml) i.e.:
```
<destinationPolicy>
   <policyMap><policyEntries> 
      <policyEntry topic="COMMAND.>">
         <dispatchPolicy>
           <strictOrderDispatchPolicy />
         </dispatchPolicy>
         <subscriptionRecoveryPolicy>
            <noSubscriptionRecoveryPolicy />
         </subscriptionRecoveryPolicy>
      </policyEntry>
   </policyEntries></policyMap>
</destinationPolicy>
```
