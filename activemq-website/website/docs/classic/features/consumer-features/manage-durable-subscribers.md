---
title: "Manage Durable Subscribers"
slug: /manage-durable-subscribers
---


[Durable topic subscribers](how-do-durable-queues-and-topics-work) that are offline for a long period of time are usually not desired in the system. The reason for that is that broker needs to keep all the messages sent to those topics for the said subscribers. And this message piling can over time exhaust broker store limits for example and lead to the overall slowdown of the system.

You can always manually unsubscribe inactive durable subscriber using management tools like [JConsole](jmx) or [Web Console](web-console), but clearly there's more that can be done to help manage systems that use durable subscribers (perhaps coming from environments that they don't control)

Staring with version 5.6 we introduced a few improvements in this area.

### Expiring messages

Some applications send message with specified time to live. If those messages are kept on the broker for the offline durable subscriber we need to remove them when they reach their expiry time. Just as we do with queues, now we check for those messages every 30 seconds by default, which can be tuned with the appropriate [destination policy](per-destination-policies). For example, the following entry
```
<policyEntry topic=">" expireMessagesPeriod="300000"/>
```
will configure the broker to check for expired messages every 5 minutes.

### Removing inactive subscribers

The other thing we can do is to automatically unsubscribe durable subscribers that are not active for some period of time. For that purpose we introduced two new broker properties:

property|default|description
---|---|---
offlineDurableSubscriberTimeout|-1|Amount of time (in milliseconds) after which we remove inactive durable subs. Default -1, means don't remove them
offlineDurableSubscriberTaskSchedule|300000|How often we check (in milliseconds)

An example configuration could look like
```
<broker name="localhost" offlineDurableSubscriberTimeout="86400000" offlineDurableSubscriberTaskSchedule="3600000">
```
which means that we check every hour and remove subscriber that has been offline for a day.

