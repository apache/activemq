---
title: "Durable Queue Memory Management"
slug: /durable-queue-memory-management
---


The current 3.x code can have issues with massive queues with un-acknowledged messages; its complex due to consumption from multiple points in the queue, consumers coming & going, selectors matching only certain points in the queue etc.

This page outlines design the design proposal.

TargetArea
----------

Maintains TargetLists for each consumer (the messages that have matched one or more consumers's selectors and are gonna be used to try dispatch stuff to the consumer DispatchArea). TargetEntry objects are added to the TargetList as messages arrive from the Queue.

A TargetEntry can have either a direct Message reference or a CacheReference to an entry in a cache.

### Eager loading

Each TargetList has a watermark to indicate the first TargetEntry which is a CacheReference. The idea is that under ideal scenarios, each TargetList has the watermark equal to the high water mark (i.e. that there are enough direct message references in RAM).

So we can have a background thread increasing the watermark up to the ideal level - which means it starts at the current watermark and directly loads the objects; de-referencing the CacheReference and replacing with direct references.

### PendingCache

TargetEntry objects added to the TargetList which are above the high watermark have their Message objects evicted from direct references into the PendingCache.

The Target eager loader will move objects from this cache to the TargetList when they are available. If they are not in the PendingCache, they are loaded from the QueueArea.

### QueueArea

This represents the actual queue messages. There's also a matching HeadQueueCache which is a simple memory bound queue. FIFO so newer stuff is evicted from the end.

Back

### Linking the Caches

The PendingCache and HeadQueueCache are linked together, along with the TargetArea/DispatchArea so that there is a fixed amount of RAM used in all the caches.

As the PendingCache grows, the HeadQueueCache shrinks. HeadQueueCache is only used for new consumers arriving. On startup this will stick around a while until lots of messages start being delivered to consumers where it'll shrink back to zero if there is sufficient dispatching.

Under heavy load, the HeadQueueCache will be pretty much empty unless new consumers come along.

