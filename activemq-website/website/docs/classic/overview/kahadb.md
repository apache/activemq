---
title: "KahaDB"
slug: /kahadb
---

KahaDB is a file based persistence database that is local to the message broker that is using it. It has been optimized for fast persistence. It is the the default storage mechanism since **ActiveMQ 5.4**. KahaDB uses less file descriptors and provides faster recovery than its predecessor, the [AMQ Message Store](amq-message-store).

Configuration
-------------

To use KahaDB as the broker's persistence adapter configure ActiveMQ as follows (example):
```
 <broker brokerName="broker">
    <persistenceAdapter>
      <kahaDB directory="activemq-data" journalMaxFileLength="32mb"/>
    </persistenceAdapter>
 </broker>
```

### KahaDB Properties

Property|Default|Comments
---|---|---
`archiveCorruptedIndex`|`false`|If `true`, corrupted indexes found at startup will be archived (not deleted).
`archiveDataLogs`|`false`|If `true`, will move a message data log to the archive directory instead of deleting it.
`checkForCorruptJournalFiles`|`false`|If `true`, will check for corrupt journal files on startup and try and recover them.
`checkpointInterval`|`5000`|Time (ms) before check-pointing the journal.
`checksumJournalFiles`|`true`|Create a checksum for a journal file. The presence of a checksum is required in order for the persistence adapter to be able to detect corrupt journal files. Before **ActiveMQ 5.9.0**: the default is `false`.
`cleanupInterval`|`30000`|The interval (in ms) between consecutive checks that determine which journal files, if any, are eligible for removal from the message store. An eligible journal file is one that has no outstanding references.
`compactAcksAfterNoGC`|`10`|From **ActiveMQ 5.14.0**: when the acknowledgement compaction feature is enabled this value controls how many store GC cycles must be completed with no other files being cleaned up before the compaction logic is triggered to possibly compact older acknowledgements spread across journal files into a new log file.  The lower the value set the faster the compaction may occur which can impact performance if it runs to often.
`compactAcksIgnoresStoreGrowth`|`false`|From **ActiveMQ 5.14.0**: when the acknowledgement compaction feature is enabled this value controls whether compaction is run when the store is still growing or if it should only occur when the store has stopped growing (either due to idle or store limits reached).  If enabled the compaction runs regardless of the store still having room or being active which can decrease overall performance but reclaim space faster. 
`concurrentStoreAndDispatchQueues`|`true`|Enable the dispatching of Queue messages to interested clients to happen concurrently with message storage.
`concurrentStoreAndDispatchTopics`|`false`|Enable the dispatching of Topic messages to interested clients to happen concurrently with message storage. **Enabling this property is not recommended.**
`directory`|`activemq-data`|The path to the directory to use to store the message store data and log files.
`directoryArchive`|`null`|Define the directory to move data logs to when they all the messages they contain have been consumed.
`enableAckCompaction`|`true`|From **ActiveMQ 5.14.0**: this setting controls whether the store will perform periodic compaction of older journal log files that contain only Message acknowledgements. By compacting these older acknowledgements into new journal log files the older files can be removed freeing space and allowing the message store to continue to operate without hitting store size limits.
`enableIndexWriteAsync`|`false`|If `true`, the index is updated asynchronously.
`enableJournalDiskSyncs`|`true`|Ensure every journal write is followed by a disk sync (JMS durability requirement). This property is deprecated as of **ActiveMQ** **5.14.0**. From **ActiveMQ** **5.14.0**: see `journalDiskSyncStrategy`.
`ignoreMissingJournalfiles`|`false`|If `true`, reports of missing journal files are ignored.
`indexCacheSize`|`10000`|Number of index pages cached in memory.
`indexDirectory`||From **ActiveMQ 5.10.0**: If set, configures where the KahaDB index files (`db.data` and `db.redo`) will be stored. If not set, the index files are stored in the directory specified by the `directory` attribute.
`indexWriteBatchSize`|`1000`|Number of indexes written in a batch.
`journalDiskSyncInterval`|`1000`|Interval (ms) for when to perform a disk sync when `journalDiskSyncStrategy=periodic`. A sync will only be performed if a write has occurred to the journal since the last disk sync or when the journal rolls over to a new journal file.
`journalDiskSyncStrategy`|`always`|From **ActiveMQ 5.14.0**: this setting configures the disk sync policy. The list of available sync strategies are (in order of decreasing safety, and increasing performance): `always` Ensure every journal write is followed by a disk sync (JMS durability requirement). This is the safest option but is also the slowest because it requires a sync after every message write. This is equivalent to the deprecated property `enableJournalDiskSyncs=true`. `periodic` The disk will be synced at set intervals (if a write has occurred) instead of after every journal write which will reduce the load on the disk and should improve throughput. The disk will also be synced when rolling over to a new journal file. The default interval is 1 second. The default interval offers very good performance, whilst being safer than `never` disk syncing, as data loss is limited to a maximum of 1 second's worth. See `journalDiskSyncInterval` to change the frequency of disk syncs. `never` A sync will never be explicitly called and it will be up to the operating system to flush to disk. This is equivalent to setting the deprecated property `enableJournalDiskSyncs=false`. This is the fastest option but is the least safe as there's no guarantee as to when data is flushed to disk. Consequently message loss _can_ occur on broker failure.
`journalMaxFileLength`|`32mb`|A hint to set the maximum size of the message data logs.
`maxAsyncJobs`|`10000`|The maximum number of asynchronous messages that will be queued awaiting storage (should be the same as the number of concurrent MessageProducers).
`preallocationScope`|`entire_journal`|From **ActiveMQ 5.14.0**: this setting configures how journal data files are preallocated. The default strategy preallocates the journal file on first use using the appender thread. `entire_journal_async` will use preallocate ahead of time in a separate thread. `none` disables preallocation. On SSD, using `entire_journal_async` avoids delaying writes pending preallocation on first use. **Note**: on HDD the additional thread contention for disk has a negative impact. Therefore use the default.
`preallocationStrategy`|`sparse_file`|From **ActiveMQ 5.12.0**: This setting configures how the broker will try to preallocate the journal files when a new journal file is needed. `sparse_file` - sets the file length, but does not populate it with any data. `os_kernel_copy` - delegates the preallocation to the Operating System. `zeros`  - each preallocated journal file contains nothing but `0x00` throughout.
`storeOpenWireVersion`|`11`|Determines the version of OpenWire commands that are marshaled to the KahaDB journal. Before **ActiveMQ 5.12.0**: the default value is `6`. Some features of the broker depend on information stored in the OpenWire commands from newer protocol revisions and these may not work correctly if the store version is set to a lower value.  KahaDB stores from broker versions greater than 5.9.0 will in many cases still be readable by the broker but will cause the broker to continue using the older store version meaning newer features may not work as intended.  For KahaDB stores that were created in versions prior to **ActiveMQ 5.9.0** it will be necessary to manually set `storeOpenWireVersion="6"` in order to start a broker without error. 

> For tuning locking properties see the options listed at [Pluggable storage lockers.](pluggable-storage-lockers)

### Slow File System Access Diagnostic Logging

You can configure a non zero threshold in milliseconds for database updates. If database operation is slower than that threshold (for example if you set it to `500`), you may see messages like:
```
Slow KahaDB access: cleanup took 1277 | org.apache.activemq.store.kahadb.MessageDatabase | ActiveMQ Journal Checkpoint Worker
```
You can configure a threshold used to log these messages by using a system property and adjust it to your disk speed so that you can easily pick up runtime anomalies.
```
-Dorg.apache.activemq.store.kahadb.LOG_SLOW_ACCESS_TIME=1500
```

Multi(m) kahaDB Persistence Adapter
===================================

From **ActiveMQ 5.6**: it's possible to distribute destinations stores across multiple kahdb persistence adapters. When would you do this? If you have one fast producer/consumer destination and another periodic producer destination that has irregular batch consumption then disk usage can grow out of hand as unconsumed messages become distributed across multiple journal files. Having a separate journal for each ensures minimal journal usage. Also, some destination may be critical and require disk synchronization while others may not. In these cases you can use the `mKahaDB` persistence adapter and filter destinations using wildcards, just like with destination policy entries.

### Transactions

Transactions can span multiple journals if the destinations are distributed. This means that two phase completion is necessary, which does impose a performance (additional disk sync) penalty to record the commit outcome. This penalty is only imposed if more than one journal is involved in a transaction.

### Configuration

Each instance of `kahaDB` can be configured independently. If no destination is supplied to a `filteredKahaDB`, the implicit default value will match any destination, queue or topic. This is a handy catch all. If no matching persistence adapter can be found, destination creation will fail with an exception. The `filteredKahaDB` shares its wildcard matching rules with [Per Destination Policies](per-destination-policies).

From ActiveMQ 5.15, `filteredKahaDB` support a StoreUsage attribute named `usage`. This allows individual disk limits to be imposed on matching queues.
```
<broker brokerName="broker">

 <persistenceAdapter>
  <mKahaDB directory="${activemq.base}/data/kahadb">
    <filteredPersistenceAdapters>
      <!-- match all queues -->
      <filteredKahaDB queue=">">
        <usage>
         <storeUsage limit="1g" />
        </usage>
        <persistenceAdapter>
          <kahaDB journalMaxFileLength="32mb"/>
        </persistenceAdapter>
      </filteredKahaDB>
      
      <!-- match all destinations -->
      <filteredKahaDB>
        <persistenceAdapter>
          <kahaDB enableJournalDiskSyncs="false"/>
        </persistenceAdapter>
      </filteredKahaDB>
    </filteredPersistenceAdapters>
  </mKahaDB>
 </persistenceAdapter>

</broker>
```

### Automatic Per Destination Persistence Adapter

Set `perDestination="true"` on the catch all, i.e., when no explicit destination is set, `filteredKahaDB` entry. Each matching destination will be assigned its own `kahaDB` instance.
```
<broker brokerName="broker">

 <persistenceAdapter>
  <mKahaDB directory="${activemq.base}/data/kahadb">
    <filteredPersistenceAdapters>
      <!-- kahaDB per destinations -->
      <filteredKahaDB perDestination="true">
        <persistenceAdapter>
          <kahaDB journalMaxFileLength="32mb"/>
        </persistenceAdapter>
      </filteredKahaDB>
    </filteredPersistenceAdapters>
  </mKahaDB>
 </persistenceAdapter>

</broker>
```

> Specifying both `perDestination="true"` _and_ `queue=">"` on the same `filteredKahaDB` entry has not been tested. It _may_ result in the following exception being raised:
> 
> ```
> Reason: java.io.IOException: File '/opt/java/apache-activemq-5.9.0/data/mKahaDB/lock' could not be locked as lock is already held for this jvm`
> ```

