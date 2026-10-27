---
title: "LevelDB Store"
slug: /leveldb-store
---

> **Warning**
> 
> The LevelDB store has been removed from production.
> This page only serves as an archival page for previous releases.
> Use [KahaDB](kahadb) instead.

> **Version Compatibility**
> 
> Available in ActiveMQ 5.8.0 but became unsupported as of 5.14.2, and was removed in 5.17.0

The LevelDB Store is a file based persistence database that is local to the message broker that is using it. It has been optimized to provide even faster persistence than KahaDB. It's similar to KahahDB but instead of using a custom B-Tree implementation to index the write ahead logs, it uses [LevelDB](https://code.google.com/p/leveldb/) based indexes which have several nice properties due to the 'append only' files access patterns :

*   Fast updates (No need to do random disk updates)
*   Concurrent reads
*   Fast index snapshots using hard links

Both KahaDB and the LevelDB store have to do periodic garbage collection cycles to determine which log files can deleted. In the case of KahaDB, this can be quite expensive as you increase the amount of data stored and can cause read/write stalls while the collection occurs. The LevelDB store uses a much cheaper algorithm to determine when log files can be collected and avoids those stalls.

Configuration
-------------

You can configure ActiveMQ to use LevelDB for its persistence adapter - like below :
```
  <broker brokerName="broker" ... >
    ...
    <persistenceAdapter>
      <levelDB directory="activemq-data"/>
    </persistenceAdapter>
    ...
  </broker>
```
### LevelDB Properties

property name|default value|Comments
---|---|---
`directory`|`LevelDB`|The directory which the store will use to hold it's data files. The store will create the directory if it does not already exist.
`sync`|`true`|If set to false, then the store does not sync logging operations to disk
`logSize`|`104857600` (100 MB)|The max size (in bytes) of each data log file before log file rotation occurs.
`verifyChecksums`|`false`|Set to true to force checksum verification of all data that is read from the file system.
`paranoidChecks`|`false`|Make the store error out as soon as possible if it detects internal corruption.
`indexFactory`|`org.fusesource.leveldbjni.JniDBFactory`, `org.iq80.leveldb.impl.Iq80DBFactory`|The factory classes to use when creating the LevelDB indexes
`indexMaxOpenFiles`|`1000|Number of open files that can be used by the index.
`indexBlockRestartInterval`|16|Number keys between restart points for delta encoding of keys.
`indexWriteBufferSize`|`6291456` (6 MB)|Amount of index data to build up in memory before converting to a sorted on-disk file.
`indexBlockSize`|`4096` (4 K)|The size of index data packed per block.
`indexCacheSize`|`268435456` (256 MB)|The maximum amount of off-heap memory to use to cache index blocks.
`indexCompression`|`snappy`|The type of compression to apply to the index blocks. Can be snappy or none.
`logCompression`|`none`|The type of compression to apply to the log records. Can be snappy or none.

For tuning locking properties please take a look at [Pluggable storage lockers](pluggable-storage-lockers)

Also See
--------

*   [Replicated LevelDB Store](replicated-leveldb-store) An extended version of this store which self replicates to other broker nodes to increase message availability.

