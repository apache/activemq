---
title: "AMQ Message Store"
description: "Configure the ActiveMQ Classic AMQ Message Store with amqPersistenceAdapter: journal data logs, Kaha reference store, properties, directory layout and recovery."
slug: /amq-message-store
---


The Basics
----------

This is the default storage for AcitveMQ 5 and above. The AMQ Message Store is an embeddable transactional message storage solution that is extremely fast and reliable.  
message commands are written to a transactional journal - which consists of rolling data logs - which means writing is extremely fast and the state of the store is easily recoverable.

Messages themselves are persisted in the data logs of the journal - with references to their location being held by a reference store (by default Kaha) for fast retrevial.

![](/img/amqstore.png)
  
References to messages are held in memory, and periodically inserted into the reference store to improve performance.  
  
The messages are stored in data logs, which are individual files, typically 32mb in size (though this is configurable, they can be larger if the size of a message is large than the file size). When all the messages in a data log have been successfully consumed, the data log file is marked as being ready to be deleted - or archived - which will happen at the next clean up period.

Configuration
-------------

By default ActiveMQ will use the the AMQ Store - and its default settings. You can configure the properties of the AMQ Store however, by explictly defining its persistence adapter (amqPersistenceAdapter):
```
 <broker brokerName="broker" persistent="true" useShutdownHook="false">
    <persistenceAdapter>
      <amqPersistenceAdapter directory="${activemq.base}/activemq-data" maxFileLength="32mb"/>
    </persistenceAdapter>
    <transportConnectors>
      <transportConnector uri="tcp://localhost:61616"/>
    </transportConnectors>
  </broker>
```
The above shows the configuration required to set the AMQ Store through its amqPersistenceAdapter - and explicity setting the directory and maxFileLength properties.

### AMQ Store Properties

property name|default value|Comments
---|---|---
`directory`|activemq-data|the path to the directory to use to store the message store data and log files
`useNIO`|true|use NIO to write messages to the data logs
`syncOnWrite`|false|sync every write to disk
`maxFileLength`|32mb|a hint to set the maximum size of the message data logs
`persistentIndex`|true|use a persistent index for the message logs. If this is false, an in-memory structure is maintained
`maxCheckpointMessageAddSize`|4kb|the maximum number of messages to keep in a transaction before automatically committing
`cleanupInterval`|30000|time (ms) before checking for a discarding/moving message data logs that are no longer used
`indexBinSize`|1024|default number of bins used by the index. The bigger the bin size - the better the relative performance of the index
`indexKeySize`|96|the size of the index key - the key is the message id
`indexPageSize`|16kb|the size of the index page - the bigger the page - the better the write performance of the index
`directoryArchive`|archive|the path to the directory to use to store discarded data logs
`archiveDataLogs`|false|if true data logs are moved to the archive directory instead of being deleted

Data Structure
--------------

In the data directory defined for the AMQ Store there is the following directory structure:

![](/img/amqdir.png)

### Top level

the message broker's name is used to distinguish its directory of message data. By default, the broker name is local host.  
Below this top level directory are the following sub directories:

### archive

message data logs are moved here when they are discarded.  
n.b. this directory only exists when the property archiveDataLogs is enabled

### journal

Used to hold the message data logs

### kr-store

The directory structure of the Kaha reference store (if used)

##### data

The indexes used to reference the message data logs in the journal for fast retrieval

##### state

The state of the store - i.e. names of durable subscribers - the reason for this is described in [Recovery](#recovery-)

### tmp-storage

use to hold data files for transient messages that may be stored on disk to alleviate memory consumption - e.g. non-persistent topic messages awaiting delivery to an active, but slow subscriber.

Recovery <a name="#recovery"></a>
--------

If the message broker does not shutdown properly, then the reference store indexes are cleaned and the message data files (which contain messages/acknowledgements and transactional boundaries) are replayed to rebuild up the message store state. It is possbile to force automatic recovery if using the Kaha reference store (the default) by deleting the kr-store/state/ directory.

