---
title: "How Do Transactions Work"
slug: /how-do-transactions-work
---

There are two levels of transaction support in ActiveMQ:

*   JMS Transactions - the `commit()`/`rollback()` methods on a Session (which is like doing `commit()`/`rollback()` on a JDBC connection)
*   XA Transactions - where the [XASession](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/ActiveMQXASession.html) acts as an [XAResource](http://java.sun.com/j2ee/1.4/docs/api/javax/transaction/xa/XAReDevelopers/source) by communicating with the Message Broker, rather like a JDBC Connection takes place in an XA transaction by communicating with the database.

They are both implemented in a similar manner. When operations are carried out on a transacted (or XA transacted) session, a transaction command is sent to the broker, with a unique transaction ID which is then followed by all the usual commands (send message, acknowledge message etc). When a `commit()` or `rollback()` is called on the Session, this command is sent to the broker for it to commit or rollback the transaction.

Now the operations carried out on a transacted session inside a transaction, like a send message or acknowledge message, do not really perform a real send or acknowledge until the commit occurs. So the Broker explicitly handles these cases separately - essentially buffering up the commands until the commit occurs when the messages are really sent or acknowledged.

ActiveMQ uses [TransactionStore](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/store/TransactionStore.html) (implemented by all persistence adapters) to handle transactions. `TransactionStore` will cache all messages and ACKs until commit or rollback occurs. Besides storing messages, the broker will withhold dispatching any of the messages until the session commit. If you wanna see the code, take a look at [MemoryTransactionStore](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/store/memory/MemoryTransactionStore.html) which proxies transactions for Memory and JDBC persistence adapters.  The only real difference with XA transactions is that at the PREPARE stage we MUST write every command we have received (the send message or acknowledge message commands) to a persistent store so that we can recover properly.

