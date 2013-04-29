package org.apache.activemq.leveldb.replicated

import org.apache.activemq.broker.{LockableServiceSupport, BrokerService, BrokerServiceAware, ConnectionContext}
import org.apache.activemq.command._
import org.apache.activemq.leveldb.LevelDBStore
import org.apache.activemq.store._
import org.apache.activemq.usage.SystemUsage
import java.io.File
import java.io.IOException
import java.util.Set
import org.apache.activemq.util.{ServiceStopper, ServiceSupport}

/**
 */
abstract class ProxyLevelDBStore extends LockableServiceSupport with BrokerServiceAware with PersistenceAdapter with TransactionStore with PListStore {

  def proxy_target: LevelDBStore

  def beginTransaction(context: ConnectionContext) {
    proxy_target.beginTransaction(context)
  }

  def getLastProducerSequenceId(id: ProducerId): Long = {
    return proxy_target.getLastProducerSequenceId(id)
  }

  def createTopicMessageStore(destination: ActiveMQTopic): TopicMessageStore = {
    return proxy_target.createTopicMessageStore(destination)
  }

  def setDirectory(dir: File) {
    proxy_target.setDirectory(dir)
  }

  def checkpoint(sync: Boolean) {
    proxy_target.checkpoint(sync)
  }

  def createTransactionStore: TransactionStore = {
    return proxy_target.createTransactionStore
  }

  def setUsageManager(usageManager: SystemUsage) {
    proxy_target.setUsageManager(usageManager)
  }

  def commitTransaction(context: ConnectionContext) {
    proxy_target.commitTransaction(context)
  }

  def getLastMessageBrokerSequenceId: Long = {
    return proxy_target.getLastMessageBrokerSequenceId
  }

  def setBrokerName(brokerName: String) {
    proxy_target.setBrokerName(brokerName)
  }

  def rollbackTransaction(context: ConnectionContext) {
    proxy_target.rollbackTransaction(context)
  }

  def removeTopicMessageStore(destination: ActiveMQTopic) {
    proxy_target.removeTopicMessageStore(destination)
  }

  def getDirectory: File = {
    return proxy_target.getDirectory
  }

  def size: Long = {
    return proxy_target.size
  }

  def removeQueueMessageStore(destination: ActiveMQQueue) {
    proxy_target.removeQueueMessageStore(destination)
  }

  def createQueueMessageStore(destination: ActiveMQQueue): MessageStore = {
    return proxy_target.createQueueMessageStore(destination)
  }

  def deleteAllMessages {
    proxy_target.deleteAllMessages
  }

  def getDestinations: Set[ActiveMQDestination] = {
    return proxy_target.getDestinations
  }

  def rollback(txid: TransactionId) {
    proxy_target.rollback(txid)
  }

  def recover(listener: TransactionRecoveryListener) {
    proxy_target.recover(listener)
  }

  def prepare(txid: TransactionId) {
    proxy_target.prepare(txid)
  }

  def commit(txid: TransactionId, wasPrepared: Boolean, preCommit: Runnable, postCommit: Runnable) {
    proxy_target.commit(txid, wasPrepared, preCommit, postCommit)
  }

  def getPList(name: String): PList = {
    return proxy_target.getPList(name)
  }

  def removePList(name: String): Boolean = {
    return proxy_target.removePList(name)
  }
}