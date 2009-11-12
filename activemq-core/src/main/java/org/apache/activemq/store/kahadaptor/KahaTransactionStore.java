/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store.kahadaptor;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.xa.XAException;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.kaha.RuntimeStoreException;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.ProxyMessageStore;
import org.apache.activemq.store.ProxyTopicMessageStore;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionRecoveryListener;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.journal.JournalPersistenceAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Provides a TransactionStore implementation that can create transaction aware
 * MessageStore objects from non transaction aware MessageStore objects.
 * 
 * @version $Revision: 1.4 $
 */
public class KahaTransactionStore implements TransactionStore, BrokerServiceAware {	
    private static final Log LOG = LogFactory.getLog(KahaTransactionStore.class);
	
    private Map transactions = new ConcurrentHashMap();
    private Map prepared;
    private KahaPersistenceAdapter adaptor;
    
    private BrokerService brokerService;

    KahaTransactionStore(KahaPersistenceAdapter adaptor, Map preparedMap) {
        this.adaptor = adaptor;
        this.prepared = preparedMap;
    }

    public MessageStore proxy(MessageStore messageStore) {
        return new ProxyMessageStore(messageStore) {
            public void addMessage(ConnectionContext context, final Message send) throws IOException {
                KahaTransactionStore.this.addMessage(getDelegate(), send);
            }

            public void removeMessage(ConnectionContext context, final MessageAck ack) throws IOException {
                KahaTransactionStore.this.removeMessage(getDelegate(), ack);
            }
        };
    }

    public TopicMessageStore proxy(TopicMessageStore messageStore) {
        return new ProxyTopicMessageStore(messageStore) {
            public void addMessage(ConnectionContext context, final Message send) throws IOException {
                KahaTransactionStore.this.addMessage(getDelegate(), send);
            }

            public void removeMessage(ConnectionContext context, final MessageAck ack) throws IOException {
                KahaTransactionStore.this.removeMessage(getDelegate(), ack);
            }
        };
    }

    /**
     * @see org.apache.activemq.store.TransactionStore#prepare(TransactionId)
     */
    public void prepare(TransactionId txid) {
        KahaTransaction tx = getTx(txid);
        if (tx != null) {
            tx.prepare();
            prepared.put(txid, tx);
        }
    }

    /**
     * @throws XAException
     * @see org.apache.activemq.store.TransactionStore#commit(org.apache.activemq.service.Transaction)
     */
    public void commit(TransactionId txid, boolean wasPrepared) throws IOException {
        KahaTransaction tx = getTx(txid);
        if (tx != null) {
            tx.commit(this);
            removeTx(txid);
        }
    }

    /**
     * @see org.apache.activemq.store.TransactionStore#rollback(TransactionId)
     */
    public void rollback(TransactionId txid) {
        KahaTransaction tx = getTx(txid);
        if (tx != null) {
            tx.rollback();
            removeTx(txid);
        }
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }

    public synchronized void recover(TransactionRecoveryListener listener) throws IOException {
        for (Iterator i = prepared.entrySet().iterator(); i.hasNext();) {
            Map.Entry entry = (Entry)i.next();
            XATransactionId xid = (XATransactionId)entry.getKey();
            KahaTransaction kt = (KahaTransaction)entry.getValue();
            listener.recover(xid, kt.getMessages(), kt.getAcks());
        }
    }

    /**
     * @param message
     * @throws IOException
     */
    void addMessage(final MessageStore destination, final Message message) throws IOException {
    	try {
    		if (message.isInTransaction()) {
    			KahaTransaction tx = getOrCreateTx(message.getTransactionId());
    			tx.add((KahaMessageStore)destination, message);
    		} else {
    			destination.addMessage(null, message);
    		}
    	} catch (RuntimeStoreException rse) {
    	    stopBroker();
    	    throw rse;
    	}
    }

    /**
     * @param ack
     * @throws IOException
     */
    final void removeMessage(final MessageStore destination, final MessageAck ack) throws IOException {
    	try {
    		if (ack.isInTransaction()) {
    			KahaTransaction tx = getOrCreateTx(ack.getTransactionId());
    			tx.add((KahaMessageStore)destination, ack);
    		} else {
    			destination.removeMessage(null, ack);
    		}
    	} catch (RuntimeStoreException rse) {
    	    stopBroker();
    	    throw rse;
    	}
    }

    protected synchronized KahaTransaction getTx(TransactionId key) {
        KahaTransaction result = (KahaTransaction)transactions.get(key);
        if (result == null) {
            result = (KahaTransaction)prepared.get(key);
        }
        return result;
    }

    protected synchronized KahaTransaction getOrCreateTx(TransactionId key) {
        KahaTransaction result = (KahaTransaction)transactions.get(key);
        if (result == null) {
            result = new KahaTransaction();
            transactions.put(key, result);
        }
        return result;
    }

    protected synchronized void removeTx(TransactionId key) {
        transactions.remove(key);
        prepared.remove(key);
    }

    public void delete() {
        transactions.clear();
        prepared.clear();
    }

    protected MessageStore getStoreById(Object id) {
        return adaptor.retrieveMessageStore(id);
    }

	public void setBrokerService(BrokerService brokerService) {
		this.brokerService = brokerService;
	}
	
    protected void stopBroker() {
        new Thread() {
           public void run() {
        	   try {
    	            brokerService.stop();
    	        } catch (Exception e) {
    	            LOG.warn("Failure occured while stopping broker", e);
    	        }    			
    		}
    	}.start();
    }
}
