/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store.jpa;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.jpa.model.StoredMessage;
import org.apache.activemq.store.jpa.model.StoredSubscription;
import org.apache.activemq.store.jpa.model.StoredSubscription.SubscriptionId;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOExceptionSupport;

public class JPATopicMessageStore extends JPAMessageStore implements TopicMessageStore {
    private Map<SubscriptionId,AtomicLong> subscriberLastMessageMap=new ConcurrentHashMap<SubscriptionId,AtomicLong>();

	public JPATopicMessageStore(JPAPersistenceAdapter adapter, ActiveMQDestination destination) {
		super(adapter, destination);
	}

	public void acknowledge(ConnectionContext context, String clientId, String subscriptionName, MessageId messageId) throws IOException {
		EntityManager manager = adapter.beginEntityManager(context);
		try {
			StoredSubscription ss = findStoredSubscription(manager, clientId, subscriptionName);
			ss.setLastAckedId(messageId.getBrokerSequenceId());
		} catch (Throwable e) {
			adapter.rollbackEntityManager(context,manager);
			throw IOExceptionSupport.create(e);
		}
		adapter.commitEntityManager(context,manager);
	}

	public void addSubsciption(SubscriptionInfo info, boolean retroactive) throws IOException {
		EntityManager manager = adapter.beginEntityManager(null);
		try {			
			StoredSubscription ss = new StoredSubscription();
			ss.setClientId(info.getClientId());
			ss.setSubscriptionName(info.getSubscriptionName());
			ss.setDestination(destinationName);
			ss.setSelector(info.getSelector());
			ss.setSubscribedDestination(info.getSubscribedDestination().getQualifiedName());
			ss.setLastAckedId(-1);
			
			if( !retroactive ) {
				Query query = manager.createQuery("select max(m.id) from StoredMessage m");
				Long rc = (Long) query.getSingleResult();
				if( rc != null ) {
					ss.setLastAckedId(rc);
				}
			}
			
			manager.persist(ss);
		} catch (Throwable e) {
			adapter.rollbackEntityManager(null,manager);
			throw IOExceptionSupport.create(e);
		}
		adapter.commitEntityManager(null,manager);
	}

	public void deleteSubscription(String clientId, String subscriptionName) throws IOException {
		EntityManager manager = adapter.beginEntityManager(null);
		try {			
			StoredSubscription ss = findStoredSubscription(manager, clientId, subscriptionName);
			manager.remove(ss);
		} catch (Throwable e) {
			adapter.rollbackEntityManager(null,manager);
			throw IOExceptionSupport.create(e);
		}
		adapter.commitEntityManager(null,manager);
	}

	private StoredSubscription findStoredSubscription(EntityManager manager, String clientId, String subscriptionName) {
		Query query = manager.createQuery(
				"select ss from StoredSubscription ss " +
				"where ss.clientId=?1 " +
				"and ss.subscriptionName=?2 " +
				"and ss.destination=?3");
		query.setParameter(1, clientId);
		query.setParameter(2, subscriptionName);
		query.setParameter(3, destinationName);
		List<StoredSubscription> resultList = query.getResultList();
		if( resultList.isEmpty() )
			return null;
		return resultList.get(0); 
	}

	public SubscriptionInfo[] getAllSubscriptions() throws IOException {
		SubscriptionInfo rc[];
		EntityManager manager = adapter.beginEntityManager(null);
		try {
			ArrayList<SubscriptionInfo> l = new ArrayList<SubscriptionInfo>();
			
			Query query = manager.createQuery("select ss from StoredSubscription ss where ss.destination=?1");
			query.setParameter(1, destinationName);
			for (StoredSubscription ss : (List<StoredSubscription>)query.getResultList()) {
				SubscriptionInfo info = new SubscriptionInfo();
				info.setClientId(ss.getClientId());
				info.setDestination(destination);
				info.setSelector(ss.getSelector());
				info.setSubscriptionName(ss.getSubscriptionName());
				info.setSubscribedDestination(toSubscribedDestination(ss));
				l.add(info);
	        }
			
			rc = new SubscriptionInfo[l.size()];
			l.toArray(rc);
		} catch (Throwable e) {
			adapter.rollbackEntityManager(null,manager);
			throw IOExceptionSupport.create(e);
		}
		adapter.commitEntityManager(null,manager);		
		return rc;
	}

	public int getMessageCount(String clientId, String subscriptionName) throws IOException {
		Long rc;
		EntityManager manager = adapter.beginEntityManager(null);
		try {	
			Query query = manager.createQuery(
					"select count(m) FROM StoredMessage m, StoredSubscription ss " +
					"where ss.clientId=?1 " +
					"and   ss.subscriptionName=?2 " +
					"and   ss.destination=?3 " +
					"and   m.destination=ss.destination and m.id > ss.lastAckedId");
			query.setParameter(1, clientId);
			query.setParameter(2, subscriptionName);
			query.setParameter(3, destinationName);
	        rc = (Long) query.getSingleResult();	        
		} catch (Throwable e) {
			adapter.rollbackEntityManager(null,manager);
			throw IOExceptionSupport.create(e);
		}
		adapter.commitEntityManager(null,manager);
		return rc.intValue();
	}

	public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
		SubscriptionInfo rc=null;
		EntityManager manager = adapter.beginEntityManager(null);
		try {			
			StoredSubscription ss = findStoredSubscription(manager, clientId, subscriptionName);
			if( ss != null ) {
				rc = new SubscriptionInfo();
				rc.setClientId(ss.getClientId());
				rc.setDestination(destination);
				rc.setSelector(ss.getSelector());
				rc.setSubscriptionName(ss.getSubscriptionName());
				rc.setSubscribedDestination(toSubscribedDestination(ss));
			}
		} catch (Throwable e) {
			adapter.rollbackEntityManager(null,manager);
			throw IOExceptionSupport.create(e);
		}
		adapter.commitEntityManager(null,manager);
		return rc;
	}

	private ActiveMQDestination toSubscribedDestination(StoredSubscription ss) {
		if( ss.getSubscribedDestination() == null )
			return null;
		return ActiveMQDestination.createDestination(ss.getSubscribedDestination(), ActiveMQDestination.QUEUE_TYPE);
	}

	public void recoverNextMessages(String clientId, String subscriptionName, int maxReturned, MessageRecoveryListener listener) throws Exception {
		EntityManager manager = adapter.beginEntityManager(null);
		try {
			SubscriptionId id = new SubscriptionId();
			id.setClientId(clientId);
			id.setSubscriptionName(subscriptionName);
			id.setDestination(destinationName);
	
			AtomicLong last=subscriberLastMessageMap.get(id);
	        if(last==null){
	    		StoredSubscription ss = findStoredSubscription(manager, clientId, subscriptionName);
	            last=new AtomicLong(ss.getLastAckedId());
	            subscriberLastMessageMap.put(id,last);
	        }
	        final AtomicLong lastMessageId=last;
			
			Query query = manager.createQuery("select m from StoredMessage m where m.destination=?1 and m.id>?2 order by m.id asc");
			query.setParameter(1, destinationName);
			query.setParameter(2, lastMessageId.get());
			query.setMaxResults(maxReturned);
			int count = 0;
			for (StoredMessage m : (List<StoredMessage>)query.getResultList()) {
				Message message = (Message) wireFormat.unmarshal(new ByteSequence(m.getData()));
				listener.recoverMessage(message);
				lastMessageId.set(m.getId());
				count++;
				if( count >= maxReturned ) { 
					return;
				}
	        }        
		} catch (Throwable e) {
			adapter.rollbackEntityManager(null,manager);
			throw IOExceptionSupport.create(e);
		}
		adapter.commitEntityManager(null,manager);
	}

	public void recoverSubscription(String clientId, String subscriptionName, MessageRecoveryListener listener) throws Exception {
		EntityManager manager = adapter.beginEntityManager(null);
		try {
	
			StoredSubscription ss = findStoredSubscription(manager, clientId, subscriptionName);
			
			Query query = manager.createQuery("select m from StoredMessage m where m.destination=?1 and m.id>?2 order by m.id asc");
			query.setParameter(1, destinationName);
			query.setParameter(2, ss.getLastAckedId());
			for (StoredMessage m : (List<StoredMessage>)query.getResultList()) {
				Message message = (Message) wireFormat.unmarshal(new ByteSequence(m.getData()));
				listener.recoverMessage(message);
	        }
		} catch (Throwable e) {
			adapter.rollbackEntityManager(null,manager);
			throw IOExceptionSupport.create(e);
		}
		adapter.commitEntityManager(null,manager);
	}

	public void resetBatching(String clientId, String subscriptionName) {
		SubscriptionId id = new SubscriptionId();
		id.setClientId(clientId);
		id.setSubscriptionName(subscriptionName);
		id.setDestination(destinationName);

        subscriberLastMessageMap.remove(id);
	}

}
