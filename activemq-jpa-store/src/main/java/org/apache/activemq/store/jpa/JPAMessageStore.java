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
package org.apache.activemq.store.jpa;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.jpa.model.StoredMessage;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;

public class JPAMessageStore implements MessageStore {

	protected final JPAPersistenceAdapter adapter;
	protected final WireFormat wireFormat;
	protected final ActiveMQDestination destination;
	protected final String destinationName;
    protected AtomicLong lastMessageId = new AtomicLong(-1);

	public JPAMessageStore(JPAPersistenceAdapter adapter, ActiveMQDestination destination) {
		this.adapter = adapter;
		this.destination = destination;
		this.destinationName = destination.getQualifiedName();
		this.wireFormat = this.adapter.getWireFormat();
	}

	public void addMessage(ConnectionContext context, Message message) throws IOException {
		
		EntityManager manager = adapter.beginEntityManager(context);
		try {
			
			ByteSequence sequence = wireFormat.marshal(message);
			sequence.compact();
			
			StoredMessage sm = new StoredMessage();
			sm.setDestination(destinationName);
			sm.setId(message.getMessageId().getBrokerSequenceId());
			sm.setMessageId(message.getMessageId().toString());
			sm.setExiration(message.getExpiration());
			sm.setData(sequence.data);
		
			manager.persist(sm);
			
		} catch (Throwable e) {
			adapter.rollbackEntityManager(context,manager);
			throw IOExceptionSupport.create(e);
		}
		adapter.commitEntityManager(context,manager);		
	}

	public ActiveMQDestination getDestination() {
		return destination;
	}

	public Message getMessage(MessageId identity) throws IOException {
		Message rc;
		EntityManager manager = adapter.beginEntityManager(null);
		try {
			StoredMessage message=null;
			if( identity.getBrokerSequenceId()!= 0 ) {
				message = manager.find(StoredMessage.class, identity.getBrokerSequenceId());			
			} else {
				Query query = manager.createQuery("select m from StoredMessage m where m.messageId=?1");
				query.setParameter(1, identity.toString());
				message = (StoredMessage) query.getSingleResult();
			}
			
			rc = (Message) wireFormat.unmarshal(new ByteSequence(message.getData()));
		} catch (Throwable e) {
			adapter.rollbackEntityManager(null,manager);
			throw IOExceptionSupport.create(e);
		}
		adapter.commitEntityManager(null,manager);
		return rc;
	}

	public int getMessageCount() throws IOException {
		Long rc;
		EntityManager manager = adapter.beginEntityManager(null);
		try {
			Query query = manager.createQuery("select count(m) from StoredMessage m");
			rc = (Long) query.getSingleResult();
		} catch (Throwable e) {
			adapter.rollbackEntityManager(null,manager);
			throw IOExceptionSupport.create(e);
		}
		adapter.commitEntityManager(null,manager);
		return rc.intValue();
	}

	@SuppressWarnings("unchecked")
	public void recover(MessageRecoveryListener container) throws Exception {
		EntityManager manager = adapter.beginEntityManager(null);
		try {
			Query query = manager.createQuery("select m from StoredMessage m where m.destination=?1 order by m.id asc");
			query.setParameter(1, destinationName);
			for (StoredMessage m : (List<StoredMessage>)query.getResultList()) {
				Message message = (Message) wireFormat.unmarshal(new ByteSequence(m.getData()));
				container.recoverMessage(message);
	        }
		} catch (Throwable e) {
			adapter.rollbackEntityManager(null,manager);
			throw IOExceptionSupport.create(e);
		}
		adapter.commitEntityManager(null,manager);
	}

	@SuppressWarnings("unchecked")
	public void recoverNextMessages(int maxReturned, MessageRecoveryListener listener) throws Exception {
		
		EntityManager manager = adapter.beginEntityManager(null);
		try {
			
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

	public void removeAllMessages(ConnectionContext context) throws IOException {
		EntityManager manager = adapter.beginEntityManager(context);
		try {
			Query query = manager.createQuery("delete from StoredMessage m where m.destination=?1");
			query.setParameter(1, destinationName);
			query.executeUpdate();
		} catch (Throwable e) {
			adapter.rollbackEntityManager(context,manager);
			throw IOExceptionSupport.create(e);
		}
		adapter.commitEntityManager(context,manager);
	}

	public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException {
		EntityManager manager = adapter.beginEntityManager(context);
		try {
			Query query = manager.createQuery("delete from StoredMessage m where m.id=?1");
			query.setParameter(1, ack.getLastMessageId().getBrokerSequenceId());
			query.executeUpdate();
		} catch (Throwable e) {
			adapter.rollbackEntityManager(context,manager);
			throw IOExceptionSupport.create(e);
		}
		adapter.commitEntityManager(context,manager);
	}

	public void resetBatching() {
        lastMessageId.set(-1);
	}

	public void setUsageManager(UsageManager usageManager) {
	}

	public void start() throws Exception {
	}

	public void stop() throws Exception {
	}

}
