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

package org.apache.activemq;

import java.lang.reflect.Field;

import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.SessionId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * This test verifies that when there seems that there are no messages left queued on the listeners,
 * no messages are dispatched from the session if the listener queue is not empty.
 */
public class ActiveMQOrderLostTest {

	private ConnectionInfo connectionInfo = Mockito.mock(ConnectionInfo.class);

	private ActiveMQConnection connection = Mockito.mock(ActiveMQConnection.class);

	private ConsumerId consumerId = new ConsumerId();

	private SessionId sessionId = Mockito.mock(SessionId.class);

	private ActiveMQMessageConsumer consumer = Mockito.mock(ActiveMQMessageConsumer.class);
	
	private ActiveMQSessionExecutor executor;

	@Before
	public void before() throws Exception
	{
		Mockito.when(connection.getConnectionInfo()).thenReturn(connectionInfo);
		Mockito.when(connectionInfo.getConnectionId()).thenReturn(new ConnectionId());
		Mockito.when(connection.isMessagePrioritySupported()).thenReturn(false);
		Field unconsumedMessages = ActiveMQMessageConsumer.class.getDeclaredField("unconsumedMessages");
		unconsumedMessages.setAccessible(true);
		Mockito.when(consumer.isDurableSubscriber()).thenReturn(false);
		Mockito.when(consumer.getConsumerId()).thenReturn(consumerId);
		MessageDispatchChannel channel = Mockito.mock(MessageDispatchChannel.class);
		unconsumedMessages.set(consumer, channel);
		Mockito.when(consumer.unconsumedMessages.isEmpty()).thenReturn(false);
		Mockito.doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				return null;
			}
		}).when(connection).addDispatcher(Mockito.any(), Mockito.any());
		ActiveMQSession session = new ActiveMQSession(connection, sessionId, 1, true);
		session.addConsumer(consumer);
		executor = new ActiveMQSessionExecutor(session);
		Field field = ActiveMQSessionExecutor.class.getDeclaredField("messageQueue");
		MessageDispatchChannel messageDispatchChannel = Mockito.mock(MessageDispatchChannel.class);
		MessageDispatch message = Mockito.mock(MessageDispatch.class);
		Mockito.when(message.getConsumerId()).thenReturn(consumerId);
		Mockito.when(messageDispatchChannel.dequeueNoWait()).thenReturn(message);
        field.setAccessible(true);
        field.set(executor, messageDispatchChannel);
	}
	@Test
	public void orderLostWhenUnconsumedMessagesAreNotStarted() throws Exception {
		executor.iterate();
		Mockito.verify(consumer, Mockito.never()).dispatch(Mockito.any());
	}
}
