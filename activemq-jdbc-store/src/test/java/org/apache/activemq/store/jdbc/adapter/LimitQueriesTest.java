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
package org.apache.activemq.store.jdbc.adapter;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.jdbc.JDBCMessageIdScanListener;
import org.apache.activemq.store.jdbc.JDBCMessageRecoveryListener;
import org.apache.activemq.store.jdbc.Statements;
import org.apache.activemq.store.jdbc.TransactionContext;
import org.apache.activemq.util.DataByteArrayOutputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.sql.Connection;

import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LimitQueriesTest {

    @Mock
    private TransactionContext transactionContext;

    @Mock(answer = RETURNS_DEEP_STUBS)
    private Connection connection;


    @org.junit.Test
    public void callsLimit() throws Exception {
        final Statements statements = new Statements();

        final DefaultJDBCAdapter defaultJDBCAdapter = spy(new DefaultJDBCAdapter());
        defaultJDBCAdapter.statements = statements;

        when(transactionContext.getConnection()).thenReturn(connection);

        defaultJDBCAdapter.doMessageIdScan(transactionContext, 10, mock(JDBCMessageIdScanListener.class));
        verify(defaultJDBCAdapter).limitQuery(anyString());
        reset(defaultJDBCAdapter);

        defaultJDBCAdapter.doRecoverNextMessages(transactionContext, mock(ActiveMQDestination.class), "foo",
                                                 "bar", 10l, 11l, 20, mock(JDBCMessageRecoveryListener.class));
        verify(defaultJDBCAdapter).limitQuery(anyString());
        reset(defaultJDBCAdapter);

        defaultJDBCAdapter.doRecoverNextMessagesWithPriority(transactionContext, mock(ActiveMQDestination.class), "foo",
                                                             "bar", 10l, 20l, 2, mock(JDBCMessageRecoveryListener.class));
        verify(defaultJDBCAdapter).limitQuery(anyString());
        reset(defaultJDBCAdapter);

        defaultJDBCAdapter.doRecoverNextMessages(transactionContext, mock(ActiveMQDestination.class), "foo",
                                                 "bar", 10l, 20l, 2, mock(JDBCMessageRecoveryListener.class));
        verify(defaultJDBCAdapter).limitQuery(anyString());
        reset(defaultJDBCAdapter);
    }

    @Test
    public void dontCallLimitQuery() throws Exception {

        final Statements statements = new Statements();

        final DefaultJDBCAdapter defaultJDBCAdapter = spy(new DefaultJDBCAdapter());
        defaultJDBCAdapter.statements = statements;

        when(transactionContext.getConnection()).thenReturn(connection);
        when(transactionContext.getExclusiveConnection()).thenReturn(connection);

        JDBCMessageRecoveryListener messageRecoveryListener = mock(JDBCMessageRecoveryListener.class);
        ActiveMQDestination destination = mock(ActiveMQDestination.class);
        final MessageId messageId = mock(MessageId.class, RETURNS_DEEP_STUBS);
        final XATransactionId xaTransactionId = mock(XATransactionId.class, RETURNS_DEEP_STUBS);
        when(xaTransactionId.getEncodedXidBytes()).thenReturn(new byte[] {0, 1});
        when(xaTransactionId.internalOutputStream()).thenReturn(new DataByteArrayOutputStream());

        // call some methods and assert that limit is never called
        try {
            defaultJDBCAdapter.doUpdateMessage(transactionContext, destination, messageId, new byte[0]);
        } catch (final IOException e) {
            // Expected exception due to the mock setup
        }
        defaultJDBCAdapter.doDeleteSubscription(transactionContext, destination, "clientId", "subName");
        defaultJDBCAdapter.doDeleteOldMessages(transactionContext);
        defaultJDBCAdapter.doRecoverSubscription(transactionContext, destination, "clientId", "subName", messageRecoveryListener);
        defaultJDBCAdapter.doCreateTables(transactionContext);
        defaultJDBCAdapter.doDropTables(transactionContext);
        defaultJDBCAdapter.doAddMessage(transactionContext, 10l, messageId, destination,
                                        new byte[0], 10l, (byte) 1, xaTransactionId);
        defaultJDBCAdapter.doRemoveMessage(transactionContext, (byte) 1, xaTransactionId);
        defaultJDBCAdapter.doSetLastAck(transactionContext, destination, xaTransactionId,
                                        "subName", "bar", 1L, 1L);

        verify(defaultJDBCAdapter, never()).limitQuery(anyString());
        verify(defaultJDBCAdapter, never()).limitQuery(anyString());
    }

}