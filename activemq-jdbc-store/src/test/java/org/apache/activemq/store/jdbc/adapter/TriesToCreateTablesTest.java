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

import org.apache.activemq.store.jdbc.Statements;
import org.apache.activemq.store.jdbc.TransactionContext;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.MessageLayout;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TriesToCreateTablesTest {

    private static final String CREATE_STATEMENT1 = "createStatement1";
    private static final String CREATE_STATEMENT2 = "createStatement2";
    private static final String[] CREATE_STATEMENTS = new String[] { CREATE_STATEMENT1, CREATE_STATEMENT2 };
    private static final int VENDOR_CODE = 1;
    private static final String SQL_STATE = "SqlState";
    private static final String MY_REASON = "MyReason";

    private DefaultJDBCAdapter defaultJDBCAdapter;

    private List<LogEvent> loggingEvents = new ArrayList<>();

    @Mock
    private TransactionContext transactionContext;

    @Mock(answer = RETURNS_DEEP_STUBS)
    private Connection connection;

    @Mock
    private Statements statements;

    @Mock
    private ResultSet resultSet;

    @Mock
    private Statement statement1, statement2;

    @Test
    public void triesToCreateTheTablesWhenMessageTableExistsAndLogsSqlExceptionsInDebugLevel() throws SQLException, IOException {
        final var logger = org.apache.logging.log4j.core.Logger.class.cast(LogManager.getRootLogger());
        final var appender = new AbstractAppender("testAppender", new AbstractFilter() {}, new MessageLayout(), false, new Property[0]) {
            @Override
            public void append(LogEvent event) {
                loggingEvents.add(event.toImmutable());
            }
        };
        appender.start();

        logger.get().addAppender(appender, Level.DEBUG, new AbstractFilter() {});
        logger.addAppender(appender);

        defaultJDBCAdapter = new DefaultJDBCAdapter();
        defaultJDBCAdapter.statements = statements;

        when(statements.getCreateSchemaStatements()).thenReturn(CREATE_STATEMENTS);
        when(transactionContext.getConnection()).thenReturn(connection);
        when(connection.getMetaData().getTables(null, null, this.statements.getFullMessageTableName(),new String[] { "TABLE" })).thenReturn(resultSet);
        when(connection.createStatement()).thenReturn(statement1, statement2);
        when(connection.getAutoCommit()).thenReturn(true);

        when(resultSet.next()).thenReturn(true);
        when(statement1.execute(CREATE_STATEMENT1)).thenThrow(new SQLException(MY_REASON, SQL_STATE, VENDOR_CODE));

        defaultJDBCAdapter.doCreateTables(transactionContext);

        InOrder inOrder = inOrder(resultSet, connection, statement1, statement2);
        inOrder.verify(resultSet).next();
        inOrder.verify(resultSet).close();
        inOrder.verify(connection).createStatement();
        inOrder.verify(statement1).execute(CREATE_STATEMENT1);
        inOrder.verify(statement1).close();
        inOrder.verify(connection).createStatement();
        inOrder.verify(statement2).execute(CREATE_STATEMENT2);
        inOrder.verify(statement2).close();

        assertEquals(3, loggingEvents.size());
        assertLog(0, Level.DEBUG, "Executing SQL: " + CREATE_STATEMENT1);
        assertLog(1, Level.DEBUG, "Could not create JDBC tables; The message table already existed. Failure was: " + CREATE_STATEMENT1 + " Message: " + MY_REASON + " SQLState: " + SQL_STATE	+ " Vendor code: " + VENDOR_CODE);
        assertLog(2, Level.DEBUG, "Executing SQL: " + CREATE_STATEMENT2);

        loggingEvents = new ArrayList<>();
    }

    private void assertLog(int messageNumber, Level level, String message) {
        LogEvent loggingEvent = loggingEvents.get(messageNumber);
        assertEquals(level, loggingEvent.getLevel());
        assertEquals(message, loggingEvent.getMessage().getFormattedMessage());
    }

}
