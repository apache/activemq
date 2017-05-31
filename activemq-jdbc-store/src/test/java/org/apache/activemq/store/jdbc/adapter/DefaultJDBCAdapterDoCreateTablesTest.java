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

import static org.apache.log4j.Level.DEBUG;
import static org.apache.log4j.Level.WARN;
import static org.junit.Assert.assertEquals;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.store.jdbc.Statements;
import org.apache.activemq.store.jdbc.TransactionContext;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultJDBCAdapterDoCreateTablesTest {

	private static final String CREATE_STATEMENT1 = "createStatement1";
	private static final String CREATE_STATEMENT2 = "createStatement2";
	private static final String[] CREATE_STATEMENTS = new String[] { CREATE_STATEMENT1, CREATE_STATEMENT2 };
	private static final int VENDOR_CODE = 1;
	private static final String SQL_STATE = "SqlState";
	private static final String MY_REASON = "MyReason";

	private DefaultJDBCAdapter defaultJDBCAdapter;

	private List<LoggingEvent> loggingEvents = new ArrayList<>();

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

	@Before
	public void setUp() throws IOException, SQLException {
		DefaultTestAppender appender = new DefaultTestAppender() {
			@Override
			public void doAppend(LoggingEvent event) {
				loggingEvents.add(event);
			}
		};
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.DEBUG);
		rootLogger.addAppender(appender);


		defaultJDBCAdapter = new DefaultJDBCAdapter();
		defaultJDBCAdapter.statements = statements;

		when(statements.getCreateSchemaStatements()).thenReturn(CREATE_STATEMENTS);
		when(transactionContext.getConnection()).thenReturn(connection);
		when(connection.getMetaData().getTables(null, null, this.statements.getFullMessageTableName(),new String[] { "TABLE" })).thenReturn(resultSet);
		when(connection.createStatement()).thenReturn(statement1, statement2);
		when(connection.getAutoCommit()).thenReturn(true);
	}

	@After
	public void tearDown() {
		loggingEvents = new ArrayList<>();
	}

	@Test
	public void createsTheTablesWhenNoMessageTableExistsAndLogsSqlExceptionsInWarnLevel() throws IOException, SQLException {
		when(resultSet.next()).thenReturn(false);
		when(statement2.execute(CREATE_STATEMENT2)).thenThrow(new SQLException(MY_REASON, SQL_STATE, VENDOR_CODE));

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

		assertEquals(4, loggingEvents.size());
		assertLog(0, DEBUG, "Executing SQL: " + CREATE_STATEMENT1);
		assertLog(1, DEBUG, "Executing SQL: " + CREATE_STATEMENT2);
		assertLog(2, WARN, "Could not create JDBC tables; they could already exist. Failure was: " + CREATE_STATEMENT2 + " Message: " + MY_REASON + " SQLState: " + SQL_STATE + " Vendor code: " + VENDOR_CODE);
		assertLog(3, WARN, "Failure details: " + MY_REASON);
	}

	@Test
	public void triesTocreateTheTablesWhenMessageTableExistsAndLogsSqlExceptionsInDebugLevel() throws SQLException, IOException {
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
		assertLog(0, DEBUG, "Executing SQL: " + CREATE_STATEMENT1);
		assertLog(1, DEBUG, "Could not create JDBC tables; The message table already existed. Failure was: " + CREATE_STATEMENT1 + " Message: " + MY_REASON + " SQLState: " + SQL_STATE	+ " Vendor code: " + VENDOR_CODE);
		assertLog(2, DEBUG, "Executing SQL: " + CREATE_STATEMENT2);
	}

	@Test
	public void commitsTheTransactionWhenAutoCommitIsDisabled() throws SQLException, IOException {
		when(connection.getAutoCommit()).thenReturn(false);
		when(resultSet.next()).thenReturn(false);

		defaultJDBCAdapter.doCreateTables(transactionContext);

		InOrder inOrder = inOrder(resultSet, connection, statement1, statement2);
		inOrder.verify(resultSet).next();
		inOrder.verify(resultSet).close();
		inOrder.verify(connection).createStatement();
		inOrder.verify(statement1).execute(CREATE_STATEMENT1);
		inOrder.verify(connection).commit();
		inOrder.verify(statement1).close();
		inOrder.verify(connection).createStatement();
		inOrder.verify(statement2).execute(CREATE_STATEMENT2);
		inOrder.verify(connection).commit();
		inOrder.verify(statement2).close();

		assertEquals(2, loggingEvents.size());
		assertLog(0, DEBUG, "Executing SQL: " + CREATE_STATEMENT1);
		assertLog(1, DEBUG, "Executing SQL: " + CREATE_STATEMENT2);
	}

	private void assertLog(int messageNumber, Level level, String message) {
		LoggingEvent loggingEvent = loggingEvents.get(messageNumber);
		assertEquals(level, loggingEvent.getLevel());
		assertEquals(message, loggingEvent.getMessage());
	}
}