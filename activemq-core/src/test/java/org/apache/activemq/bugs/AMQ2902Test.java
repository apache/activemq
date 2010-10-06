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
package org.apache.activemq.bugs;

import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.transport.TransportDisposedIOException;
import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

public class AMQ2902Test extends TestCase {
    final AtomicBoolean gotExceptionInLog = new AtomicBoolean(Boolean.FALSE);

    Appender appender = new Appender() {
        public void addFilter(Filter newFilter) {
        }

        public Filter getFilter() {
            return null;
        }

        public void clearFilters() {
        }

        public void close() {
        }

        public void doAppend(LoggingEvent event) {
            if (event.getThrowableInformation() != null
                    && event.getThrowableInformation().getThrowable() instanceof TransportDisposedIOException) {
                gotExceptionInLog.set(Boolean.TRUE);
            }
            return;
        }

        public String getName() {
            return "AMQ2902TestAppender";
        }

        public void setErrorHandler(ErrorHandler errorHandler) {
        }

        public ErrorHandler getErrorHandler() {
            return null;
        }

        public void setLayout(Layout layout) {
        }

        public Layout getLayout() {
            return null;
        }

        public void setName(String name) {
        }

        public boolean requiresLayout() {
            return false;
        }
    };

    public void testNoExceptionOnClosewithStartStop() throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                "vm://localhost?broker.persistent=false");
        Connection connection = connectionFactory.createConnection();
        connection.start();
        connection.stop();
        connection.close();
    }

    public void testNoExceptionOnClose() throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                "vm://localhost?broker.persistent=false");
        Connection connection = connectionFactory.createConnection();
        connection.close();
    }

    public void setUp() throws Exception {
        gotExceptionInLog.set(Boolean.FALSE);
        Logger.getRootLogger().addAppender(appender);
        Logger.getLogger(TransportConnection.class.getName() + ".Transport").setLevel(Level.DEBUG);
    }

    public void tearDown() throws Exception {
        Logger.getRootLogger().removeAppender(appender);
        assertFalse("got unexpected ex in log on graceful close", gotExceptionInLog.get());
    }
}
