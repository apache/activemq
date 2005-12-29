/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker;

import java.io.IOException;

import org.apache.activemq.Service;
import org.apache.activemq.broker.AbstractConnection;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.Connection;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.util.ServiceSupport;
import org.axiondb.engine.commands.ShutdownCommand;

import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue;

public class StubConnection implements Service {

    private final BlockingQueue dispatchQueue = new LinkedBlockingQueue();
    private Connection connection;
    private Transport transport;
    boolean shuttingDown = false;

    public StubConnection(BrokerService broker) throws Exception {
        this(broker, null);
    }

    public StubConnection(BrokerService broker, TaskRunnerFactory taskRunnerFactory) throws Exception {
        connection = new AbstractConnection(null, broker.getBroker(), null) {
            protected void dispatch(Command command) {
                try {
                    StubConnection.this.dispatch(command);
                }
                catch (Exception e) {
                    serviceException(e);
                }
            }
        };
    }

    protected void dispatch(Command command) throws InterruptedException, IOException {
        dispatchQueue.put(command);
    }

    public StubConnection(Connection connection) {
        this.connection = connection;
    }

    public StubConnection(Transport transport) throws Exception {
        this.transport = transport;
        transport.setTransportListener(new TransportListener() {
            public void onCommand(Command command) {
                try {
                    if (command.getClass() == ShutdownCommand.class) {
                        shuttingDown = true;
                    }
                    StubConnection.this.dispatch(command);
                }
                catch (Exception e) {
                    onException(new IOException("" + e));
                }
            }

            public void onException(IOException error) {
                if (!shuttingDown) {
                    error.printStackTrace();
                }
            }
        });
        transport.start();
    }

    public BlockingQueue getDispatchQueue() {
        return dispatchQueue;
    }

    public void send(Command command) throws Throwable {
        if( command instanceof Message ) {
            Message message = (Message) command;
            message.setProducerId(message.getMessageId().getProducerId());
        }
        command.setResponseRequired(false);
        if (connection != null) {
            Response response = connection.service(command);
            if (response != null && response.isException()) {
                ExceptionResponse er = (ExceptionResponse) response;
                throw er.getException();
            }
        }
        else if (transport != null) {
            transport.oneway(command);
        }
    }

    public Response request(Command command) throws Throwable {
        if( command instanceof Message ) {
            Message message = (Message) command;
            message.setProducerId(message.getMessageId().getProducerId());
        }
        command.setResponseRequired(true);
        if (connection != null) {
            Response response = connection.service(command);
            if (response != null && response.isException()) {
                ExceptionResponse er = (ExceptionResponse) response;
                throw er.getException();
            }
            return response;
        }
        else if (transport != null) {
            Response response = transport.request(command);
            if (response != null && response.isException()) {
                ExceptionResponse er = (ExceptionResponse) response;
                throw er.getException();
            }
            return response;
        }
        return null;
    }

    public Connection getConnection() {
        return connection;
    }

    public Transport getTransport() {
        return transport;
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
        shuttingDown = true;
        if (transport != null) {
            try {
                transport.oneway(new ShutdownInfo());
            }
            catch (IOException e) {
            }
            ServiceSupport.dispose(transport);
        }
    }
}
