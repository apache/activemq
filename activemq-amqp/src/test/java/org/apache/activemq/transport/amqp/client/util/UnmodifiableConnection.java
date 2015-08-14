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
package org.apache.activemq.transport.amqp.client.util;

import java.util.EnumSet;
import java.util.Map;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Record;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.reactor.Reactor;

/**
 * Unmodifiable Connection wrapper used to prevent test code from accidentally
 * modifying Connection state.
 */
public class UnmodifiableConnection implements Connection {

    private final Connection connection;

    public UnmodifiableConnection(Connection connection) {
        this.connection = connection;
    }

    @Override
    public EndpointState getLocalState() {
        return connection.getLocalState();
    }

    @Override
    public EndpointState getRemoteState() {
        return connection.getRemoteState();
    }

    @Override
    public ErrorCondition getCondition() {
        return connection.getCondition();
    }

    @Override
    public void setCondition(ErrorCondition condition) {
        throw new UnsupportedOperationException("Cannot alter the Connection");
    }

    @Override
    public ErrorCondition getRemoteCondition() {
        return connection.getRemoteCondition();
    }

    @Override
    public void free() {
        throw new UnsupportedOperationException("Cannot alter the Connection");
    }

    @Override
    public void open() {
        throw new UnsupportedOperationException("Cannot alter the Connection");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Cannot alter the Connection");
    }

    @Override
    public Session session() {
        throw new UnsupportedOperationException("Cannot alter the Connection");
    }

    @Override
    public Session sessionHead(EnumSet<EndpointState> local, EnumSet<EndpointState> remote) {
        Session head = connection.sessionHead(local, remote);
        if (head != null) {
            head = new UnmodifiableSession(head);
        }

        return head;
    }

    @Override
    public Link linkHead(EnumSet<EndpointState> local, EnumSet<EndpointState> remote) {
        // TODO - If implemented this method should return an unmodifiable link isntance.
        return null;
    }

    @Override
    public Delivery getWorkHead() {
        // TODO - If implemented this method should return an unmodifiable delivery isntance.
        return null;
    }

    @Override
    public void setContainer(String container) {
        throw new UnsupportedOperationException("Cannot alter the Connection");
    }

    @Override
    public void setHostname(String hostname) {
        throw new UnsupportedOperationException("Cannot alter the Connection");
    }

    @Override
    public String getHostname() {
        return connection.getHostname();
    }

    @Override
    public String getRemoteContainer() {
        return connection.getRemoteContainer();
    }

    @Override
    public String getRemoteHostname() {
        return connection.getRemoteHostname();
    }

    @Override
    public void setOfferedCapabilities(Symbol[] capabilities) {
        throw new UnsupportedOperationException("Cannot alter the Connection");
    }

    @Override
    public void setDesiredCapabilities(Symbol[] capabilities) {
        throw new UnsupportedOperationException("Cannot alter the Connection");
    }

    @Override
    public Symbol[] getRemoteOfferedCapabilities() {
        return connection.getRemoteOfferedCapabilities();
    }

    @Override
    public Symbol[] getRemoteDesiredCapabilities() {
        return connection.getRemoteDesiredCapabilities();
    }

    @Override
    public Map<Symbol, Object> getRemoteProperties() {
        return connection.getRemoteProperties();
    }

    @Override
    public void setProperties(Map<Symbol, Object> properties) {
        throw new UnsupportedOperationException("Cannot alter the Connection");
    }

    @Override
    public Object getContext() {
        return connection.getContext();
    }

    @Override
    public void setContext(Object context) {
        throw new UnsupportedOperationException("Cannot alter the Connection");
    }

    @Override
    public void collect(Collector collector) {
        throw new UnsupportedOperationException("Cannot alter the Connection");
    }

    @Override
    public String getContainer() {
        return connection.getContainer();
    }

    @Override
    public Transport getTransport() {
        return new UnmodifiableTransport(connection.getTransport());
    }

    @Override
    public Record attachments() {
        return connection.attachments();
    }

    @Override
    public Reactor getReactor() {
        return connection.getReactor();
    }
}
