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

import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Record;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;

/**
 * Unmodifiable Session wrapper used to prevent test code from accidentally
 * modifying Session state.
 */
public class UnmodifiableSession implements Session {

    private final Session session;

    public UnmodifiableSession(Session session) {
        this.session = session;
    }

    @Override
    public EndpointState getLocalState() {
        return session.getLocalState();
    }

    @Override
    public EndpointState getRemoteState() {
        return session.getRemoteState();
    }

    @Override
    public ErrorCondition getCondition() {
        return session.getCondition();
    }

    @Override
    public void setCondition(ErrorCondition condition) {
        throw new UnsupportedOperationException("Cannot alter the Session");
    }

    @Override
    public ErrorCondition getRemoteCondition() {
        return session.getRemoteCondition();
    }

    @Override
    public void free() {
        throw new UnsupportedOperationException("Cannot alter the Session");
    }

    @Override
    public void open() {
        throw new UnsupportedOperationException("Cannot alter the Session");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Cannot alter the Session");
    }

    @Override
    public void setContext(Object o) {
        throw new UnsupportedOperationException("Cannot alter the Session");
    }

    @Override
    public Object getContext() {
        return session.getContext();
    }

    @Override
    public Sender sender(String name) {
        throw new UnsupportedOperationException("Cannot alter the Session");
    }

    @Override
    public Receiver receiver(String name) {
        throw new UnsupportedOperationException("Cannot alter the Session");
    }

    @Override
    public Session next(EnumSet<EndpointState> local, EnumSet<EndpointState> remote) {
        Session next = session.next(local, remote);
        if (next != null) {
            next = new UnmodifiableSession(next);
        }

        return next;
    }

    @Override
    public Connection getConnection() {
        return new UnmodifiableConnection(session.getConnection());
    }

    @Override
    public int getIncomingCapacity() {
        return session.getIncomingCapacity();
    }

    @Override
    public void setIncomingCapacity(int bytes) {
        throw new UnsupportedOperationException("Cannot alter the Session");
    }

    @Override
    public int getIncomingBytes() {
        return session.getIncomingBytes();
    }

    @Override
    public int getOutgoingBytes() {
        return session.getOutgoingBytes();
    }

    @Override
    public Record attachments() {
        return session.attachments();
    }

    @Override
    public long getOutgoingWindow() {
        return session.getOutgoingWindow();
    }

    @Override
    public void setOutgoingWindow(long outgoingWindowSize) {
        throw new UnsupportedOperationException("Cannot alter the Session");
    }
}
