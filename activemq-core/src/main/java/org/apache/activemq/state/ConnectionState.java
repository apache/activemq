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

package org.apache.activemq.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;

public class ConnectionState {

    ConnectionInfo info;
    private final ConcurrentHashMap transactions = new ConcurrentHashMap();
    private final ConcurrentHashMap sessions = new ConcurrentHashMap();
    private final List tempDestinations = Collections.synchronizedList(new ArrayList());
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public ConnectionState(ConnectionInfo info) {
        this.info = info;
        // Add the default session id.
        addSession(new SessionInfo(info, -1));
    }

    public String toString() {
        return info.toString();
    }

    public void reset(ConnectionInfo info) {
        this.info = info;
        transactions.clear();
        sessions.clear();
        tempDestinations.clear();
        shutdown.set(false);
    }

    public void addTempDestination(DestinationInfo info) {
        checkShutdown();
        tempDestinations.add(info);
    }

    public void removeTempDestination(ActiveMQDestination destination) {
        for (Iterator iter = tempDestinations.iterator(); iter.hasNext();) {
            DestinationInfo di = (DestinationInfo)iter.next();
            if (di.getDestination().equals(destination)) {
                iter.remove();
            }
        }
    }

    public void addTransactionState(TransactionId id) {
        checkShutdown();
        transactions.put(id, new TransactionState(id));
    }

    public TransactionState getTransactionState(TransactionId id) {
        return (TransactionState)transactions.get(id);
    }

    public Collection getTransactionStates() {
        return transactions.values();
    }

    public TransactionState removeTransactionState(TransactionId id) {
        return (TransactionState)transactions.remove(id);
    }

    public void addSession(SessionInfo info) {
        checkShutdown();
        sessions.put(info.getSessionId(), new SessionState(info));
    }

    public SessionState removeSession(SessionId id) {
        return (SessionState)sessions.remove(id);
    }

    public SessionState getSessionState(SessionId id) {
        return (SessionState)sessions.get(id);
    }

    public ConnectionInfo getInfo() {
        return info;
    }

    public Set getSessionIds() {
        return sessions.keySet();
    }

    public List getTempDesinations() {
        return tempDestinations;
    }

    public Collection getSessionStates() {
        return sessions.values();
    }

    private void checkShutdown() {
        if (shutdown.get())
            throw new IllegalStateException("Disposed");
    }

    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            for (Iterator iter = sessions.values().iterator(); iter.hasNext();) {
                SessionState ss = (SessionState)iter.next();
                ss.shutdown();
            }
        }
    }
}
