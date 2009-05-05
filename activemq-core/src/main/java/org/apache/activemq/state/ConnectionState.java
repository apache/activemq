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
    private final ConcurrentHashMap<TransactionId, TransactionState> transactions = new ConcurrentHashMap<TransactionId, TransactionState>();
    private final ConcurrentHashMap<SessionId, SessionState> sessions = new ConcurrentHashMap<SessionId, SessionState>();
    private final List<DestinationInfo> tempDestinations = Collections.synchronizedList(new ArrayList<DestinationInfo>());
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
        // Add the default session id.
        addSession(new SessionInfo(info, -1));
    }

    public void addTempDestination(DestinationInfo info) {
        checkShutdown();
        tempDestinations.add(info);
    }

    public void removeTempDestination(ActiveMQDestination destination) {
        for (Iterator<DestinationInfo> iter = tempDestinations.iterator(); iter.hasNext();) {
            DestinationInfo di = iter.next();
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
        return transactions.get(id);
    }

    public Collection<TransactionState> getTransactionStates() {
        return transactions.values();
    }

    public TransactionState removeTransactionState(TransactionId id) {
        return transactions.remove(id);
    }

    public void addSession(SessionInfo info) {
        checkShutdown();
        sessions.put(info.getSessionId(), new SessionState(info));
    }

    public SessionState removeSession(SessionId id) {
        return sessions.remove(id);
    }

    public SessionState getSessionState(SessionId id) {
        return sessions.get(id);
    }

    public ConnectionInfo getInfo() {
        return info;
    }

    public Set<SessionId> getSessionIds() {
        return sessions.keySet();
    }

    public List<DestinationInfo> getTempDesinations() {
        return tempDestinations;
    }

    public Collection<SessionState> getSessionStates() {
        return sessions.values();
    }

    private void checkShutdown() {
        if (shutdown.get()) {
            throw new IllegalStateException("Disposed");
        }
    }

    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            for (Iterator<SessionState> iter = sessions.values().iterator(); iter.hasNext();) {
                SessionState ss = iter.next();
                ss.shutdown();
            }
        }
    }
}
