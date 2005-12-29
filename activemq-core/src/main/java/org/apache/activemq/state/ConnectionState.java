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

package org.apache.activemq.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

public class ConnectionState {
    
    final ConnectionInfo info;
    private final ConcurrentHashMap sessions = new ConcurrentHashMap();
    private final List tempDestinations = Collections.synchronizedList(new ArrayList());
    
    public ConnectionState(ConnectionInfo info) {
        this.info = info;
        // Add the default session id.
        addSession(new SessionInfo(info, -1));
    }
    
    public String toString() {
        return info.toString();
    }

    public void addTempDestination(ActiveMQDestination destination) {
        tempDestinations.add(destination);
    }

    public void removeTempDestination(ActiveMQDestination destination) {
        tempDestinations.remove(destination);
    }

    public void addSession(SessionInfo info) {
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
}