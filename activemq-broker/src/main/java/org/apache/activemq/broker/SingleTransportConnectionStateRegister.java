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
package org.apache.activemq.broker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.util.Objects;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SessionId;

/**
 * 
 */

public class SingleTransportConnectionStateRegister implements TransportConnectionStateRegister{

    // Only store connectionState because it contains the connectionId inside
    // ConnectionInfo
    private TransportConnectionState connectionState;

    public synchronized TransportConnectionState registerConnectionState(ConnectionId connectionId,
                                                               TransportConnectionState state) {
        Objects.requireNonNull(connectionId, "connectionId must not be null");
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(state.getInfo(), "state must not have a null ConnectionInfo");
        if (!connectionId.equals(state.getInfo().getConnectionId())) {
            throw new IllegalArgumentException("connectionId and stateInfo must have same connectionId");
        }
        this.connectionState = state;
        return connectionState;
    }

    public synchronized TransportConnectionState unregisterConnectionState(ConnectionId connectionId) {
        TransportConnectionState existing = findConnectionState(connectionId);
        if (existing != null) {
            this.connectionState = null;
            return existing;
        }
        return null;
    }

    public synchronized List<TransportConnectionState> listConnectionStates() {
        List<TransportConnectionState> rc = new ArrayList<>();
        if (connectionState != null) {
            rc.add(connectionState);
        }
        return rc;
    }

    public synchronized TransportConnectionState lookupConnectionState(String connectionId) {
        TransportConnectionState cs = findConnectionState(connectionId);
        if (cs == null) {
            throw new IllegalStateException(
                                            "Cannot lookup a connectionId for a connection that had not been registered: "
                                                + connectionId);
        }
        return cs;
    }

    public synchronized TransportConnectionState lookupConnectionState(ConsumerId id) {
        TransportConnectionState cs = findConnectionState(id.getConnectionId());
        if (cs == null) {
            throw new IllegalStateException(
                                            "Cannot lookup a consumer from a connection that had not been registered: "
                                                + id.getParentId().getParentId());
        }
        return cs;
    }

    public synchronized TransportConnectionState lookupConnectionState(ProducerId id) {
        TransportConnectionState cs = findConnectionState(id.getConnectionId());
        if (cs == null) {
            throw new IllegalStateException(
                                            "Cannot lookup a producer from a connection that had not been registered: "
                                                + id.getParentId().getParentId());
        }
        return cs;
    }

    public synchronized TransportConnectionState lookupConnectionState(SessionId id) {
        TransportConnectionState cs = findConnectionState(id.getConnectionId());
        if (cs == null) {
            throw new IllegalStateException(
                                            "Cannot lookup a session from a connection that had not been registered: "
                                                + id.getParentId());
        }
        return cs;
    }

    public synchronized TransportConnectionState lookupConnectionState(ConnectionId connectionId) {
        // If null then this will just return a null value, this is expected in some cases
        // (such as duplex bridging)
        // This could be improved in the future to error if not registered, but the important
        // thing is to not return an existing registered state, unless it matches the connectionId
        return findConnectionState(connectionId);
    }

	public synchronized boolean doesHandleMultipleConnectionStates() {
		return false;
	}

	public synchronized boolean isEmpty() {
		return connectionState == null;
	}

	public synchronized void intialize(TransportConnectionStateRegister other) {
		if (other.isEmpty()){
			clear();
		} else {
			Map<ConnectionId, TransportConnectionState> map = other.mapStates();
			Iterator<Entry<ConnectionId, TransportConnectionState>> i = map.entrySet().iterator();
			Map.Entry<ConnectionId, TransportConnectionState> entry = i.next();
			connectionState = entry.getValue();
		}
	}

	public synchronized Map<ConnectionId, TransportConnectionState> mapStates() {
		Map<ConnectionId, TransportConnectionState> map = new HashMap<>();
		if (!isEmpty()) {
			map.put(getConnectionId(), connectionState);
		}
		return map;
	}

	public synchronized void clear() {
		connectionState = null;
	}

    private ConnectionId getConnectionId() {
        return !isEmpty() && connectionState.getInfo() != null ?
                connectionState.getInfo().getConnectionId() : null;
    }

    private TransportConnectionState findConnectionState(String connectionId) {
        return findConnectionState(new ConnectionId(connectionId));
    }

    private TransportConnectionState findConnectionState(ConnectionId connectionId) {
        // Only return the state if provided id is not null, and it matches the
        // existing connectionId of the state
        if (connectionId != null && connectionId.equals(getConnectionId())) {
            return connectionState;
        }
        return null;
    }
}
