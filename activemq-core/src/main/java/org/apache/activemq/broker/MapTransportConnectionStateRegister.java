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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SessionId;

/**
 * 
 */

public class MapTransportConnectionStateRegister  implements TransportConnectionStateRegister{

    private  Map <ConnectionId,TransportConnectionState>connectionStates = new ConcurrentHashMap<ConnectionId,TransportConnectionState>();

    public TransportConnectionState registerConnectionState(ConnectionId connectionId,
                                                               TransportConnectionState state) {
        TransportConnectionState rc = connectionStates.put(connectionId, state);
        return rc;
    }

    public TransportConnectionState unregisterConnectionState(ConnectionId connectionId) {
        TransportConnectionState rc = connectionStates.remove(connectionId);
        return rc;
    }

    public List<TransportConnectionState> listConnectionStates() {
    	
        List<TransportConnectionState> rc = new ArrayList<TransportConnectionState>();
        rc.addAll(connectionStates.values());
        return rc;
    }

    public TransportConnectionState lookupConnectionState(String connectionId) {
        return connectionStates.get(new ConnectionId(connectionId));
    }

    public TransportConnectionState lookupConnectionState(ConsumerId id) {
        TransportConnectionState cs = lookupConnectionState(id.getConnectionId());
        if (cs == null) {
            throw new IllegalStateException(
                                            "Cannot lookup a consumer from a connection that had not been registered: "
                                                + id.getParentId().getParentId());
        }
        return cs;
    }

    public TransportConnectionState lookupConnectionState(ProducerId id) {
    	 TransportConnectionState cs = lookupConnectionState(id.getConnectionId());
        if (cs == null) {
            throw new IllegalStateException(
                                            "Cannot lookup a producer from a connection that had not been registered: "
                                                + id.getParentId().getParentId());
        }
        return cs;
    }

    public TransportConnectionState lookupConnectionState(SessionId id) {
    	 TransportConnectionState cs = lookupConnectionState(id.getConnectionId());
        if (cs == null) {
            throw new IllegalStateException(
                                            "Cannot lookup a session from a connection that had not been registered: "
                                                + id.getParentId());
        }
        return cs;
    }

    public TransportConnectionState lookupConnectionState(ConnectionId connectionId) {
        TransportConnectionState cs = connectionStates.get(connectionId);
        if (cs == null) {
            throw new IllegalStateException("Cannot lookup a connection that had not been registered: "
                                            + connectionId);
        }
        return cs;
    }

	

	public boolean doesHandleMultipleConnectionStates() {
		return true;
	}

	public boolean isEmpty() {
		return connectionStates.isEmpty();
	}

	public void clear() {
		connectionStates.clear();
		
	}

	public void intialize(TransportConnectionStateRegister other) {
		connectionStates.clear();
		connectionStates.putAll(other.mapStates());
		
	}

	public Map<ConnectionId, TransportConnectionState> mapStates() {
		HashMap<ConnectionId, TransportConnectionState> map = new HashMap<ConnectionId, TransportConnectionState>(connectionStates);
		return map;
	}

}
