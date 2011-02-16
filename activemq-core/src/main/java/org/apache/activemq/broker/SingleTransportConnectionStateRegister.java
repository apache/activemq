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

import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SessionId;

/**
 * 
 */

public class SingleTransportConnectionStateRegister  implements TransportConnectionStateRegister{

    private  TransportConnectionState connectionState;
    private ConnectionId connectionId;

    public TransportConnectionState registerConnectionState(ConnectionId connectionId,
                                                               TransportConnectionState state) {
        TransportConnectionState rc = connectionState;
        connectionState = state;
        this.connectionId = connectionId;
        return rc;
    }

    public synchronized TransportConnectionState unregisterConnectionState(ConnectionId connectionId) {
        TransportConnectionState rc = null;
        
       
        if (connectionId != null && connectionState != null && this.connectionId!=null){
        if (this.connectionId.equals(connectionId)){
			rc = connectionState;
			connectionState = null;
			connectionId = null;
		}
        }
        return rc;
    }

    public synchronized List<TransportConnectionState> listConnectionStates() {
        List<TransportConnectionState> rc = new ArrayList<TransportConnectionState>();
        if (connectionState != null) {
            rc.add(connectionState);
        }
        return rc;
    }

    public synchronized TransportConnectionState lookupConnectionState(String connectionId) {
        TransportConnectionState cs = connectionState;
        if (cs == null) {
            throw new IllegalStateException(
                                            "Cannot lookup a connectionId for a connection that had not been registered: "
                                                + connectionId);
        }
        return cs;
    }

    public synchronized TransportConnectionState lookupConnectionState(ConsumerId id) {
        TransportConnectionState cs = connectionState;
        if (cs == null) {
            throw new IllegalStateException(
                                            "Cannot lookup a consumer from a connection that had not been registered: "
                                                + id.getParentId().getParentId());
        }
        return cs;
    }

    public synchronized TransportConnectionState lookupConnectionState(ProducerId id) {
        TransportConnectionState cs = connectionState;
        if (cs == null) {
            throw new IllegalStateException(
                                            "Cannot lookup a producer from a connection that had not been registered: "
                                                + id.getParentId().getParentId());
        }
        return cs;
    }

    public synchronized TransportConnectionState lookupConnectionState(SessionId id) {
        TransportConnectionState cs = connectionState;
        if (cs == null) {
            throw new IllegalStateException(
                                            "Cannot lookup a session from a connection that had not been registered: "
                                                + id.getParentId());
        }
        return cs;
    }

    public synchronized TransportConnectionState lookupConnectionState(ConnectionId connectionId) {
        TransportConnectionState cs = connectionState;
        return cs;
    }

	public synchronized boolean doesHandleMultipleConnectionStates() {
		return false;
	}

	public synchronized boolean isEmpty() {
		return connectionState == null;
	}

	public void intialize(TransportConnectionStateRegister other) {
		
		if (other.isEmpty()){
			clear();
		}else{
			Map map = other.mapStates();
			Iterator i = map.entrySet().iterator();
			Map.Entry<ConnectionId, TransportConnectionState> entry = (Entry<ConnectionId, TransportConnectionState>) i.next();
			connectionId = entry.getKey();
			connectionState =entry.getValue();
		}
		
	}

	public Map<ConnectionId, TransportConnectionState> mapStates() {
		Map<ConnectionId, TransportConnectionState> map = new HashMap<ConnectionId, TransportConnectionState>();
		if (!isEmpty()) {
			map.put(connectionId, connectionState);
		}
		return map;
	}

	public void clear() {
		connectionState=null;
		connectionId=null;
		
	}

}
