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

import java.util.List;
import java.util.Map;

import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SessionId;

/**
 * 
 */

public interface TransportConnectionStateRegister{

    TransportConnectionState registerConnectionState(ConnectionId connectionId,
                                                               TransportConnectionState state);
    
    TransportConnectionState unregisterConnectionState(ConnectionId connectionId);

    List<TransportConnectionState> listConnectionStates();
    
    Map<ConnectionId,TransportConnectionState>mapStates();
    
    TransportConnectionState lookupConnectionState(String connectionId);
    
    TransportConnectionState lookupConnectionState(ConsumerId id);
    
    TransportConnectionState lookupConnectionState(ProducerId id);
    
    TransportConnectionState lookupConnectionState(SessionId id);

    TransportConnectionState lookupConnectionState(ConnectionId connectionId);
        
    boolean isEmpty();
    
    boolean doesHandleMultipleConnectionStates();
    
    void intialize(TransportConnectionStateRegister other);
    
    void clear();

}
