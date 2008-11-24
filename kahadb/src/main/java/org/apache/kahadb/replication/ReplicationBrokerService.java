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
package org.apache.kahadb.replication;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.broker.BrokerService;

/**
 * This broker service actually does not do anything.  It allows you to create an activemq.xml file
 * which does not actually start a broker.  Used in conjunction with the ReplicationService since
 * he will create the actual BrokerService
 * 
 * @author chirino
 * @org.apache.xbean.XBean element="kahadbReplicationBroker"
 */
public class ReplicationBrokerService extends BrokerService {

    ReplicationService replicationService;
    AtomicBoolean started = new AtomicBoolean();

    public ReplicationService getReplicationService() {
        return replicationService;
    }

    public void setReplicationService(ReplicationService replicationService) {
        this.replicationService = replicationService;
    }
    
    @Override
    public void start() throws Exception {
        if( started.compareAndSet(false, true) ) {
            replicationService.start();
        }
    }
    
    @Override
    public void stop() throws Exception {
        if( started.compareAndSet(true, false) ) {
            replicationService.stop();
        }
    }
}
