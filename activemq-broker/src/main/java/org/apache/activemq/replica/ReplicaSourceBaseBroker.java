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
package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ReplicaSourceBaseBroker extends BrokerFilter {
    private final ReplicationMessageProducer replicationMessageProducer;
    protected final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();

    private final AtomicBoolean initialized = new AtomicBoolean();

    ReplicaSourceBaseBroker(Broker next, ReplicationMessageProducer replicationMessageProducer) {
        super(next);
        this.replicationMessageProducer = replicationMessageProducer;
    }

    @Override
    public void start() throws Exception {
        initialized.compareAndSet(false, true);
        super.start();
    }


    protected void enqueueReplicaEvent(ConnectionContext connectionContext, ReplicaEvent event) throws Exception {
        if (isReplicaContext(connectionContext)) {
            return;
        }
        if (!initialized.get()) {
            return;
        }
        replicationMessageProducer.enqueueReplicaEvent(connectionContext, event);
    }

    protected boolean isReplicaContext(ConnectionContext initialContext) {
        return initialContext != null && ReplicaSupport.REPLICATION_PLUGIN_USER_NAME.equals(initialContext.getUserName());
    }

}
