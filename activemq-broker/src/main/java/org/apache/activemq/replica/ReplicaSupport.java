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

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Connector;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.util.LongSequenceGenerator;

import java.util.Set;
import java.util.UUID;

public class ReplicaSupport {

    private ReplicaSupport() {
        // Intentionally hidden
    }

    public static final int INTERMEDIATE_QUEUE_PREFETCH_SIZE = 10000;

    public static final String REPLICATION_CONNECTOR_NAME = "replication";

    public static final String REPLICATION_PLUGIN_CONNECTION_ID = "replicationID" + UUID.randomUUID();

    public static final LongSequenceGenerator LOCAL_TRANSACTION_ID_GENERATOR = new LongSequenceGenerator();

    public static final String REPLICATION_QUEUE_PREFIX = "ActiveMQ.Plugin.Replication.";
    public static final String MAIN_REPLICATION_QUEUE_NAME = REPLICATION_QUEUE_PREFIX + "Queue";
    public static final String INTERMEDIATE_REPLICATION_QUEUE_NAME = REPLICATION_QUEUE_PREFIX + "Intermediate.Queue";
    public static final String SEQUENCE_REPLICATION_QUEUE_NAME = REPLICATION_QUEUE_PREFIX + "Sequence.Queue";
    public static final String REPLICATION_ROLE_QUEUE_NAME = REPLICATION_QUEUE_PREFIX + "Role.Queue";
    public static final String REPLICATION_ROLE_ADVISORY_TOPIC_NAME = REPLICATION_QUEUE_PREFIX + "Role.Advisory.Topic";
    public static final String REPLICATION_PLUGIN_USER_NAME = "replication_plugin";

    public static final String TRANSACTION_ONE_PHASE_PROPERTY = "TRANSACTION_ONE_PHASE_PROPERTY";
    public static final String CLIENT_ID_PROPERTY = "CLIENT_ID_PROPERTY";
    public static final String IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY = "IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY";
    public static final String ORIGINAL_MESSAGE_DESTINATION_PROPERTY = "ORIGINAL_MESSAGE_DESTINATION_PROPERTY";
    public static final String IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY = "IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY";
    public static final String MESSAGE_ID_PROPERTY = "MessageIdProperty";
    public static final String MESSAGE_IDS_PROPERTY = "MessageIdsProperty";
    public static final String SEQUENCE_PROPERTY = "SEQUENCE_PROPERTY";

    public static final Object INTERMEDIATE_QUEUE_MUTEX = new Object();

    public static final Set<String> DELETABLE_REPLICATION_DESTINATION_NAMES = Set.of(MAIN_REPLICATION_QUEUE_NAME,
            INTERMEDIATE_REPLICATION_QUEUE_NAME, SEQUENCE_REPLICATION_QUEUE_NAME);
    public static final Set<String> REPLICATION_DESTINATION_NAMES = Set.of(MAIN_REPLICATION_QUEUE_NAME,
            INTERMEDIATE_REPLICATION_QUEUE_NAME, SEQUENCE_REPLICATION_QUEUE_NAME, REPLICATION_ROLE_QUEUE_NAME,
            REPLICATION_ROLE_ADVISORY_TOPIC_NAME);


    public static boolean isReplicationDestination(ActiveMQDestination destination) {
        return REPLICATION_DESTINATION_NAMES.contains(destination.getPhysicalName());
    }

    public static boolean isMainReplicationQueue(ActiveMQDestination destination) {
        return MAIN_REPLICATION_QUEUE_NAME.equals(destination.getPhysicalName());
    }

    public static boolean isIntermediateReplicationQueue(ActiveMQDestination destination) {
        return INTERMEDIATE_REPLICATION_QUEUE_NAME.equals(destination.getPhysicalName());
    }

    public static boolean isReplicationRoleAdvisoryTopic(ActiveMQDestination destination) {
        return REPLICATION_ROLE_ADVISORY_TOPIC_NAME.equals(destination.getPhysicalName());
    }

    public static boolean isReplicationTransport(Connector connector) {
        return connector instanceof TransportConnector && ((TransportConnector) connector).getName().equals(REPLICATION_CONNECTOR_NAME);
    }

    public static boolean isAdvisoryDestination(ActiveMQDestination destination) {
        return destination.getPhysicalName().startsWith(AdvisorySupport.ADVISORY_TOPIC_PREFIX);
    }
}
