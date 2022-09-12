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

public class ReplicaSupport {

    private ReplicaSupport() {
        // Intentionally hidden
    }

    public static final String REPLICATION_QUEUE_NAME = "ActiveMQ.Plugin.Replication.Queue";
    public static final String REPLICATION_PLUGIN_USER_NAME = "replication_plugin";

    public static final String TRANSACTION_ONE_PHASE_PROPERTY = "TRANSACTION_ONE_PHASE_PROPERTY";
    public static final String CLIENT_ID_PROPERTY = "CLIENT_ID_PROPERTY";
    public static final String MESSAGE_IDS_PROPERTY = "MESSAGE_IDS_PROPERTY";
}
