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
package org.apache.activemq.store.kahadb;

import java.io.IOException;

import org.apache.activemq.store.kahadb.data.KahaAckMessageFileMapCommand;
import org.apache.activemq.store.kahadb.data.KahaAddMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaAddScheduledJobCommand;
import org.apache.activemq.store.kahadb.data.KahaCommitCommand;
import org.apache.activemq.store.kahadb.data.KahaDestroySchedulerCommand;
import org.apache.activemq.store.kahadb.data.KahaPrepareCommand;
import org.apache.activemq.store.kahadb.data.KahaProducerAuditCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveDestinationCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveScheduledJobCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveScheduledJobsCommand;
import org.apache.activemq.store.kahadb.data.KahaRescheduleJobCommand;
import org.apache.activemq.store.kahadb.data.KahaRollbackCommand;
import org.apache.activemq.store.kahadb.data.KahaSubscriptionCommand;
import org.apache.activemq.store.kahadb.data.KahaTraceCommand;
import org.apache.activemq.store.kahadb.data.KahaUpdateMessageCommand;

public class Visitor {

    public void visit(KahaTraceCommand command) {
    }

    public void visit(KahaRollbackCommand command) throws IOException {
    }

    public void visit(KahaRemoveMessageCommand command) throws IOException {
    }

    public void visit(KahaPrepareCommand command) throws IOException {
    }

    public void visit(KahaCommitCommand command) throws IOException {
    }

    public void visit(KahaAddMessageCommand command) throws IOException {
    }

    public void visit(KahaRemoveDestinationCommand command) throws IOException {
    }

    public void visit(KahaSubscriptionCommand kahaUpdateSubscriptionCommand) throws IOException {
    }

    public void visit(KahaProducerAuditCommand kahaProducerAuditCommand) throws IOException {
    }

    public void visit(KahaAckMessageFileMapCommand kahaProducerAuditCommand) throws IOException {
    }

    public void visit(KahaAddScheduledJobCommand kahaAddScheduledJobCommand) throws IOException {
    }

    public void visit(KahaRescheduleJobCommand KahaRescheduleJobCommand) throws IOException {
    }

    public void visit(KahaRemoveScheduledJobCommand kahaRemoveScheduledJobCommand) throws IOException {
    }

    public void visit(KahaRemoveScheduledJobsCommand kahaRemoveScheduledJobsCommand) throws IOException {
    }

    public void visit(KahaDestroySchedulerCommand KahaDestroySchedulerCommand) throws IOException {
    }

    public void visit(KahaUpdateMessageCommand kahaUpdateMessageCommand) throws IOException {
    }
}
