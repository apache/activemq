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
package org.apache.activemq.spring.geronimo;

import org.springframework.beans.factory.FactoryBean;
import org.apache.geronimo.connector.outbound.connectiontracking.ConnectionTrackingCoordinator;
import org.apache.geronimo.connector.outbound.connectiontracking.GeronimoTransactionListener;
import org.apache.geronimo.transaction.manager.GeronimoTransactionManager;

/**
 * @org.apache.xbean.XBean element="connectionTracker"
 */
public class ConnectionTrackerFactoryBean implements FactoryBean {
    private ConnectionTrackingCoordinator coordinator;

    private GeronimoTransactionManager geronimoTransactionManager;

    public Object getObject() throws Exception {
        if (coordinator == null) {
            coordinator = new ConnectionTrackingCoordinator();
            if (geronimoTransactionManager != null) {
                GeronimoTransactionListener transactionListener = new GeronimoTransactionListener(coordinator);
                geronimoTransactionManager.addTransactionAssociationListener(transactionListener);
            }
        }
        return coordinator;
    }

    public Class<?> getObjectType() {
        return ConnectionTrackingCoordinator.class;
    }

    public boolean isSingleton() {
        return true;
    }

    public GeronimoTransactionManager getGeronimoTransactionManager() {
        return geronimoTransactionManager;
    }

    public void setGeronimoTransactionManager(GeronimoTransactionManager geronimoTransactionManager) {
        this.geronimoTransactionManager = geronimoTransactionManager;
    }
}
