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

package org.apache.activemq.broker.scheduler;

import java.io.File;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.JobSchedulerViewMBean;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.IOHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 * Base class for tests of the Broker's JobSchedulerStore.
 */
public class JobSchedulerTestSupport {

    @Rule public TestName name = new TestName();

    enum RestartType {
        NORMAL,
        FULL_RECOVERY
    }

    protected String connectionUri;
    protected BrokerService broker;
    protected JobScheduler jobScheduler;
    protected Queue destination;

    @Before
    public void setUp() throws Exception {
        connectionUri = "vm://localhost";
        destination = new ActiveMQQueue(name.getMethodName());

        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();

        jobScheduler = broker.getJobSchedulerStore().getJobScheduler("JMS");
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    protected Connection createConnection() throws Exception {
        return createConnectionFactory().createConnection();
    }

    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(connectionUri);
    }

    protected BrokerService createBroker() throws Exception {
        return createBroker(true);
    }

    protected boolean isUseJmx() {
        return false;
    }

    protected boolean isPersistent() {
        return true;
    }

    protected JobSchedulerViewMBean getJobSchedulerMBean() throws Exception {
        ObjectName objectName = broker.getAdminView().getJMSJobScheduler();
        JobSchedulerViewMBean scheduler = null;
        if (objectName != null) {
            scheduler = (JobSchedulerViewMBean) broker.getManagementContext()
                .newProxyInstance(objectName, JobSchedulerViewMBean.class, true);
        }

        return scheduler;
    }

    protected BrokerService createBroker(boolean delete) throws Exception {
        File schedulerDirectory = new File("target/scheduler");
        if (delete) {
            IOHelper.mkdirs(schedulerDirectory);
            IOHelper.deleteChildren(schedulerDirectory);
        }

        BrokerService answer = new BrokerService();
        answer.setPersistent(isPersistent());
        answer.setDeleteAllMessagesOnStartup(true);
        answer.setDataDirectory("target");
        answer.setSchedulerDirectoryFile(schedulerDirectory);
        answer.setSchedulerSupport(true);
        answer.setUseJmx(isUseJmx());
        return answer;
    }

    protected void restartBroker(RestartType restartType) throws Exception {
        tearDown();

        if (restartType == RestartType.FULL_RECOVERY)  {
            File dir = broker.getSchedulerDirectoryFile();

            if (dir != null) {
                IOHelper.deleteFile(new File(dir, "scheduleDB.data"));
                IOHelper.deleteFile(new File(dir, "scheduleDB.redo"));
            }
        }

        broker = createBroker(false);

        broker.start();
        broker.waitUntilStarted();
    }
}
