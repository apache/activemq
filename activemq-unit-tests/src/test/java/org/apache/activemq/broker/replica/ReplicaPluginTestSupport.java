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
package org.apache.activemq.broker.replica;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.AutoFailTestSupport;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.replica.ReplicaPlugin;
import org.apache.activemq.replica.ReplicaRole;
import org.apache.activemq.replica.jmx.ReplicationViewMBean;
import org.apache.commons.io.FileUtils;

import javax.jms.ConnectionFactory;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.transaction.xa.Xid;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

public abstract class ReplicaPluginTestSupport extends AutoFailTestSupport {

    protected static final int LONG_TIMEOUT = 15000;
    protected static final int SHORT_TIMEOUT = 6000;

    protected static final String FIRST_KAHADB_DIRECTORY = "target/activemq-data/first/";
    protected static final String SECOND_KAHADB_DIRECTORY = "target/activemq-data/second/";

    protected String firstBindAddress = "vm://firstBroker";
    protected String firstReplicaBindAddress = "tcp://localhost:61610";
    protected String secondReplicaBindAddress = "tcp://localhost:61611";
    protected String secondBindAddress = "vm://secondBroker";

    protected BrokerService firstBroker;
    protected BrokerService secondBroker;

    protected boolean useTopic;

    protected ConnectionFactory firstBrokerConnectionFactory;
    protected ConnectionFactory secondBrokerConnectionFactory;

    protected ActiveMQXAConnectionFactory firstBrokerXAConnectionFactory;
    protected ActiveMQXAConnectionFactory secondBrokerXAConnectionFactory;

    protected ActiveMQDestination destination;

    private static long txGenerator = 67;

    @Override
    protected void setUp() throws Exception {
        if (firstBroker == null) {
            firstBroker = createFirstBroker();
        }
        if (secondBroker == null) {
            secondBroker = createSecondBroker();
        }

        startFirstBroker();
        startSecondBroker();

        firstBrokerConnectionFactory = new ActiveMQConnectionFactory(firstBindAddress);
        secondBrokerConnectionFactory = new ActiveMQConnectionFactory(secondBindAddress);

        firstBrokerXAConnectionFactory = new ActiveMQXAConnectionFactory(firstBindAddress);
        secondBrokerXAConnectionFactory = new ActiveMQXAConnectionFactory(secondBindAddress);

        destination = createDestination();
    }

    @Override
    protected void tearDown() throws Exception {
        if (firstBroker != null) {
            try {
                firstBroker.stop();
            } catch (Exception e) {
            }
        }
        if (secondBroker != null) {
            try {
                secondBroker.stop();
            } catch (Exception e) {
            }
        }
    }

    protected BrokerService createFirstBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(true);
        answer.setPersistent(false);
        answer.getManagementContext().setCreateConnector(false);
        answer.addConnector(firstBindAddress);
        answer.setDataDirectory(FIRST_KAHADB_DIRECTORY);
        answer.setBrokerName("firstBroker");

        ReplicaPlugin replicaPlugin = new ReplicaPlugin();
        replicaPlugin.setRole(ReplicaRole.source);
        replicaPlugin.setTransportConnectorUri(firstReplicaBindAddress);
        replicaPlugin.setOtherBrokerUri(secondReplicaBindAddress);
        replicaPlugin.setControlWebConsoleAccess(false);

        answer.setPlugins(new BrokerPlugin[]{replicaPlugin});
        answer.setSchedulerSupport(true);
        return answer;
    }

    protected BrokerService createSecondBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(true);
        answer.setPersistent(false);
        answer.getManagementContext().setCreateConnector(false);
        answer.addConnector(secondBindAddress);
        answer.setDataDirectory(SECOND_KAHADB_DIRECTORY);
        answer.setBrokerName("secondBroker");

        ReplicaPlugin replicaPlugin = new ReplicaPlugin();
        replicaPlugin.setRole(ReplicaRole.replica);
        replicaPlugin.setTransportConnectorUri(secondReplicaBindAddress);
        replicaPlugin.setOtherBrokerUri(firstReplicaBindAddress);
        replicaPlugin.setControlWebConsoleAccess(false);

        answer.setPlugins(new BrokerPlugin[]{replicaPlugin});
        answer.setSchedulerSupport(true);
        return answer;
    }

    protected void startFirstBroker() throws Exception {
        firstBroker.start();
    }

    protected void startSecondBroker() throws Exception {
        secondBroker.start();
    }

    protected ActiveMQDestination createDestination() {
        return createDestination(getDestinationString());
    }

    protected ActiveMQDestination createDestination(String subject) {
        if (useTopic) {
            return new ActiveMQTopic(subject);
        } else {
            return new ActiveMQQueue(subject);
        }
    }

    protected String getDestinationString() {
        return getClass().getName() + "." + getName();
    }

    protected Xid createXid() throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream os = new DataOutputStream(baos);
        os.writeLong(++txGenerator);
        os.close();
        final byte[] bs = baos.toByteArray();

        return new Xid() {

            public int getFormatId() {
                return 86;
            }


            public byte[] getGlobalTransactionId() {
                return bs;
            }


            public byte[] getBranchQualifier() {
                return bs;
            }
        };
    }

    protected QueueViewMBean getQueueView(BrokerService broker, String queueName) throws MalformedObjectNameException {
        MBeanServer mbeanServer = broker.getManagementContext().getMBeanServer();
        String objectNameStr = broker.getBrokerObjectName().toString();
        objectNameStr += ",destinationType=Queue,destinationName="+queueName;
        ObjectName queueViewMBeanName = assertRegisteredObjectName(mbeanServer, objectNameStr);
        return MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);
    }

    protected TopicViewMBean getTopicView(BrokerService broker, String topicName) throws MalformedObjectNameException {
        MBeanServer mbeanServer = broker.getManagementContext().getMBeanServer();
        String objectNameStr = broker.getBrokerObjectName().toString();
        objectNameStr += ",destinationType=Topic,destinationName=" + topicName;
        ObjectName topicViewMBeanName = assertRegisteredObjectName(mbeanServer, objectNameStr);
        return MBeanServerInvocationHandler.newProxyInstance(mbeanServer, topicViewMBeanName, TopicViewMBean.class, true);
    }

    protected ReplicationViewMBean getReplicationView(BrokerService broker) throws Exception {
        MBeanServer mbeanServer = broker.getManagementContext().getMBeanServer();
        String objectNameStr = broker.getBrokerObjectName().toString();
        objectNameStr += ",service=Plugins,instanceName=ReplicationPlugin";
        ObjectName replicaViewMBeanName = assertRegisteredObjectName(mbeanServer, objectNameStr);
        return MBeanServerInvocationHandler.newProxyInstance(mbeanServer, replicaViewMBeanName, ReplicationViewMBean.class, true);
    }

    protected ObjectName assertRegisteredObjectName(MBeanServer mbeanServer, String name) throws MalformedObjectNameException, NullPointerException {
        ObjectName objectName = new ObjectName(name);
        if (mbeanServer.isRegistered(objectName)) {
            System.out.println("Bean Registered: " + objectName);
        } else {
            fail("Could not find MBean!: " + objectName);
        }
        return objectName;
    }

    protected void cleanKahaDB(String filePath) throws IOException {
        File kahaDBFile = new File(filePath);
        if (kahaDBFile.exists()) {
            FileUtils.cleanDirectory(kahaDBFile);
        }
    }
}
