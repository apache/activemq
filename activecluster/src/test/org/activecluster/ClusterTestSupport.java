/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activecluster;

import org.activecluster.impl.ActiveMQClusterFactory;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Topic;

/**
 * @version $Revision: 1.3 $
 */
public abstract class ClusterTestSupport extends TestSupport {

    protected Cluster cluster;
    protected StubMessageListener clusterListener = new StubMessageListener();
    protected StubMessageListener inboxListener = new StubMessageListener();
    private MessageConsumer clusterConsumer;
    private MessageConsumer inboxConsumer;


    protected void sendMessageToNode(Node node, String text) throws Exception {
        Message message = cluster.createTextMessage(text);
        cluster.send(node.getDestination(), message);
    }

    protected void sendMessageToCluster(String text) throws Exception {
        Message message = cluster.createTextMessage(text);
        cluster.send(cluster.getDestination(), message);
    }

    protected void subscribeToCluster() throws Exception {

        // listen to cluster messages
        String clusterDestination = cluster.getDestination();
        assertTrue("Local destination must not be null", clusterDestination != null);
        clusterConsumer = cluster.createConsumer(clusterDestination);
        clusterConsumer.setMessageListener(clusterListener);

        // listen to inbox messages (individual messages)
        String localDestination = cluster.getLocalNode().getDestination();
        assertTrue("Local destination must not be null", localDestination != null);

        System.out.println("Consuming from local destination: " + localDestination);
        inboxConsumer = cluster.createConsumer(localDestination);
        inboxConsumer.setMessageListener(inboxListener);
    }

    protected void setUp() throws Exception {
    }

    protected void tearDown() throws Exception {
        if (cluster != null) {
            cluster.stop();
        }
    }
}
