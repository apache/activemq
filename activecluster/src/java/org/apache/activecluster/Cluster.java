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
 * 
 **/

package org.apache.activecluster;

import java.io.Serializable;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.apache.activecluster.election.ElectionStrategy;

/**
 * Represents a logical connection to a cluster. From this object you can
 * obtain the destination to send messages to, view the members of the cluster,
 * watch cluster events (nodes joining, leaving, updating their state) as well
 * as viewing each members state.
 * <p/>
 * You may also update the local node's state.
 *
 * @version $Revision: 1.5 $
 */
public interface Cluster extends Service {

    /**
     * Returns the destination used to send a message to all members of the cluster
     *
     * @return the destination to send messages to all members of the cluster
     */
    public Destination getDestination();

    /**
     * A snapshot of the nodes in the cluster indexed by the Destination
     * @return a Map containing all the nodes in the cluster, where key=node destination,value=node
     */
    public Map getNodes();

    /**
     * Adds a new listener to cluster events
     *
     * @param listener
     */
    public void addClusterListener(ClusterListener listener);

    /**
     * Removes a listener to cluster events
     *
     * @param listener
     */
    public void removeClusterListener(ClusterListener listener);

    /**
     * The local Node which allows you to mutate the state or subscribe to the
     * nodes temporary queue for inbound messages direct to the Node
     * @return the Node representing this peer in the cluster
     */
    public LocalNode getLocalNode();

    /**
     * Allows overriding of the default election strategy with a custom
     * implementation.
     * @param strategy 
     */
    public void setElectionStrategy(ElectionStrategy strategy);

    
    // Messaging helper methods
    //-------------------------------------------------------------------------

    /**
     * Sends a message to a destination, which could be to the entire group
     * or could be a single Node's destination
     *
     * @param destination is either the group topic or a node's destination
     * @param message     the message to be sent
     * @throws JMSException
     */
    public void send(Destination destination, Message message) throws JMSException;

    
    /**
     * Creates a consumer of all the messags sent to the given destination,
     * including messages sent via the send() messages
     *
     * @param destination
     * @return a newly  created message consumer
     * @throws JMSException
     */
    public MessageConsumer createConsumer(Destination destination) throws JMSException;

    /**
     * Creates a consumer of all message sent to the given destination,
     * including messages sent via the send() message with an optional SQL 92 based selector to filter
     * messages
     *
     * @param destination
     * @param selector
     * @return a newly  created message consumer
     * @throws JMSException
     */
    public MessageConsumer createConsumer(Destination destination, String selector) throws JMSException;

    /**
     * Creates a consumer of all message sent to the given destination,
     * including messages sent via the send() message with an optional SQL 92 based selector to filter
     * messages along with optionally ignoring local traffic - messages sent via the send()
     * method on this object.
     *
     * @param destination the destination to consume from
     * @param selector    an optional SQL 92 filter of messages which could be null
     * @param noLocal     which if true messages sent via send() on this object will not be delivered to the consumer
     * @return a newly  created message consumer
     * @throws JMSException
     */
    public MessageConsumer createConsumer(Destination destination, String selector, boolean noLocal) throws JMSException;


    // Message factory methods
    //-------------------------------------------------------------------------

    /**
     * Creates a new message without a body
     * @return the create  Message
     *
     * @throws JMSException
     */
    public Message createMessage() throws JMSException;

    /**
     * Creates a new bytes message
     * @return the create BytesMessage
     *
     * @throws JMSException
     */
    public BytesMessage createBytesMessage() throws JMSException;

    /**
     * Creates a new {@link MapMessage}
     * @return the created MapMessage
     *
     * @throws JMSException
     */
    public MapMessage createMapMessage() throws JMSException;

    /**
     * Creates a new {@link ObjectMessage}
     * @return the created ObjectMessage
     *
     * @throws JMSException
     */
    public ObjectMessage createObjectMessage() throws JMSException;

    /**
     * Creates a new {@link ObjectMessage}
     *
     * @param object
     * @return the createdObjectMessage
     * @throws JMSException
     */
    public ObjectMessage createObjectMessage(Serializable object) throws JMSException;

    /**
     * Creates a new {@link StreamMessage}
     * @return the create StreamMessage
     *
     * @throws JMSException
     */
    public StreamMessage createStreamMessage() throws JMSException;

    /**
     * Creates a new {@link TextMessage}
     * @return the create TextMessage
     *
     * @throws JMSException
     */
    public TextMessage createTextMessage() throws JMSException;

    /**
     * Creates a new {@link TextMessage}
     *
     * @param text
     * @return the create TextMessage
     * @throws JMSException
     */
    public TextMessage createTextMessage(String text) throws JMSException;
    
    /**
     * Create a named Destination
     * @param name
     * @return the Destinatiion 
     * @throws JMSException
     */
    public Destination createDestination(String name) throws JMSException;
    
    /**
     * wait until a the cardimality of the cluster is reaches the expected count. This method will return false if the
     * cluster isn't started or stopped while waiting
     *
     * @param expectedCount the number of expected members of a cluster
     * @param timeout       timeout in milliseconds
     * @return true if the cluster is fully connected
     * @throws InterruptedException
     */
    boolean waitForClusterToComplete(int expectedCount, long timeout) throws InterruptedException;
}
