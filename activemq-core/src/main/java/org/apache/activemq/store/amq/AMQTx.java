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

package org.apache.activemq.store.amq;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.activemq.command.JournalTopicAck;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.kaha.impl.async.Location;

/**
 */
/**
 * Operations
 * 
 * @version $Revision: 1.6 $
 */
public class AMQTx {

    private final Location location;
    private List<AMQTxOperation> operations = new ArrayList<AMQTxOperation>();

    public AMQTx(Location location) {
        this.location = location;
    }

    public void add(AMQMessageStore store, Message msg, Location location) {
        operations.add(new AMQTxOperation(AMQTxOperation.ADD_OPERATION_TYPE, store.getDestination(), msg,
                                          location));
    }

    public void add(AMQMessageStore store, MessageAck ack) {
        operations.add(new AMQTxOperation(AMQTxOperation.REMOVE_OPERATION_TYPE, store.getDestination(), ack,
                                          null));
    }

    public void add(AMQTopicMessageStore store, JournalTopicAck ack) {
        operations.add(new AMQTxOperation(AMQTxOperation.ACK_OPERATION_TYPE, store.getDestination(), ack,
                                          null));
    }

    public Message[] getMessages() {
        List<Object> list = new ArrayList<Object>();
        for (Iterator<AMQTxOperation> iter = operations.iterator(); iter.hasNext();) {
            AMQTxOperation op = iter.next();
            if (op.getOperationType() == AMQTxOperation.ADD_OPERATION_TYPE) {
                list.add(op.getData());
            }
        }
        Message rc[] = new Message[list.size()];
        list.toArray(rc);
        return rc;
    }

    public MessageAck[] getAcks() {
        List<Object> list = new ArrayList<Object>();
        for (Iterator<AMQTxOperation> iter = operations.iterator(); iter.hasNext();) {
            AMQTxOperation op = iter.next();
            if (op.getOperationType() == AMQTxOperation.REMOVE_OPERATION_TYPE) {
                list.add(op.getData());
            }
        }
        MessageAck rc[] = new MessageAck[list.size()];
        list.toArray(rc);
        return rc;
    }

    /**
     * @return the location
     */
    public Location getLocation() {
        return this.location;
    }

    public List<AMQTxOperation> getOperations() {
        return operations;
    }

    public void setOperations(List<AMQTxOperation> operations) {
        this.operations = operations;
    }
}
