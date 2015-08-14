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
package org.apache.activemq.transport.amqp.client.util;

import java.util.EnumSet;

import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.amqp.transport.Source;
import org.apache.qpid.proton.amqp.transport.Target;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Record;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;

/**
 * Unmodifiable Session wrapper used to prevent test code from accidentally
 * modifying Session state.
 */
public class UnmodifiableLink implements Link {

    private final Link link;

    public UnmodifiableLink(Link link) {
        this.link = link;
    }

    @Override
    public EndpointState getLocalState() {
        return link.getLocalState();
    }

    @Override
    public EndpointState getRemoteState() {
        return link.getRemoteState();
    }

    @Override
    public ErrorCondition getCondition() {
        return link.getCondition();
    }

    @Override
    public void setCondition(ErrorCondition condition) {
        throw new UnsupportedOperationException("Cannot alter the Link state");
    }

    @Override
    public ErrorCondition getRemoteCondition() {
        return link.getRemoteCondition();
    }

    @Override
    public void free() {
        throw new UnsupportedOperationException("Cannot alter the Link state");
    }

    @Override
    public void open() {
        throw new UnsupportedOperationException("Cannot alter the Link state");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Cannot alter the Link state");
    }

    @Override
    public void setContext(Object o) {
        throw new UnsupportedOperationException("Cannot alter the Link state");
    }

    @Override
    public Object getContext() {
        return link.getContext();
    }

    @Override
    public String getName() {
        return link.getName();
    }

    @Override
    public Delivery delivery(byte[] tag) {
        throw new UnsupportedOperationException("Cannot alter the Link state");
    }

    @Override
    public Delivery delivery(byte[] tag, int offset, int length) {
        throw new UnsupportedOperationException("Cannot alter the Link state");
    }

    @Override
    public Delivery head() {
        return new UnmodifiableDelivery(link.head());
    }

    @Override
    public Delivery current() {
        return new UnmodifiableDelivery(link.current());
    }

    @Override
    public boolean advance() {
        throw new UnsupportedOperationException("Cannot alter the Link state");
    }

    @Override
    public Source getSource() {
        // TODO Figure out a simple way to wrap the odd Source types in Proton-J
        return link.getSource();
    }

    @Override
    public Target getTarget() {
        // TODO Figure out a simple way to wrap the odd Source types in Proton-J
        return link.getTarget();
    }

    @Override
    public void setSource(Source address) {
        throw new UnsupportedOperationException("Cannot alter the Link state");
    }

    @Override
    public void setTarget(Target address) {
        throw new UnsupportedOperationException("Cannot alter the Link state");
    }

    @Override
    public Source getRemoteSource() {
        // TODO Figure out a simple way to wrap the odd Source types in Proton-J
        return link.getRemoteSource();
    }

    @Override
    public Target getRemoteTarget() {
        // TODO Figure out a simple way to wrap the odd Target types in Proton-J
        return link.getRemoteTarget();
    }

    @Override
    public Link next(EnumSet<EndpointState> local, EnumSet<EndpointState> remote) {
        Link next = link.next(local, remote);

        if (next != null) {
            if (next instanceof Sender) {
                next = new UnmodifiableSender((Sender) next);
            } else {
                next = new UnmodifiableReceiver((Receiver) next);
            }
        }

        return next;
    }

    @Override
    public int getCredit() {
        return link.getCredit();
    }

    @Override
    public int getQueued() {
        return link.getQueued();
    }

    @Override
    public int getUnsettled() {
        return link.getUnsettled();
    }

    @Override
    public Session getSession() {
        return new UnmodifiableSession(link.getSession());
    }

    @Override
    public SenderSettleMode getSenderSettleMode() {
        return link.getSenderSettleMode();
    }

    @Override
    public void setSenderSettleMode(SenderSettleMode senderSettleMode) {
        throw new UnsupportedOperationException("Cannot alter the Link state");
    }

    @Override
    public SenderSettleMode getRemoteSenderSettleMode() {
        return link.getRemoteSenderSettleMode();
    }

    @Override
    public ReceiverSettleMode getReceiverSettleMode() {
        return link.getReceiverSettleMode();
    }

    @Override
    public void setReceiverSettleMode(ReceiverSettleMode receiverSettleMode) {
        throw new UnsupportedOperationException("Cannot alter the Link state");
    }

    @Override
    public ReceiverSettleMode getRemoteReceiverSettleMode() {
        return link.getRemoteReceiverSettleMode();
    }

    @Override
    public void setRemoteSenderSettleMode(SenderSettleMode remoteSenderSettleMode) {
        throw new UnsupportedOperationException("Cannot alter the Link state");
    }

    @Override
    public int drained() {
        return link.drained();  // TODO - Is this a mutating call?
    }

    @Override
    public int getRemoteCredit() {
        return link.getRemoteCredit();
    }

    @Override
    public boolean getDrain() {
        return link.getDrain();
    }

    @Override
    public void detach() {
        throw new UnsupportedOperationException("Cannot alter the Link state");
    }

    @Override
    public boolean detached() {
        return link.detached();
    }

    public Record attachments() {
        return link.attachments();
    }
}
