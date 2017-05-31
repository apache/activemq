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
package org.apache.activemq.transport.amqp.protocol;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.command.Command;
import org.apache.activemq.transport.amqp.ResponseHandler;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Sender;

/**
 * Abstract AmqpLink implementation that provide basic Link services.
 */
public abstract class AmqpAbstractLink<LINK_TYPE extends Link> implements AmqpLink {

    protected final AmqpSession session;
    protected final LINK_TYPE endpoint;

    protected boolean closed;
    protected boolean opened;
    protected List<Runnable> closeActions = new ArrayList<Runnable>();

    /**
     * Creates a new AmqpLink type.
     *
     * @param session
     *        the AmqpSession that servers as the parent of this Link.
     * @param endpoint
     *        the link endpoint this object represents.
     */
    public AmqpAbstractLink(AmqpSession session, LINK_TYPE endpoint) {
        this.session = session;
        this.endpoint = endpoint;
    }

    @Override
    public void open() {
        if (!opened) {
            getEndpoint().setContext(this);
            getEndpoint().open();

            opened = true;
        }
    }

    @Override
    public void detach() {
        if (!closed) {
            if (getEndpoint() != null) {
                getEndpoint().setContext(null);
                getEndpoint().detach();
                getEndpoint().free();
            }
        }
    }

    @Override
    public void close(ErrorCondition error) {
        if (!closed) {

            if (getEndpoint() != null) {
                if (getEndpoint() instanceof Sender) {
                    getEndpoint().setSource(null);
                } else {
                    getEndpoint().setTarget(null);
                }
                getEndpoint().setCondition(error);
            }

            close();
        }
    }

    @Override
    public void close() {
        if (!closed) {

            if (getEndpoint() != null) {
                getEndpoint().setContext(null);
                getEndpoint().close();
                getEndpoint().free();
            }

            for (Runnable action : closeActions) {
                action.run();
            }

            closeActions.clear();
            opened = false;
            closed = true;
        }
    }

    /**
     * @return true if this link has already been opened.
     */
    public boolean isOpened() {
        return opened;
    }

    /**
     * @return true if this link has already been closed.
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * @return the Proton Link type this link represents.
     */
    public LINK_TYPE getEndpoint() {
        return endpoint;
    }

    /**
     * @return the parent AmqpSession for this Link instance.
     */
    public AmqpSession getSession() {
        return session;
    }

    @Override
    public void addCloseAction(Runnable action) {
        closeActions.add(action);
    }

    /**
     * Shortcut method to hand off an ActiveMQ Command to the broker and assign
     * a ResponseHandler to deal with any reply from the broker.
     *
     * @param command
     *        the Command object to send to the Broker.
     */
    protected void sendToActiveMQ(Command command) {
        session.getConnection().sendToActiveMQ(command, null);
    }

    /**
     * Shortcut method to hand off an ActiveMQ Command to the broker and assign
     * a ResponseHandler to deal with any reply from the broker.
     *
     * @param command
     *        the Command object to send to the Broker.
     * @param handler
     *        the ResponseHandler that will handle the Broker's response.
     */
    protected void sendToActiveMQ(Command command, ResponseHandler handler) {
        session.getConnection().sendToActiveMQ(command, handler);
    }
}
