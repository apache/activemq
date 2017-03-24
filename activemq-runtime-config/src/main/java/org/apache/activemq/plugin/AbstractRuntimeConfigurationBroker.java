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
package org.apache.activemq.plugin;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import javax.management.ObjectName;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractRuntimeConfigurationBroker extends BrokerFilter {

    public static final Logger LOG = LoggerFactory.getLogger(AbstractRuntimeConfigurationBroker.class);
    protected final ReentrantReadWriteLock addDestinationBarrier = new ReentrantReadWriteLock();
    protected final ReentrantReadWriteLock addConnectionBarrier = new ReentrantReadWriteLock();
    protected Runnable monitorTask;
    protected ConcurrentLinkedQueue<Runnable> addDestinationWork = new ConcurrentLinkedQueue<Runnable>();
    protected ConcurrentLinkedQueue<Runnable> addConnectionWork = new ConcurrentLinkedQueue<Runnable>();
    protected ObjectName objectName;
    protected String infoString;

    public AbstractRuntimeConfigurationBroker(Broker next) {
        super(next);
    }

    @Override
    public void start() throws Exception {
        super.start();
    }

    @Override
    public void stop() throws Exception {
        if (monitorTask != null) {
            try {
                this.getBrokerService().getScheduler().cancel(monitorTask);
            } catch (Exception letsNotStopStop) {
                LOG.warn("Failed to cancel config monitor task", letsNotStopStop);
            }
        }
        unregisterMbean();
        super.stop();
    }

    protected void registerMbean() {

    }

    protected void unregisterMbean() {

    }

    // modification to virtual destinations interceptor needs exclusive access to destination add
    @Override
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination, boolean createIfTemporary) throws Exception {
        Runnable work = addDestinationWork.poll();
        if (work != null) {
            try {
                addDestinationBarrier.writeLock().lockInterruptibly();
                do {
                    work.run();
                    work = addDestinationWork.poll();
                } while (work != null);
                return super.addDestination(context, destination, createIfTemporary);
            } finally {
                addDestinationBarrier.writeLock().unlock();
            }
        } else {
            try {
                addDestinationBarrier.readLock().lockInterruptibly();
                return super.addDestination(context, destination, createIfTemporary);
            } finally {
                addDestinationBarrier.readLock().unlock();
            }
        }
    }

    // modification to authentication plugin needs exclusive access to connection add
    @Override
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        Runnable work = addConnectionWork.poll();
        if (work != null) {
            try {
                addConnectionBarrier.writeLock().lockInterruptibly();
                do {
                    work.run();
                    work = addConnectionWork.poll();
                } while (work != null);
                super.addConnection(context, info);
            } finally {
                addConnectionBarrier.writeLock().unlock();
            }
        } else {
            try {
                addConnectionBarrier.readLock().lockInterruptibly();
                super.addConnection(context, info);
            } finally {
                addConnectionBarrier.readLock().unlock();
            }
        }
    }

    /**
     * Apply the destination work immediately instead of waiting for
     * a connection add or destination add
     *
     * @throws Exception
     */
    protected void applyDestinationWork() throws Exception {
        Runnable work = addDestinationWork.poll();
        if (work != null) {
            try {
                addDestinationBarrier.writeLock().lockInterruptibly();
                do {
                    work.run();
                    work = addDestinationWork.poll();
                } while (work != null);
            } finally {
                addDestinationBarrier.writeLock().unlock();
            }
        }
    }

    public void debug(String s) {
        LOG.debug(s);
    }

    public void info(String s) {
        LOG.info(filterPasswords(s));
        if (infoString != null) {
            infoString += s;
            infoString += ";";
        }
    }

    public void info(String s, Throwable t) {
        LOG.info(filterPasswords(s), t);
        if (infoString != null) {
            infoString += s;
            infoString += ", " + t;
            infoString += ";";
        }
    }

    Pattern matchPassword = Pattern.compile("password=.*,");
    protected String filterPasswords(Object toEscape) {
        return matchPassword.matcher(toEscape.toString()).replaceAll("password=???,");
    }

}