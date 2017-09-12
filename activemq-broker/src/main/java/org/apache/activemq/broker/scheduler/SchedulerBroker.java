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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.Connector;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.ConnectionStatistics;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.usage.JobSchedulerUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.activemq.util.TypeConversionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerBroker extends BrokerFilter implements JobListener {
    private static final Logger LOG = LoggerFactory.getLogger(SchedulerBroker.class);
    private static final IdGenerator ID_GENERATOR = new IdGenerator();
    private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
    private final AtomicBoolean started = new AtomicBoolean();
    private final WireFormat wireFormat = new OpenWireFormat();
    private final ConnectionContext context = new ConnectionContext();
    private final ProducerId producerId = new ProducerId();
    private final SystemUsage systemUsage;

    private final JobSchedulerStore store;
    private JobScheduler scheduler;

    public SchedulerBroker(BrokerService brokerService, Broker next, JobSchedulerStore store) throws Exception {
        super(next);

        this.store = store;
        this.producerId.setConnectionId(ID_GENERATOR.generateId());
        this.context.setSecurityContext(SecurityContext.BROKER_SECURITY_CONTEXT);
        // we only get response on unexpected error
        this.context.setConnection(new Connection() {
            @Override
            public Connector getConnector() {
                return null;
            }

            @Override
            public void dispatchSync(Command message) {
                if (message instanceof ExceptionResponse) {
                    LOG.warn("Unexpected response: " + message);
                }
            }

            @Override
            public void dispatchAsync(Command command) {
                if (command instanceof ExceptionResponse) {
                    LOG.warn("Unexpected response: " + command);
                }
            }

            @Override
            public Response service(Command command) {
                return null;
            }

            @Override
            public void serviceException(Throwable error) {
                LOG.warn("Unexpected exception: " + error, error);
            }

            @Override
            public boolean isSlow() {
                return false;
            }

            @Override
            public boolean isBlocked() {
                return false;
            }

            @Override
            public boolean isConnected() {
                return false;
            }

            @Override
            public boolean isActive() {
                return false;
            }

            @Override
            public int getDispatchQueueSize() {
                return 0;
            }

            @Override
            public ConnectionStatistics getStatistics() {
                return null;
            }

            @Override
            public boolean isManageable() {
                return false;
            }

            @Override
            public String getRemoteAddress() {
                return null;
            }

            @Override
            public void serviceExceptionAsync(IOException e) {
                LOG.warn("Unexpected async ioexception: " + e, e);
            }

            @Override
            public String getConnectionId() {
                return null;
            }

            @Override
            public boolean isNetworkConnection() {
                return false;
            }

            @Override
            public boolean isFaultTolerantConnection() {
                return false;
            }

            @Override
            public void updateClient(ConnectionControl control) {}

            @Override
            public int getActiveTransactionCount() {
                return 0;
            }

            @Override
            public Long getOldestActiveTransactionDuration() {
                return null;
            }

            @Override
            public void start() throws Exception {}

            @Override
            public void stop() throws Exception {}
        });
        this.context.setBroker(next);
        this.systemUsage = brokerService.getSystemUsage();

        wireFormat.setVersion(brokerService.getStoreOpenWireVersion());
    }

    public synchronized JobScheduler getJobScheduler() throws Exception {
        return new JobSchedulerFacade(this);
    }

    @Override
    public void start() throws Exception {
        this.started.set(true);
        getInternalScheduler();
        super.start();
    }

    @Override
    public void stop() throws Exception {
        if (this.started.compareAndSet(true, false)) {

            if (this.store != null) {
                this.store.stop();
            }
            if (this.scheduler != null) {
                this.scheduler.removeListener(this);
                this.scheduler = null;
            }
        }
        super.stop();
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, final Message messageSend) throws Exception {
        ConnectionContext context = producerExchange.getConnectionContext();

        final String jobId = (String) messageSend.getProperty(ScheduledMessage.AMQ_SCHEDULED_ID);
        final Object cronValue = messageSend.getProperty(ScheduledMessage.AMQ_SCHEDULED_CRON);
        final Object periodValue = messageSend.getProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD);
        final Object delayValue = messageSend.getProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY);

        String physicalName = messageSend.getDestination().getPhysicalName();
        boolean schedularManage = physicalName.regionMatches(true, 0, ScheduledMessage.AMQ_SCHEDULER_MANAGEMENT_DESTINATION, 0,
            ScheduledMessage.AMQ_SCHEDULER_MANAGEMENT_DESTINATION.length());

        if (schedularManage == true) {

            JobScheduler scheduler = getInternalScheduler();
            ActiveMQDestination replyTo = messageSend.getReplyTo();

            String action = (String) messageSend.getProperty(ScheduledMessage.AMQ_SCHEDULER_ACTION);

            if (action != null) {

                Object startTime = messageSend.getProperty(ScheduledMessage.AMQ_SCHEDULER_ACTION_START_TIME);
                Object endTime = messageSend.getProperty(ScheduledMessage.AMQ_SCHEDULER_ACTION_END_TIME);

                if (replyTo != null && action.equals(ScheduledMessage.AMQ_SCHEDULER_ACTION_BROWSE)) {

                    if (startTime != null && endTime != null) {

                        long start = (Long) TypeConversionSupport.convert(startTime, Long.class);
                        long finish = (Long) TypeConversionSupport.convert(endTime, Long.class);

                        for (Job job : scheduler.getAllJobs(start, finish)) {
                            sendScheduledJob(producerExchange.getConnectionContext(), job, replyTo);
                        }
                    } else {
                        for (Job job : scheduler.getAllJobs()) {
                            sendScheduledJob(producerExchange.getConnectionContext(), job, replyTo);
                        }
                    }
                }
                if (jobId != null && action.equals(ScheduledMessage.AMQ_SCHEDULER_ACTION_REMOVE)) {
                    scheduler.remove(jobId);
                } else if (action.equals(ScheduledMessage.AMQ_SCHEDULER_ACTION_REMOVEALL)) {

                    if (startTime != null && endTime != null) {

                        long start = (Long) TypeConversionSupport.convert(startTime, Long.class);
                        long finish = (Long) TypeConversionSupport.convert(endTime, Long.class);

                        scheduler.removeAllJobs(start, finish);
                    } else {
                        scheduler.removeAllJobs();
                    }
                }
            }

        } else if ((cronValue != null || periodValue != null || delayValue != null) && jobId == null) {

            // Check for room in the job scheduler store
            if (systemUsage.getJobSchedulerUsage() != null) {
                JobSchedulerUsage usage = systemUsage.getJobSchedulerUsage();
                if (usage.isFull()) {
                    final String logMessage = "Job Scheduler Store is Full (" +
                        usage.getPercentUsage() + "% of " + usage.getLimit() +
                        "). Stopping producer (" + messageSend.getProducerId() +
                        ") to prevent flooding of the job scheduler store." +
                        " See http://activemq.apache.org/producer-flow-control.html for more info";

                    long start = System.currentTimeMillis();
                    long nextWarn = start;
                    while (!usage.waitForSpace(1000)) {
                        if (context.getStopping().get()) {
                            throw new IOException("Connection closed, send aborted.");
                        }

                        long now = System.currentTimeMillis();
                        if (now >= nextWarn) {
                            LOG.info("" + usage + ": " + logMessage + " (blocking for: " + (now - start) / 1000 + "s)");
                            nextWarn = now + 30000l;
                        }
                    }
                }
            }

            if (context.isInTransaction()) {
                context.getTransaction().addSynchronization(new Synchronization() {
                    @Override
                    public void afterCommit() throws Exception {
                        doSchedule(messageSend, cronValue, periodValue, delayValue);
                    }
                });
            } else {
                doSchedule(messageSend, cronValue, periodValue, delayValue);
            }
        } else {
            super.send(producerExchange, messageSend);
        }
    }

    private void doSchedule(Message messageSend, Object cronValue, Object periodValue, Object delayValue) throws Exception {
        long delay = 0;
        long period = 0;
        int repeat = 0;
        String cronEntry = "";

        // clear transaction context
        Message msg = messageSend.copy();
        msg.setTransactionId(null);
        org.apache.activemq.util.ByteSequence packet = wireFormat.marshal(msg);
        if (cronValue != null) {
            cronEntry = cronValue.toString();
        }
        if (periodValue != null) {
            period = (Long) TypeConversionSupport.convert(periodValue, Long.class);
        }
        if (delayValue != null) {
            delay = (Long) TypeConversionSupport.convert(delayValue, Long.class);
        }
        Object repeatValue = msg.getProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT);
        if (repeatValue != null) {
            repeat = (Integer) TypeConversionSupport.convert(repeatValue, Integer.class);
        }

        getInternalScheduler().schedule(msg.getMessageId().toString(),
            new ByteSequence(packet.data, packet.offset, packet.length), cronEntry, delay, period, repeat);
    }

    @Override
    public void scheduledJob(String id, ByteSequence job) {
        org.apache.activemq.util.ByteSequence packet = new org.apache.activemq.util.ByteSequence(job.getData(), job.getOffset(), job.getLength());
        try {
            Message messageSend = (Message) wireFormat.unmarshal(packet);
            messageSend.setOriginalTransactionId(null);
            Object repeatValue = messageSend.getProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT);
            Object cronValue = messageSend.getProperty(ScheduledMessage.AMQ_SCHEDULED_CRON);
            String cronStr = cronValue != null ? cronValue.toString() : null;
            int repeat = 0;
            if (repeatValue != null) {
                repeat = (Integer) TypeConversionSupport.convert(repeatValue, Integer.class);
            }

            if (repeat != 0 || cronStr != null && cronStr.length() > 0) {
                // create a unique id - the original message could be sent
                // lots of times
                messageSend.setMessageId(new MessageId(producerId, messageIdGenerator.getNextSequenceId()));
            }

            // Add the jobId as a property
            messageSend.setProperty("scheduledJobId", id);

            // if this goes across a network - we don't want it rescheduled
            messageSend.removeProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD);
            messageSend.removeProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY);
            messageSend.removeProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT);
            messageSend.removeProperty(ScheduledMessage.AMQ_SCHEDULED_CRON);

            if (messageSend.getTimestamp() > 0 && messageSend.getExpiration() > 0) {

                long oldExpiration = messageSend.getExpiration();
                long newTimeStamp = System.currentTimeMillis();
                long timeToLive = 0;
                long oldTimestamp = messageSend.getTimestamp();

                if (oldExpiration > 0) {
                    timeToLive = oldExpiration - oldTimestamp;
                }

                long expiration = timeToLive + newTimeStamp;

                if (expiration > oldExpiration) {
                    if (timeToLive > 0 && expiration > 0) {
                        messageSend.setExpiration(expiration);
                    }
                    messageSend.setTimestamp(newTimeStamp);
                    LOG.debug("Set message {} timestamp from {} to {}", new Object[]{ messageSend.getMessageId(), oldTimestamp, newTimeStamp });
                }
            }

            // Repackage the message contents prior to send now that all updates are complete.
            messageSend.beforeMarshall(wireFormat);

            final ProducerBrokerExchange producerExchange = new ProducerBrokerExchange();
            producerExchange.setConnectionContext(context);
            producerExchange.setMutable(true);
            producerExchange.setProducerState(new ProducerState(new ProducerInfo()));
            super.send(producerExchange, messageSend);
        } catch (Exception e) {
            LOG.error("Failed to send scheduled message {}", id, e);
        }
    }

    protected synchronized JobScheduler getInternalScheduler() throws Exception {
        if (this.started.get()) {
            if (this.scheduler == null && store != null) {
                this.scheduler = store.getJobScheduler("JMS");
                this.scheduler.addListener(this);
                this.scheduler.startDispatching();
            }
            return this.scheduler;
        }
        return null;
    }

    protected void sendScheduledJob(ConnectionContext context, Job job, ActiveMQDestination replyTo) throws Exception {

        org.apache.activemq.util.ByteSequence packet = new org.apache.activemq.util.ByteSequence(job.getPayload());
        try {
            Message msg = (Message) this.wireFormat.unmarshal(packet);
            msg.setOriginalTransactionId(null);
            msg.setPersistent(false);
            msg.setType(AdvisorySupport.ADIVSORY_MESSAGE_TYPE);
            msg.setMessageId(new MessageId(this.producerId, this.messageIdGenerator.getNextSequenceId()));

            // Preserve original destination
            msg.setOriginalDestination(msg.getDestination());

            msg.setDestination(replyTo);
            msg.setResponseRequired(false);
            msg.setProducerId(this.producerId);

            // Add the jobId as a property
            msg.setProperty("scheduledJobId", job.getJobId());

            final boolean originalFlowControl = context.isProducerFlowControl();
            final ProducerBrokerExchange producerExchange = new ProducerBrokerExchange();
            producerExchange.setConnectionContext(context);
            producerExchange.setMutable(true);
            producerExchange.setProducerState(new ProducerState(new ProducerInfo()));
            try {
                context.setProducerFlowControl(false);
                this.next.send(producerExchange, msg);
            } finally {
                context.setProducerFlowControl(originalFlowControl);
            }
        } catch (Exception e) {
            LOG.error("Failed to send scheduled message {}", job.getJobId(), e);
        }
    }
}
