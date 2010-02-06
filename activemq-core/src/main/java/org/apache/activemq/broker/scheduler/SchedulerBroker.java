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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.activemq.util.TypeConversionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kahadb.util.ByteSequence;

public class SchedulerBroker extends BrokerFilter implements JobListener {
    private static final Log LOG = LogFactory.getLog(SchedulerBroker.class);
    private static final IdGenerator ID_GENERATOR = new IdGenerator();
    private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
    private final AtomicBoolean started = new AtomicBoolean();
    private final WireFormat wireFormat = new OpenWireFormat();
    private final ConnectionContext context = new ConnectionContext();
    private final ProducerId producerId = new ProducerId();
    private File directory;

    private JobSchedulerStore store;
    private JobScheduler scheduler;

    public SchedulerBroker(Broker next, File directory) throws Exception {
        super(next);
        this.directory = directory;
        this.producerId.setConnectionId(ID_GENERATOR.generateId());
        this.context.setSecurityContext(SecurityContext.BROKER_SECURITY_CONTEXT);
        context.setBroker(next);
        LOG.info("Scheduler using directory: " + directory);

    }
   
    public synchronized  JobScheduler getJobScheduler() throws Exception {
        return new JobSchedulerFacade(this);
    }
   
    /**
     * @return the directory
     */
    public File getDirectory() {
        return this.directory;
    }
    /**
     * @param directory
     *            the directory to set
     */
    public void setDirectory(File directory) {
        this.directory = directory;
    }

    @Override
    public void start() throws Exception {
        this.started.set(true);
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
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        long start = 0;
        long period = 0;
        int repeat = 0;

        Object periodValue = messageSend.getProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD);

        if (periodValue != null) {
            period = (Long) TypeConversionSupport.convert(periodValue, Long.class);
            Object startValue = messageSend.getProperty(ScheduledMessage.AMQ_SCHEDULED_START);
            if (startValue != null) {
                start = (Long) TypeConversionSupport.convert(startValue, Long.class);
            }
            Object repeatValue = messageSend.getProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT);
            if (repeatValue != null) {
                repeat = (Integer) TypeConversionSupport.convert(repeatValue, Integer.class);
            }
            org.apache.activemq.util.ByteSequence packet = wireFormat.marshal(messageSend);
            getInternalScheduler().schedule( messageSend.getMessageId().toString(),
                    new ByteSequence(packet.data, packet.offset, packet.length),start, period, repeat);

        } else {

            super.send(producerExchange, messageSend);
        }
    }

    public void scheduledJob(String id, ByteSequence job) {
        org.apache.activemq.util.ByteSequence packet = new org.apache.activemq.util.ByteSequence(job.getData(), job
                .getOffset(), job.getLength());
        try {
            Message messageSend = (Message) this.wireFormat.unmarshal(packet);
            messageSend.setOriginalTransactionId(null);
            Object repeatValue = messageSend.getProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT);
            if (repeatValue != null) {
                int repeat = (Integer) TypeConversionSupport.convert(repeatValue, Integer.class);
                if (repeat != 0) {
                    //create a unique id - the original message could be sent lots of times
                    messageSend.setMessageId(new MessageId(this.producerId, this.messageIdGenerator.getNextSequenceId()));
                }
            }   
            //Add the jobId as a property
            messageSend.setProperty("scheduledJobId", id);
           
            //if this goes across a network - we don't want it rescheduled
            messageSend.removeProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD);
            messageSend.removeProperty(ScheduledMessage.AMQ_SCHEDULED_START);
            messageSend.removeProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT);
            
            
            final ProducerBrokerExchange producerExchange = new ProducerBrokerExchange();
            producerExchange.setConnectionContext(context);
            producerExchange.setMutable(true);
            producerExchange.setProducerState(new ProducerState(new ProducerInfo()));
            super.send(producerExchange, messageSend);
        } catch (Exception e) {
            LOG.error("Failed to send scheduled message " + id, e);
        }

    }
    
    protected synchronized  JobScheduler getInternalScheduler() throws Exception {
        if (this.started.get()) {
            if (this.scheduler == null) {
                this.scheduler = getStore().getJobScheduler("JMS");
                this.scheduler.addListener(this);
            }
            return this.scheduler;
        }
        return null;
    }

    

    private JobSchedulerStore getStore() throws Exception {
        if (started.get()) {
            if (this.store == null) {
                this.store = new JobSchedulerStore();
                this.store.setDirectory(directory);
                this.store.start();
            }
            return this.store;
        }
        return null;
    }

}
