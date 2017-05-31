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

package org.apache.activemq.camel.camelplugin;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerContext;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.spring.Utils;
import org.apache.activemq.usage.Usage;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.model.RoutesDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * A StatisticsBroker You can retrieve a Map Message for a Destination - or
 * Broker containing statistics as key-value pairs The message must contain a
 * replyTo Destination - else its ignored
 *
 */
public class CamelRoutesBroker extends BrokerFilter {
    private static Logger LOG = LoggerFactory.getLogger(CamelRoutesBroker.class);
    private String routesFile = "";
    private int checkPeriod = 1000;
    private Resource theRoutes;
    private DefaultCamelContext camelContext;
    private long lastRoutesModified = -1;
    private CountDownLatch countDownLatch;

    /**
     * Overide methods to pause the broker whilst camel routes are loaded
     */
    @Override
    public void send(ProducerBrokerExchange producerExchange, Message message) throws Exception {
        blockWhileLoadingCamelRoutes();
        super.send(producerExchange, message);
    }

    @Override
    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        blockWhileLoadingCamelRoutes();
        super.acknowledge(consumerExchange, ack);
    }

    @Override
    public Response messagePull(ConnectionContext context, MessagePull pull) throws Exception {
        blockWhileLoadingCamelRoutes();
        return super.messagePull(context, pull);
    }

    @Override
    public void processConsumerControl(ConsumerBrokerExchange consumerExchange, ConsumerControl control) {
        blockWhileLoadingCamelRoutes();
        super.processConsumerControl(consumerExchange, control);
    }

    @Override
    public void reapplyInterceptor() {
        blockWhileLoadingCamelRoutes();
        super.reapplyInterceptor();
    }

    @Override
    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        blockWhileLoadingCamelRoutes();
        super.beginTransaction(context, xid);
    }

    @Override
    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        blockWhileLoadingCamelRoutes();
        return super.prepareTransaction(context, xid);
    }

    @Override
    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        blockWhileLoadingCamelRoutes();
        super.rollbackTransaction(context, xid);
    }

    @Override
    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
        blockWhileLoadingCamelRoutes();
        super.commitTransaction(context, xid, onePhase);
    }

    @Override
    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception {
        blockWhileLoadingCamelRoutes();
        super.forgetTransaction(context, transactionId);
    }

    @Override
    public void preProcessDispatch(MessageDispatch messageDispatch) {
        blockWhileLoadingCamelRoutes();
        super.preProcessDispatch(messageDispatch);
    }

    @Override
    public void postProcessDispatch(MessageDispatch messageDispatch) {
        blockWhileLoadingCamelRoutes();
        super.postProcessDispatch(messageDispatch);
    }

    @Override
    public boolean sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference, Subscription subscription, Throwable poisonCause) {
        blockWhileLoadingCamelRoutes();
        return super.sendToDeadLetterQueue(context, messageReference, subscription, poisonCause);
    }

    @Override
    public void messageConsumed(ConnectionContext context, MessageReference messageReference) {
        blockWhileLoadingCamelRoutes();
        super.messageConsumed(context, messageReference);
    }

    @Override
    public void messageDelivered(ConnectionContext context, MessageReference messageReference) {
        blockWhileLoadingCamelRoutes();
        super.messageDelivered(context, messageReference);
    }

    @Override
    public void messageDiscarded(ConnectionContext context, Subscription sub, MessageReference messageReference) {
        blockWhileLoadingCamelRoutes();
        super.messageDiscarded(context, sub, messageReference);
    }

    @Override
    public void isFull(ConnectionContext context, Destination destination, Usage<?> usage) {
        blockWhileLoadingCamelRoutes();
        super.isFull(context, destination, usage);
    }

    @Override
    public void nowMasterBroker() {
        blockWhileLoadingCamelRoutes();
        super.nowMasterBroker();
    }

    /*
     * Properties
     */

    public String getRoutesFile() {
        return routesFile;
    }

    public void setRoutesFile(String routesFile) {
        this.routesFile = routesFile;
    }

    public int getCheckPeriod() {
        return checkPeriod;
    }

    public void setCheckPeriod(int checkPeriod) {
        this.checkPeriod = checkPeriod;
    }

    public CamelRoutesBroker(Broker next) {
        super(next);
    }

    @Override
    public void start() throws Exception {
        super.start();
        LOG.info("Starting CamelRoutesBroker");

        camelContext = new DefaultCamelContext();
        camelContext.setName("EmbeddedCamel-" + getBrokerName());
        camelContext.start();

        getBrokerService().getScheduler().executePeriodically(new Runnable() {
            @Override
            public void run() {
                try {
                    loadCamelRoutes();
                } catch (Throwable e) {
                    LOG.error("Failed to load Camel Routes", e);
                }

            }
        }, getCheckPeriod());
    }



    @Override
    public void stop() throws Exception {
        CountDownLatch latch = this.countDownLatch;
        if (latch != null){
            latch.countDown();
        }
        if (camelContext != null){
            camelContext.stop();
        }
        super.stop();
    }

    private void loadCamelRoutes() throws Exception{
        if (theRoutes == null) {
            String fileToUse = getRoutesFile();
            if (fileToUse == null || fileToUse.trim().isEmpty()) {
                BrokerContext brokerContext = getBrokerService().getBrokerContext();
                if (brokerContext != null) {
                    String uri = brokerContext.getConfigurationUrl();
                    Resource resource = Utils.resourceFromString(uri);
                    if (resource.exists()) {
                        fileToUse = resource.getFile().getParent();
                        fileToUse += File.separator;
                        fileToUse += "routes.xml";
                    }
                }
            }
            if (fileToUse != null && !fileToUse.isEmpty()){
                theRoutes = Utils.resourceFromString(fileToUse);
                setRoutesFile(theRoutes.getFile().getAbsolutePath());
            }
        }
        if (!isStopped() && camelContext != null && theRoutes != null && theRoutes.exists()){
            long lastModified = theRoutes.lastModified();
            if (lastModified != lastRoutesModified){
                CountDownLatch latch = new CountDownLatch(1);
                this.countDownLatch = latch;
                lastRoutesModified = lastModified;

                List<RouteDefinition> currentRoutes = camelContext.getRouteDefinitions();
                for (RouteDefinition rd:currentRoutes){
                    camelContext.stopRoute(rd);
                    camelContext.removeRouteDefinition(rd);
                }
                InputStream is = theRoutes.getInputStream();
                RoutesDefinition routesDefinition = camelContext.loadRoutesDefinition(is);

                for (RouteDefinition rd: routesDefinition.getRoutes()){
                    camelContext.startRoute(rd);
                }
                is.close();
                latch.countDown();
                this.countDownLatch=null;
            }


        }
    }

    private void blockWhileLoadingCamelRoutes(){
        CountDownLatch latch = this.countDownLatch;
        if (latch != null){
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
