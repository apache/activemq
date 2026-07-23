/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.web.stubs;

import org.apache.activemq.broker.jmx.BrokerViewMBean;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import java.io.IOException;
import java.util.Map;

/**
 * Stub implementation of the BrokerViewMBean interface for testing purposes. All methods return empty responses or equivalents.
 */
public class StubBrokerViewMBean implements BrokerViewMBean {
    @Override
    public String getBrokerId() {
        return "";
    }

    @Override
    public String getBrokerName() {
        return "";
    }

    @Override
    public String getBrokerVersion() {
        return "";
    }

    @Override
    public String getUptime() {
        return "";
    }

    @Override
    public long getUptimeMillis() {
        return 0;
    }

    @Override
    public int getCurrentConnectionsCount() {
        return 0;
    }

    @Override
    public long getTotalConnectionsCount() {
        return 0;
    }

    @Override
    public void gc() throws Exception {

    }

    @Override
    public void resetStatistics() {

    }

    @Override
    public void enableStatistics() {

    }

    @Override
    public void disableStatistics() {

    }

    @Override
    public boolean isStatisticsEnabled() {
        return false;
    }

    @Override
    public long getTotalEnqueueCount() {
        return 0;
    }

    @Override
    public long getTotalDequeueCount() {
        return 0;
    }

    @Override
    public long getTotalConsumerCount() {
        return 0;
    }

    @Override
    public long getTotalProducerCount() {
        return 0;
    }

    @Override
    public long getTotalMessageCount() {
        return 0;
    }

    @Override
    public long getAverageMessageSize() {
        return 0;
    }

    @Override
    public long getMaxMessageSize() {
        return 0;
    }

    @Override
    public long getMinMessageSize() {
        return 0;
    }

    @Override
    public int getMemoryPercentUsage() {
        return 0;
    }

    @Override
    public long getMemoryLimit() {
        return 0;
    }

    @Override
    public void setMemoryLimit(long limit) {

    }

    @Override
    public int getStorePercentUsage() {
        return 0;
    }

    @Override
    public long getStoreLimit() {
        return 0;
    }

    @Override
    public void setStoreLimit(long limit) {

    }

    @Override
    public int getTempPercentUsage() {
        return 0;
    }

    @Override
    public long getTempLimit() {
        return 0;
    }

    @Override
    public void setTempLimit(long limit) {

    }

    @Override
    public int getJobSchedulerStorePercentUsage() {
        return 0;
    }

    @Override
    public long getJobSchedulerStoreLimit() {
        return 0;
    }

    @Override
    public void setJobSchedulerStoreLimit(long limit) {

    }

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public boolean isSlave() {
        return false;
    }

    @Override
    public void terminateJVM(int exitCode) {

    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }

    @Override
    public void restart() throws Exception {

    }

    @Override
    public void stopGracefully(String connectorName, String queueName, long timeout, long pollInterval) throws Exception {

    }

    @Override
    public ObjectName[] getTopics() {
        return new ObjectName[0];
    }

    @Override
    public int getTotalTopicsCount() {
        return 0;
    }

    @Override
    public int getTotalManagedTopicsCount() {
        return 0;
    }

    @Override
    public int getTotalTemporaryTopicsCount() {
        return 0;
    }

    @Override
    public ObjectName[] getQueues() {
        return new ObjectName[0];
    }

    @Override
    public int getTotalQueuesCount() {
        return 0;
    }

    @Override
    public int getTotalManagedQueuesCount() {
        return 0;
    }

    @Override
    public int getTotalTemporaryQueuesCount() {
        return 0;
    }

    @Override
    public String queryQueues(String filter, int page, int pageSize) throws IOException {
        return "";
    }

    @Override
    public String queryTopics(String filter, int page, int pageSize) throws IOException {
        return "";
    }

    @Override
    public CompositeData[] browseQueue(String queueName) throws OpenDataException, MalformedObjectNameException {
        return new CompositeData[0];
    }

    @Override
    public ObjectName[] getTemporaryTopics() {
        return new ObjectName[0];
    }

    @Override
    public ObjectName[] getTemporaryQueues() {
        return new ObjectName[0];
    }

    @Override
    public ObjectName[] getTopicSubscribers() {
        return new ObjectName[0];
    }

    @Override
    public ObjectName[] getDurableTopicSubscribers() {
        return new ObjectName[0];
    }

    @Override
    public ObjectName[] getInactiveDurableTopicSubscribers() {
        return new ObjectName[0];
    }

    @Override
    public ObjectName[] getQueueSubscribers() {
        return new ObjectName[0];
    }

    @Override
    public ObjectName[] getTemporaryTopicSubscribers() {
        return new ObjectName[0];
    }

    @Override
    public ObjectName[] getTemporaryQueueSubscribers() {
        return new ObjectName[0];
    }

    @Override
    public ObjectName[] getTopicProducers() {
        return new ObjectName[0];
    }

    @Override
    public ObjectName[] getQueueProducers() {
        return new ObjectName[0];
    }

    @Override
    public ObjectName[] getTemporaryTopicProducers() {
        return new ObjectName[0];
    }

    @Override
    public ObjectName[] getTemporaryQueueProducers() {
        return new ObjectName[0];
    }

    @Override
    public ObjectName[] getDynamicDestinationProducers() {
        return new ObjectName[0];
    }

    @Override
    public String addConnector(String discoveryAddress) throws Exception {
        return "";
    }

    @Override
    public String addNetworkConnector(String discoveryAddress) throws Exception {
        return "";
    }

    @Override
    public boolean removeConnector(String connectorName) throws Exception {
        return false;
    }

    @Override
    public boolean removeNetworkConnector(String connectorName) throws Exception {
        return false;
    }

    @Override
    public void addTopic(String name) throws Exception {

    }

    @Override
    public void addQueue(String name) throws Exception {

    }

    @Override
    public void removeTopic(String name) throws Exception {

    }

    @Override
    public void removeQueue(String name) throws Exception {

    }

    @Override
    public ObjectName createDurableSubscriber(String clientId, String subscriberName, String topicName, String selector) throws Exception {
        return null;
    }

    @Override
    public void destroyDurableSubscriber(String clientId, String subscriberName) throws Exception {

    }

    @Override
    public void reloadLog4jProperties() throws Throwable {

    }

    @Override
    public String getVMURL() {
        return "";
    }

    @Override
    public Map<String, String> getTransportConnectors() {
        return Map.of();
    }

    @Override
    public String getTransportConnectorByType(String type) {
        return "";
    }

    @Override
    public String getDataDirectory() {
        return "";
    }

    @Override
    public ObjectName getJMSJobScheduler() {
        return null;
    }

    @Override
    public int getMaxUncommittedCount() {
        return 0;
    }

    @Override
    public void setMaxUncommittedCount(int maxUncommittedCount) {

    }

    @Override
    public long getTotalMaxUncommittedExceededCount() {
        return 0;
    }

    @Override
    public boolean isDedicatedTaskRunner() {
        return false;
    }

    @Override
    public boolean isVirtualThreadTaskRunner() {
        return false;
    }
}
