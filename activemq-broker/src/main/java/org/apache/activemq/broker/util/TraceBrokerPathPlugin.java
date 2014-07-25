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
package org.apache.activemq.broker.util;

import java.io.IOException;

import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.command.MessageDispatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The TraceBrokerPathPlugin can be used in a network of Brokers. Each Broker
 * that has the plugin configured, will add it's brokerName to the content
 * of a JMS Property. If all Brokers have this property enabled, the path the
 * message actually took through the network can be seen in the defined property.
 *
 * @org.apache.xbean.XBean element="traceBrokerPathPlugin"
 *
 */
@SuppressWarnings("unchecked")
public class TraceBrokerPathPlugin extends BrokerPluginSupport {

    private String stampProperty = "BrokerPath";
    private static final Logger LOG = LoggerFactory.getLogger(TraceBrokerPathPlugin.class);

    public String getStampProperty() {
        return stampProperty;
    }

    public void setStampProperty(String stampProperty) {
        if (stampProperty != null && !stampProperty.isEmpty()) {
            this.stampProperty = stampProperty;
        }
    }

    public void preProcessDispatch(MessageDispatch messageDispatch) {
        try {
            if (messageDispatch != null && messageDispatch.getMessage() != null) {
                String brokerStamp = (String)messageDispatch.getMessage().getProperty(getStampProperty());
                if (brokerStamp == null) {
                    brokerStamp = getBrokerName();
                } else {
                    brokerStamp += "," + getBrokerName();
                }
                messageDispatch.getMessage().setProperty(getStampProperty(), brokerStamp);
                messageDispatch.getMessage().setMarshalledProperties(null);
            }
        } catch (IOException ioe) {
            LOG.warn("Setting broker property failed", ioe);
        }
        super.preProcessDispatch(messageDispatch);
    }
}
