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
package org.apache.activemq.broker.region.policy;

import java.util.List;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.filter.MessageEvaluationContext;

/**
 * ClientIdFilterDispatchPolicy dispatches messages in a topic to a given
 * client. Then the message with a "PTP_CLIENTID" property, can be received by a
 * mqtt client with the same clientId.
 *
 * @author kimmking (kimmking@163.com)
 * @date 2013-12-20
 * @org.apache.xbean.XBean
 */
public class ClientIdFilterDispatchPolicy extends SimpleDispatchPolicy {

    public static final String PTP_CLIENTID = "PTP_CLIENTID";
    public static final String PTP_SUFFIX = ".PTP";

    private String ptpClientId = PTP_CLIENTID;
    private String ptpSuffix = PTP_SUFFIX;

    public boolean dispatch(MessageReference node, MessageEvaluationContext msgContext, List<Subscription> consumers) throws Exception {

        Object _clientId = node.getMessage().getProperty(ptpClientId);
        if (_clientId == null) return super.dispatch(node, msgContext, consumers);

        ActiveMQDestination _destination = node.getMessage().getDestination();
        int count = 0;
        for (Subscription sub : consumers) {
            // Don't deliver to browsers
            if (sub.getConsumerInfo().isBrowser()) {
                continue;
            }
            // Only dispatch to interested subscriptions
            if (!sub.matches(node, msgContext)) {
                sub.unmatched(node);
                continue;
            }
            if (_clientId != null && _destination.isTopic() && _clientId.equals(sub.getContext().getClientId())
                    && _destination.getQualifiedName().endsWith(this.ptpSuffix)) {
                sub.add(node);
                count++;
            } else {
                sub.unmatched(node);
            }
        }

        return count > 0;
    }

    public String getPtpClientId() {
        return ptpClientId;
    }

    public void setPtpClientId(String ptpClientId) {
        this.ptpClientId = ptpClientId;
    }

    public String getPtpSuffix() {
        return ptpSuffix;
    }

    public void setPtpSuffix(String ptpSuffix) {
        this.ptpSuffix = ptpSuffix;
    }

}