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

package org.apache.activemq.store;

import org.apache.activemq.management.CountStatisticImpl;
import org.apache.activemq.management.SizeStatisticImpl;
import org.apache.activemq.management.StatsImpl;


public abstract class AbstractMessageStoreStatistics extends StatsImpl {

    protected final CountStatisticImpl messageCount;
    protected final SizeStatisticImpl messageSize;


    protected AbstractMessageStoreStatistics(String countDescription, String sizeDescription) {
        this(true, countDescription, sizeDescription);
    }

    protected AbstractMessageStoreStatistics(boolean enabled, String countDescription, String sizeDescription) {

        messageCount = new CountStatisticImpl("messageCount", countDescription);
        messageSize = new SizeStatisticImpl("messageSize", sizeDescription);

        addStatistic("messageCount", messageCount);
        addStatistic("messageSize", messageSize);

        this.setEnabled(enabled);
    }


    public CountStatisticImpl getMessageCount() {
        return messageCount;
    }

    public SizeStatisticImpl getMessageSize() {
        return messageSize;
    }

    @Override
    public void reset() {
        if (this.isDoReset()) {
            super.reset();
            messageCount.reset();
            messageSize.reset();
        }
    }

    @Override
    public void setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        messageCount.setEnabled(enabled);
        messageSize.setEnabled(enabled);
    }

    public void setParent(AbstractMessageStoreStatistics parent) {
        if (parent != null) {
            messageCount.setParent(parent.messageCount);
            messageSize.setParent(parent.messageSize);
        } else {
            messageCount.setParent(null);
            messageSize.setParent(null);
        }
    }

}
