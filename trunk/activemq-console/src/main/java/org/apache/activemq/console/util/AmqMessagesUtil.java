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
package org.apache.activemq.console.util;

import java.net.URI;
import java.util.List;
import java.util.Set;

import javax.jms.Destination;

import org.apache.activemq.console.filter.AmqMessagesQueryFilter;
import org.apache.activemq.console.filter.GroupPropertiesViewFilter;
import org.apache.activemq.console.filter.MapTransformFilter;
import org.apache.activemq.console.filter.PropertiesViewFilter;
import org.apache.activemq.console.filter.QueryFilter;
import org.apache.activemq.console.filter.StubQueryFilter;
import org.apache.activemq.console.filter.WildcardToMsgSelectorTransformFilter;

public final class AmqMessagesUtil {

    public static final String JMS_MESSAGE_HEADER_PREFIX = "JMS_HEADER_FIELD:";
    public static final String JMS_MESSAGE_CUSTOM_PREFIX = "JMS_CUSTOM_FIELD:";
    public static final String JMS_MESSAGE_BODY_PREFIX = "JMS_BODY_FIELD:";

    private AmqMessagesUtil() {
    }

    public static List getAllMessages(URI brokerUrl, Destination dest) throws Exception {
        return getMessages(brokerUrl, dest, "");
    }

    public static List getMessages(URI brokerUrl, Destination dest, String selector) throws Exception {
        return createMessageQueryFilter(brokerUrl, dest).query(selector);
    }

    public static List getMessages(URI brokerUrl, Destination dest, List selectors) throws Exception {
        return createMessageQueryFilter(brokerUrl, dest).query(selectors);
    }

    public static List filterMessagesView(List messages, Set groupViews, Set attributeViews) throws Exception {
        return (new PropertiesViewFilter(attributeViews, new GroupPropertiesViewFilter(groupViews, new MapTransformFilter(new StubQueryFilter(messages))))).query("");
    }

    public static QueryFilter createMessageQueryFilter(URI brokerUrl, Destination dest) {
        return new WildcardToMsgSelectorTransformFilter(new AmqMessagesQueryFilter(brokerUrl, dest));
    }
}
