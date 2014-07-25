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
package org.apache.activemq.broker.region.group;

import java.io.IOException;
import java.util.Map;

import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;

public class GroupFactoryFinder {
    private static final FactoryFinder GROUP_FACTORY_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/groups/");

    private GroupFactoryFinder() {
    }

    public static MessageGroupMapFactory createMessageGroupMapFactory(String type) throws IOException {
        try {
            Map<String,String> properties = null;
            String factoryType = type.trim();
            int p = factoryType.indexOf('?');
            if (p >= 0){
                String propertiesString = factoryType.substring(p+1);
                factoryType = factoryType.substring(0,p);
                properties = URISupport.parseQuery(propertiesString);
            }
            MessageGroupMapFactory result =  (MessageGroupMapFactory)GROUP_FACTORY_FINDER.newInstance(factoryType);
            if (properties != null && result != null){
                IntrospectionSupport.setProperties(result,properties);
            }
            return result;

        } catch (Throwable e) {
            throw IOExceptionSupport.create("Could not load " + type + " factory:" + e, e);
        }
    }


}
