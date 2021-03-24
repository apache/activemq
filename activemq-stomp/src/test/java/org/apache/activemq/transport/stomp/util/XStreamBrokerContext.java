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
package org.apache.activemq.transport.stomp.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.broker.BrokerContext;
import org.apache.activemq.transport.stomp.SamplePojo;

import com.thoughtworks.xstream.XStream;
import org.apache.activemq.transport.stomp.XStreamSupport;

public class XStreamBrokerContext implements BrokerContext {

    private final Map<String, XStream> beansMap = new HashMap<String, XStream>();

    public XStreamBrokerContext() {

        XStream stream = XStreamSupport.createXStream();
        stream.processAnnotations(SamplePojo.class);

        beansMap.put("xstream", stream);
    }

    @Override
    public Object getBean(String name) {
        return this.beansMap.get(name);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map getBeansOfType(Class type) {

        if (type.equals(XStream.class)) {
            return this.beansMap;
        }

        return null;
    }

    @Override
    public String getConfigurationUrl() {
        return null;
    }

}
