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
package org.apache.activemq.broker.view;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;

/**
 * A <a href="http://www.graphviz.org/">DOT</a> 
 * file creator plugin which creates a DOT file showing the current topic & queue hierarchies.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision: $
 */
public class DestinationDotFilePlugin implements BrokerPlugin {
    private String file = "ActiveMQDestinations.dot";

    public Broker installPlugin(Broker broker) {
        return new DestinationDotFileInterceptor(broker, file);
    }

    public String getFile() {
        return file;
    }

    /**
     * Sets the destination file name to create the destination diagram
     */
    public void setFile(String file) {
        this.file = file;
    }


}
