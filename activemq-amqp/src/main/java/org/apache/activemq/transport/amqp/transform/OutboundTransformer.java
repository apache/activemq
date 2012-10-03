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
package org.apache.activemq.transport.amqp.transform;

import org.apache.qpid.proton.engine.Delivery;

import javax.jms.Message;

/**
* @author <a href="http://hiramchirino.com">Hiram Chirino</a>
*/
public abstract class OutboundTransformer {

    JMSVendor vendor;
    String prefixVendor = "JMS_AMQP_";

    public OutboundTransformer(JMSVendor vendor) {
        this.vendor = vendor;
    }

    public abstract byte[] transform(Message jms) throws Exception;

    public String getPrefixVendor() {
        return prefixVendor;
    }

    public void setPrefixVendor(String prefixVendor) {
        this.prefixVendor = prefixVendor;
    }

    public JMSVendor getVendor() {
        return vendor;
    }

    public void setVendor(JMSVendor vendor) {
        this.vendor = vendor;
    }
}
