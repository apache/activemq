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

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;

/**
* @author <a href="http://hiramchirino.com">Hiram Chirino</a>
*/
public class AutoOutboundTransformer extends JMSMappingOutboundTransformer {

    public AutoOutboundTransformer(JMSVendor vendor) {
        super(vendor);
    }

    @Override
    public EncodedMessage transform(Message msg) throws Exception {
        if( msg == null )
            return null;
        if( msg.getBooleanProperty(prefixVendor + "NATIVE") ) {
            if( msg instanceof BytesMessage ) {
                return AMQPNativeOutboundTransformer.transform(this, (BytesMessage)msg);
            } else {
                return null;
            }
        } else {
            return JMSMappingOutboundTransformer.transform(this, msg);
        }
    }

}
