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
import javax.jms.Message;
import javax.jms.MessageFormatException;

/**
* @author <a href="http://hiramchirino.com">Hiram Chirino</a>
*/
public class AMQPNativeOutboundTransformer extends OutboundTransformer {

    public AMQPNativeOutboundTransformer(JMSVendor vendor) {
        super(vendor);
    }

    @Override
    public byte[] transform(Message jms) throws Exception {
        if( jms == null )
            return null;
        if( !(jms instanceof BytesMessage) )
            return null;

        long messageFormat;
        try {
            if( !jms.getBooleanProperty(prefixVendor + "NATIVE") ) {
                return null;
            }
            messageFormat = jms.getLongProperty(prefixVendor + "MESSAGE_FORMAT");
        } catch (MessageFormatException e) {
            return null;
        }

        // TODO: Proton should probably expose a way to set the msg format
        // delivery.settMessageFormat(messageFormat);

        BytesMessage bytesMessage = (BytesMessage) jms;
        byte data[] = new byte[(int) bytesMessage.getBodyLength()];
        bytesMessage.readBytes(data);
        return data;
    }


}
