/**
 * 
 * Copyright 2004 Protique Ltd
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * 
 **/
package org.activemq.transport.jrms;

import EDU.oswego.cs.dl.util.concurrent.SynchronizedBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.activemq.io.WireFormat;
import org.activemq.transport.TransportServerChannelSupport;

import javax.jms.JMSException;
import java.net.URI;

/**
 * A JRMS implementation of TransportServerChannel
 *
 * @version $Revision$
 */
public class JRMSTransportServerChannel extends TransportServerChannelSupport {

    private static final Log log = LogFactory.getLog(JRMSTransportServerChannel.class);

    private SynchronizedBoolean started;


    public JRMSTransportServerChannel(WireFormat wireFormat, URI bindAddr) {
        super(bindAddr);
        started = new SynchronizedBoolean(false);
    }


    /**
     * start listeneing for events
     *
     * @throws JMSException if an error occurs
     */
    public void start() throws JMSException {
        if (started.commit(false, true)) {
            log.info("JRMS ServerChannel at: " + getUrl());
        }
    }


    /**
     * @return pretty print of this
     */
    public String toString() {
        return "JRMSTransportServerChannel@" + getUrl();
    }
}