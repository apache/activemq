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
package org.apache.activemq.transport.stomp;

import java.io.IOException;

import javax.jms.JMSException;

import org.apache.activemq.command.Command;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The StompTransportFilter normally sits on top of a TcpTransport
 * that has been configured with the StompWireFormat and is used to
 * convert STOMP commands to ActiveMQ commands.
 *
 * All of the coversion work is done by delegating to the ProtocolConverter.
 *
 * @author <a href="http://hiramchirino.com">chirino</a>
 */
public class StompTransportFilter extends TransportFilter {
	static final private Log log = LogFactory.getLog(StompTransportFilter.class);
    private final ProtocolConverter protocolConverter;

    private final Object sendToActiveMQMutex = new Object();
    private final Object sendToStompMutex = new Object();

    private final FrameTranslator frameTranslator;

    private boolean trace;
    
    public StompTransportFilter(Transport next, FrameTranslator translator) {
		super(next);
        this.frameTranslator = translator;
        this.protocolConverter = new ProtocolConverter(this, translator);
    }

	public void oneway(Object o) throws IOException {
        try {
        	final Command command = (Command) o;
        	protocolConverter.onActiveMQCommad(command);
		} catch (JMSException e) {
			throw IOExceptionSupport.create(e);
		}
	}

	public void onCommand(Object command) {
        try {
    		if( trace ) {
    			log.trace("Received: \n"+command);
    		}
        	protocolConverter.onStompCommad((StompFrame) command);
		} catch (IOException e) {
			onException(e);
		} catch (JMSException e) {
			onException(IOExceptionSupport.create(e));
		}
	}

	public void sendToActiveMQ(Command command) {
		synchronized(sendToActiveMQMutex) {
			transportListener.onCommand(command);
		}
	}

	public void sendToStomp(StompFrame command) throws IOException {
		if( trace ) {
			log.trace("Sending: \n"+command);
		}
		synchronized(sendToStompMutex) {
			next.oneway(command);
		}
	}

    public FrameTranslator getFrameTranslator()
    {
        return frameTranslator;
    }

	public boolean isTrace() {
		return trace;
	}

	public void setTrace(boolean trace) {
		this.trace = trace;
	}
}
