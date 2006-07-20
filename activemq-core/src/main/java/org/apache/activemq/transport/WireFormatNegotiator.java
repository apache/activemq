/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */
package org.apache.activemq.transport;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.CountDownLatch;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;


public class WireFormatNegotiator extends TransportFilter {

    private static final Log log = LogFactory.getLog(WireFormatNegotiator.class);
    
    private OpenWireFormat wireFormat;
    private final int minimumVersion;
    
    private final AtomicBoolean firstStart=new AtomicBoolean(true);
    private final CountDownLatch readyCountDownLatch = new CountDownLatch(1);
    private final CountDownLatch wireInfoSentDownLatch = new CountDownLatch(1);
    
    /**
     * Negotiator
     * 
     * @param next
     * @param preferedFormat
     */
    public WireFormatNegotiator(Transport next, OpenWireFormat wireFormat, int minimumVersion) {
        super(next);
        this.wireFormat = wireFormat;
        this.minimumVersion = minimumVersion;
    }

    
    public void start() throws Exception {
        super.start();
        if( firstStart.compareAndSet(true, false) ) {
        	try {
        		WireFormatInfo info = wireFormat.getPreferedWireFormatInfo();
                if (log.isDebugEnabled()) {
                    log.debug("Sending: " + info);
                }
	            sendWireFormat(info);
        	} finally {
        		wireInfoSentDownLatch.countDown();
        	}
        }
    }
    
    public void stop() throws Exception {
    	super.stop();
        readyCountDownLatch.countDown();
    }
    
    public void oneway(Command command) throws IOException {
        try {
            readyCountDownLatch.await();
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
        super.oneway(command);
    }

 
    public void onCommand(Command command) {
        if( command.isWireFormatInfo() ) {
            WireFormatInfo info = (WireFormatInfo) command;
            if (log.isDebugEnabled()) {
                log.debug("Received WireFormat: " + info);
            }
            
            try {
                wireInfoSentDownLatch.await();
                
                if (log.isDebugEnabled()) {
                    log.debug(this + " before negotiation: " + wireFormat);
                }
                if( !info.isValid() ) {
                    onException(new IOException("Remote wire format magic is invalid"));
                } else if( info.getVersion() < minimumVersion ) {
                    onException(new IOException("Remote wire format ("+info.getVersion()+") is lower the minimum version required ("+minimumVersion+")"));
                }
                
                wireFormat.renegociatWireFormat(info);
                
                if (log.isDebugEnabled()) {
                    log.debug(this + " after negotiation: " + wireFormat);
                }
	
            } catch (IOException e) {
                onException(e);
            } catch (InterruptedException e) {
                onException((IOException) new InterruptedIOException().initCause(e));
			}
            readyCountDownLatch.countDown();
            onWireFormatNegotiated(info);
        }
        getTransportListener().onCommand(command);
    }


    public void onException(IOException error) {
        readyCountDownLatch.countDown();
    	super.onException(error);
    }
    
    public String toString() {
        return next.toString();
    }

    protected void sendWireFormat(WireFormatInfo info) throws IOException {
        next.oneway(info);
    }
    
    protected void onWireFormatNegotiated(WireFormatInfo info) {
    }
}
