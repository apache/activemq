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

import org.activeio.command.WireFormat;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.openwire.OpenWireFormat;

import edu.emory.mathcs.backport.java.util.concurrent.CountDownLatch;


public class WireFormatNegotiator extends TransportFilter {

    private final WireFormat wireFormat;
    private final int minimumVersion;
    
    private boolean firstStart=true;
    private CountDownLatch readyCountDownLatch = new CountDownLatch(1);
    
    /**
     * Negotiator
     * 
     * @param next
     * @param preferedFormat
     */
    public WireFormatNegotiator(Transport next, WireFormat wireFormat, int minimumVersion) {
        super(next);
        this.wireFormat = wireFormat;
        this.minimumVersion = minimumVersion;
    }

    
    public void start() throws Exception {
        super.start();
        if( firstStart ) {
            WireFormatInfo info = createWireFormatInfo();
            next.oneway(info);
        }
    }
    
    public void oneway(Command command) throws IOException {
        try {
            readyCountDownLatch.await();
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
        super.oneway(command);
    }

    protected WireFormatInfo createWireFormatInfo() {
        WireFormatInfo info = new WireFormatInfo();
        info.setVersion(wireFormat.getVersion());
        if ( wireFormat instanceof OpenWireFormat ) {
            info.setStackTraceEnabled(((OpenWireFormat)wireFormat).isStackTraceEnabled());
            info.setTcpNoDelayEnabled(((OpenWireFormat)wireFormat).isTcpNoDelayEnabled());
            info.setCacheEnabled(((OpenWireFormat)wireFormat).isCacheEnabled());
        }            
        return info;
    }
 
    public void onCommand(Command command) {
        if( command.isWireFormatInfo() ) {
            WireFormatInfo info = (WireFormatInfo) command;
            if( !info.isValid() ) {
                commandListener.onException(new IOException("Remote wire format magic is invalid"));
            } else if( info.getVersion() < minimumVersion ) {
                commandListener.onException(new IOException("Remote wire format ("+info.getVersion()+") is lower the minimum version required ("+minimumVersion+")"));
            } else if ( info.getVersion()!=wireFormat.getVersion() ) {
                // Match the remote side.
                wireFormat.setVersion(info.getVersion());
            }
            if ( wireFormat instanceof OpenWireFormat ) {
                if( !info.isStackTraceEnabled() ) {
                    ((OpenWireFormat)wireFormat).setStackTraceEnabled(false);
                }
                if( info.isTcpNoDelayEnabled() ) {
                    ((OpenWireFormat)wireFormat).setTcpNoDelayEnabled(true);
                }
                if( !info.isCacheEnabled() ) {
                    ((OpenWireFormat)wireFormat).setCacheEnabled(false);
                }
            }
                
            readyCountDownLatch.countDown();
            
        }
        commandListener.onCommand(command);
    }
    
    public String toString() {
        return next.toString();
    }
}
