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
package org.apache.activemq.transport;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Negotiates the wire format with a new connection
 */
public class WireFormatNegotiator extends TransportFilter {

    private static final Log LOG = LogFactory.getLog(WireFormatNegotiator.class);

    private OpenWireFormat wireFormat;
    private final int minimumVersion;
    private long negotiateTimeout = 15000L;

    private final AtomicBoolean firstStart = new AtomicBoolean(true);
    private final CountDownLatch readyCountDownLatch = new CountDownLatch(1);
    private final CountDownLatch wireInfoSentDownLatch = new CountDownLatch(1);

    /**
     * Negotiator
     * 
     * @param next
     */
    public WireFormatNegotiator(Transport next, OpenWireFormat wireFormat, int minimumVersion) {
        super(next);
        this.wireFormat = wireFormat;
        if (minimumVersion <= 0) {
            minimumVersion = 1;
        }
        this.minimumVersion = minimumVersion;
    }

    public void start() throws Exception {
        super.start();
        if (firstStart.compareAndSet(true, false)) {
            try {
                WireFormatInfo info = wireFormat.getPreferedWireFormatInfo();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Sending: " + info);
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

    public void oneway(Object command) throws IOException {
        try {
            if (!readyCountDownLatch.await(negotiateTimeout, TimeUnit.MILLISECONDS))
                throw new IOException("Wire format negotiation timeout: peer did not send his wire format.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        }
        super.oneway(command);
    }

    public void onCommand(Object o) {
        Command command = (Command)o;
        if (command.isWireFormatInfo()) {
            WireFormatInfo info = (WireFormatInfo)command;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Received WireFormat: " + info);
            }

            try {
                wireInfoSentDownLatch.await();

                if (LOG.isDebugEnabled()) {
                    LOG.debug(this + " before negotiation: " + wireFormat);
                }
                if (!info.isValid()) {
                    onException(new IOException("Remote wire format magic is invalid"));
                } else if (info.getVersion() < minimumVersion) {
                    onException(new IOException("Remote wire format (" + info.getVersion() + ") is lower the minimum version required (" + minimumVersion + ")"));
                }

                wireFormat.renegotiateWireFormat(info);

                if (LOG.isDebugEnabled()) {
                    LOG.debug(this + " after negotiation: " + wireFormat);
                }

            } catch (IOException e) {
                onException(e);
            } catch (InterruptedException e) {
                onException((IOException)new InterruptedIOException().initCause(e));
            } catch (Exception e) {
                onException(IOExceptionSupport.create(e));
            }
            readyCountDownLatch.countDown();
            onWireFormatNegotiated(info);
        }
        getTransportListener().onCommand(command);
    }

    public void onException(IOException error) {
        readyCountDownLatch.countDown();
        /*
         * try { super.oneway(new ExceptionResponse(error)); } catch
         * (IOException e) { // ignore as we are already throwing an exception }
         */
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

    public long getNegotiateTimeout() {
        return negotiateTimeout;
    }

    public void setNegotiateTimeout(long negotiateTimeout) {
        this.negotiateTimeout = negotiateTimeout;
    }
}
