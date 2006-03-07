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
package org.apache.activemq.transport.activeio;

import java.io.IOException;
import java.net.SocketException;

import org.activeio.command.AsyncCommandChannel;
import org.activeio.net.SocketMetadata;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.management.CountStatisticImpl;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;

/**
 * An implementation of the {@link Transport} interface using ActiveIO
 * 
 * @version $Revision$
 */
public class ActiveIOTransport implements Transport {

    private AsyncCommandChannel commandChannel;
    private TransportListener transportListener;
    private long timeout = 2000;

    private int minmumWireFormatVersion = 0;
    private long maxInactivityDuration = 60000;
    private boolean trace = false;
    private long stopTimeout = 2000;
    private CountStatisticImpl readCounter;
    private CountStatisticImpl writeCounter;


    public ActiveIOTransport(AsyncCommandChannel commandChannel) {
        this.commandChannel = commandChannel;
        this.commandChannel.setCommandListener(new org.activeio.command.CommandListener() {
            public void onCommand(Object command) {
                if (command.getClass() == WireFormatInfo.class) {
                    WireFormatInfo info = (WireFormatInfo) command;
                    try {
                        if (info.isTcpNoDelayEnabled()) {
                            enableTcpNodeDelay();
                        }
                    }
                    catch (IOException e) {
                        onError(e);
                    }
                }
                transportListener.onCommand((Command) command);
            }

            public void onError(Exception e) {
                if (e instanceof IOException) {
                    transportListener.onException((IOException) e);
                }
                else {
                    transportListener.onException((IOException) new IOException().initCause(e));
                }
            }
        });

    }

    private void enableTcpNodeDelay() throws SocketException {
        SocketMetadata sm = (SocketMetadata) commandChannel.getAdapter(SocketMetadata.class);
        if (sm != null) {
            sm.setTcpNoDelay(true);
        }
    }

    public void oneway(Command command) throws IOException {
        if (command.getClass() == WireFormatInfo.class) {
            WireFormatInfo info = (WireFormatInfo) command;
            if (info.isTcpNoDelayEnabled()) {
                enableTcpNodeDelay();
            }
        }
        commandChannel.writeCommand(command);
    }

    public FutureResponse asyncRequest(Command command) throws IOException {
        throw new AssertionError("Unsupported Method");
    }

    public Response request(Command command) throws IOException {
        throw new AssertionError("Unsupported Method");
    }

    public void start() throws Exception {
        commandChannel.start();
    }

    public void stop() throws Exception {
        commandChannel.stop(stopTimeout);
        commandChannel.dispose();
    }

    // Properties
    // -------------------------------------------------------------------------

    public TransportListener getTransportListener() {
        return transportListener;
    }

    public void setTransportListener(TransportListener listener) {
        this.transportListener = listener;
    }

    public AsyncCommandChannel getCommandChannel() {
        return commandChannel;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public Object narrow(Class target) {
        if (target.isAssignableFrom(getClass())) {
            return this;
        }
        return null;
    }

    public int getMinmumWireFormatVersion() {
        return minmumWireFormatVersion;
    }

    public void setMinmumWireFormatVersion(int minmumWireFormatVersion) {
        this.minmumWireFormatVersion = minmumWireFormatVersion;
    }

    public long getMaxInactivityDuration() {
        return maxInactivityDuration;
    }

    public void setMaxInactivityDuration(long maxInactivityDuration) {
        this.maxInactivityDuration = maxInactivityDuration;
    }

    public long getStopTimeout() {
        return stopTimeout;
    }

    public void setStopTimeout(long stopTimeout) {
        this.stopTimeout = stopTimeout;
    }

    public boolean isTrace() {
        return trace;
    }

    public void setTrace(boolean trace) {
        this.trace = trace;
    }

    public void setReadCounter(CountStatisticImpl readCounter) {
        this.readCounter = readCounter;
    }

    public void setWriteCounter(CountStatisticImpl writeCounter) {
        this.writeCounter = writeCounter;
    }

    public CountStatisticImpl getReadCounter() {
        return readCounter;
    }

    public CountStatisticImpl getWriteCounter() {
        return writeCounter;
    }

}
