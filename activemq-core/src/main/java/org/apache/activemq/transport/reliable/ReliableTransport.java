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
package org.apache.activemq.transport.reliable;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.Response;
import org.apache.activemq.openwire.CommandIdComparator;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.IntSequenceGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * This interceptor deals with out of order commands together with being able to
 * handle dropped commands and the re-requesting dropped commands.
 * 
 * @version $Revision$
 */
public class ReliableTransport extends ResponseCorrelator {
    private static final Log log = LogFactory.getLog(ReliableTransport.class);

    private ReplayStrategy replayStrategy;
    private SortedSet commands = new TreeSet(new CommandIdComparator());
    private int expectedCounter = 1;
    private int requestTimeout = 2000;

    public ReliableTransport(Transport next, ReplayStrategy replayStrategy) {
        super(next);
        this.replayStrategy = replayStrategy;
    }

    public ReliableTransport(Transport next, IntSequenceGenerator sequenceGenerator, ReplayStrategy replayStrategy) {
        super(next, sequenceGenerator);
        this.replayStrategy = replayStrategy;
    }


    public Response request(Command command) throws IOException {
        FutureResponse response = asyncRequest(command);
        while (true) {
            Response result = response.getResult(requestTimeout);
            if (result != null) {
                return result;
            }
            replayRequest(command, response);
        }
    }

    public Response request(Command command, int timeout) throws IOException {
        FutureResponse response = asyncRequest(command);
        while (timeout > 0) {
            int time = timeout;
            if (timeout > requestTimeout) { 
                time = requestTimeout;
            }
            Response result = response.getResult(time);
            if (result != null) {
                return result;
            }
            replayRequest(command, response);
            timeout -= time;
        }
        return response.getResult(0);
    }

    public void onCommand(Command command) {
        // lets pass wireformat through
        if (command.isWireFormatInfo()) {
            super.onCommand(command);
            return;
        }

        int actualCounter = command.getCommandId();
        boolean valid = expectedCounter == actualCounter;

        if (!valid) {
            synchronized (commands) {
                try {
                    boolean keep = replayStrategy.onDroppedPackets(this, expectedCounter, actualCounter);

                    if (keep) {
                        // lets add it to the list for later on
                        if (log.isDebugEnabled()) {
                            log.debug("Received out of order command which is being buffered for later: " + command);
                        }
                        commands.add(command);
                    }
                }
                catch (IOException e) {
                    getTransportListener().onException(e);
                }

                if (!commands.isEmpty()) {
                    // lets see if the first item in the set is the next
                    // expected
                    command = (Command) commands.first();
                    valid = expectedCounter == command.getCommandId();
                    if (valid) {
                        commands.remove(command);
                    }
                }
            }
        }

        while (valid) {
            // we've got a valid header so increment counter
            replayStrategy.onReceivedPacket(this, expectedCounter);
            expectedCounter++;
            super.onCommand(command);

            synchronized (commands) {
                // we could have more commands left
                valid = !commands.isEmpty();
                if (valid) {
                    // lets see if the first item in the set is the next
                    // expected
                    command = (Command) commands.first();
                    valid = expectedCounter == command.getCommandId();
                    if (valid) {
                        commands.remove(command);
                    }
                }
            }
        }
    }

    public int getBufferedCommandCount() {
        synchronized (commands) {
            return commands.size();
        }
    }

    public int getExpectedCounter() {
        return expectedCounter;
    }

    /**
     * This property should never really be set - but is mutable primarily for
     * test cases
     */
    public void setExpectedCounter(int expectedCounter) {
        this.expectedCounter = expectedCounter;
    }

    
    public int getRequestTimeout() {
        return requestTimeout;
    }

    /**
     * Sets the default timeout of requests before starting to request commands are replayed
     */
    public void setRequestTimeout(int requestTimeout) {
        this.requestTimeout = requestTimeout;
    }


    public ReplayStrategy getReplayStrategy() {
        return replayStrategy;
    }


    public String toString() {
        return next.toString();
    }
    /**
     * Lets attempt to replay the request as a command may have disappeared
     */
    protected void replayRequest(Command command, FutureResponse response) {
        log.debug("Still waiting for response on: " + this + " to command: " + command);
    }
}
