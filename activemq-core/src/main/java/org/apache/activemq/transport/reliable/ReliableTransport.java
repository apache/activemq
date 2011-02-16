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
package org.apache.activemq.transport.reliable;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.ReplayCommand;
import org.apache.activemq.command.Response;
import org.apache.activemq.openwire.CommandIdComparator;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.udp.UdpTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This interceptor deals with out of order commands together with being able to
 * handle dropped commands and the re-requesting dropped commands.
 * 
 * 
 */
public class ReliableTransport extends ResponseCorrelator {
    private static final Logger LOG = LoggerFactory.getLogger(ReliableTransport.class);

    private ReplayStrategy replayStrategy;
    private SortedSet<Command> commands = new TreeSet<Command>(new CommandIdComparator());
    private int expectedCounter = 1;
    private int replayBufferCommandCount = 50;
    private int requestTimeout = 2000;
    private ReplayBuffer replayBuffer;
    private Replayer replayer;
    private UdpTransport udpTransport;

    public ReliableTransport(Transport next, ReplayStrategy replayStrategy) {
        super(next);
        this.replayStrategy = replayStrategy;
    }

    public ReliableTransport(Transport next, UdpTransport udpTransport) throws IOException {
        super(next, udpTransport.getSequenceGenerator());
        this.udpTransport = udpTransport;
        this.replayer = udpTransport.createReplayer();
    }

    /**
     * Requests that a range of commands be replayed
     */
    public void requestReplay(int fromCommandId, int toCommandId) {
        ReplayCommand replay = new ReplayCommand();
        replay.setFirstNakNumber(fromCommandId);
        replay.setLastNakNumber(toCommandId);
        try {
            oneway(replay);
        } catch (IOException e) {
            getTransportListener().onException(e);
        }
    }

    public Object request(Object o) throws IOException {
        final Command command = (Command)o;
        FutureResponse response = asyncRequest(command, null);
        while (true) {
            Response result = response.getResult(requestTimeout);
            if (result != null) {
                return result;
            }
            onMissingResponse(command, response);
        }
    }

    public Object request(Object o, int timeout) throws IOException {
        final Command command = (Command)o;
        FutureResponse response = asyncRequest(command, null);
        while (timeout > 0) {
            int time = timeout;
            if (timeout > requestTimeout) {
                time = requestTimeout;
            }
            Response result = response.getResult(time);
            if (result != null) {
                return result;
            }
            onMissingResponse(command, response);
            timeout -= time;
        }
        return response.getResult(0);
    }

    public void onCommand(Object o) {
        Command command = (Command)o;
        // lets pass wireformat through
        if (command.isWireFormatInfo()) {
            super.onCommand(command);
            return;
        } else if (command.getDataStructureType() == ReplayCommand.DATA_STRUCTURE_TYPE) {
            replayCommands((ReplayCommand)command);
            return;
        }

        int actualCounter = command.getCommandId();
        boolean valid = expectedCounter == actualCounter;

        if (!valid) {
            synchronized (commands) {
                int nextCounter = actualCounter;
                boolean empty = commands.isEmpty();
                if (!empty) {
                    Command nextAvailable = commands.first();
                    nextCounter = nextAvailable.getCommandId();
                }

                try {
                    boolean keep = replayStrategy.onDroppedPackets(this, expectedCounter, actualCounter, nextCounter);

                    if (keep) {
                        // lets add it to the list for later on
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Received out of order command which is being buffered for later: " + command);
                        }
                        commands.add(command);
                    }
                } catch (IOException e) {
                    onException(e);
                }

                if (!empty) {
                    // lets see if the first item in the set is the next
                    // expected
                    command = commands.first();
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
                    command = commands.first();
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
     * Sets the default timeout of requests before starting to request commands
     * are replayed
     */
    public void setRequestTimeout(int requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    public ReplayStrategy getReplayStrategy() {
        return replayStrategy;
    }

    public ReplayBuffer getReplayBuffer() {
        if (replayBuffer == null) {
            replayBuffer = createReplayBuffer();
        }
        return replayBuffer;
    }

    public void setReplayBuffer(ReplayBuffer replayBuffer) {
        this.replayBuffer = replayBuffer;
    }

    public int getReplayBufferCommandCount() {
        return replayBufferCommandCount;
    }

    /**
     * Sets the default number of commands which are buffered
     */
    public void setReplayBufferCommandCount(int replayBufferSize) {
        this.replayBufferCommandCount = replayBufferSize;
    }

    public void setReplayStrategy(ReplayStrategy replayStrategy) {
        this.replayStrategy = replayStrategy;
    }

    public Replayer getReplayer() {
        return replayer;
    }

    public void setReplayer(Replayer replayer) {
        this.replayer = replayer;
    }

    public String toString() {
        return next.toString();
    }

    public void start() throws Exception {
        if (udpTransport != null) {
            udpTransport.setReplayBuffer(getReplayBuffer());
        }
        if (replayStrategy == null) {
            throw new IllegalArgumentException("Property replayStrategy not specified");
        }
        super.start();
    }

    /**
     * Lets attempt to replay the request as a command may have disappeared
     */
    protected void onMissingResponse(Command command, FutureResponse response) {
        LOG.debug("Still waiting for response on: " + this + " to command: " + command + " sending replay message");

        int commandId = command.getCommandId();
        requestReplay(commandId, commandId);
    }

    protected ReplayBuffer createReplayBuffer() {
        return new DefaultReplayBuffer(getReplayBufferCommandCount());
    }

    protected void replayCommands(ReplayCommand command) {
        try {
            if (replayer == null) {
                onException(new IOException("Cannot replay commands. No replayer property configured"));
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Processing replay command: " + command);
            }
            getReplayBuffer().replayMessages(command.getFirstNakNumber(), command.getLastNakNumber(), replayer);

            // TODO we could proactively remove ack'd stuff from the replay
            // buffer
            // if we only have a single client talking to us
        } catch (IOException e) {
            onException(e);
        }
    }

}
