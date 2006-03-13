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
import org.apache.activemq.openwire.CommandIdComparator;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFilter;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * This interceptor deals with out of order commands together with being able to
 * handle dropped commands and the re-requesting dropped commands.
 * 
 * @version $Revision$
 */
public class ReliableTransport extends TransportFilter {
    private ReplayStrategy replayStrategy;
    private SortedSet commands = new TreeSet(new CommandIdComparator());
    private int expectedCounter = 1;

    public ReliableTransport(Transport next, ReplayStrategy replayStrategy) {
        super(next);
        this.replayStrategy = replayStrategy;
    }

    public void onCommand(Command command) {
        int actualCounter = command.getCommandId();
        boolean valid = expectedCounter == actualCounter;

        if (!valid) {
            synchronized (commands) {
                try {
                    boolean keep = replayStrategy.onDroppedPackets(this, expectedCounter, actualCounter);
                    
                    if (keep) {
                        // lets add it to the list for later on
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
            getTransportListener().onCommand(command);

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
     * This property should never really be set - but is mutable primarily for test cases
     */
    public void setExpectedCounter(int expectedCounter) {
        this.expectedCounter = expectedCounter;
    }

    public String toString() {
        return next.toString();
    }

}
