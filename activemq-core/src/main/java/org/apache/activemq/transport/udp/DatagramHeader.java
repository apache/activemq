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
package org.apache.activemq.transport.udp;

import org.apache.activemq.command.Command;

/**
 * Represents a header used when sending data grams
 * 
 * @version $Revision$
 */
public class DatagramHeader implements Comparable {

    private String producerId;
    private long counter;
    private boolean partial;
    private boolean complete;
    private int dataSize;

    // transient caches
    private transient byte[] partialData;
    private transient Command command;

    public int hashCode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + (int) (counter ^ (counter >>> 32));
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final DatagramHeader other = (DatagramHeader) obj;
        if (counter != other.counter)
            return false;
        return true;
    }

    public int compareTo(DatagramHeader that) {
        return (int) (this.counter - that.counter);
    }

    public int compareTo(Object that) {
        if (that instanceof DatagramHeader) {
            return compareTo((DatagramHeader) that);
        }
        return getClass().getName().compareTo(that.getClass().getName());
    }

    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }

    public long getCounter() {
        return counter;
    }

    public void setCounter(long counter) {
        this.counter = counter;
    }

    public boolean isPartial() {
        return partial;
    }

    public void setPartial(boolean partial) {
        this.partial = partial;
    }

    public String getProducerId() {
        return producerId;
    }

    public void setProducerId(String producerId) {
        this.producerId = producerId;
    }

    public int getDataSize() {
        return dataSize;
    }

    public void setDataSize(int dataSize) {
        this.dataSize = dataSize;
    }

    public void incrementCounter() {
        counter++;
    }

    public byte getFlags() {
        byte answer = 0;
        if (partial) {
            answer |= 0x1;
        }
        if (complete) {
            answer |= 0x2;
        }
        return answer;
    }

    public void setFlags(byte flags) {
        partial = (flags & 0x1) != 0;
        complete = (flags & 0x2) != 0;
    }

    public Command getCommand() {
        return command;
    }

    public void setCommand(Command command) {
        this.command = command;
    }

    public byte[] getPartialData() {
        return partialData;
    }

    public void setPartialData(byte[] partialData) {
        this.partialData = partialData;
    }

    // Transient cached properties

}
