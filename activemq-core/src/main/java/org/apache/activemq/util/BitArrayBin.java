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
package org.apache.activemq.util;

import java.util.LinkedList;

/**
 * Holder for many bitArrays - used for message audit
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class BitArrayBin {

    private LinkedList<BitArray> list;
    private int maxNumberOfArrays;
    private int firstIndex = -1;
    private int firstBin = -1;
    private long lastBitSet=-1;

    /**
     * Create a BitArrayBin to a certain window size (number of messages to
     * keep)
     * 
     * @param windowSize
     */
    public BitArrayBin(int windowSize) {
        maxNumberOfArrays = ((windowSize + 1) / BitArray.LONG_SIZE) + 1;
        maxNumberOfArrays = Math.max(maxNumberOfArrays, 1);
        list = new LinkedList<BitArray>();
        for (int i = 0; i < maxNumberOfArrays; i++) {
            list.add(new BitArray());
        }
    }

    /**
     * Set a bit
     * 
     * @param index
     * @param value
     * @return true if set
     */
    public boolean setBit(long index, boolean value) {
        boolean answer = true;
        BitArray ba = getBitArray(index);
        if (ba != null) {
            int offset = getOffset(index);
            if (offset >= 0) {
                answer = ba.set(offset, value);
            }
            if (value) {
                lastBitSet=index;
            }else {
                lastBitSet=-1;
            }
        }
        return answer;
    }
    
    /**
     * Test if in order
     * @param index
     * @return true if next message is in order
     */
    public boolean isInOrder(long index) {
        if (lastBitSet== -1) {
            return true;
        }
        return lastBitSet+1==index;
    }

    /**
     * Get the boolean value at the index
     * 
     * @param index
     * @return true/false
     */
    public boolean getBit(long index) {
        boolean answer = index >= firstIndex;
        BitArray ba = getBitArray(index);
        if (ba != null) {
            int offset = getOffset(index);
            if (offset >= 0) {
                answer = ba.get(offset);
                return answer;
            }
        } else {
            // gone passed range for previous bins so assume set
            answer = true;
        }
        return answer;
    }

    /**
     * Get the BitArray for the index
     * 
     * @param index
     * @return BitArray
     */
    private BitArray getBitArray(long index) {
        int bin = getBin(index);
        BitArray answer = null;
        if (bin >= 0) {
            if (firstIndex < 0) {
                firstIndex = 0;
            }
            if (bin >= list.size()) {
                list.removeFirst();
                firstIndex += BitArray.LONG_SIZE;
                list.add(new BitArray());
                bin = list.size() - 1;
            }
            answer = list.get(bin);
        }
        return answer;
    }

    /**
     * Get the index of the bin from the total index
     * 
     * @param index
     * @return the index of the bin
     */
    private int getBin(long index) {
        int answer = 0;
        if (firstBin < 0) {
            firstBin = 0;
        } else if (firstIndex >= 0) {
            answer = (int)((index - firstIndex) / BitArray.LONG_SIZE);
        }
        return answer;
    }

    /**
     * Get the offset into a bin from the total index
     * 
     * @param index
     * @return the relative offset into a bin
     */
    private int getOffset(long index) {
        int answer = 0;
        if (firstIndex >= 0) {
            answer = (int)((index - firstIndex) - (BitArray.LONG_SIZE * getBin(index)));
        }
        return answer;
    }
}
