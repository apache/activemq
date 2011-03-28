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

import java.io.Serializable;
import java.util.LinkedList;

/**
 * Holder for many bitArrays - used for message audit
 * 
 * 
 */
public class BitArrayBin implements Serializable {

    private static final long serialVersionUID = 1L;
    private LinkedList<BitArray> list;
    private int maxNumberOfArrays;
    private int firstIndex = -1;
    private long lastInOrderBit=-1;

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
            list.add(null);
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
        boolean answer = false;
        BitArray ba = getBitArray(index);
        if (ba != null) {
            int offset = getOffset(index);
            if (offset >= 0) {
                answer = ba.set(offset, value);
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
        boolean result = false;
        if (lastInOrderBit == -1) {
            result = true;
        } else {
            result = lastInOrderBit + 1 == index;
        }
        lastInOrderBit = index;
        return result;

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
            if (bin >= maxNumberOfArrays) {
                int overShoot = bin - maxNumberOfArrays + 1;
                while (overShoot > 0) {
                    list.removeFirst();
                    firstIndex += BitArray.LONG_SIZE;
                    list.add(new BitArray());
                    overShoot--;
                }
                
                bin = maxNumberOfArrays - 1;
            }
            answer = list.get(bin);
            if (answer == null) {
                answer = new BitArray();
                list.set(bin, answer);
            }
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
        if (firstIndex < 0) {
            firstIndex = (int) (index - (index % BitArray.LONG_SIZE));
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

    public long getLastSetIndex() {
        long result = -1;
        
        if (firstIndex >=0) {
            result = firstIndex;   
            BitArray last = null;
            for (int lastBitArrayIndex = maxNumberOfArrays -1; lastBitArrayIndex >= 0; lastBitArrayIndex--) {
                last = list.get(lastBitArrayIndex);
                if (last != null) {
                    result += last.length() -1;
                    result += lastBitArrayIndex * BitArray.LONG_SIZE;
                    break;
                }
            }
        }
        return result;
    }
}
