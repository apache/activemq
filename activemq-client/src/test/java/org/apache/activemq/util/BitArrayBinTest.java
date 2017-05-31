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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class BitArrayBinTest {

    @Test
    public void testSetAroundWindow() throws Exception {
        doTestSetAroundWindow(500, 2000);
        doTestSetAroundWindow(512, 2000);
        doTestSetAroundWindow(128, 512);
    }

    @Test
    public void testSetHiLo() throws Exception {
        BitArrayBin toTest = new BitArrayBin(50);
        toTest.setBit(0, true);
        toTest.setBit(100, true);
        toTest.setBit(150, true);
        assertTrue("set", toTest.getBit(0));

        toTest.setBit(0, true);
        assertTrue("set", toTest.getBit(0));
    }

    private void doTestSetAroundWindow(int window, int dataSize) throws Exception {

        BitArrayBin toTest = new BitArrayBin(window);

        for (int i = 0; i <= dataSize; i++) {
            assertTrue("not already set", !toTest.setBit(i, Boolean.TRUE));
            assertEquals("current is max", i, toTest.getLastSetIndex());
        }

        assertEquals("last is max", dataSize, toTest.getLastSetIndex());

        int windowOfValidData = roundWindow(dataSize, window);
        int i = dataSize;
        for (; i >= dataSize - windowOfValidData; i--) {
            assertTrue("was already set, id=" + i, toTest.setBit(i, Boolean.TRUE));
        }

        assertEquals("last is still max", dataSize, toTest.getLastSetIndex());

        for (; i >= 0; i--) {
            assertTrue("was not already set, id=" + i, !toTest.setBit(i, Boolean.TRUE));
        }

        for (int j = dataSize + 1; j <= (2 * dataSize); j++) {
            assertTrue("not already set: id=" + j, !toTest.setBit(j, Boolean.TRUE));
        }

        assertEquals("last still max*2", 2 * dataSize, toTest.getLastSetIndex());
    }

    @Test
    public void testSetUnsetAroundWindow() throws Exception {
        doTestSetUnSetAroundWindow(500, 2000);
        doTestSetUnSetAroundWindow(512, 2000);
        doTestSetUnSetAroundWindow(128, 512);
    }

    private void doTestSetUnSetAroundWindow(int dataSize, int window) throws Exception {

        BitArrayBin toTest = new BitArrayBin(window);

        for (int i = 0; i <= dataSize; i++) {
            assertTrue("not already set", !toTest.setBit(i, Boolean.TRUE));
        }

        int windowOfValidData = roundWindow(dataSize, window);
        for (int i = dataSize; i >= 0 && i >= dataSize - windowOfValidData; i--) {
            assertTrue("was already set, id=" + i, toTest.setBit(i, Boolean.FALSE));
        }

        for (int i = 0; i <= dataSize; i++) {
            assertTrue("not already set, id:" + i, !toTest.setBit(i, Boolean.TRUE));
        }

        for (int j = 2 * dataSize; j < 4 * dataSize; j++) {
            assertTrue("not already set: id=" + j, !toTest.setBit(j, Boolean.TRUE));
        }
    }

    @Test
    public void testSetAroundLongSizeMultiplier() throws Exception {
        int window = 512;
        int dataSize = 1000;
        for (int muliplier = 1; muliplier < 8; muliplier++) {
            for (int value = 0; value < dataSize; value++) {
                BitArrayBin toTest = new BitArrayBin(window);

                int instance = value + muliplier * BitArray.LONG_SIZE;
                assertTrue("not already set: id=" + instance, !toTest.setBit(instance, Boolean.TRUE));
                assertTrue("not already set: id=" + value, !toTest.setBit(value, Boolean.TRUE));
                assertEquals("max set correct", instance, toTest.getLastSetIndex());
            }
        }
    }

    @Test
    public void testLargeGapInData() throws Exception {
        doTestLargeGapInData(128);
        doTestLargeGapInData(500);
    }

    public void doTestLargeGapInData(int window) throws Exception {
        BitArrayBin toTest = new BitArrayBin(window);

        int instance = BitArray.LONG_SIZE;
        assertTrue("not already set: id=" + instance, !toTest.setBit(instance, Boolean.TRUE));

        instance = 12 * BitArray.LONG_SIZE;
        assertTrue("not already set: id=" + instance, !toTest.setBit(instance, Boolean.TRUE));

        instance = 9 * BitArray.LONG_SIZE;
        assertTrue("not already set: id=" + instance, !toTest.setBit(instance, Boolean.TRUE));
    }

    @Test
    public void testLastSeq() throws Exception {
        BitArrayBin toTest = new BitArrayBin(512);
        assertEquals("last not set", -1, toTest.getLastSetIndex());

        toTest.setBit(1, Boolean.TRUE);
        assertEquals("last correct", 1, toTest.getLastSetIndex());

        toTest.setBit(64, Boolean.TRUE);
        assertEquals("last correct", 64, toTest.getLastSetIndex());

        toTest.setBit(68, Boolean.TRUE);
        assertEquals("last correct", 68, toTest.getLastSetIndex());
    }

    // window moves in increments of BitArray.LONG_SIZE.
    // valid data window on low end can be larger than window
    private int roundWindow(int dataSetEnd, int windowSize) {

        int validData = dataSetEnd - windowSize;
        int validDataBin = validData / BitArray.LONG_SIZE;
        validDataBin += (windowSize % BitArray.LONG_SIZE > 0 ? 1 : 0);
        int startOfValid = validDataBin * BitArray.LONG_SIZE;

        return dataSetEnd - startOfValid;
    }

    @Test
    public void testLargeNumber() throws Exception {
        BitArrayBin toTest = new BitArrayBin(1024);
        toTest.setBit(1, true);
        long largeNum = Integer.MAX_VALUE * 2L + 100L;
        toTest.setBit(largeNum, true);
        assertTrue("set", toTest.getBit(largeNum));
    }

    //This test is slightly different in that it doesn't set bit 1 to
    //true as above which was causing a different result before AMQ-6431
    @Test
    public void testLargeNumber2() {
        BitArrayBin toTest = new BitArrayBin(1024);
        long largeNum = Integer.MAX_VALUE * 2L + 100L;
        toTest.setBit(largeNum, true);
        assertTrue(toTest.getBit(largeNum));
    }
}
