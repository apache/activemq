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
package org.apache.activemq.console.filter;

import junit.framework.TestCase;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.console.util.AmqMessagesUtil;

public class TestMapTransformFilter extends TestCase {

    private static final Object[][] testData = new Object[][] {
            { new byte[] { 1, 2, 3, 4 } },
            { new int[] { 1, 2, 3, 4 } },
            { new long[] { 1, 2, 3, 4 } },
            { new double[] { 1, 2, 3, 4 } },
            { new float[] { 1, 2, 3, 4 } },
            { new char[] { '1', '2', '3', '4' } },

            { new Integer[] { 1, 2, 3, 4 } },
            { new Byte[] { 1, 2, 3, 4 } },
            { new Long[] { 1L, 2L, 3L, 4L } },
            { new Double[] { 1d, 2d, 3d, 4d } },
            { new Float[] { 1f, 2f, 3f, 4f } },
            { new Character[] { '1', '2', '3', '4' } },

            { new String[] { "abc", "def" } },
            { new int[] { 1, } },
            { new int[] {,} },
            { "abc"},
            {(byte)1},
            { (int)1 },
            { (long)1 },
            { (double)1d },
            { (float)1f },
            { (char)'1' },

    };

    public void testFetDisplayString() {
        MapTransformFilter filter = new MapTransformFilter(null);
        for (Object[] objectArray : testData) {
            filter.getDisplayString(objectArray[0]);
        }
    }

    public void testOriginaDest() throws Exception {
        MapTransformFilter filter = new MapTransformFilter(null);
        ActiveMQMessage mqMessage = new ActiveMQMessage();
        mqMessage.setOriginalDestination(new ActiveMQQueue("O"));
        assertTrue(filter.transformToMap(mqMessage).containsKey(AmqMessagesUtil.JMS_MESSAGE_CUSTOM_PREFIX + "OriginalDestination"));
    }

}
