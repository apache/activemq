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

import junit.framework.TestCase;

public class DataByteArrayInputStreamTest extends TestCase {

    /**
     * https://issues.apache.org/activemq/browse/AMQ-1911
     */
    public void testNonAscii() throws Exception {
        doMarshallUnMarshallValidation("meißen");
        
        String accumulator = new String();
        
        int test = 0; // int to get Supplementary chars
        while(Character.isDefined(test)) {
            String toTest = String.valueOf((char)test);
            accumulator += toTest;
            doMarshallUnMarshallValidation(toTest);
            test++;
        }
        
        int massiveThreeByteCharValue = 0x0FFF;
        String toTest = String.valueOf((char)massiveThreeByteCharValue);
        accumulator += toTest;
        doMarshallUnMarshallValidation(String.valueOf((char)massiveThreeByteCharValue));
        
        // Altogether
        doMarshallUnMarshallValidation(accumulator);
        
        // the three byte values
        char t = '\u0800';
        final char max =  '\uffff';
        accumulator = String.valueOf(t);
        while (t < max) {
            String val = String.valueOf(t);
            accumulator += val;
            doMarshallUnMarshallValidation(val);
            t++;
        }
        
        // Altogether so long as it is not too big
        while (accumulator.length() > 20000) {
            accumulator = accumulator.substring(20000);
        }
        doMarshallUnMarshallValidation(accumulator);
    }
    
    void doMarshallUnMarshallValidation(String value) throws Exception {        
        DataByteArrayOutputStream out = new DataByteArrayOutputStream();
        out.writeBoolean(true);
        out.writeUTF(value);
        out.close();
        
        DataByteArrayInputStream in = new DataByteArrayInputStream(out.getData());
        in.readBoolean();
        String readBack = in.readUTF();
        assertEquals(value, readBack);
    }
}
