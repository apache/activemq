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
package org.apache.activemq.openwire.tool;

/**
 * A simple helper class to help auto-generate test data when code generating test cases
 * 
 * 
 */
public class TestDataGenerator {
    private int stringCounter;

    private boolean boolCounter;
    private byte byteCounter;
    private char charCounter = 'a';
    private short shortCounter;
    private int intCounter;
    private long longCounter;
    
    public String createByte() {
        return "(byte) " + (++byteCounter);
    }
    
    public String createChar() {
        return "'" + (charCounter++) + "'";
    }
    
    public String createShort() {
        return "(short) " + (++shortCounter);
    }

    public int createInt() {
        return ++intCounter;
    }

    public long createLong() {
        return ++longCounter;
    }

    public String createString(String property) {
        return property + ":" + (++stringCounter);
    }

    public boolean createBool() {
        boolCounter = !boolCounter;
        return boolCounter;
    }
    
    public String createByteArray(String property) {
        return "\"" + createString(property) + "\".getBytes()";
    }
}
