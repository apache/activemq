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
package org.apache.activemq.transport.stomp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.junit.Test;

public class StompFrameTest {
    StompFrame underTest = new StompFrame();

    @Test
    public void testNoPasscodeInToString() throws Exception {
        HashMap<String, String> headers = new HashMap<String, String>();
        headers.put("userName", "bob");
        headers.put("passcode", "please");
        underTest.setHeaders(headers);

        assertEquals("no password present", -1, underTest.toString().indexOf("please"));
        assertTrue("*** present", underTest.toString().indexOf("***") > 0);
    }
}
