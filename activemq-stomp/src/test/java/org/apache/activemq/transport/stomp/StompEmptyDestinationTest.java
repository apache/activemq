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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StompEmptyDestinationTest extends  StompTestSupport{
    private static final Logger LOG = LoggerFactory.getLogger(StompEmptyDestinationTest.class);

    @Test(timeout = 60000)
    public void testEmptyDestinationOnSubscribe() throws Exception{
        stompConnect();
        stompConnection.sendFrame("CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL);
        StompFrame frame = stompConnection.receive();
        assertTrue(frame.toString().startsWith("CONNECTED"));
        String send = "SUBSCRIBE\r\n" +
                "id:1\r\n" +
                "destination:\r\n" +
                "receipt:1\r\n" +
                "\r\n"+
                "\u0000\r\n";

        stompConnection.sendFrame(send);
        StompFrame receipt = stompConnection.receive();
        LOG.info("Broker sent: " + receipt);
       assertTrue(receipt.getAction().startsWith("ERROR"));
       String errorMessage = receipt.getHeaders().get("message");
       assertEquals("Invalid empty or 'null' Destination header", errorMessage);
    }


}
