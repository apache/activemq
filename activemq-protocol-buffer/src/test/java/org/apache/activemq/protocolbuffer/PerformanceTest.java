/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.protocolbuffer;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import junit.framework.TestCase;

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * @version $Revision: 1.1 $
 */
public class PerformanceTest extends TestCase {
    protected int messageCount = 10;
    protected String fileName = "target/messages.openwire";


    public void testPerformance() throws Exception {
        FileOutputStream out = new FileOutputStream(fileName);
        CodedOutputStream cout = CodedOutputStream.newInstance(out);
        OpenWire.Destination destination = OpenWire.Destination.newBuilder().setName("FOO.BAR").setType(OpenWire.Destination.DestinationType.QUEUE).build();

        for (int i = 0; i < messageCount; i++) {
            OpenWire.Message message = OpenWire.Message.newBuilder()
                    .setDestination(destination)
                    .setPersistent(true)
                    .setProducerId(1234)
                    .setProducerCounter(i)
                    .build();
            //.setType("type:" + i)

            System.out.println("Writing message: " + i + " = " + message);
            int size = message.getSerializedSize();
            cout.writeRawVarint32(size);
            message.writeTo(cout);
            cout.flush();
        }
        out.close();

        // now lets try read them!
        FileInputStream in = new FileInputStream(fileName);
        CodedInputStream cin = CodedInputStream.newInstance(in);
        for (int i = 0; i < messageCount; i++) {
            int size = cin.readRawVarint32();
            int previous = cin.pushLimit(size);
            OpenWire.Message message = OpenWire.Message.parseFrom(cin);
            cin.popLimit(previous);
            System.out.println("Reading message: " + i + " = " + message);
        }
        in.close();
    }

}
