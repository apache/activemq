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

import org.apache.activemq.command.*;
import org.apache.activemq.openwire.OpenWireFormat;

import java.io.*;

/**
 * @version $Revision: 1.1 $
 */
public class OpenWirePerformanceTest extends TestSupport {

    protected String fileName = "target/openwire.openwire";
    protected OpenWireFormat openWireFormat = createOpenWireFormat();
    protected ActiveMQDestination destination = new ActiveMQQueue("FOO.BAR");
    protected ProducerId producerId = new ProducerId(new SessionId(new ConnectionId("abc"), 1), 1);

    public void testPerformance() throws Exception {
        OutputStream out = new BufferedOutputStream(new FileOutputStream(fileName));
        DataOutputStream ds = new DataOutputStream(out);

        StopWatch watch = createStopWatch("writer");
        for (long i = 0; i < messageCount; i++) {
            watch.start();
            Message message = new ActiveMQMessage();

            message.setDestination(destination);
            message.setPersistent(true);
            message.setCorrelationId("ABCD");
            //message.setType("type:" + i);

            if (useProducerId) {
                message.setProducerId(producerId);
                message.setMessageId(new MessageId(producerId, i));
            }

            if (verbose) {
                System.out.println("Writing message: " + i + " = " + message);
            }
/*
            byte[] bytes = message.toByteArray();
            int size = bytes.length;
            out.write(size);
            //System.out.println("writing bytes: " + size);
            out.write(bytes);
*/

            openWireFormat.marshal(message, ds);
            watch.stop();
        }
        out.close();

        // now lets try read them!
        StopWatch watch2 = createStopWatch("reader");
        InputStream in = new BufferedInputStream(new FileInputStream(fileName));
        DataInput dis = new DataInputStream(in);

        for (long i = 0; i < messageCount; i++) {
            watch2.start();

            Object message = openWireFormat.unmarshal(dis);
/*
            int size = in.read();
            byte[] data = new byte[size];
            in.read(data);
*/
            if (verbose) {
                System.out.println("Reading message: " + i + " = " + message);
            }
            watch2.stop();
        }
        in.close();
    }

    protected OpenWireFormat createOpenWireFormat() {
        OpenWireFormat wf = new OpenWireFormat();
        wf.setCacheEnabled(true);
        wf.setStackTraceEnabled(false);
        wf.setVersion(OpenWireFormat.DEFAULT_VERSION);
        return wf;
    }

}