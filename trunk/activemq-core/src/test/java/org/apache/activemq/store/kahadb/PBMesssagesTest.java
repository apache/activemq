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
package org.apache.activemq.store.kahadb;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.activemq.protobuf.Buffer;
import org.apache.kahadb.journal.Location;
import org.apache.activemq.store.kahadb.data.KahaAddMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.apache.activemq.store.kahadb.data.KahaEntryType;
import org.apache.activemq.store.kahadb.data.KahaDestination.DestinationType;
import org.apache.kahadb.util.ByteSequence;
import org.apache.kahadb.util.DataByteArrayInputStream;
import org.apache.kahadb.util.DataByteArrayOutputStream;

public class PBMesssagesTest extends TestCase {

    public void testKahaAddMessageCommand() throws IOException {

       KahaAddMessageCommand expected = new KahaAddMessageCommand();
       expected.setDestination(new KahaDestination().setName("Foo").setType(DestinationType.QUEUE));
       expected.setMessage(new Buffer(new byte[] {1,2,3,4,5,6} ));
       expected.setMessageId("Hello World");
       
       int size = expected.serializedSizeFramed();
       DataByteArrayOutputStream os = new DataByteArrayOutputStream(size + 1);
       os.writeByte(expected.type().getNumber());
       expected.writeFramed(os);
       ByteSequence seq = os.toByteSequence();
       
       DataByteArrayInputStream is = new DataByteArrayInputStream(seq);
       KahaEntryType type = KahaEntryType.valueOf(is.readByte());
       JournalCommand message = (JournalCommand)type.createMessage();
       message.mergeFramed(is);
       
       assertEquals(expected, message);
    }
    
}
