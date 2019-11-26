/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.command;

import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;

// https://issues.apache.org/jira/browse/AMQ-7353

public class VisibilityTest {

    // seems to reproduce easily with a direct referene to bytesSequence
    // a simpler message LocalMessage with less logic, reproduces ok.
    // however adding more logic to org.apache.activemq.command.VisibilityTest.LocalMessage.beforeMarshall will throw it off.
    // It could be down to cache lines, it is a brittle test - but it does demonstrate the problem in theory
    // an allocation in one thread may not be fully visible in another even after the init has complete!
    // I wanted to prove the need for the volatile to avoid the npe, doing the extra work when it is not visible is fine
    // but the NPE is a real problem when it happens.
    static ActiveMQBytesMessage bytesMessage;
    static ByteSequence byteSequence;
    static LocalMessage localMessage;

    static class LocalMessage {
        public HashMap<String, Object>  properties = new HashMap<>();
        public /* the fix */ volatile ByteSequence marshalledProperties;
        public int total;
        public void setBooleanProperty(String name, boolean v) {
            properties.put(name, v);
        }

        public void beforeMarshall(WireFormat ignored) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            // putting the following (real work) in and it won't reproduce
            //DataOutputStream os = new DataOutputStream(baos);
            //MarshallingSupport.marshalPrimitiveMap(properties, os);
            //os.close();
            total += properties.size();
            marshalledProperties = baos.toByteSequence();
        }

        public ByteSequence getMarshalledProperties() {
            return marshalledProperties;
        }
    }

    public static int checkNull() {
        ByteSequence local = byteSequence;
        if (local != null) {
            // if local is non null, the internal buffer may not be visible!
            return local.getData().length;
        }
        return 0;
    }

    public static int checkNullReference() {
        ActiveMQBytesMessage message = bytesMessage;
        if (message != null) {
            ByteSequence local = message.getMarshalledProperties();
            if (local != null) {
                // if local is non null, the internal buffer may not be visible!
                return local.getData().length;
            }
        }
        return 0;
    }

    public static int checkNullReferenceOnLocalMessage() {
        LocalMessage message = localMessage;
        if (message != null) {
            ByteSequence local = message.getMarshalledProperties();
            if (local != null) {
                // if local is non null, the internal buffer may not be visible!
                return local.getData().length;
            }
        }
        return 0;
    }

    @Ignore
    public void doTestNested() throws Exception {
        final AtomicBoolean gotError = new AtomicBoolean();
        final Thread tryingToMarshall = new Thread(new Runnable() {
            @Override
            public void run() {
                long total = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        total += checkNullReference();
                    } catch (Throwable t) {
                        t.printStackTrace();
                        gotError.set(true);
                    }
                }
                System.out.println("from other thread " + total);
            }
        });
        long len = 0;
        tryingToMarshall.start();
        for (int t = 0; t < 10; t++) {
            for (int i = 0; i < 1000_000; i++) {
                // real world
                ActiveMQBytesMessage message = new ActiveMQBytesMessage();
                // needs non null properties to init marshalledProperties
                message.setBooleanProperty("B", true);
                message.beforeMarshall(null);
                bytesMessage = message;
                // local access after publish
                len += message.getMarshalledProperties().getData().length;
            }
        }
        tryingToMarshall.interrupt();
        tryingToMarshall.join();
        System.out.println(len);
        assertFalse("no errors, no npe!", gotError.get());
    }


    @Test
    public void doTestNestedLocalMessage() throws Exception {
        final AtomicBoolean gotError = new AtomicBoolean();
        final Thread tryingToMarshall = new Thread(new Runnable() {
            @Override
            public void run() {
                long total = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        total += checkNullReferenceOnLocalMessage();
                    } catch (Throwable t) {
                        t.printStackTrace();
                        gotError.set(true);
                    }
                }
                System.out.println("from other thread " + total);
            }
        });
        long len = 0;
        tryingToMarshall.start();
        for (int t = 0; t < 10; t++) {
            for (int i = 0; i < 1000_000; i++) {
                // real world
                LocalMessage message = new LocalMessage();
                // needs non null properties to init marshalledProperties
                message.setBooleanProperty("B", true);
                message.beforeMarshall(null);
                localMessage = message;
                // local access after publish
                len += message.getMarshalledProperties().getData().length;
            }
        }
        tryingToMarshall.interrupt();
        tryingToMarshall.join();
        System.out.println(len);
        assertFalse("no errors, no npe!", gotError.get());
    }

    @Ignore
    public void doTestDirect() throws Exception {
        final AtomicBoolean gotError = new AtomicBoolean();
        final Thread tryingToMarshall = new Thread(new Runnable() {
            @Override
            public void run() {
                long total = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        total += checkNull();
                    } catch (Throwable t) {
                        t.printStackTrace();
                        gotError.set(true);
                    }
                }
                System.out.println("from other thread " + total);
            }
        });
        long len = 0;
        tryingToMarshall.start();
        for (int t = 0; t < 10; t++) {
            for (int i = 0; i < 1000_000; i++) {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                byteSequence = byteArrayOutputStream.toByteSequence();
                // local access after publish
                len += byteSequence.getData().length;
            }
        }
        tryingToMarshall.interrupt();
        tryingToMarshall.join();
        System.out.println(len);
        assertFalse("no errors, no npe!", gotError.get());
    }

}