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
package org.apache.activemq.openwire;


import jakarta.jms.InvalidClientIDException;
import jakarta.jms.JMSException;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.MaxFrameSizeExceededException;
import org.apache.activemq.command.WireFormatInfo;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class OpenWireUtilTest {

    @Test
    public void testValidateIsThrowable() {
        OpenWireUtil.validateIsThrowable(Exception.class);
        OpenWireUtil.validateIsThrowable(Throwable.class);
        OpenWireUtil.validateIsThrowable(JMSException.class);
        OpenWireUtil.validateIsThrowable(InvalidClientIDException.class);

        try {
            OpenWireUtil.validateIsThrowable(String.class);
            fail("Not a valid Throwable");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            OpenWireUtil.validateIsThrowable(ActiveMQConnection.class);
            fail("Not a valid Throwable");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testConvertJmsPackage() {
        // should not change
        assertEquals(InvalidClientIDException.class.getName(),
            OpenWireUtil.convertJmsPackage(InvalidClientIDException.class.getName()));

        // should convert to correct exception type
        assertEquals(InvalidClientIDException.class.getName(),
            OpenWireUtil.convertJmsPackage(OpenWireUtil.jmsPackageToReplace + ".InvalidClientIDException"));
    }

    @Test
    public void testValidateBufferSize() throws IOException {
        OpenWireFormatFactory factory = new OpenWireFormatFactory();

        var wireFormat = (OpenWireFormat) factory.createWireFormat();

        // Nothing set, no validation
        OpenWireUtil.validateBufferSize(wireFormat, 2048);

        // verify max frame check works
        try {
            wireFormat.setMaxFrameSize(1024);
            OpenWireUtil.validateBufferSize(wireFormat, 2048);
            fail("should have failed");
        } catch (MaxFrameSizeExceededException e) {
            // expected
        }

        // rest max frame size back so we can test validating current size
        // is less than expected buffer size
        wireFormat.setMaxFrameSize(OpenWireFormat.DEFAULT_MAX_FRAME_SIZE);
        WireFormatInfo wfi = new WireFormatInfo();
        wfi.setProperty("test", "test");

        // should be no error for the first 2 calls, last call should
        // go over frame size and error
        initContext(wireFormat, 2048);
        OpenWireUtil.validateBufferSize(wireFormat, 1024);
        OpenWireUtil.validateBufferSize(wireFormat, 1024);
        try {
            OpenWireUtil.validateBufferSize(wireFormat, 1);
            fail("should have failed");
        } catch (IOException e) {
            // expected
        }
    }

    @SuppressWarnings("unchecked")
    private void initContext(OpenWireFormat format, int frameSize) throws IOException {
        try {
            Field mcThreadLocalField = OpenWireFormat.class.getDeclaredField("marshallingContext");
            mcThreadLocalField.setAccessible(true);
            var mcThreadLocal = (ThreadLocal<OpenWireFormat.MarshallingContext>) mcThreadLocalField.get(format);
            var context = new OpenWireFormat.MarshallingContext();
            context.setFrameSize(frameSize);
            mcThreadLocal.set(context);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

}
