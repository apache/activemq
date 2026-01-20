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

import java.io.IOException;
import org.apache.activemq.util.IOExceptionSupport;

public class OpenWireUtil {

    static final String jmsPackageToReplace = "javax.jms";
    static final String jmsPackageToUse = "jakarta.jms";

    /**
     * Verify that the provided class extends {@link Throwable} and throw an
     * {@link IllegalArgumentException} if it does not.
     *
     * @param clazz
     */
    public static void validateIsThrowable(Class<?> clazz) {
        if (!Throwable.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException("Class " + clazz + " is not assignable to Throwable");
        }
    }

    /**
     * Verify that the buffer size that will be allocated will not push the total allocated
     * size of this frame above the expected frame size. This is an estimate as the current
     * size is only tracked when calls to this method are made and is primarily intended
     * to prevent large arrays from being created due to an invalid size.
     *
     * Also verify the size against configured max frame size.
     * This check is a sanity check in case of corrupt packets contain invalid size values.
     *
     * @param wireFormat configured OpenWireFormat
     * @param size buffer size to verify
     * @throws IOException If size is larger than currentFrameSize or maxFrameSize
     */
    public static void validateBufferSize(OpenWireFormat wireFormat, int size) throws IOException {
        validateLessThanFrameSize(wireFormat, size);

        // if currentFrameSize is set and was checked above then this check should not be needed,
        // but it doesn't hurt to verify again in case the max frame size check was missed
        // somehow
        if (wireFormat.isMaxFrameSizeEnabled() && size > wireFormat.getMaxFrameSize()) {
            throw IOExceptionSupport.createFrameSizeException(size,  wireFormat.getMaxFrameSize());
        }
    }

    // Verify total tracked sizes will not exceed the overall size of the frame
    private static void validateLessThanFrameSize(OpenWireFormat wireFormat, int size)
        throws IOException {
        final var context = wireFormat.getMarshallingContext();
        // No information on current frame size so just return
        if (context == null || context.getFrameSize() < 0) {
            return;
        }

        // Increment existing estimated buffer size with new size
        context.increment(size);

        // We should never be trying to allocate a buffer that is going to push the total
        // size greater than the entire frame itself
        if (context.getEstimatedAllocated() > context.getFrameSize()) {
            throw IOExceptionSupport.createFrameSizeBufferException(
                context.getEstimatedAllocated(), context.getFrameSize());
        }
    }

    /**
     * This method can be used to convert from javax -> jakarta or
     * vice versa depending on the version used by the client
     *
     * @param className
     * @return
     */
    public static String convertJmsPackage(String className) {
        if (className != null && className.startsWith(jmsPackageToReplace)) {
            return className.replace(jmsPackageToReplace, jmsPackageToUse);
        }
        return className;
    }

}
