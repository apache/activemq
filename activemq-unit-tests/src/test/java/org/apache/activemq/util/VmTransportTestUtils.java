/*
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

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentMap;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.vm.VMTransportFactory;

public class VmTransportTestUtils {

    public static void resetVmTransportFactory() throws Exception {
        resetVmTransportFactory(VMTransportFactory.DEFAULT_ALLOWED_SCHEMES);
    }

    @SuppressWarnings("unchecked")
    public static void resetVmTransportFactory(String allowedSchemes) throws Exception {
        if (allowedSchemes == null) {
            System.clearProperty(VMTransportFactory.VM_TRANSPORT_FACTORY_SCHEMES_ENABLED_PROP);
        } else {
            // set property to allowed schemes
            System.setProperty(VMTransportFactory.VM_TRANSPORT_FACTORY_SCHEMES_ENABLED_PROP,
                    allowedSchemes);
        }

        // clear any cached factory so the next call will create a new transport and use
        // the correct property setting
        Field factoriesField = TransportFactory.class.getDeclaredField("TRANSPORT_FACTORYS");
        factoriesField.setAccessible(true);
        ConcurrentMap<String, TransportFactory> factories =
                (ConcurrentMap<String, TransportFactory>) factoriesField.get(TransportFactory.class);
        factories.remove("vm");
    }
}
