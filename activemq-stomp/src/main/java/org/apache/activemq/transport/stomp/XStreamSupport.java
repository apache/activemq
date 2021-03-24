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

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.security.AnyTypePermission;
import com.thoughtworks.xstream.security.NoTypePermission;
import com.thoughtworks.xstream.security.PrimitiveTypePermission;
import org.apache.activemq.util.ClassLoadingAwareObjectInputStream;

import java.util.Collection;
import java.util.Map;

public class XStreamSupport {

    public static XStream createXStream() {
        XStream stream = new XStream();
        stream.addPermission(NoTypePermission.NONE);
        stream.addPermission(PrimitiveTypePermission.PRIMITIVES);
        stream.allowTypeHierarchy(Collection.class);
        stream.allowTypeHierarchy(Map.class);
        stream.allowTypes(new Class[]{String.class});
        if (ClassLoadingAwareObjectInputStream.isAllAllowed()) {
            stream.addPermission(AnyTypePermission.ANY);
        } else {
            for (String packageName : ClassLoadingAwareObjectInputStream.serializablePackages) {
                stream.allowTypesByWildcard(new String[]{packageName + ".**"});
            }
        }
        return stream;
    }

}
