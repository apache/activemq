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
package org.apache.activemq.kaha.impl.container;

import java.util.Map;
import org.apache.activemq.kaha.MapContainer;

/**
 * Map.Entry implementation for a container
 * 
 * @version $Revision: 1.2 $
 */
class ContainerMapEntry implements Map.Entry {

    private MapContainer container;
    private Object key;

    ContainerMapEntry(MapContainer container, Object key) {
        this.container = container;
        this.key = key;

    }

    public Object getKey() {
        return key;
    }

    public Object getValue() {
        return container.get(key);
    }

    public Object setValue(Object value) {
        return container.put(key, value);
    }
}
