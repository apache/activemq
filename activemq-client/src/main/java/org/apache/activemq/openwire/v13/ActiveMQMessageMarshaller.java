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
package org.apache.activemq.openwire.v13;

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.DataStructure;

/**
 * OpenWire v13 marshaller for {@link ActiveMQMessage}.
 */
public class ActiveMQMessageMarshaller extends MessageMarshaller {

    @Override
    public byte getDataStructureType() {
        return ActiveMQMessage.DATA_STRUCTURE_TYPE;
    }

    @Override
    public DataStructure createObject() {
        return new ActiveMQMessage();
    }
}
