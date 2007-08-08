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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.activemq.command.DataStructure;

public interface DataStreamMarshaller {

    byte getDataStructureType();
    DataStructure createObject();

    int tightMarshal1(OpenWireFormat format, Object c, BooleanStream bs) throws IOException;
    void tightMarshal2(OpenWireFormat format, Object c, DataOutput ds, BooleanStream bs) throws IOException;
    void tightUnmarshal(OpenWireFormat format, Object data, DataInput dis, BooleanStream bs) throws IOException;

    void looseMarshal(OpenWireFormat format, Object c, DataOutput ds) throws IOException;
    void looseUnmarshal(OpenWireFormat format, Object data, DataInput dis) throws IOException;
    
}
