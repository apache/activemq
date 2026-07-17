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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.SharedConsumerInfo;
import org.apache.activemq.openwire.BooleanStream;
import org.apache.activemq.openwire.OpenWireFormat;

/**
 * OpenWire v13 marshaller for {@link ConsumerInfo}.
 *
 * <p>Extends the v12 marshaller with two additional boolean fields:
 * {@code shared} and {@code durable}. Creates {@link SharedConsumerInfo}
 * instances on unmarshal so the broker can detect shared consumers via
 * {@code instanceof}.
 */
public class ConsumerInfoMarshaller extends org.apache.activemq.openwire.v12.ConsumerInfoMarshaller {

    @Override
    public DataStructure createObject() {
        return new SharedConsumerInfo();
    }

    @Override
    public void tightUnmarshal(OpenWireFormat wireFormat, Object o, DataInput dataIn, BooleanStream bs) throws IOException {
        super.tightUnmarshal(wireFormat, o, dataIn, bs);

        SharedConsumerInfo info = (SharedConsumerInfo) o;
        info.setShared(bs.readBoolean());
        info.setDurable(bs.readBoolean());
    }

    @Override
    public int tightMarshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {
        int rc = super.tightMarshal1(wireFormat, o, bs);

        if (o instanceof SharedConsumerInfo) {
            SharedConsumerInfo info = (SharedConsumerInfo) o;
            bs.writeBoolean(info.isShared());
            bs.writeBoolean(info.isDurable());
        } else {
            bs.writeBoolean(false);
            bs.writeBoolean(false);
        }

        return rc + 0;
    }

    @Override
    public void tightMarshal2(OpenWireFormat wireFormat, Object o, DataOutput dataOut, BooleanStream bs) throws IOException {
        super.tightMarshal2(wireFormat, o, dataOut, bs);

        bs.readBoolean();
        bs.readBoolean();
    }

    @Override
    public void looseUnmarshal(OpenWireFormat wireFormat, Object o, DataInput dataIn) throws IOException {
        super.looseUnmarshal(wireFormat, o, dataIn);

        SharedConsumerInfo info = (SharedConsumerInfo) o;
        info.setShared(dataIn.readBoolean());
        info.setDurable(dataIn.readBoolean());
    }

    @Override
    public void looseMarshal(OpenWireFormat wireFormat, Object o, DataOutput dataOut) throws IOException {
        super.looseMarshal(wireFormat, o, dataOut);

        if (o instanceof SharedConsumerInfo) {
            SharedConsumerInfo info = (SharedConsumerInfo) o;
            dataOut.writeBoolean(info.isShared());
            dataOut.writeBoolean(info.isDurable());
        } else {
            dataOut.writeBoolean(false);
            dataOut.writeBoolean(false);
        }
    }
}
