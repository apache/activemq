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

import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.SharedSubscriptionInfo;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.openwire.BooleanStream;
import org.apache.activemq.openwire.OpenWireFormat;

/**
 * OpenWire v13 marshaller for {@link SubscriptionInfo}.
 *
 * <p>Extends the v12 marshaller with one additional boolean field:
 * {@code shared}. Creates {@link SharedSubscriptionInfo} instances on
 * unmarshal so recovery code can detect shared subscriptions via
 * {@code instanceof}.
 */
public class SubscriptionInfoMarshaller extends org.apache.activemq.openwire.v12.SubscriptionInfoMarshaller {

    @Override
    public DataStructure createObject() {
        return new SharedSubscriptionInfo();
    }

    @Override
    public void tightUnmarshal(OpenWireFormat wireFormat, Object o, DataInput dataIn, BooleanStream bs) throws IOException {
        super.tightUnmarshal(wireFormat, o, dataIn, bs);

        SharedSubscriptionInfo info = (SharedSubscriptionInfo) o;
        info.setShared(bs.readBoolean());
    }

    @Override
    public int tightMarshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {
        int rc = super.tightMarshal1(wireFormat, o, bs);

        if (o instanceof SharedSubscriptionInfo) {
            SharedSubscriptionInfo info = (SharedSubscriptionInfo) o;
            bs.writeBoolean(info.isShared());
        } else {
            bs.writeBoolean(false);
        }

        return rc + 0;
    }

    @Override
    public void tightMarshal2(OpenWireFormat wireFormat, Object o, DataOutput dataOut, BooleanStream bs) throws IOException {
        super.tightMarshal2(wireFormat, o, dataOut, bs);

        bs.readBoolean();
    }

    @Override
    public void looseUnmarshal(OpenWireFormat wireFormat, Object o, DataInput dataIn) throws IOException {
        super.looseUnmarshal(wireFormat, o, dataIn);

        SharedSubscriptionInfo info = (SharedSubscriptionInfo) o;
        info.setShared(dataIn.readBoolean());
    }

    @Override
    public void looseMarshal(OpenWireFormat wireFormat, Object o, DataOutput dataOut) throws IOException {
        super.looseMarshal(wireFormat, o, dataOut);

        if (o instanceof SharedSubscriptionInfo) {
            SharedSubscriptionInfo info = (SharedSubscriptionInfo) o;
            dataOut.writeBoolean(info.isShared());
        } else {
            dataOut.writeBoolean(false);
        }
    }
}
