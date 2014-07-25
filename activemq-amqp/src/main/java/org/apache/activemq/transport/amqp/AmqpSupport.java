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
package org.apache.activemq.transport.amqp;

import java.nio.ByteBuffer;

import org.fusesource.hawtbuf.Buffer;

/**
 */
public class AmqpSupport {

    static public Buffer toBuffer(ByteBuffer data) {
        if (data == null) {
            return null;
        }
        Buffer rc;
        if (data.isDirect()) {
            rc = new Buffer(data.remaining());
            data.get(rc.data);
        } else {
            rc = new Buffer(data);
            data.position(data.position() + data.remaining());
        }
        return rc;
    }
}
