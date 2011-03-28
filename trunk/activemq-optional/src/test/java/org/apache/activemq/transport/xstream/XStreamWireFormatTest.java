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
package org.apache.activemq.transport.xstream;

import java.io.IOException;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.MessageTest;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class XStreamWireFormatTest extends MessageTest {
    private static final Logger LOG = LoggerFactory.getLogger(XStreamWireFormatTest.class);

    public void assertBeanMarshalls(Object original) throws IOException {
        super.assertBeanMarshalls(original);

        String xml = getXStreamWireFormat().marshalText((Command) original);
        LOG.info(original.getClass().getName() + " as XML is:");
        LOG.info(xml);
    }

    protected XStreamWireFormat getXStreamWireFormat() {
        return (XStreamWireFormat) wireFormat;
    }

    protected WireFormat createWireFormat() {
        return new XStreamWireFormat();
    }
}
