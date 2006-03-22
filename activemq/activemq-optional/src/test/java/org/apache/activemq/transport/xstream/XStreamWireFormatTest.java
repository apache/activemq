/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.xstream;

import org.activeio.command.WireFormat;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.MessageTest;
import org.apache.activemq.transport.xstream.XStreamWireFormat;

import java.io.IOException;

/**
 * @version $Revision$
 */
public class XStreamWireFormatTest extends MessageTest {

    public void assertBeanMarshalls(Object original) throws IOException {
        super.assertBeanMarshalls(original);

        String xml = getXStreamWireFormat().toString((Command) original);
        System.out.println(original.getClass().getName() + " as XML is:");
        System.out.println(xml);
    }

    protected XStreamWireFormat getXStreamWireFormat() {
        return (XStreamWireFormat) wireFormat;
    }

    protected WireFormat createWireFormat() {
        return new XStreamWireFormat();
    }
}
