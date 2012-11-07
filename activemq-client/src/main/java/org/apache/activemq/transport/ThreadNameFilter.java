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

package org.apache.activemq.transport;

import java.io.IOException;

/**
 * The thread name filter, modifies the name of the thread during the invocation to a transport.
 * It appends the remote address, so that a call stuck in a transport method such as socketWrite0
 * will have the destination information in the thread name.
 * This is extremely useful for thread dumps when debugging.
 * To enable this transport, in the transport URI, simpley add<br/>
 * <code>transport.threadName</code>.<br/>
 * For example:</br>
 * <pre><code>
 * &lt;transportConnector 
 *     name=&quot;tcp1&quot; 
 *     uri=&quot;tcp://127.0.0.1:61616?transport.soTimeout=10000&amp;transport.threadName"
 * /&gt;
 * </code></pre>
 * @author Filip Hanik
 *
 */
public class ThreadNameFilter extends TransportFilter {

    public ThreadNameFilter(Transport next) {
        super(next);
    }

    @Override
    public void oneway(Object command) throws IOException {
        String address =(next!=null?next.getRemoteAddress():null); 
        if (address!=null) {
            String name = Thread.currentThread().getName();
            try {
                String sendname = name + " - SendTo:"+address;
                Thread.currentThread().setName(sendname);
                super.oneway(command);
            }finally {
                Thread.currentThread().setName(name);
            }
        } else {
            super.oneway(command);
        }
    }
    
    

}
