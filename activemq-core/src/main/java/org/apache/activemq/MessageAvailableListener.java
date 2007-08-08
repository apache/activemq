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
package org.apache.activemq;

import javax.jms.MessageConsumer;

/**
 * A listener which is notified if a message is available for processing via the
 * receive methods. Typically on receiving this notification you can call 
 * {@link MessageConsumer#receiveNoWait()} to get the new message immediately.
 * 
 * Note that this notification just indicates a message is available for synchronous consumption,
 * it does not actually consume the message.
 * 
 * @version $Revision: 1.1 $
 */
public interface MessageAvailableListener {

    void onMessageAvailable(MessageConsumer consumer);

}
