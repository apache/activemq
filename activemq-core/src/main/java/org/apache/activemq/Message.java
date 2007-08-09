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

/**
 * Represents the JMS extension methods in Apache ActiveMQ
 *
 * @version $Revision: $
 */
public interface Message extends javax.jms.Message {

    /**
     * Returns the MIME type of this mesage. This can be used in selectors to filter on
     * the MIME types of the different JMS messages, or in the case of {@link org.apache.activemq.BlobMessage}
     * it allows you to create a selector on the MIME type of the BLOB body
     */
    String getJMSXMimeType();

}
