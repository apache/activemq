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
* A holder for different thread priorites used in ActiveMQ
* 
* 
*/

public interface ThreadPriorities {
    int INBOUND_BROKER_CONNECTION = 6;
    int OUT_BOUND_BROKER_DISPATCH = 6;
    int INBOUND_CLIENT_CONNECTION = 7;
    int INBOUND_CLIENT_SESSION = 7;
    int BROKER_MANAGEMENT = 9;
}
