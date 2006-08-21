/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ACTIVEMQ_CONNECTOR_STOMP_STOMPSELECTOR_H_
#define ACTIVEMQ_CONNECTOR_STOMP_STOMPSELECTOR_H_

#include <cms/Message.h>
#include <string>

namespace activemq{
namespace connector{
namespace stomp{
    
    /**
     * Since the stomp protocol doesn't have a consumer-based selector
     * mechanism, we have to do the selector logic on the client
     * side.  This class provides the selector algorithm that is
     * needed to determine if a given message is to be selected for
     * a given consumer's selector string.
     */
    class StompSelector{
    public:
    
        static bool isSelected( const std::string& selector,
            cms::Message* msg )
        {
            return true;
        }
        
    };
    
}}}

#endif /*ACTIVEMQ_CONNECTOR_STOMP_STOMPSELECTOR_H_*/
