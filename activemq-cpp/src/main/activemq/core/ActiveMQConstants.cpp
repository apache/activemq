/*
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
#include "ActiveMQConstants.h"
#include <stdio.h>

using namespace std;
using namespace activemq;
using namespace activemq::core;

////////////////////////////////////////////////////////////////////////////////
string ActiveMQConstants::StaticInitializer::destOptions[NUM_OPTIONS];
string ActiveMQConstants::StaticInitializer::uriParams[NUM_PARAMS];

map< std::string, ActiveMQConstants::DestinationOption > 
    ActiveMQConstants::StaticInitializer::destOptionMap;
map< std::string, ActiveMQConstants::URIParam > 
    ActiveMQConstants::StaticInitializer::uriParamsMap;

ActiveMQConstants::StaticInitializer ActiveMQConstants::staticInits;

////////////////////////////////////////////////////////////////////////////////
ActiveMQConstants::StaticInitializer::StaticInitializer(){
    
    destOptions[CONSUMER_PREFECTCHSIZE] = "consumer.prefetchSize";
    destOptions[CUNSUMER_MAXPENDINGMSGLIMIT] = "consumer.maximumPendingMessageLimit";
    destOptions[CONSUMER_NOLOCAL] = "consumer.noLocal";
    destOptions[CONSUMER_DISPATCHASYNC] = "consumer.dispatchAsync";
    destOptions[CONSUMER_RETROACTIVE] = "consumer.retroactive";
    destOptions[CONSUMER_SELECTOR] = "consumer.selector";
    destOptions[CONSUMER_EXCLUSIVE] = "consumer.exclusive";
    destOptions[CONSUMER_PRIORITY] = "consumer.priority";
    
    uriParams[PARAM_USERNAME] = "username";
    uriParams[PARAM_PASSWORD] = "password";
    uriParams[PARAM_CLIENTID] = "client-id";    

    for( int ix=0; ix<NUM_OPTIONS; ++ix ){
        destOptionMap[destOptions[ix]] = (DestinationOption)ix;
    }
    for( int ix=0; ix<NUM_PARAMS; ++ix ){
        uriParamsMap[uriParams[ix]] = (URIParam)ix;
    }
}
