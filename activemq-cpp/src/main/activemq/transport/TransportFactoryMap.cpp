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

#include "TransportFactoryMap.h"

using namespace activemq::transport;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
TransportFactoryMap& TransportFactoryMap::getInstance(void)
{
    // Static instance of this Map, create here so that one will
    // always exist, the one and only Connector Map.      
    static TransportFactoryMap instance;
    
    return instance;
} 

////////////////////////////////////////////////////////////////////////////////
void TransportFactoryMap::registerTransportFactory( 
    const std::string& name, 
    TransportFactory* factory )
{
    factoryMap[name] = factory;
}

////////////////////////////////////////////////////////////////////////////////
void TransportFactoryMap::unregisterTransportFactory( const std::string& name ){
    factoryMap.erase( name );
}

////////////////////////////////////////////////////////////////////////////////
TransportFactory* TransportFactoryMap::lookup( const std::string& name )
{
    map<string, TransportFactory*>::const_iterator itr = 
    factoryMap.find(name);

    if( itr != factoryMap.end() )
    {
        return itr->second;
    }

    // Didn't find it, return nothing, not a single thing.
    return NULL;
}

////////////////////////////////////////////////////////////////////////////////
size_t TransportFactoryMap::getFactoryNames( 
    std::vector< std::string >& factoryList )
{    
    map<string, TransportFactory*>::const_iterator itr =
    factoryMap.begin();
  
    for(; itr != factoryMap.end(); ++itr)
    {
        factoryList.insert( factoryList.end(), itr->first );
    }
  
    return factoryMap.size();
}
