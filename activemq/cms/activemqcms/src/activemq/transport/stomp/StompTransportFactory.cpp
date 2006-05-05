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

#include "StompTransportFactory.h"
#include "StompTransport.h"
#include <sstream>

using namespace activemq::transport;
using namespace activemq::transport::stomp;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
Transport* StompTransportFactory::createTransport( const char* brokerUrl ){
    
    string temp = brokerUrl;
    unsigned int ix = temp.find( ':' );
    if( ix == string::npos ){
        throw new ActiveMQException( "StompTransportFactory::createTransport - no port provided in url" );
    }
    
    brokerHost = temp.substr(0,ix);
    
    sscanf(brokerUrl+ix+1, "%d", &brokerPort );
    
    return new StompTransport( brokerHost.c_str(), brokerPort, "", "" );
}

////////////////////////////////////////////////////////////////////////////////        
Transport* StompTransportFactory::createTransport( const char* brokerUrl, 
    const char* userName,
    const char* password )
{
    this->userName = userName;
    this->password = password;
    
    parseUrl( brokerUrl );
    return new StompTransport( brokerHost.c_str(), 
        brokerPort, 
        userName, 
        password );
}

////////////////////////////////////////////////////////////////////////////////
void StompTransportFactory::parseUrl( const char* brokerUrl )
{   
    string url = brokerUrl;
    unsigned int portStartIx = url.rfind( ':' );
    if( portStartIx == string::npos || portStartIx==(url.length()-1) ){
        brokerPort = 61626;
    }
    else{
        
        try{
            stringstream stream( url.substr( portStartIx+1 ) );
            stream >> brokerPort;
        }catch( ... ){
            brokerPort = 61626;
            portStartIx = string::npos;
        }
    }
    
    brokerHost = url.substr( 0, portStartIx );    
}

