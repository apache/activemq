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
#include "ActiveMQConnectionFactory.h"

#include <activemq/util/Guid.h>
#include <activemq/util/SimpleProperties.h>
#include <activemq/util/StringTokenizer.h>
#include <activemq/connector/ConnectorFactoryMap.h>
#include <activemq/network/SocketFactory.h>
#include <activemq/transport/TransportFactoryMap.h>
#include <activemq/network/Socket.h>
#include <activemq/exceptions/NullPointerException.h>
#include <activemq/core/ActiveMQConnection.h>
#include <activemq/util/StringTokenizer.h>
#include <activemq/support/LibraryInit.h>

using namespace std;
using namespace activemq;
using namespace activemq::core;
using namespace activemq::util;
using namespace activemq::connector;
using namespace activemq::exceptions;
using namespace activemq::network;
using namespace activemq::transport;

////////////////////////////////////////////////////////////////////////////////
ActiveMQConnectionFactory::ActiveMQConnectionFactory(void)
{
    brokerURL = "tcp://localhost:61616";
    
    this->username = "";
    this->password = "";
    this->clientId = "";
}

////////////////////////////////////////////////////////////////////////////////
ActiveMQConnectionFactory::ActiveMQConnectionFactory(const std::string& url,
                                                     const std::string& username,
                                                     const std::string& password,
                                                     const std::string& clientId)
{
    brokerURL = url;

    this->username = username;
    this->password = password;
    this->clientId = clientId;
}

////////////////////////////////////////////////////////////////////////////////
cms::Connection* ActiveMQConnectionFactory::createConnection(void) 
throw ( cms::CMSException )
{
    return createConnection(username, password);
}

////////////////////////////////////////////////////////////////////////////////
cms::Connection* ActiveMQConnectionFactory::createConnection(
    const std::string& username,
    const std::string& password,
    const std::string& clientId ) 
       throw ( cms::CMSException )
{
    // Declared here so that they can be deleted in the catch block
    SimpleProperties* properties = NULL;
    Transport* transport = NULL;
    Connector* connector = NULL;
    ActiveMQConnectionData* connectionData = NULL;
    ActiveMQConnection* connection = NULL;
    
    this->username = username;
    this->password = password;
    this->clientId = clientId;

    try
    {
        properties = new SimpleProperties;

        // if no Client Id specified, create one
        if( this->clientId == "" )
        {
            this->clientId = Guid::createGUIDString();
        }

        // Store login data in the properties
        properties->setProperty( "username", this->username );
        properties->setProperty( "password", this->password );
        properties->setProperty( "clientId", this->clientId );

        // Parse out the properties from the URI
        parseURL( brokerURL, *properties );

        // Create the Transport that the Connector will use.
        string factoryName = 
            properties->getProperty( "transport", "tcp" );
        TransportFactory* factory = 
            TransportFactoryMap::getInstance().lookup( factoryName );
        if( factory == NULL ){
            throw ActiveMQException( 
                __FILE__, __LINE__, 
                "ActiveMQConnectionFactory::createConnection - "
                "unknown transport factory");
        }
        
        // Create the transport.
        transport = factory->createTransport( *properties );
        if( transport == NULL ){
            throw ActiveMQException( 
                __FILE__, __LINE__, 
                "ActiveMQConnectionFactory::createConnection - "
                "failed creating new Transport");
        }

        // What wire format are we using, defaults to Stomp
        std::string wireFormat = 
            properties->getProperty( "wireFormat", "stomp" );

        // Now try and find a factory to create the Connector
        ConnectorFactory* connectorfactory = 
            ConnectorFactoryMap::getInstance()->lookup( wireFormat );

        if( connectorfactory == NULL )
        {
            throw NullPointerException(
                __FILE__, __LINE__,
                "ActiveMQConnectionFactory::createConnection - "
                "Connector for Wire Format not registered in Map");
        }

        // Create the Connector.
        connector = connectorfactory->createConnector( *properties, transport );

        if(connector == NULL)
        {
            throw NullPointerException(
                __FILE__, __LINE__,
                "ActiveMQConnectionFactory::createConnection - "
                "Failed to Create the Connector");
        }
        
        // Start the Connector
        connector->start();

        // Create Holder and store the data for the Connection
        connectionData = new ActiveMQConnectionData(
            connector, transport, properties );

        // Create and Return the new connection object.
        connection = new ActiveMQConnection( connectionData );
        
        return connection;
    }
    catch( exceptions::ActiveMQException& ex )
    {
        ex.setMark( __FILE__, __LINE__ );

        delete connection;
        delete connector;
        delete transport;
        delete properties;
        
        throw ex;
    }
    catch( ... )
    {
        exceptions::ActiveMQException ex( 
           __FILE__, __LINE__, 
           "ActiveMQConnectionFactory::create - "
           "caught unknown exception" );

        delete connection;
        delete connector;
        delete transport;
        delete properties;

        throw ex;
    }
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQConnectionFactory::parseURL(const std::string& URI, 
                                         Properties& properties)
    throw ( exceptions::IllegalArgumentException )
{
    try
    {
        StringTokenizer tokenizer(URI, ":/");
    
        std::vector<std::string> tokens;
    
        // Require that there be three tokens at the least, these are
        // transport, url, port.
        if(tokenizer.countTokens() < 3)
        {
            throw exceptions::IllegalArgumentException(
                __FILE__, __LINE__,
                (string("ActiveMQConnectionFactory::parseURL - "
                        "Marlformed URI: ") + URI).c_str());
        }
    
        // First element should be the Transport Type, following that is the
        // URL and any params.  
        properties.setProperty( "transport", tokenizer.nextToken() );

        // Parse URL and Port as one item, optional params follow the ?
        // and then each param set is delimited with & we extract first
        // three chars as they are the left over ://
        properties.setProperty( "uri", tokenizer.nextToken("&?").substr(3) );
    
        // Now get all the optional parameters and store them as properties
        int count = tokenizer.toArray(tokens);
        
        for(int i = 0; i < count; ++i)
        {
            tokenizer.reset(tokens[i], "=");
    
            if(tokenizer.countTokens() != 2)
            {
                throw exceptions::IllegalArgumentException(
                    __FILE__, __LINE__,
                    (string("ActiveMQConnectionFactory::parseURL - "
                           "Marlformed Parameter = ") + tokens[i]).c_str());
            }
    
            // Store this param as a property
            properties.setProperty(tokenizer.nextToken(), tokenizer.nextToken());
        }
    }
    AMQ_CATCH_RETHROW( IllegalArgumentException )
    AMQ_CATCH_EXCEPTION_CONVERT( ActiveMQException, IllegalArgumentException )
    AMQ_CATCHALL_THROW( IllegalArgumentException )
}
