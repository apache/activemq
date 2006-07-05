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
#ifndef _ACTIVEMQ_CORE_ACTIVEMQCONNECTIONDATA_H_
#define _ACTIVEMQ_CORE_ACTIVEMQCONNECTIONDATA_H_

#include <activemq/connector/Connector.h>
#include <activemq/transport/Transport.h>
#include <activemq/exceptions/IllegalArgumentException.h>
#include <activemq/util/Properties.h>

namespace activemq{
namespace core{

    /**
     * Container of the Data that is needed when creating a new connection
     * object.  Each ActiveMQConnection owns one of these objects.  This
     * object knows how to clean up the Connection Dependencies correctly
     */
    class ActiveMQConnectionData
    {
    private:
    
        // Connector Object
        connector::Connector* connector;
                
        // Transport we are using
        transport::Transport* transport;
        
        // Properties used to configure this connection.
        util::Properties* properties;
        
    public:

        /**
         * Creates a new Connection Data object, passing it the data that
         * it will manage for the parent connection object.
         * @param A connector instance
         * @param A Socket instance
         * @param A Transport instance
         * @throw IllegalArgumentException if an element is NULL
         */
        ActiveMQConnectionData( connector::Connector* connector,
                                transport::Transport* transport,
                                util::Properties* properties )
        {
            if( connector  == NULL || 
                transport  == NULL || 
                properties == NULL )
            {
                throw exceptions::IllegalArgumentException(
                    __FILE__, __LINE__,
                    "ActiveMQConnectionData::ActiveMQConnectionData - "
                    "Required Parameter was NULL.");
            }

            this->connector = connector;
            this->transport = transport;
            this->properties = properties;
        }
        
        virtual ~ActiveMQConnectionData(void) 
        {
            try
            {
                connector->close();
                delete connector;
                
                transport->close();
                delete transport;
                
                delete properties;
            }
            AMQ_CATCH_NOTHROW( exceptions::ActiveMQException )
            AMQ_CATCHALL_NOTHROW( )
        }

        /**
         * Get the Connector that this Connection Data object holds
         * @return Connector Pointer
         */
        virtual connector::Connector* getConnector(void){
            return connector;
        }

        /**
         * Get the Connector that this Connection Data object holds
         * @return Connector Pointer
         */
        virtual transport::Transport* getTransport(void){
            return transport;
        }

        /**
         * Gets a reference to the properties that were used to configure
         * this Connection.
         * @return Properties object reference.
         */
        virtual const util::Properties& getProperties(void) const {
            return *properties;
        }
        
    };

}}

#endif /*_ACTIVEMQ_CORE_ACTIVEMQCONNECTIONDATA_H_*/
