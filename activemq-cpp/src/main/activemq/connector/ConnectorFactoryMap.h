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
#ifndef CONNECTORFACTORYMAP_H_
#define CONNECTORFACTORYMAP_H_

#include <map>
#include <vector>
#include <string>

#include <activemq/exceptions/ActiveMQException.h>
#include <activemq/connector/ConnectorFactory.h>

namespace activemq{
namespace connector{

    /**
     * Lookup Map for Connector Factories.  Use the Connector name to
     * find the associated factory.  This class does not take ownership
     * of the stored factories, they must be deallocated somewhere.
     */
    class ConnectorFactoryMap
    {
    public:
      
        /**
         * Gets a singleton instance of this class.
         */
        static ConnectorFactoryMap* getInstance(void);

        /**
         * Registers a new Connector Factory with this map
         * @param name to associate the factory with
         * @param factory to store.
        */
        void registerConnectorFactory( const std::string& name, 
                                       ConnectorFactory* factory );
      
        /**
         * Unregisters a Connector Factory with this map
         * @param name of the factory to remove
         */
        void unregisterConnectorFactory( const std::string& name );

        /**
         * Lookup the named factory in the Map
         * @param the factory name to lookup
         * @return the factory assciated with the name, or NULL
         */
        ConnectorFactory* lookup( const std::string& name );
      
        /**
         * Fetch a list of factory names that this Map contains
         * @param vector object to receive the list
         * @returns count of factories.
         */
        std::size_t getFactoryNames( std::vector< std::string >& factoryList );

    private:
   
        // Hidden Contrustor, prevents instantiation
        ConnectorFactoryMap() {};
      
        // Hidden Destructor.
        virtual ~ConnectorFactoryMap() {};
 
        // Hidden Copy Constructore
        ConnectorFactoryMap( const ConnectorFactoryMap& factoryMap );
      
        // Hidden Assignment operator
        ConnectorFactoryMap operator=( const ConnectorFactoryMap& factoryMap );

        // Map of Factories
        std::map< std::string, ConnectorFactory* > factoryMap;
      
    };

}}

#endif /*CONNECTORFACTORYMAP_H_*/
