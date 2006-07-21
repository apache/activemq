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
#ifndef CONNECTORFACTORYMAPREGISTRAR_H_
#define CONNECTORFACTORYMAPREGISTRAR_H_

#include <string>

#include <activemq/connector/ConnectorFactoryMap.h>

namespace activemq{
namespace connector{

    /**
     * Registers the passed in factory into the factory map, this class
     * can manage the lifetime of the registered factory (default behaviour).
     */
    class ConnectorFactoryMapRegistrar
    {
    public:
   
        /** 
         * Constructor for this class
         * @param name of the factory to register
         * @param the factory
         * @param boolean indicating if this object manages the lifetime of 
         *        the factory that is being registered.
         */
        ConnectorFactoryMapRegistrar( const std::string& name, 
                                      ConnectorFactory*  factory,
                                      bool               manageLifetime = true )
        {       
            // Register it in the map.
            ConnectorFactoryMap::getInstance()->
                registerConnectorFactory(name, factory);

            // Store for later deletion            
            this->factory        = factory;
            this->manageLifetime = manageLifetime;
            this->name           = name;
        }
      
        virtual ~ConnectorFactoryMapRegistrar(void)
        {
            try
            {
                // UnRegister it in the map.
                ConnectorFactoryMap::getInstance()->
                    unregisterConnectorFactory( name );
            
                if( manageLifetime )
                {
                    delete factory;
                }
            }
            catch(...) {}
        }
      
        /**
         * get a reference to the factory that this class is holding
         * @return reference to a factory class
         */
        virtual ConnectorFactory& getFactory(void) {
            return *factory;
        }
      
    private:
      
        std::string       name;
        ConnectorFactory* factory;
        bool              manageLifetime;

    }; 
      
}}

#endif /*CONNECTORFACTORYMAPREGISTRAR_H_*/
