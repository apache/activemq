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
#ifndef ACTIVEMQ_CORE_ACTIVEMQCONSTANTS_H_
#define ACTIVEMQ_CORE_ACTIVEMQCONSTANTS_H_

#include <activemq/exceptions/IllegalArgumentException.h>

#include <string>
#include <map>

namespace activemq{
namespace core{
    
    /**
     * Class holding constant values for various ActiveMQ specific things
     * Each constant is defined as an enumeration and has functions that
     * convert back an forth between string and enum values.
     */
    class ActiveMQConstants{    
    public:
    
        /**
         * These values represent the options that can be appended to an
         * Destination name, i.e. /topic/foo?consumer.exclusive=true
         */
        enum DestinationOption{
            CONSUMER_PREFECTCHSIZE,
            CUNSUMER_MAXPENDINGMSGLIMIT,
            CONSUMER_NOLOCAL,
            CONSUMER_DISPATCHASYNC,
            CONSUMER_RETROACTIVE,
            CONSUMER_SELECTOR,
            CONSUMER_EXCLUSIVE,
            CONSUMER_PRIORITY,
            NUM_OPTIONS
        };

        /**
         * These values represent the parameters that can be added to the
         * connection URI that affect the ActiveMQ Core API
         */        
        enum URIParam
        {
            PARAM_USERNAME,
            PARAM_PASSWORD,
            PARAM_CLIENTID,
            NUM_PARAMS
        };
        
        static const std::string& toString( const DestinationOption option ){
            return StaticInitializer::destOptions[option];
        }
        
        static DestinationOption toDestinationOption( const std::string& option ){     
            std::map< std::string, DestinationOption >::iterator iter = 
                StaticInitializer::destOptionMap.find( option );

            if( iter == StaticInitializer::destOptionMap.end() ){
                return NUM_OPTIONS;
            }
                    
            return iter->second;
        }             
        
        static const std::string& toString( const URIParam option ){
            return StaticInitializer::uriParams[option];
        }
        
        static URIParam toURIOption( const std::string& option ){     
            std::map< std::string, URIParam >::iterator iter = 
                StaticInitializer::uriParamsMap.find( option );

            if( iter == StaticInitializer::uriParamsMap.end() ){
                return NUM_PARAMS;
            }
                    
            return iter->second;
        }             

        class StaticInitializer{
        public:
            StaticInitializer();
            virtual ~StaticInitializer(){}
            
            static std::string destOptions[NUM_OPTIONS];
            static std::string uriParams[NUM_PARAMS];
            static std::map<std::string, DestinationOption> destOptionMap;
            static std::map<std::string, URIParam> uriParamsMap;
        };
        
    private:
    
        static StaticInitializer staticInits;        

    };
    
}}

#endif /*ACTIVEMQ_CORE_ACTIVEMQCONSTANTS_H_*/
