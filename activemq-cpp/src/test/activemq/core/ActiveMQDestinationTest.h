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

#ifndef ACTIVEMQDESTINATIONTEST_H_
#define ACTIVEMQDESTINATIONTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/core/ActiveMQDestination.h>
#include <cms/Topic.h>

namespace activemq{
namespace core{

    class ActiveMQDestinationTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( ActiveMQDestinationTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();

    public:
    
        class MyDestination : public ActiveMQDestination< cms::Topic >
        {
        public:

            MyDestination( const cms::Destination* dest ) : 
                ActiveMQDestination< cms::Topic >( dest ) {}
        
            MyDestination( const std::string& name )
                : ActiveMQDestination< cms::Topic >( name, cms::Destination::TOPIC )
            {}
            
            virtual ~MyDestination() {}
        
            /**
             * Converts the Destination Name into a String 
             * @return string name
             */
            virtual std::string toString(void) const {
                return getName();
            }
    
            /**
             * Converts the Destination to a String value representing the
             * Provider specific name fot this destination, which is not
             * necessarily equal to the User Supplied name of the Destination
             * @return Provider specific Name
             */
            virtual std::string toProviderString(void) const {
                return getName();
            }
            
            /**
             * Creates a new instance of this destination type that is a
             * copy of this one, and returns it.
             * @returns cloned copy of this object
             */
            virtual cms::Destination* clone(void) const {
                return new MyDestination( this );
            }
  
              /**
             * Gets the name of this topic.
             * @return The topic name.
             */
            virtual std::string getTopicName(void) 
                const throw( cms::CMSException ) { return getName(); }

        };

    	ActiveMQDestinationTest() {}
    	virtual ~ActiveMQDestinationTest() {}

        virtual void test()
        {
            MyDestination dest( "test" );

            CPPUNIT_ASSERT( dest.getTopicName() == "test" );

            MyDestination dest1( "test1?value1=1&value2=2" );

            CPPUNIT_ASSERT( dest1.getTopicName() == "test1" );
            CPPUNIT_ASSERT( dest1.getProperties().hasProperty( "value1" ) == true );
            CPPUNIT_ASSERT( dest1.getProperties().hasProperty( "value2" ) == true );
            CPPUNIT_ASSERT( dest1.getProperties().hasProperty( "value3" ) != true );

            std::string value1 = dest1.getProperties().getProperty( "value1" );
            std::string value2 = dest1.getProperties().getProperty( "value2" );

            CPPUNIT_ASSERT( value1 == "1" );
            CPPUNIT_ASSERT( value2 == "2" );

            MyDestination* dest2 = 
                dynamic_cast< MyDestination* >( dest1.clone() );

            CPPUNIT_ASSERT( dest2 != NULL );

            CPPUNIT_ASSERT( dest2->getTopicName() == "test1" );
            CPPUNIT_ASSERT( dest2->getProperties().hasProperty( "value1" ) == true );
            CPPUNIT_ASSERT( dest2->getProperties().hasProperty( "value2" ) == true );
            CPPUNIT_ASSERT( dest2->getProperties().hasProperty( "value3" ) != true );

            value1 = dest2->getProperties().getProperty( "value1" );
            value2 = dest2->getProperties().getProperty( "value2" );

            CPPUNIT_ASSERT( value1 == "1" );
            CPPUNIT_ASSERT( value2 == "2" );

            delete dest2;

            MyDestination dest3("dummy");
            dest3.copy( dest1 );

            CPPUNIT_ASSERT( dest3.getTopicName() == "test1" );
            CPPUNIT_ASSERT( dest3.getProperties().hasProperty( "value1" ) == true );
            CPPUNIT_ASSERT( dest3.getProperties().hasProperty( "value2" ) == true );
            CPPUNIT_ASSERT( dest3.getProperties().hasProperty( "value3" ) != true );

            value1 = dest3.getProperties().getProperty( "value1" );
            value2 = dest3.getProperties().getProperty( "value2" );

            CPPUNIT_ASSERT( value1 == "1" );
            CPPUNIT_ASSERT( value2 == "2" );

        }
        
    };

}}

#endif /*ACTIVEMQDESTINATIONTEST_H_*/
