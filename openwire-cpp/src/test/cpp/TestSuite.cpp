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
#include "TestSuite.hpp"

#include "TestSynchQueue.hpp"
#include "TestAsynchQueue.hpp"
#include "TestAsynchTopic.hpp"
#include "TestLocalTXCommit.hpp"

/*
 *
 */
TestSuite::TestSuite()
{
    this->name       = NULL ;
    this->trace      = false ;
    this->connection = NULL ;
}

/*
 *
 */
TestSuite::~TestSuite()
{
    // no-op
}

/*
 *
 */
void TestSuite::setURI(const char* uri)
{
    this->uri = new Uri(uri) ;
}

/*
 *
 */
void TestSuite::setSingle(const char* name)
{
    this->name = name ;
}

/*
 *
 */
void TestSuite::setUp() throw (TraceException)
{
    p<IConnectionFactory> factory ;

    cout << "Connecting to ActiveMQ broker..." << endl ;
    factory = new ConnectionFactory( this->uri ) ;
    connection = factory->createConnection() ;

    this->unitTests["SynchQueue"]    = new TestSynchQueue( connection ) ;
    this->unitTests["AsynchQueue"]   = new TestAsynchQueue( connection ) ;
    this->unitTests["AsynchTopic"]   = new TestAsynchTopic( connection ) ;
    this->unitTests["LocalTXCommit"] = new TestLocalTXCommit( connection ) ;
}

/*
 *
 */
void TestSuite::execute() throw (TraceException)
{

    try
    {
        // Set up test suite
        setUp() ;
    }
    catch( TraceException& e )
    {
        cout << "Failed to set up test suite. " << e.what() << endl ;
        exit(-1) ;
    }

    if( name != NULL )
        runSingle( name ) ;
    else
        runAll() ;

    try
    {
        // Tear down test suite
        tearDown() ;
    }
    catch( TraceException& e )
    {
        cout << "Failed to tear down test suite. " << e.what() << endl ;
        exit(-1) ;
    }
}

/*
 *
 */
void TestSuite::tearDown() throw (TraceException)
{
    if( connection != NULL )
        connection->close() ;
}

/*
 *
 */
p<string> TestSuite::toString()
{
    p<string> str = new string("ActiveMQ C++ Client Test Suite") ;
    return str ;
}

/*
 *
 */
void TestSuite::runSingle(const char* name) throw (TraceException)
{
    // Find unit test
    map< string, p<IUnitTest> >::iterator tempIter ;
    string key = string(name) ;

    try
    {
        // Locate given unit test
        tempIter = unitTests.find(key) ;
        if( tempIter == unitTests.end() )
        {
            cout << "No unit test named [" << name << "] found." << endl ;
            exit(-1) ;
        }
        string info ;

        info.assign( tempIter->second->toString()->c_str() ) ;

        // Pad with spaces up to 71 chars
        for( int i = (int)info.length() ; i < 71 ; i++ )
            info.append(" ") ;

        cout << info.c_str() ;

        tempIter->second->setUp() ;
        tempIter->second->execute() ;
        tempIter->second->tearDown() ;

        cout << "[  OK  ]" << endl ;
    }
    catch( TraceException& e )
    {
        cout << "[FAILED]" << endl ;
        cout << "  " << e.what() << endl ;
    }
}

/*
 *
 */
void TestSuite::runAll() throw (TraceException)
{
    map< string, p<IUnitTest> >::iterator tempIter ;

    // Loop through and execute all unit tests
    for( tempIter = unitTests.begin() ;
         tempIter != unitTests.end() ;
         tempIter++ )
    {
        string info ;

        info.assign( tempIter->second->toString()->c_str() ) ;

        // Pad with spaces up to 71 chars
        for( int i = (int)info.length() ; i < 71 ; i++ )
            info.append(" ") ;

        cout << info.c_str() ;

        try
        {
            tempIter->second->setUp() ;
            tempIter->second->execute() ;
            tempIter->second->tearDown() ;

            cout << "[  OK  ]" << endl ;
        }
        catch( TraceException& e )
        {
            cout << "[FAILED]" << endl ;
            cout << "  " << e.what() << endl ;
        }
    }
}

/*
 * Main entry point.
 */
int main(int argc, char *argv[])
{
    TestSuite suite ;

    // Print usage if no arguments was supplied
    if( argc <= 1 )
    {
        cout << "usage: test \"uri\" [name]" << endl ;
        cout << "  uri    The URI to the ActiveMQ broker, surrounded with quotation marks" << endl ;
        cout << "  name   The name of a single unit test to execute" << endl ;
        exit(-1) ;
    }

    // Check cmdline args for URI
    // Sample URI: "tcp://192.168.64.142:61616?trace=false&protocol=openwire&encoding=none"
    for( int i = 0 ; i < argc ; i++ )
    {
        // Skip program name
        if( i == 0 )
            continue ;

        // Assume URI
        if( i == 1 )
            suite.setURI( argv[i] ) ;
        // Assume unit test name
        if( i == 2 )
            suite.setSingle( argv[i] ) ;
    }
    suite.execute() ;
}
