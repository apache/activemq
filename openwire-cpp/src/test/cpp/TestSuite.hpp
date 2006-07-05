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
#ifndef TestSuite_hpp_
#define TestSuite_hpp_

#include <exception>
#include <iostream>
#include <map>
#include <string>

#include "cms/IConnection.hpp"
#include "cms/IConnectionFactory.hpp"
#include "activemq/ConnectionFactory.hpp"
#include "activemq/Connection.hpp"
#include "ppr/TraceException.hpp"
#include "ppr/net/Uri.hpp"
#include "ppr/util/ifr/p"

#include "IUnitTest.hpp"

using namespace apache::activemq;
using namespace apache::cms;
using namespace apache::ppr;
using namespace apache::ppr::net;
using namespace ifr;
using namespace std;

class TestSuite : public IUnitTest
{
private:
    p<IConnection>              connection ;
    p<Uri>                      uri ;
    map< string, p<IUnitTest> > unitTests ;
    const char*                 name ;
    bool                        trace ;

public:
    TestSuite() ;
    virtual ~TestSuite() ;

    virtual void setURI(const char* uri) ;
    virtual void setSingle(const char* name) ;

    virtual void setUp() throw (TraceException) ;
    virtual void execute() throw (TraceException) ;
    virtual void tearDown() throw (TraceException) ;
    virtual p<string> toString() ;

protected:
    void runSingle(const char* name) throw (TraceException) ;
    void runAll() throw (TraceException) ;
} ;

#endif /*TestSuite_hpp_*/
