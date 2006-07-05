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
#ifndef TestSynchQueue_hpp_
#define TestSynchQueue_hpp_

#include <exception>
#include <string>

#include "cms/IConnection.hpp"
#include "ppr/TraceException.hpp"
#include "ppr/util/ifr/p"

#include "IUnitTest.hpp"

using namespace apache::cms;
using namespace apache::ppr;
using namespace ifr;
using namespace std;

class TestSynchQueue : public IUnitTest
{
private:
    p<IConnection> connection ;
    p<ISession>    session ;

public:
    TestSynchQueue(p<IConnection> connection) ;
    virtual ~TestSynchQueue() ;

    virtual void setUp() throw (exception) ;
    virtual void execute() throw (exception) ;
    virtual void tearDown() throw (exception) ;
    virtual p<string> toString() ;
} ;

#endif /*TestSynchQueue_hpp_*/
