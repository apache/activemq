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
#ifndef TestListener_hpp_
#define TestListener_hpp_

#include <iostream>
#include <string>

#include "cms/IMessage.hpp"
#include "cms/IBytesMessage.hpp"
#include "cms/IMessageListener.hpp"
#include "ppr/util/ifr/p"

using namespace apache::cms;
using namespace ifr;
using namespace std;

/*
 * 
 */
class TestListener : public IMessageListener
{
public:
    TestListener() ;
    virtual ~TestListener() ;

    virtual void onMessage(p<IMessage> message) ;
} ;

#endif /*TestListener_hpp_*/
