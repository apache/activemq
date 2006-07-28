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
#ifndef Cms_IMapMessage_hpp_
#define Cms_IMapMessage_hpp_

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

#include "cms/IMessage.hpp"
#include "ppr/util/MapItemHolder.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace cms
  {
    using namespace apache::ppr::util;
    using namespace ifr;

/*
 * 
 */
struct IMapMessage : IMessage
{
    virtual p<PropertyMap> getBody() = 0 ;
} ;

/* namespace */
  }
}

#endif /*Cms_IMapMessage_hpp_*/
