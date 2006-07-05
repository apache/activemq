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
#ifndef Cms_IQueue_hpp_
#define Cms_IQueue_hpp_

#include <string>
#include "cms/IDestination.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace cms
  {
    using namespace ifr;
    using namespace std;

/*
 * 
 */
struct IQueue : IDestination
{
    virtual p<string> getQueueName() = 0 ;
} ;

/* namespace */
  }
}

#endif /*Cms_IQueue_hpp_*/
