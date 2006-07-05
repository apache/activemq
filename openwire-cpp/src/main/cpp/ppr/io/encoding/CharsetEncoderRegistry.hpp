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
#ifndef Ppr_CharsetEncoderRegistry_hpp_
#define Ppr_CharsetEncoderRegistry_hpp_

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

#include <string>
#include <map>
#include "ppr/io/encoding/ICharsetEncoder.hpp"
#include "ppr/IllegalArgumentException.hpp"
#include "ppr/io/encoding/AsciiToUTF8Encoder.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace ppr
  {
    namespace io
    {
      namespace encoding
      {
        using namespace ifr ;
        using namespace std ;
        using namespace apache::ppr ;

/*
 * The character set encoder registry maintains all available
 * encoders/decoders.
 */
class CharsetEncoderRegistry
{
private:
    static map<string, p<ICharsetEncoder> > encoders ;

public:
    // Name of the default encoder
    static const char* DEFAULT ;

protected:
    CharsetEncoderRegistry() ;

public:
    virtual ~CharsetEncoderRegistry() ;

	static p<ICharsetEncoder> getEncoder() ;
	static p<ICharsetEncoder> getEncoder(const char* name) ;

	static void addEncoder(const char* name, p<ICharsetEncoder> encoder) throw(IllegalArgumentException) ;
	static void removeEncoder(const char* name) throw(IllegalArgumentException) ;

    class MapInitializer
    {
    public:
        MapInitializer() 
        {
            // Add the default set of encoders
            CharsetEncoderRegistry::addEncoder(AsciiToUTF8Encoder::NAME, new AsciiToUTF8Encoder() ) ;
        }
    } ;

    friend class CharsetEncoderRegistry::MapInitializer ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*Ppr_CharsetEncoderRegistry_hpp_*/
