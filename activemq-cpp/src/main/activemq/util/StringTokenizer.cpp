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
#include "StringTokenizer.h"

using namespace std;
using namespace activemq;
using namespace activemq::util;
using namespace activemq::exceptions;

////////////////////////////////////////////////////////////////////////////////
StringTokenizer::StringTokenizer(const std::string& str, 
                                 const std::string& delim, 
                                 bool returnDelims)
{
   // store off the data
   this->str = str;
   this->delim = delim;
   this->returnDelims = returnDelims;
   
   // Start and the beginning
   pos = 0;
}

////////////////////////////////////////////////////////////////////////////////
StringTokenizer::~StringTokenizer(void)
{
}

////////////////////////////////////////////////////////////////////////////////
int StringTokenizer::countTokens(void) const
{
   int count = 0;
   string::size_type localPos = pos;
   string::size_type lastPos  = pos;

   while(localPos != string::npos)
   {
      if(returnDelims && str.find_first_of(delim, localPos) == localPos)
      {
         count += 1;
         localPos += 1;

         continue;
      }

      // Find first token by spanning the fist non-delimiter, to the
      // next delimiter, skipping any delimiters that are at the curret
      // location.
      lastPos  = str.find_first_not_of(delim, localPos);
      localPos = str.find_first_of(delim, lastPos);

      if(lastPos != string::npos)
      {
         count++;
      }
   }

	return count;
}

////////////////////////////////////////////////////////////////////////////////
bool StringTokenizer::hasMoreTokens(void) const
{
   string::size_type nextpos = 
      returnDelims ? str.find_first_of(delim, pos) :
                     str.find_first_not_of(delim, pos);

   return (nextpos != string::npos);
}

////////////////////////////////////////////////////////////////////////////////
std::string StringTokenizer::nextToken(void)
   throw ( exceptions::NoSuchElementException )
{
   if(pos == string::npos)
   {
      throw NoSuchElementException(
         __FILE__, __LINE__,
         "StringTokenizer::nextToken - No more Tokens available");
   }

   if(returnDelims)
   {
      // if char at current pos is a delim return it and advance pos
      if(str.find_first_of(delim, pos) == pos)
      {
         return str.substr(pos++, 1);
      }
   }

   // Skip delimiters at beginning.
   string::size_type lastPos = str.find_first_not_of(delim, pos);
   
   // Find the next delimiter in the string, the charactors in between
   // will be the token to return.  If this returns string::npos then
   // there are no more delimiters in the string.
   pos = str.find_first_of(delim, lastPos);

   if(string::npos != lastPos)
   {
      // Found a token, count it, if the pos of the next delim is npos
      // then we set length to copy to npos so that all the remianing
      // portion of the string is copied, otherwise we set it to the 
      return str.substr(lastPos, 
                        pos == string::npos ? pos : pos-lastPos);
   }
   else
   {
      throw NoSuchElementException(
         __FILE__, __LINE__,
         "StringTokenizer::nextToken - No more Tokens available");
   }
}

////////////////////////////////////////////////////////////////////////////////
std::string StringTokenizer::nextToken(const std::string& delim)
   throw ( exceptions::NoSuchElementException )
{
    this->delim = delim;

    return nextToken();
}

////////////////////////////////////////////////////////////////////////////////
unsigned int StringTokenizer::toArray(std::vector<std::string>& array)
{
    int count = 0;

    while(hasMoreTokens())
    {
        array.push_back(nextToken());
        count++;
    }

    return count;
}

////////////////////////////////////////////////////////////////////////////////
void StringTokenizer::reset(const std::string& str,
                            const std::string& delim,
                            bool returnDelims)
{
    if(str != "")
    {
        this->str = str;
    }

    if(delim != "")
    {
        this->delim = delim;
    }

    this->returnDelims = returnDelims;

    // Begin at the Beginning
    this->pos = 0;
}
