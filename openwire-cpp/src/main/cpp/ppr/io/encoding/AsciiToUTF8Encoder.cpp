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
#include "ppr/io/encoding/AsciiToUTF8Encoder.hpp"

using namespace apache::ppr::io::encoding;


// Init static members
const char* AsciiToUTF8Encoder::NAME = "AsciiToUTF8" ;

/*
 *
 */
AsciiToUTF8Encoder::AsciiToUTF8Encoder()
{
    // no-op
}

/*
 *
 */
AsciiToUTF8Encoder::~AsciiToUTF8Encoder()
{
    // no-op
}

/*
 * Counts length of encoded version of given string.
 */
int AsciiToUTF8Encoder::length(p<string> str)
{
    // Assert parameter
    if( str == NULL )
    {
        // Nothing to encode
        return 0 ;
    }

    int  ch,
         utflen = 0,
         strlen = (int)str->length() ;

    for( int i = 0 ; i < strlen ; i++ )
    {
        ch = ((*str)[i] & 0xFF) ;

        // Single byte in range 0x0001 - 0x007F
        if( (ch >= 0x0001) && (ch <= 0x007F) )
            utflen++ ;

        // Triple bytes above 0x07FF (should never occur, ASCII 0x00 - 0xFF)
        else if( ch > 0x07FF )
            utflen += 3 ;

        // Double bytes for 0x0000 and in range 0x0080 - 0x07FF
        else
            utflen += 2 ;
    }
    return utflen ;
}

/*
 * Encodes given string from ASCII into modified UTF-8.
 */
p<string> AsciiToUTF8Encoder::encode(p<string> str, int *enclen) throw (CharsetEncodingException) 
{
    // Assert parameter
    if( str == NULL )
    {
        // Nothing to encode
        *enclen = 0 ;
        return NULL ;
    }

    p<string> encstr = new string("") ;
    int  ch, strlen = (int)str->length() ;

    // Init encoded length
    *enclen = 0 ;

    // Loop through string and encode each char
    for( int i = 0 ; i < strlen ; i++ )
    {
        ch = ((*str)[i] & 0xFF) ;

        // Single byte in range 0x0001 - 0x007F
        if( (ch >= 0x0001) && (ch <= 0x007F) )
        {
            encstr->append(1, (char)ch) ;
            (*enclen)++ ;
        }
        // Triple bytes above 0x07FF (should never occur, ASCII 0x00 - 0xFF)
        else if( ch > 0x07FF )
        {
            encstr->append(1, (char)( ((ch >> 12) & 0x0F) | 0xE0 )) ;
            encstr->append(1, (char)( ((ch >> 6) & 0x3F) | 0x80 )) ;
            encstr->append(1, (char)( ((ch >> 0) & 0x3F) | 0x80 )) ;
            *enclen += 3 ;
        }
        // Double bytes for 0x0000 and in range 0x0080 - 0x07FF
        else
        {
            encstr->append(1, (char)( ((ch >> 6) & 0x1F) | 0xC0 )) ;
            encstr->append(1, (char)( ((ch >> 0) & 0x3F) | 0x80 )) ;
            *enclen += 2 ;
        }
    }
    return encstr ;
}

/*
 * Decodes given string from modified UTF-8 into ASCII.
 */
p<string> AsciiToUTF8Encoder::decode(p<string> str) throw (CharsetEncodingException)
{
    // Assert argument
    if( str == NULL || str->length() == 0 )
        return NULL ;

    p<string>     decstr = new string("") ;
    int           length = (int)str->length() ;
    unsigned char ch, ch2, ch3;
    int           i = 0 ;

    // Loop through and decode each char
    while( i < length )
    {
        ch = ((*str)[i] & 0xFF) ;

        switch( ch >> 4 )
        {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:   // Single byte char, 0xxxxxxx
                i++ ;
                decstr->append( 1, (char)ch ) ;
                break ;
            case 12:
            case 13:  // Double bytes char, 110xxxxx 10xxxxxx
                i += 2 ;

                if( i > length )
                    throw CharsetEncodingException("Missing character in double pair") ;

                ch2 = (*str)[i - 1] ;
                if( (ch2 & 0xC0) != 0x80 )
                    throw CharsetEncodingException("Invalid second character in double byte pair") ;

                decstr->append( 1, (char)(((ch & 0x1F) << 6) | (ch2 & 0x3F)) ) ;
                break ;
            case 14:  // Triple bytes char, 1110xxxx 10xxxxxx 10xxxxxx
                i += 3 ;
                if( i > length )
                    throw CharsetEncodingException("Missing character in triple set") ;

                ch2 = (*str)[i - 2] ;
                ch3 = (*str)[i - 1] ;
                if( ((ch2 & 0xC0) != 0x80) || ((ch3 & 0xC0) != 0x80) )
                    throw CharsetEncodingException("Invalid second and/or third character in triple set") ;

                decstr->append( 1, (char)(((ch & 0x0F) << 12) | ((ch2 & 0x3F) << 6) | ((ch3 & 0x3F) << 0)) ) ;
                break ;
            default:  // Unsupported, 10xxxxxx 1111xxxx
                throw CharsetEncodingException("Unsupported type flag") ;
        }
    }
    return decstr ;
}
