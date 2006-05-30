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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_STOMPFRAMEWRAPPER_H_
#define ACTIVEMQ_TRANSPORT_STOMP_STOMPFRAMEWRAPPER_H_
 
#include <vector>
#include <string>
#include <map>
#include <list>

namespace activemq{
namespace transport{
namespace stomp{

    /**
     * A Stomp-level message frame that encloses all messages
     * to and from the broker.
     * @author Nathan Mittler
     */
	class StompFrame{
		
	public: // StandardHeader enumeration
	
		enum StandardHeader{
			HEADER_DESTINATION,
			HEADER_TRANSACTIONID,
			HEADER_CONTENTLENGTH,
			HEADER_SESSIONID,
			HEADER_RECEIPTID,
			HEADER_MESSAGEID,
			HEADER_ACK,
			HEADER_LOGIN,
			HEADER_PASSWORD,
			HEADER_MESSAGE,
			NUM_STANDARD_HEADERS
		};		
		
		static const char* toString( const StandardHeader header ){
			return standardHeaders[header];
		}
		
		static int getStandardHeaderLength( const StandardHeader header ){
			return standardHeaderLengths[header];
		}
		
		static StandardHeader toStandardHeader( const char* header ){			
			for( int ix=0; ix<NUM_STANDARD_HEADERS; ++ix ){
				if( header == standardHeaders[ix] || 
					strcmp(header, standardHeaders[ix]) == 0 ){
					return (StandardHeader)ix;
				}
			}			
			return NUM_STANDARD_HEADERS;
		}
		
	public: // Command enumeration
	
		enum Command{
			COMMAND_CONNECT,
			COMMAND_CONNECTED,
			COMMAND_DISCONNECT,
			COMMAND_SUBSCRIBE,
			COMMAND_UNSUBSCRIBE,
			COMMAND_MESSAGE,
			COMMAND_SEND,
			COMMAND_BEGIN,
			COMMAND_COMMIT,
			COMMAND_ABORT,
			COMMAND_ACK,
			COMMAND_ERROR,
			NUM_COMMANDS
		};
		
		static const char* toString( const Command cmd ){
			return commands[cmd];
		}
		
		static int getCommandLength( const Command cmd ){
			return commandLengths[cmd];
		}
		
		static Command toCommand( const char* cmd ){			
			for( int ix=0; ix<NUM_COMMANDS; ++ix ){
				if( cmd == commands[ix] || 
					strcmp(cmd, commands[ix]) == 0 ){
					return (Command)ix;
				}
			}			
			return NUM_COMMANDS;
		}
		
	public: // AckMode enumeration
	
		enum AckMode{
			ACK_CLIENT,
			ACK_AUTO,
			NUM_ACK_MODES
		};
		
		static const char* toString( const AckMode mode ){
			return ackModes[mode];
		}
		
		static int getAckModeLength( const AckMode mode ){
			return ackModeLengths[mode];
		}
		
		static AckMode toAckMode( const char* mode ){			
			for( int ix=0; ix<NUM_ACK_MODES; ++ix ){
				if( mode == ackModes[ix] || 
					strcmp(mode, ackModes[ix]) == 0 ){
					return (AckMode)ix;
				}
			}			
			return NUM_ACK_MODES;
		}
		
	public:
	
		struct HeaderInfo{
			const char* key;
			int keyLength;
			const char* value;
			int valueLength;
		};
		
	public:
	
		/**
		 * Default constructor.
		 */
		StompFrame();
		
		/**
		 * Destruction - frees the memory pool.
		 */
		virtual ~StompFrame();
		
		/**
		 * Sets the command for this stomp frame.
		 * @param command The command to be set.
		 */
		virtual void setCommand( Command cmd ){
			command = cmd;
		}
		
		/**
		 * Accessor for this frame's command field.
		 */
		virtual Command getCommand() const{
			return command;
		}
			
		virtual void setHeader( const StandardHeader key,
			const char* value,
			const int valueLength ){
			setHeader( toString( key ), getStandardHeaderLength( key ),
				value,
				valueLength );				
		}
		
		/**
		 * Sets the given header in this frame.
		 * @param key The name of the header to be set.
		 * @param keyLength The string length of the key.
		 * @param value The value to set for the header.
		 * @param valueLength The length of the value string.
		 */
		virtual void setHeader( const char* key, 
			const int keyLength,
			const char* value, 
			const int valueLength );
			
		virtual const HeaderInfo* getHeaderInfo( const StandardHeader name ) const{
			return getHeaderInfo( toString( name ) );
		}
		
		/**
		 * Accessor for the value of the given header information.
		 * @param name The name of the header to lookup.
		 * @return The information for the given header.
		 */
		virtual const HeaderInfo* getHeaderInfo( const char* name ) const;
		
		virtual const HeaderInfo* getFirstHeader(){	
			pos = headers.begin();
			return getNextHeader();
		}
		virtual const HeaderInfo* getNextHeader(){
			if( pos == headers.end() ){
				return NULL;
			}
			
			const HeaderInfo* info = &(pos->second);
			
			pos++;
			return info;
		}
		virtual const int getNumHeaders() const{
			return headers.size();
		}
		
		/**
		 * Accessor for the body data of this frame.
		 */
		virtual const char* getBody() const{
			return body;
		}
		
		virtual int getBodyLength() const{ return bodyLength; }
		
		/**
		 * Sets the body data of this frame as a text string.
		 * @param text The data to set in the body.
		 * @param textLength The length of the text string.
		 */
		virtual void setBodyText( const char* text, const int textLength );
		
		/**
		 * Sets the body data of this frame as a byte sequence. Adds the
		 * content-length header to specify the number of bytes in the
		 * sequence.
		 * @param bytes The byte buffer to be set in the body.
		 * @param numBytes The number of bytes in the buffer.
		 */
		virtual void setBodyBytes( const char* bytes, const int numBytes );
		
	private:
	
		typedef std::map< std::string, HeaderInfo> headersType;
		
	private:
	
		static void staticInit();
		
	private:
	
		Command command;
		
		headersType headers;
		
		const char* body;
		int bodyLength;
		char bodyLengthStr[20];
		
		headersType::const_iterator pos;
		
		static bool staticInitialized;
		static const char* standardHeaders[NUM_STANDARD_HEADERS];
		static int standardHeaderLengths[NUM_STANDARD_HEADERS];	
		static const char* commands[NUM_COMMANDS];
		static int commandLengths[NUM_COMMANDS];	
		static const char* ackModes[NUM_ACK_MODES];
		static int ackModeLengths[NUM_ACK_MODES];
	};
	
}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_STOMPFRAMEWRAPPER_H_ */
