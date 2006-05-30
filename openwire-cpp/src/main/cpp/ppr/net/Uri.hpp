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
#ifndef Ppr_Uri_hpp_
#define Ppr_Uri_hpp_

#include <string>
#include <assert.h>
#include <stdlib.h>

namespace apache
{
  namespace ppr
  {
    namespace net
    {
      using namespace std;

/*
 * Splits an Uri string into it's basic parts.
 * @author David Fahlander, PortWise
 */
class Uri
{
public:
  // Constructs an empty URI.
  Uri() : port_(0), bits_ (IS_EMPTY) {};

  // Constructs an URI based on given absolute URI string.
  Uri(const string& absoluteUri);

  /** Construct an Uri based on an URI and a base Uri.

    Given URI can be a relative path, an absolute path or an entire Uri.
    If given URI is an absolute URI (begins with <scheme> "://", baseUri is ignored.
    If given URI begins with ".." or "." the constructed Uri will resolve the real
    path without "." or "..".

    @param uri URI relative to baseUri
    @baseUri The Uri of the document that referred to given URI.
  */
  Uri (string uri, const Uri& baseUri);
public:
  /** Get scheme.
  */
  const string& scheme() const {return scheme_;}
  /** Get host.
  */
  const string& host() const {return host_;}

  /** Returns the port (if scheme supports port).
    If the port was omitted in the string that constructed the URI, the default
    port for that scheme is returned (if there is a default port for it). If no
    default port exists for the scheme and port was omitted in the string that
    constructed the URI, 0 is returned.

    For example, the URI
      http://127.0.0.1:8081/index.html has the port 8081 and the URI
      http://www.apache.org/index.html has the port 80 (default HTTP port).

    The default port-table is defined in Uri::getDefaultPort().
  */
  unsigned short port() const {return port_;}

  /** Returns the authority.
    The authority is equivalent to the host header when an HTTP URI is used. If a default
    port is used, the port part of the authority may be omitted. For example, the URI
      http://www.apache.org/index.html has the authority "www.apache.org" and the URI
      http://127.0.0.1:8081/index.html has the authority "127.0.0.1:8081".
  */
  const string& authority() const {return authority_;}

  /** Returns the path (without query).
    If no path, an empty string is returned.
  */
  const string& path() const {return path_;}

  /** Returns the query. If no query, an empty string is returned.
  */
  const string& query() const {return query_;}

  /** Returns true if URI has a query.
  */
  bool hasQuery () const {return !query_.empty();}

  /** Returns the path and query part of the URI.
  */
  string pathAndQuery() const {return (hasQuery() ? path() + "?" + query() : path());}

  /** Returns true if the URI failed to be interpreted in any of the constructors used.
  */
  bool isFail () const { return ((bits_ & IS_FAIL) != 0); }

  /** Returns true if the URI is empty.
  */
  bool isEmpty () const { return ((bits_ & IS_EMPTY) != 0); }

  /** Returns true if the URI is a URL using a secure scheme such as https.
  */
  bool isSecure() const;

  /** Converts this URI into a string.
  */
  string toString() const {
    if (isEmpty() || isFail()) {
      return "";
    } else if (hasQuery()) {
      return scheme() + "://" + authority() + path() + "?" + query();
    } else {
      return scheme() + "://" + authority() + path();
    }
  }
public:
  /** Returns the default port if given scheme is a protocol that is recognized
      by this implementation.
  */
  static unsigned short getDefaultPort (const string& scheme);
private:
  string scheme_;
  string host_;
  unsigned short port_;
  string authority_;
  string path_;
  string query_;
private:
  static const unsigned char IS_EMPTY = 0x01;
  static const unsigned char IS_FAIL  = 0x02;
  unsigned char bits_;
};

/* namespace */
    }
  }
}

#endif /*Ppr_Uri_hpp_*/

