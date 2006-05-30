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
#include "ppr/net/Uri.hpp"

using namespace apache::ppr::net;

#ifdef unix
#define _stricmp strcasecmp
#endif

Uri::Uri (const string& absoluteUri) : port_(0), bits_ (IS_FAIL)
{
  if (absoluteUri.empty()) {
    bits_ = IS_EMPTY;
    return;
  }
  //
  // scheme
  //
  size_t pColonSlashSlash = absoluteUri.find ("://");
  if (pColonSlashSlash == absoluteUri.npos) return; // must contain "://"
  scheme_ = absoluteUri.substr (0, pColonSlashSlash);
  size_t pHost = pColonSlashSlash + 3; // strlen ("://")

  //
  // host, port, authority_
  //
  size_t pColonOrSlash = absoluteUri.find_first_of (":/", pHost);
  if (pColonOrSlash != absoluteUri.npos && absoluteUri[pColonOrSlash] == ':') {
    host_ = absoluteUri.substr (pHost, pColonOrSlash - pHost);
    long port = atol(absoluteUri.substr (pColonOrSlash + 1).c_str());
    if (port < 0 || port > 65535) return; // If port is present, it must be between 0 and 65535.
    port_ = (unsigned short) port;
    pColonOrSlash = absoluteUri.find ('/', pColonOrSlash + 1); // Make pColonOrSlash point to slash.
    if (pColonOrSlash != absoluteUri.npos) {
      authority_ = absoluteUri.substr (pHost, pColonOrSlash - pHost);
    } else {
      authority_ = absoluteUri.substr (pHost);
    }
  } else {
    port_ = getDefaultPort (scheme_);
    if (pColonOrSlash == absoluteUri.npos) {
      host_ = absoluteUri.substr (pHost);
    } else {
      host_ = absoluteUri.substr (pHost, pColonOrSlash - pHost);
    }
    authority_ = host_;
  }

  //
  // path
  //
  if (pColonOrSlash == absoluteUri.npos) {
    path_ = "/";
  } else {
    size_t pQuery = absoluteUri.find ('?', pColonOrSlash);
    if (pQuery == absoluteUri.npos) {
      path_ = absoluteUri.substr (pColonOrSlash);
    } else {
      path_ = absoluteUri.substr (pColonOrSlash, pQuery - pColonOrSlash);
      //
      // query
      //
      query_ = absoluteUri.substr (pQuery + 1);
    }
  }

  // isFail
  if (!scheme_.empty() && !path_.empty()) {
    bits_ = 0;
  }
}

Uri::Uri (string uri, const Uri& baseUri) : port_(0), bits_ (IS_FAIL)
{
  if (uri[0] == '/') {
    //
    // uri is absolute path
    //
    *this = baseUri;
    size_t pQuery = uri.find ('?');
    if (pQuery == uri.npos) {
      path_ = uri;
      query_.clear();
    } else {
      path_ = uri.substr (0, pQuery);
      query_ = uri.substr (pQuery + 1);
    }
  } else {
    size_t pColonOrSlash = uri.find_first_of ("/:");
    if (pColonOrSlash != uri.npos && uri[pColonOrSlash] == ':' && pColonOrSlash == uri.find ("://")) {
      //
      // uri is an Uri
      //
      *this = uri;
    } else {
      //
      // uri is a relative path
      //

      // Copy everything from base Uri to this instance.
      // We will change path and query only...
      *this = baseUri;

      // Resolve ".." and "." in beginning of uri:
      // Check for last slash in base Uri (where to insert uri)
      size_t pLastSlash = path_.rfind ('/');
      while (uri.length() > 0 && uri[0] == '.') {
        if (uri.length() == 1) {
          // uri is exactly "."
          uri.clear();
          break;
        } else if (uri[1] == '/') {
          // uri begins with "./"
          uri = uri.substr (2);
        } else if (uri[1] == '.') {
          //
          // Uri begins with ".." but we dont know the next character...
          //
          if (uri.length() == 2) {
            // uri is exactly ".."
            uri.clear();
            // Navigate upwards in directory tree (of path_)
            if (pLastSlash != path_.npos && pLastSlash > 0) {
              assert (string ("/1/2/").rfind ('/') == 4); // just to make sure how rfind() works...
              assert (string ("/1/2/").rfind ('/', 4) == 4); // --""--
              assert (string ("/1/2/").rfind ('/', 3) == 2); // --""--
              assert (string ("/").rfind ('/', 0) == 0); // --""--
              pLastSlash = path_.rfind ('/', pLastSlash - 1);
            }
            break;
          } else if (uri[2] == '/') {
            // uri begins with "../"
            uri = uri.substr (3);
            // Navigate upwards in directory tree (of path_)
            if (pLastSlash != path_.npos && pLastSlash > 0) {
              assert (string ("/1/2/").rfind ('/') == 4); // just to make sure how rfind() works...
              assert (string ("/1/2/").rfind ('/', 4) == 4); // --""--
              assert (string ("/1/2/").rfind ('/', 3) == 2); // --""--
              assert (string ("/").rfind ('/', 0) == 0); // --""--
              pLastSlash = path_.rfind ('/', pLastSlash - 1);
            }
          }
        } 
      }

      // Check for query in given uri (in order to split path from query, as we do in this class)
      size_t pQuery = uri.find ('?');

      if (pQuery == uri.npos) {
        // No query in the uri
        if (pLastSlash != path_.npos) {
          // Append URI after last slash in base Uri's path
          path_ = path_.substr (0, pLastSlash + 1) + uri;
        } else {
          // No slash found in base Uri's path. Append a slash in beginning.
          path_ = "/" + uri;
        }
        // Clear the query
        query_.clear();
      } else {
        // Query in the uri
        if (pLastSlash != path_.npos) {
          // Append URI after last slash in base Uri's path
          path_ = path_.substr (0, pLastSlash + 1) + uri.substr (0, pQuery);
        } else {
          // No slash found in base Uri's path. Append a slash in beginning.
          path_ = "/" + uri.substr (0, pQuery);
        }
        // Get the query from uri.
        query_ = uri.substr (pQuery + 1);
      }
      
    }
  }
}

unsigned short Uri::getDefaultPort (const string& scheme)
{
  if (_stricmp (scheme.c_str(), "http") == 0) {
    return 80;
  } else if (_stricmp (scheme.c_str(), "https") == 0) {
    return 443;
  } else if (_stricmp (scheme.c_str(), "ftp") == 0) {
    return 21;
  } else if (_stricmp (scheme.c_str(), "ssh") == 0) {
    return 22;
  } else if (_stricmp (scheme.c_str(), "telnet") == 0) {
    return 23;
  } else if (_stricmp (scheme.c_str(), "smtp") == 0) {
    return 25;
  } else if (_stricmp (scheme.c_str(), "pop3") == 0) {
    return 110;
  } else if (_stricmp (scheme.c_str(), "radius") == 0) {
    return 1812;
  } else {
    return 0;
  }
}

bool Uri::isSecure() const
{
  if (_stricmp (scheme_.c_str(), "https") == 0 ||
      _stricmp (scheme_.c_str(), "ssh") == 0 ||
      _stricmp (scheme_.c_str(), "ftps") == 0)
  {
    return true;
  } else {
    return false;
  }
}

