/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.util;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class that provides methods for parsing URI's
 *
 * This class can be used to split composite URI's into their component parts and is used to extract any
 * URI options from each URI in order to set specific properties on Beans.
 */
public class URISupport {

    /**
     * A composite URI can be split into one or more CompositeData object which each represent the
     * individual URIs that comprise the composite one.
     */
    public static class CompositeData {
        private String host;
        private String scheme;
        private String path;
        private URI components[];
        private Map<String, String> parameters;
        private String fragment;

        public URI[] getComponents() {
            return components;
        }

        public String getFragment() {
            return fragment;
        }

        public Map<String, String> getParameters() {
            return parameters;
        }

        public String getScheme() {
            return scheme;
        }

        public String getPath() {
            return path;
        }

        public String getHost() {
            return host;
        }

        public URI toURI() throws URISyntaxException {
            StringBuffer sb = new StringBuffer();
            if (scheme != null) {
                sb.append(scheme);
                sb.append(':');
            }

            if (host != null && host.length() != 0) {
                sb.append(host);
            } else {
                sb.append('(');
                for (int i = 0; i < components.length; i++) {
                    if (i != 0) {
                        sb.append(',');
                    }
                    sb.append(components[i].toString());
                }
                sb.append(')');
            }

            if (path != null) {
                sb.append('/');
                sb.append(path);
            }
            if (!parameters.isEmpty()) {
                sb.append("?");
                sb.append(createQueryString(parameters));
            }
            if (fragment != null) {
                sb.append("#");
                sb.append(fragment);
            }
            return new URI(sb.toString());
        }
    }

    /**
     * Give a URI break off any URI options and store them in a Key / Value Mapping.
     *
     * @param uri
     * 		The URI whose query should be extracted and processed.
     *
     * @return A Mapping of the URI options.
     * @throws URISyntaxException
     */
    public static Map<String, String> parseQuery(String uri) throws URISyntaxException {
        try {
            uri = uri.substring(uri.lastIndexOf("?") + 1); // get only the relevant part of the query
            Map<String, String> rc = new HashMap<String, String>();
            if (uri != null && !uri.isEmpty()) {
                String[] parameters = uri.split("&");
                for (int i = 0; i < parameters.length; i++) {
                    int p = parameters[i].indexOf("=");
                    if (p >= 0) {
                        String name = URLDecoder.decode(parameters[i].substring(0, p), "UTF-8");
                        String value = URLDecoder.decode(parameters[i].substring(p + 1), "UTF-8");
                        rc.put(name, value);
                    } else {
                        rc.put(parameters[i], null);
                    }
                }
            }
            return rc;
        } catch (UnsupportedEncodingException e) {
            throw (URISyntaxException)new URISyntaxException(e.toString(), "Invalid encoding").initCause(e);
        }
    }

    /**
     * Given a URI parse and extract any URI query options and return them as a Key / Value mapping.
     *
     * This method differs from the {@link parseQuery} method in that it handles composite URI types and
     * will extract the URI options from the outermost composite URI.
     *
     * @param uri
     * 		The URI whose query should be extracted and processed.
     *
     * @return A Mapping of the URI options.
     * @throws URISyntaxException
     */
    public static Map<String, String> parseParameters(URI uri) throws URISyntaxException {
        if (!isCompositeURI(uri)) {
            return uri.getQuery() == null ? emptyMap() : parseQuery(stripPrefix(uri.getQuery(), "?"));
        } else {
            CompositeData data = URISupport.parseComposite(uri);
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.putAll(data.getParameters());
            if (parameters.isEmpty()) {
                parameters = emptyMap();
            }

            return parameters;
        }
    }

    /**
     * Given a Key / Value mapping create and append a URI query value that represents the mapped entries, return the
     * newly updated URI that contains the value of the given URI and the appended query value.
     *
     * @param uri
     * 		The source URI that will have the Map entries appended as a URI query value.
     * @param queryParameters
     * 		The Key / Value mapping that will be transformed into a URI query string.
     *
     * @return A new URI value that combines the given URI and the constructed query string.
     * @throws URISyntaxException
     */
    public static URI applyParameters(URI uri, Map<String, String> queryParameters) throws URISyntaxException {
        return applyParameters(uri, queryParameters, "");
    }

    /**
     * Given a Key / Value mapping create and append a URI query value that represents the mapped entries, return the
     * newly updated URI that contains the value of the given URI and the appended query value.  Each entry in the query
     * string is prefixed by the supplied optionPrefix string.
     *
     * @param uri
     * 		The source URI that will have the Map entries appended as a URI query value.
     * @param queryParameters
     * 		The Key / Value mapping that will be transformed into a URI query string.
     * @param optionPrefix
     * 		A string value that when not null or empty is used to prefix each query option key.
     *
     * @return A new URI value that combines the given URI and the constructed query string.
     * @throws URISyntaxException
     */
    public static URI applyParameters(URI uri, Map<String, String> queryParameters, String optionPrefix) throws URISyntaxException {
        if (queryParameters != null && !queryParameters.isEmpty()) {
            StringBuffer newQuery = uri.getRawQuery() != null ? new StringBuffer(uri.getRawQuery()) : new StringBuffer() ;
            for ( Map.Entry<String, String> param: queryParameters.entrySet()) {
                if (param.getKey().startsWith(optionPrefix)) {
                    if (newQuery.length()!=0) {
                        newQuery.append('&');
                    }
                    final String key = param.getKey().substring(optionPrefix.length());
                    newQuery.append(key).append('=').append(param.getValue());
                }
            }
            uri = createURIWithQuery(uri, newQuery.toString());
        }
        return uri;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> emptyMap() {
        return Collections.EMPTY_MAP;
    }

    /**
     * Removes any URI query from the given uri and return a new URI that does not contain the query portion.
     *
     * @param uri
     * 		The URI whose query value is to be removed.
     *
     * @return a new URI that does not contain a query value.
     * @throws URISyntaxException
     */
    public static URI removeQuery(URI uri) throws URISyntaxException {
        return createURIWithQuery(uri, null);
    }

    /**
     * Creates a URI with the given query, removing an previous query value from the given URI.
     *
     * @param uri
     * 		The source URI whose existing query is replaced with the newly supplied one.
     * @param query
     * 		The new URI query string that should be appended to the given URI.
     *
     * @return a new URI that is a combination of the original URI and the given query string.
     * @throws URISyntaxException
     */
    public static URI createURIWithQuery(URI uri, String query) throws URISyntaxException {
        String schemeSpecificPart = uri.getRawSchemeSpecificPart();
        // strip existing query if any
        int questionMark = schemeSpecificPart.lastIndexOf("?");
        // make sure question mark is not within parentheses
        if (questionMark < schemeSpecificPart.lastIndexOf(")")) {
            questionMark = -1;
        }
        if (questionMark > 0) {
            schemeSpecificPart = schemeSpecificPart.substring(0, questionMark);
        }
        if (query != null && query.length() > 0) {
            schemeSpecificPart += "?" + query;
        }
        return new URI(uri.getScheme(), schemeSpecificPart, uri.getFragment());
    }

    /**
     * Given a composite URI, parse the individual URI elements contained within that URI and return
     * a CompsoteData instance that contains the parsed URI values.
     *
     * @param uri
     * 		The target URI that should be parsed.
     *
     * @return a new CompsiteData instance representing the parsed composite URI.
     * @throws URISyntaxException
     */
    public static CompositeData parseComposite(URI uri) throws URISyntaxException {

        CompositeData rc = new CompositeData();
        rc.scheme = uri.getScheme();
        String ssp = stripPrefix(uri.getRawSchemeSpecificPart().trim(), "//").trim();

        parseComposite(uri, rc, ssp);

        rc.fragment = uri.getFragment();
        return rc;
    }

    /**
     * Examine a URI and determine if it is a Composite type or not.
     *
     * @param uri
     * 		The URI that is to be examined.
     *
     * @return true if the given URI is a Compsote type.
     */
    public static boolean isCompositeURI(URI uri) {
        String ssp = stripPrefix(uri.getRawSchemeSpecificPart().trim(), "//").trim();

        if (ssp.indexOf('(') == 0 && checkParenthesis(ssp)) {
            return true;
        }
        return false;
    }

    /**
     * Given a string and a position in that string of an open parend, find the matching close parend.
     *
     * @param str
     * 		The string to be searched for a matching parend.
     * @param first
     * 		The index in the string of the opening parend whose close value is to be searched.
     *
     * @return the index in the string where the closing parend is located.
     * @throws URISyntaxException fi the string does not contain a matching parend.
     */
    public static int indexOfParenthesisMatch(String str, int first) throws URISyntaxException {
        int index = -1;

        if (first < 0 || first > str.length()) {
            throw new IllegalArgumentException("Invalid position for first parenthesis: " + first);
        }

        if (str.charAt(first) != '(') {
            throw new IllegalArgumentException("character at indicated position is not a parenthesis");
        }

        int depth = 1;
        char[] array = str.toCharArray();
        for (index = first + 1; index < array.length; ++index) {
            char current = array[index];
            if (current == '(') {
                depth++;
            } else if (current == ')') {
                if (--depth == 0) {
                    break;
                }
            }
        }

        if (depth != 0) {
            throw new URISyntaxException(str, "URI did not contain a matching parenthesis.");
        }

        return index;
    }

    /**
     * Given a composite URI and a CompositeData instance and the scheme specific part extracted from the source URI,
     * parse the composite URI and populate the CompositeData object with the results.  The source URI is used only
     * for logging as the ssp should have already been extracted from it and passed here.
     *
     * @param uri
     * 		The original source URI whose ssp is parsed into the composite data.
     * @param rc
     * 		The CompsositeData instance that will be populated from the given ssp.
     * @param ssp
     * 		The scheme specific part from the original string that is a composite or one or more URIs.
     *
     * @throws URISyntaxException
     */
    private static void parseComposite(URI uri, CompositeData rc, String ssp) throws URISyntaxException {
        String componentString;
        String params;

        if (!checkParenthesis(ssp)) {
            throw new URISyntaxException(uri.toString(), "Not a matching number of '(' and ')' parenthesis");
        }

        int p;
        int initialParen = ssp.indexOf("(");
        if (initialParen == 0) {

            rc.host = ssp.substring(0, initialParen);
            p = rc.host.indexOf("/");

            if (p >= 0) {
                rc.path = rc.host.substring(p);
                rc.host = rc.host.substring(0, p);
            }

            p = indexOfParenthesisMatch(ssp, initialParen);
            componentString = ssp.substring(initialParen + 1, p);
            params = ssp.substring(p + 1).trim();

        } else {
            componentString = ssp;
            params = "";
        }

        String components[] = splitComponents(componentString);
        rc.components = new URI[components.length];
        for (int i = 0; i < components.length; i++) {
            rc.components[i] = new URI(components[i].trim());
        }

        p = params.indexOf("?");
        if (p >= 0) {
            if (p > 0) {
                rc.path = stripPrefix(params.substring(0, p), "/");
            }
            rc.parameters = parseQuery(params.substring(p + 1));
        } else {
            if (params.length() > 0) {
                rc.path = stripPrefix(params, "/");
            }
            rc.parameters = emptyMap();
        }
    }

    /**
     * Given the inner portion of a composite URI, split and return each inner URI as a string
     * element in a new String array.
     *
     * @param str
     * 		The inner URI elements of a composite URI string.
     *
     * @return an array containing each inner URI from the composite one.
     */
    private static String[] splitComponents(String str) {
        List<String> l = new ArrayList<String>();

        int last = 0;
        int depth = 0;
        char chars[] = str.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            switch (chars[i]) {
            case '(':
                depth++;
                break;
            case ')':
                depth--;
                break;
            case ',':
                if (depth == 0) {
                    String s = str.substring(last, i);
                    l.add(s);
                    last = i + 1;
                }
                break;
            default:
            }
        }

        String s = str.substring(last);
        if (s.length() != 0) {
            l.add(s);
        }

        String rc[] = new String[l.size()];
        l.toArray(rc);
        return rc;
    }

    /**
     * String the given prefix from the target string and return the result.
     *
     * @param value
     * 		The string that should be trimmed of the given prefix if present.
     * @param prefix
     * 		The prefix to remove from the target string.
     *
     * @return either the original string or a new string minus the supplied prefix if present.
     */
    public static String stripPrefix(String value, String prefix) {
        if (value.startsWith(prefix)) {
            return value.substring(prefix.length());
        }
        return value;
    }

    /**
     * Strip a URI of its scheme element.
     *
     * @param uri
     * 		The URI whose scheme value should be stripped.
     *
     * @return The stripped URI value.
     * @throws URISyntaxException
     */
    public static URI stripScheme(URI uri) throws URISyntaxException {
        return new URI(stripPrefix(uri.getSchemeSpecificPart().trim(), "//"));
    }

    /**
     * Given a key / value mapping, create and return a URI formatted query string that is valid and
     * can be appended to a URI.
     *
     * @param options
     * 		The Mapping that will create the new Query string.
     *
     * @return a URI formatted query string.
     * @throws URISyntaxException
     */
    public static String createQueryString(Map<String, ? extends Object> options) throws URISyntaxException {
        try {
            if (options.size() > 0) {
                StringBuffer rc = new StringBuffer();
                boolean first = true;
                for (String key : options.keySet()) {
                    if (first) {
                        first = false;
                    } else {
                        rc.append("&");
                    }
                    String value = (String)options.get(key);
                    rc.append(URLEncoder.encode(key, "UTF-8"));
                    rc.append("=");
                    rc.append(URLEncoder.encode(value, "UTF-8"));
                }
                return rc.toString();
            } else {
                return "";
            }
        } catch (UnsupportedEncodingException e) {
            throw (URISyntaxException)new URISyntaxException(e.toString(), "Invalid encoding").initCause(e);
        }
    }

    /**
     * Creates a URI from the original URI and the remaining parameters.
     *
     * When the query options of a URI are applied to certain objects the used portion of the query options needs
     * to be removed and replaced with those that remain so that other parts of the code can attempt to apply the
     * remainder or give an error is unknown values were given.  This method is used to update a URI with those
     * remainder values.
     *
     * @param originalURI
     *		The URI whose current parameters are remove and replaced with the given remainder value.
     * @param params
     * 		The URI params that should be used to replace the current ones in the target.
     *
     * @return a new URI that matches the original one but has its query options replaced with the given ones.
     * @throws URISyntaxException
     */
    public static URI createRemainingURI(URI originalURI, Map<String, String> params) throws URISyntaxException {
        String s = createQueryString(params);
        if (s.length() == 0) {
            s = null;
        }
        return createURIWithQuery(originalURI, s);
    }

    /**
     * Given a URI value create and return a new URI that matches the target one but with the scheme value
     * supplied to this method.
     *
     * @param bindAddr
     * 		The URI whose scheme value should be altered.
     * @param scheme
     * 		The new scheme value to use for the returned URI.
     *
     * @return a new URI that is a copy of the original except that its scheme matches the supplied one.
     * @throws URISyntaxException
     */
    public static URI changeScheme(URI bindAddr, String scheme) throws URISyntaxException {
        return new URI(scheme, bindAddr.getUserInfo(), bindAddr.getHost(), bindAddr.getPort(), bindAddr
            .getPath(), bindAddr.getQuery(), bindAddr.getFragment());
    }

    /**
     * Examine the supplied string and ensure that all parends appear as matching pairs.
     *
     * @param str
     * 		The target string to examine.
     *
     * @return true if the target string has valid parend pairings.
     */
    public static boolean checkParenthesis(String str) {
        boolean result = true;
        if (str != null) {
            int open = 0;
            int closed = 0;

            int i = 0;
            while ((i = str.indexOf('(', i)) >= 0) {
                i++;
                open++;
            }
            i = 0;
            while ((i = str.indexOf(')', i)) >= 0) {
                i++;
                closed++;
            }
            result = open == closed;
        }
        return result;
    }
}
