/*
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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;

public class TransportValidationUtils {

    public static final Set<String> DENIED_TRANSPORT_SCHEMES = Set.of("vm", "http", "https",
            "multicast", "zeroconf", "discovery", "fanout", "mock", "peer", "failover",
            "proxy", "reliable", "simple", "udp", "masterslave");

    /**
     * Used to validate externally provided url is not using a denied scheme
     *
     * @param uriString
     * @throws URISyntaxException
     */
    public static void validateAllowedUrl(String uriString) throws URISyntaxException {
        validateAllowedUri(new URI(uriString));
    }

    /**
     * Used to validate externally provided URI is not using a denied scheme
     *
     * @param uri
     * @throws URISyntaxException
     */
    public static void validateAllowedUri(URI uri) throws URISyntaxException {
        validateAllowedUri(uri, 0);
    }

    // Validate the URI does not contain a denied transport scheme
    private static void validateAllowedUri(URI uri, int depth) throws URISyntaxException {
        // Don't allow more than 5 nested URIs to prevent blowing the stack
        if (depth > 5) {
            throw new IllegalArgumentException("URI can't contain more than 5 nested composite URIs");
        }

        // First check the main URI scheme
        validateAllowedScheme(uri.getScheme());

        // We need to check if the URI is composite and/or contains nested URIs
        // The utility method URISupport#isCompositeURI is not good enough here
        // because it misses if there are no parentheses and also is primarily meant
        // for checking comma separated URIs and not nested URIs.
        //
        // The best way to handle all cases is to use the same logic that the transports
        // use to process the URIs and that is to simply attempt to parse it and check each
        // of the parsed components. This wll correctly handle the case when there
        // are parentheses and also when the parentheses are skipped.
        final URISupport.CompositeData data;
        try {
            data = URISupport.parseComposite(uri);
        } catch (URISyntaxException e) {
            // If this is not a valid URI then we can stop checking
            // This can happen when parsing a nested URI and at the last portion
            return;
        }

        if (data.getComponents() != null) {
            depth++;
            for (URI component : data.getComponents()) {
                // Each URI could be a nested and/or composite URI so call validateAllowedUri()
                // to validate it. If the scheme is null then the original URI is not composite
                // or nested so we can skip the check, and we are finished.
                if (component.getScheme() != null) {
                    validateAllowedUri(component, depth);
                }
            }
        }
    }

    // Check all denied schemes
    private static void validateAllowedScheme(String scheme) {
        for (String denied : DENIED_TRANSPORT_SCHEMES) {
            // The schemes should be case-insensitive but ignore case as a precaution
            if (scheme.equalsIgnoreCase(denied)) {
                throw new IllegalArgumentException("Transport scheme '" + scheme + "' is not allowed");
            }
        }
    }
}
