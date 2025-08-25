# ActiveMQ Classic OAuth 2.0 High Level Design

## **Purpose**

In this document, we propose a design for ActiveMQ to support OAuth 2.0. The goal is to develop an open-source auth plugin for ActiveMQ classic console and direct access,
which will allow developers to authenticate and authorize clients using OAuth 2.0. The plugin will accept OAuth 2 Access Tokens in the JWT format and will authenticate these tokens
by validating their signature and claims. A set of plugin configurations is proposed, together with a way to authorize access by mapping OAuth 2 `scope` claims to ActiveMQ Roles.

## **Background**

### Glossary

1. **Resource Owner:** An entity capable of granting access to a protected resource. When the resource owner is a person, it is referred to as an end-user.
2. **Resource Server:** The server hosting the protected resources, capable of accepting and responding to protected resource requests using access tokens.
3. **Client:** An application making protected resource requests on behalf of the resource owner and with its authorization. The term "client" does not imply any particular implementation characteristics (e.g., whether the application executes on a server, a desktop, or other devices).
4. **Authorization Server:** The server issuing access tokens to the client after successfully authenticating the resource owner and obtaining authorization.
5. **Third-Party Application:** An application that needs access to a user’s resources, such as a website using Google Login or a developer tool integrating with GitHub authentication.
6. **JWT**: JSON Web Token. A string representing an encoded and signed JSON object containing set of claims (see [RFC 7519](https://datatracker.ietf.org/doc/html/rfc7519]))
7. **Claim**: A piece of information asserted about a subject. A claim is represented as a name/value pair consisting of a Claim Name and a Claim Value.
8. **JWKS**: JSON Web Key Set. A JSON document containing a set of cryptographic keys. (see [RFC 7517](https://www.rfc-editor.org/rfc/rfc7517.txt)) 
9. **Access Token**: A string representing an authorization issued to a client. (see [RFC 6749](https://datatracker.ietf.org/doc/html/rfc6749#page-10))
10. **Refresh Token**: Credentials issued to the client by the authorization server used to obtain a new access token when the current access token becomes invalid or expires.

### OAuth 2.0

[OAuth 2.0](https://datatracker.ietf.org/doc/html/rfc6749#page-24) introduces an authorization layer and separates the role of the client from that of the resource owner. The client requests access to resources controlled by the resource owner and hosted by the resource server, and is issued a different set of credentials than those of the resource owner. Instead of using the resource owner's credentials to access protected resources, access tokens are issued to third-party clients by an authorization server. And the client uses the access token to access the protected resources hosted by the resource server.

     +--------+                               +---------------+
     |        |--(1)- Authorization Request ->|   Resource    |
     |        |                               |     Owner     |
     |        |<-(2)-- Authorization Grant ---|               |
     |        |                               +---------------+
     |        |
     |        |                               +---------------+
     |        |--(3)-- Authorization Grant -->| Authorization |
     | Client |                               |     Server    |
     |        |<-(4)----- Access Token -------|               |
     |        |                               +---------------+
     |        |
     |        |                               +---------------+
     |        |--(5)----- Access Token ------>|    Resource   |
     |        |                               |     Server    |
     |        |<-(6)--- Protected Resource ---|               |
     +--------+                               +---------------+

General Protocol Flow


## High level design

We will develop a plugin in `activemq-jaas` similar to how `LDAPLoginModule` is implemented. In this plugin, we will support Client Credentials Grant flow.

     +---------+                                  +---------------+
     |         |                                  |               |
     |         |>--(1)- Client Authentication --->| Authorization |
     |         |                                  |     Server    |
     |         |<--(2)---- Access Token ---------<|               |
     |         |                                  |               |
     |         |                                  +---------------+
     | Client  |
     |         |                                  +---------------+
     |         |(3)--- Establish Connection------>|               |
     |         |          (Access Token)          |   ActiveMQ    |
     |         |<(4)------ Authenticated ---------|    Broker     |
     |         |                                  |               |
     |         |(5)-- Communicate Via Endpoint -->|               |
     +---------+                                  +---------------+

1. The web or JMS client request an access token from the token endpoint.
2. The authorization server authenticates the client, and if valid, issues an access and refresh token pair.
3. Clients create connections with the access token.
4. Broker authenticates the request and validates the access token.
5. Clients start connection and communicate with the broker.

This document focuses on how ActiveMQ should validate the OAuth access token (step 4).

### Scope

The document's scope encompasses defining the following:

1. Support for OAuth Access Tokens which:
    1. Must be in a valid JWT format ([RFC 7519](https://datatracker.ietf.org/doc/html/rfc7519))
    2. Should follow the JWT Profile for OAuth 2.0 Access Tokens ([RFC 9068](https://datatracker.ietf.org/doc/html/rfc9068))
2. OAuth Access Tokens validations by verifying the token signature using a public key obtained from the Authorization Server's "jwks_uri" ([RFC 8414](https://datatracker.ietf.org/doc/html/rfc8414))
3. OAuth Access Tokens validations by verifying a set of claims against pre-defined configuration values.
4. Exceptions to be returned to clients in order to communicate failures to validate the token.
5. Plugin configurations to be provided to the ActiveMQ broker.
6. Authorization mechanism to map OAuth 2 scope claims to ActiveMQ Groups.

### Out of Scope

The following items are not part of this document's scope:

1. Make use of Token Introspection ([RFC 7662](https://datatracker.ietf.org/doc/html/rfc7662)) or OpenID Connect (OIDC) APIs.
2. Validate "opaque" OAuth Access Tokens, tokens which are not in a JWT format or validating tokens against an Authorization Server.
3. Propose a sign-in flow using a third party Authorization Server in the ActiveMQ Web Console.
4. Support for non-standard claims.
5. Detect revoked tokens (or detecting "signed out" users whose token is still not expired)
6. Refresh expired OAuth Access Tokens

### Developer user journey

1. Create users/role/role mappings in token service.

2. Set client in config files.

3. Send an HTTP Request to Get the Token:
    1. Use a POST request to send a token request.
    2. The request should include:
        1. client_id: The client ID.
        2. client_secret: For public clients, this can be empty.
        3. username: The user credentials.
        4. password: The user password.
        5. grant_type: Using the password grant flow (password).
        6. Example request:
            ``` 
            POST  https://us-west-xyz.auth.us-west-2.amazoncognito.com/oauth2/token HTTP/1.1
            Host: localhost:8080
            Accept: */*
            Content-Length: 89
            Content-Type: application/x-www-form-urlencoded
            
            client_id=test-broker&client_secret=&username=user1&password=user1&grant_type=password
            ```

        7. Testing using curl:
             ```
             $ curl -d client_id=test-broker -d client_secret= \
                 -d username=user1 -d password=user1 -d grant_type=password \
                 https://us-west-xyz.auth.us-west-2.amazoncognito.com/oauth2/token
             ```

4. Parse the Response:
    1. Will return a JSON response containing access_token and expires_in.
    2. Extract the access_token, which will be a long string.
    3. Also, parse expires_in, which indicates the token's expiration time in seconds.
    4. Example response:
       ```
       {"access_token":"eyJhbGciOiJSUzI1NiI.....", "expires_in":300, ...}
       ```

5. Use the Token as the password field to Connect to the Broker:
    ```
    $ activemq producer --user dummy --password eyJhbGciOiJSUzI1NiI.....
    ```

6. The `OAuthLoginModule` will:
    1. Verify the Token’s Validity:
        1. If the token is invalid (e.g., a wrong token), authentication will fail.
        2. Even with the correct token, authentication will fail after 5 minutes because the token will expire.
        3. If the token claims are not the ones expected by the Broker (based on the plugin configuration), authentication will fail.

    2. Ensure Token Refresh Before Expiry:
        1. The client application needs to refresh the token or request a new one before the token expires.

## Detailed Design

### Plugin configuration

The OAuth Plugin will be configured using the following Broker configurations:

```
<broker>
    ...
    <authorizationPlugin>
        <map>
            <oauthAuthorizationMap
                <!--
                The Authorization Server issuer URL. The  token's "iss" claim must match this value in order for the token to be authorized.
                This is a MANDATORY attribute.
                -->
                issuer="https://dev-test-application.us.auth0.com/"
                
                <!--
                The URL where to obtain the Authorization Server public keys from. These keys are used to validate the Access Token signature.
                This is an OPTIONAL attribute. In case it is not informed, it will be assumed to be "${issuer}/.well-known/jwks.json" 
                -->
                jwks_uri="https://dev-test-application.us.auth0.com/.well-known/jwks.json"
                
                <!--
                A String identifying the ActiveMQ Broker. If specified, the token's "aud" claim must match this value in order for the token
                to be authorized. This is an OPTIONAL attribute. In case it is not informed, the OAuth Plugin will not validate the Access Token's
                "aud" claim.
                -->
                audience="likelyBrokerName"
            > <!-- Scope Based authorization configuration follows ... --> </oauthAuthorizationMap>
        </map>
    </authorizationPlugin>
    ...
</broker>
```

Notes:

1. The `audience` claim validation could be mandatory, but some Authorization Servers (such as AWS Cognito) do not include the `aud` claim in their tokens.
   Technically making them non-compliant with the "OAuth 2.0 Access Tokens" JWT profile, but it is still interesting to support them.

### Scope-based groups

The OAuth Plugin must use the Access Token's `scope` claim to obtain the corresponding groups which the token bearer belongs to, according to the mapping provided as part of the plugin
configuration:

```
<broker>
    ...
    <authorizationPlugin>
        <map>
            <oauthAuthorizationMap ... >
                <!-- Map an OAuth 2 scope claim value to an ActiveMQ Group. --> 
                <oauthScopeEntry 
                    <!-- The OAuth 2 scope name. This attribute is MANDATORY -->
                    scope="default-m2m-resource-server/read" 
                    <!-- One or more Group names, separated by comma. This attribute is MANDATORY -->
                    groups="guests" />
                
                <oauthScopeEntry scope="default-m2m-resource-server/write" groups="users,admins" />
            </oauthAuthorizationMap>
            
            <!-- ActiveMQ Authorization Map using the previous defined groups -->
            <authorizationMap> 
                <authorizationEntries> 
                  <authorizationEntry queue="TEST.Q" read="users" write="users" admin="users" /> 
                  <authorizationEntry topic="ActiveMQ.Advisory.>" read="*" write="*" admin="*"/> 
                </authorizationEntries>
            </authorizationMap> 
        </map>
    </authorizationPlugin>
    ...
</broker>
```

If the token does not contain any `scope` claim then the OAuth plugin must not resolve any Group.

### Jaas `LoginModule` details

The OAuth plugin must populate a `javax.security.auth.Subject` object with:

1. Exactly one `org.apache.activemq.jaas.UserPrincipal`, obtained using the following procedure:
    1. If the token contains a `sub` (Subject) claim then use its value as the UserPrincipal's name
    2. Else, if the client informed a username then use its value as the UserPrincipal's name
    3. Else use an empty string as the UserPrincipal's name
2. Zero or more `org.apache.activemq.jaas.GroupPrincipal`, obtained based on the `oauthScopeEntry` configurations.


### Authentication Exceptions

The Broker must return exceptions on authentication failures with enough information for the client to identify if a client-side
action is necessary (such as refreshing the Access Token), but not enough to expose Broker information or internal state. The following
exceptions will be thrown by the OAuth plugin:

1. `javax.security.auth.login.CredentialExpiredException`: Thrown when the OAuth Access Token is expired (based on the `exp` claim).
2. `javax.security.auth.login.FailedLoginException`: Thrown when any other part of the authorization failed, including:
    1. The token has an invalid format (failed to parse it as a JWT)
    2. The token has an invalid signature
    3. The `kid` present in the token header was not found in the JWKS
    4. The OAuth plugin failed to fetch the JWKS
    5. The token was missing a necessary claim or the claim had an unexpected value

Details on why the token authentication failed may be logged by the Broker, detailed information must not be returned to the client.

## JWT Verification Flow

A JWT consists of three parts: <base64url(header)>.<base64url(payload)>.<base64url(signature)>.

The OAuth Plugin will execute the following procedure in order to authentication an OAuth Access token:

1. Parse the JWT
    1. Extract header, payload, and signature.
    2. Read the `kid` (key ID) field from the header.

2. Fetch Public Keys (JWKS)
    1. Retrieve the JWKS (JSON Web Key Set) from the Identity Provider URL provided in the plugin configuration.
    2. Obtain the public key which matches the `kid` value present in the JWT header.
    3. If the JWT's `kid` does not match any public key then throw a `FailedLoginException` exception.
    4. If it is not possible to fetch the JWKS document then throw a `FailedLoginException` exception.

3. Cache the JWKS
    1. Don’t fetch on every token.
    2. Locally cache keys in-memory. Cached keys are discarded when the Broker is shutdown.
    3. Refresh only when `kid` doesn’t match any cached key.

4. Reconstruct the Signed Data
    1. The signed message is:
       message = base64url(header) + "." + base64url(payload)
    2. Compute: digest = SHA256(message)
    3. Verify the Signature.

5. Validate the following OAuth Token Claims:
    1. `exp`: The Token's Expiration Time must be in the future in relation to the Broker's time.
    2. `iss`: The Token's Issuer claim must match the expected Issuer set in the plugin configuration.
    3. `aud`: If an expected Audience is set in the plugin configuration, the Token's Audience must match this value.

## Appendix A - Examples of OAuth 2.0 Access Tokens

1. An Auth0 Access Token

```
Header:
{
  "alg": "RS256",
  "typ": "JWT",
  "kid": "abc123"
}

Payload:
{
  "iss": "https://dev-xyz123.us.auth0.com/",
  "sub": "abc123@clients",
  "aud": "https://dev-xyz123.us.auth0.com/api/v2/",
  "iat": 123456789,
  "exp": 123456900,
  "gty": "client-credentials",
  "azp": "abcde1234fghi567"
}
```

2. An AWS Cognito Access Token

```
Header:
{
  "kid": "abc/abc123=",
  "alg": "RS256"
}

Payload:
{
  "sub": "abc123",
  "token_use": "access",
  "scope": "default-m2m-resource-server-xyz/read",
  "auth_time": 123456789,
  "iss": "https://cognito-idp.us-west-2.amazonaws.com/us-west-2_xyz",
  "exp": 123456999,
  "iat": 123456789,
  "version": 2,
  "jti": "abc-123-def-456",
  "client_id": "abc123"
}
```