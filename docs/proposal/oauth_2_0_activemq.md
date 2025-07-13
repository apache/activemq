# ActiveMQ Classic OAuth 2.0 High Level Design 

## **Purpose**

In this document, we will mainly discuss the high level design for ActiveMQ to support OAuth 2.0. The goal is to develop an open-source auth plugin for ActiveMQ classic console and direct access, which will allow developers to authenticate and authorize clients using OAuth 2.0.

## **Background**

### Glossary

**Resource Owner:** An entity capable of granting access to a protected resource. When the resource owner is a person, it is referred to as an end-user.

**Resource Server:** The server hosting the protected resources, capable of accepting and responding to protected resource requests using access tokens.

**Client:** An application making protected resource requests on behalf of the resource owner and with its authorization. The term "client" does not imply any particular implementation characteristics (e.g., whether the application executes on a server, a desktop, or other devices).

**Authorization Server:** The server issuing access tokens to the client after successfully authenticating the resource owner and obtaining authorization.

**Third-Party Application:** An application that needs access to a user’s resources, such as a website using Google Login or a developer tool integrating with GitHub authentication.

**Approval Interaction:** The process in which the resource owner grants or denies access to a third-party application, often by clicking an "Allow" button on an OAuth authorization page.

### OAuth 2.0

[OAuth 2.0](https://datatracker.ietf.org/doc/html/rfc6749#page-24) introduces an authorization layer and separates the role of the client from that of the resource owner. The client requests access to resources controlled by the resource owner and hosted by the resource server, and is issued a different set of credentials than those of the resource owner. Instead of using the resource owner's credentials to access protected resources, access tokens are issued to third-party clients by an authorization server. And the client uses the access token to access the protected resources hosted by the resource server.

     +--------+                               +---------------+
     |        |--(A)- Authorization Request ->|   Resource    |
     |        |                               |     Owner     |
     |        |<-(B)-- Authorization Grant ---|               |
     |        |                               +---------------+
     |        |
     |        |                               +---------------+
     |        |--(C)-- Authorization Grant -->| Authorization |
     | Client |                               |     Server    |
     |        |<-(D)----- Access Token -------|               |
     |        |                               +---------------+
     |        |
     |        |                               +---------------+
     |        |--(E)----- Access Token ------>|    Resource   |
     |        |                               |     Server    |
     |        |<-(F)--- Protected Resource ---|               |
     +--------+                               +---------------+

General Protocol Flow


## High level design

We will develop a plugin in `activemq-jaas` similar to how `LDAPLoginModule` is implemented. In this plugin, we will support Client Credentials Grant flow.

     +---------+                                  +---------------+
     |         |                                  |               |
     |         |>--(A)- Client Authentication --->| Authorization |
     |         |                                  |     Server    |
     |         |<--(B)---- Access Token ---------<|               |
     |         |                                  |               |
     |         |                                  +---------------+
     | Client  |
     |         |                                  +---------------+
     |         |(C)--- Establish Connection------>|               |
     |         |          (Access Token)          |   ActiveMQ    |
     |         |<(D)------ Authenticated ---------|    Broker     |
     |         |                                  |               |
     |         |(E)-- Conmunicate Via Endpoint -->|               |
     +---------+                                  +---------------+

(A) The web or JMS client request an access token from the token endpoint.

(B) The authorization server authenticates the client, and if valid, issues an access token.

(C) Clients create connections with the token.

(D) Broker authenticates the request and validates the token.

(E) Clients start connection and communicate with the broker.


### Developer user journey

1. Create users/role/role mappings in token service.

2. Set client in config files.

3. Send an HTTP Request to Get the Token:
    1. Use a POST request to send a token request.
    2. The request should include:
        1. client_id: The client ID.
        2. client_secret: For public clients, this can be empty.
        3. username: The user credentials.
        4. password: The user password .
        5. grant_type: Using the password grant flow (password).
        6. Example request:
            ``` 
            POST /auth/realms/foo/protocol/openid-connect/token HTTP/1.1
            Host: localhost:8080
            Accept: */*
            Content-Length: 89
            Content-Type: application/x-www-form-urlencoded
            
            client_id=test-broker&client_secret=&username=user1&password=user1&grant_type=password
            ```
            
        8. Testing using curl:
            ```
            $ curl -d client_id=test-broker -d client_secret= \
                -d username=user1 -d password=user1 -d grant_type=password \
                localhost:8080/auth/realms/foo/protocol/openid-connect/token
            ```

4. Parse the Response:
    1. Will return a JSON response containing access_token and expires_in.
    2. Extract the access_token, which will be a long string.
    3. Also, parse expires_in, which indicates the token's expiration time in seconds.
    4. Example response:
       ```
       {"access_token":"eyJhbGciOiJSUzI1NiI.....", "expires_in":300, ...}
       ```

5. Use the Token to Connect to the Broker:
    ```
    $ activemq producer --user dummy --password eyJhbGciOiJSUzI1NiI.....
    ```

6. The `OAuthLoginModule` will:
    1. Verify the Token’s Validity:
        1. If the token is invalid (e.g., a wrong token), authentication will fail.
        2. Even with the correct token, authentication will fail after 5 minutes because the token will expire.
    2. Ensure Token Refresh Before Expiry:
        1. The client application needs to refresh the token or request a new one before the token expires.


## JWT Verification Flow

A JWT consists of three parts: <base64url(header)>.<base64url(payload)>.<base64url(signature)>

1. Parse the JWT
    1. Extract header, payload, and signature.
    2. Read the kid field from the header (key ID).

1. Fetch Public Keys (JWKS)
    1. Retrieve the JWKS (JSON Web Key Set) from the identity provider (configured in the plugin configuration)
    2. This provides a list of public RSA keys in JSON format.

1. Cache the JWKS
    1. Don’t fetch on every token.
    2. Cache keys locally (e.g., for 6–24 hours).
    3. Refresh only when kid doesn’t match any cached key.

1. Reconstruct the Signed Data
    1. The signed message is:
        message = base64url(header) + "." + base64url(payload)
    2. Compute: digest = SHA256(message)

1. Verify the Signature: Even if the signature is valid, you should still check:
    1. exp: expiration time
    2. aud: audience (your client ID)
    3. iss: issuer (trusted domain)

