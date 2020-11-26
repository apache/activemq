# The various keystores/truststores here were created with the following commands.
# You can source this file to run it as a sript to regenerate them.

# NOTE: This module isnt a good example of how to generate keypairs and use keystores. This should be replaced,
#       but needs various module-wide changes to the tests and their client/brokers use of keys/certs/trust.

# Clean up existing files:
# ------------------------
rm -f keystore alternative.keystore

# Create a key pair
# -----------------
keytool -storetype jks -keystore keystore -storepass password -keypass password -alias activemq -genkey -keyalg "RSA" -keysize 2048 -dname "O=ActiveMQ,CN=localhost" -validity 9999

# Create an alternative keypair, to allow use in provoking 'failure to trust' it when matched against the above
# ----------------------------------------------------------------------------------------------------------------------
keytool -storetype jks -keystore alternative.keystore -storepass password -keypass password -alias alternative -genkey -keyalg "RSA" -keysize 2048 -dname "O=Alternative,CN=localhost" -validity 9999
