---
title: "Apollo Version Migration Guide"
slug: /migration-guide
---

       

Version Migration Guide
=======================

Overview
--------

This guide helps you migrate to a new Apollo version and perserve the message data stored in the previous version of the Apollo broker. This guide assumes you are only using a single virtual host in your configuration.

Standard Migration Steps
------------------------

You should first check to see if a “data export/import” is required by consulting the Migration Notes section at the end of this docuemnt.

1.  Stop your broker instance.
    
2.  If a data export/import is required then export your broker instance data to an archive file by running:
        ```
        ${broker-path}/bin/apollo-broker store-export /tmp/broker-export.tgz
        ```
    Then move your old data directory out of the way by running:
         ```
         mv ${broker-path}/data /tmp/broker-data
         ```
3.  Install your new apollo version. Since Apollo packages include the version in thier insallation directroy you should be able to unpack it to same directory your previous vesion was unpack into.
    
4.  Switch yout broker instance to use the new Apollo version by editing the `${broker-path}/bin/apollo-broker` script and changing the line that sets the `APOLLO_HOME=` variable. Set it to the location of where the new version is installed.
    
5.  If a data export/import is required then import your broker instance data from the archive by running:
        ```
        ${broker-path}/bin/apollo-broker store-import /tmp/broker-export.tgz
        ```
6.  Start your broker.
    

Migration Notes
---------------

*   **1.0 → 1.1** : A data export/import is required if your using a LevelDB store.