---
title: "Unix Shell Script"
slug: /unix-shell-script
---

Functional overview
===================

From version 5.4.0 onwards, ActiveMQ comes with a enhanced shell script for starting, stopping and managing the broker in Unix environment. For basic [activemq](activemq-classic-command-line-tools-reference) and [activemq-admin](activemq-classic-command-line-tools-reference) scripts functionality take a look at [ActiveMQ Command Line Tools Reference](activemq-classic-command-line-tools-reference).

This `activemq` script incorporate these two scripts into one and adds some new functionalities. On top of regular `activemq` and `activemq-admin` scripts, it adds the following features:

*   **start** - starts the broker in background. It saves process id in the `${ACTIVEMQ_DATA_DIR}/activemq.pid` for future reference. It accepts the same command line arguments like the original `activemq` script, so you can do something like
    ```
    bin/activemq start xbean:conf/activemq-stomp.xml
    ```
*   **console** - starts the broker in foreground. It is basically the replacement for the old `activemq` script  
    (you can interrupt the )
    

*   **stop** - stops the broker that is running in the background. It tries first to stop the broker using [shutdown](activemq-classic-command-line-tools-reference) task. If that task doesn't return in 30 seconds (configurable), it will kill the process.

*   **restart** - stops the running instance (if there's one) and starts the new one

*   **status** - checks if activemq process is running (exitcode: 1 = broker is down, 0 = broker is up)

*   **create** - create an ACTIVEMQ_BASE directory that separates a broker instance from its binaries, i.e., it creates copies of the ActiveMQ configuration files into the command-line specified directory and points to the binaries. You can start the broker with the `/bin` scripts in the newly created instance.
    ```
    bin/activemq create /path/to/brokers/mybroker 
    ```
*   **browse** - Display selected messages in a specified destination
    ```
    bin/activemq browse FOO.BAR # use broker connection from configuration
    bin/activemq browse --amqurl tcp://localhost:61616 FOO.BAR
    ```
*   **bstat** - Performs a predefined query that displays useful statistics regarding the specified broker
    
*   **dstat** - Performs a predefined query that displays useful tabular statistics regarding the specified destination type
    
*   **decrypt** - Decrypts given text
    ```
    bin/activemq decrypt --input 'BkiT42A0CZfL1SanJIgxvQ==' --password asdasdasdasd
    ```
*   **encrypt** - Encrypts given text
    ```
    bin/activemq encrypt --password TESTPASSWORD --input FOOBAR
    ```
*   **export** - Exports a stopped brokers data files to an archive file for importing to another brokers like ActiveMQ Artemis
    ```
    $ bin/activemq export --file=archivefile
    ```
*   **list** - Lists all available brokers in the specified JMX context
    
*   **purge** - Delete selected destination's messages that matches the message selector
    ```
    bin/activemq purge FOO.BAR # use jmx access data from configuration
    bin/activemq purge --jmxurl service:jmx:rmi:///jndi/rmi://127.0.0.1:11098/jmxrmi --jmxuser controlRole --jmxpassword abcd1234 FOO.BAR
    ```
*   **query** - Display selected broker component's attributes and statistics
    ```
    bin/activemq query # use jmx access data from configuration
    bin/activemq query --jmxurl service:jmx:rmi:///jndi/rmi://127.0.0.1:11098/jmxrmi --jmxuser controlRole --jmxpassword abcd1234
    ```

Configuring the init-script
===========================

For ActiveMQ server to run properly some basic system variables must be set. These variables define how much memory broker will have, SSL, logging, JAAS options and similar.

Version 5.11.0 and higher
-------------------------

### Configuration file of the init-script

Since version 5.11.0, all default configuration variables are moved to the new `env` script file, so that they can be easily inspected and configured in a easy way.  
There are also other configuration locations

The script scans the following locations for configurations in the following order:

1.  /etc/default/activemq
2.  $HOME/.activemqrc
3.  $INSTALLDIR/apache-activemq-<version>/bin/env

The first existing file is read exclusively - all other files are ignored.  
Undefined values are discovered in a automated way.

### Versions prior 5.11.0

These parameters were configurable in `activemq` script itself.  
  
You could use `setup` command to create additional system wide defaults file to configure the broker.  
(modify the settings in this file if neccessary - the default entries look a bit garbled due to an accidentally applied bugfix in this release)
```
./activemq setup /etc/default/activemq
```
The configuration of the script can be placed at `/etc/default/activemq` or `$HOME/.activemqrc`. Note, this is the configuration for the _script_ not for configuring ActiveMQ.

To use addtional configurations for running multiple instances on the same operating system rename or symlink script to a name matching to `activemq-instance-<INSTANCENAME>`. This changes the configuration location to `/etc/default/activemq-instance-<INSTANCENAME>` and `$HOME/.activemqrc-instance-<INSTANCENAME>`. Configuration files in /etc have higher precedence.

Running activemq as a unix daemon
---------------------------------

*   Install activemq broker as "root" to be run as "activemq"
    ```
    useradd -m activemq -d /srv/activemq
    cd /srv/activemq
    tar zxvf apache-activemq-<version>-bin.tar.gz$ ln -snf apache-activemq-<version> current
    chown -R activemq:users apache-activemq-<version>
    ```
*   Create a global default configuration and edit the configuration
    ```
    cp apache-activemq-<version>/bin/env /etc/default/activemq
    sed -i '~s/^ACTIVEMQ\_USER=""/ACTIVEMQ\_USER="activemq"/' /etc/default/activemq
    vim /etc/default/activemq
    chmod 644 /etc/default/activemq
    ```
    *   Configure the java heap to a size suitable to your system environment and usage
    *   Consider to move the folders "data", "tmp" and "conf" out of the installation path
*   Install the init script
    ```
    ln -snf  /srv/activemq/current/bin/activemq /etc/init.d/activemq
    ```
*   Activate the init script at system startup  
    (TODO: uncomplete)
    ```
    # RHEL
    chkconfig --add activemq
    chkconfig activemq on
    
    # Debian/Ubuntu
    update-rc.d defaults activemq
    ```

Running multiple instances
--------------------------

To use additional configurations for running multiple instances on the same operating system rename or symlink script to a name matching to `activemq-instance-<INSTANCENAME>`.  
This changes the configuration location to `/etc/default/activemq-instance-<INSTANCENAME>` and `$HOME/.activemqrc-instance-<INSTANCENAME>`. Configuration files in /etc have higher precedence.

Example procedure suitable to the procedure "Running activemq as a unix daemon"

`Example**
```
mkdir /srv/activemq/instance1
cp -av /srv/activemq/current/conf/ /srv/activemq/instance1/
mkdir /srv/activemq/instance1/{data,tmp}
ln -snf  /srv/activemq/current/bin/activemq /etc/init.d/activemq-instance-test1
cp /srv/activemq/install/bin/env /etc/default/activemq-instance-instance-test1
```
Modify the configuration variables in /etc/default/activemq-instance-instance-test1
```
ACTIVEMQ_HOME="/srv/activemq/current/"
ACTIVEMQ_CONF="/srv/activemq/instance1/conf"
ACTIVEMQ_DATA="/srv/activemq/instance1/data"
ACTIVEMQ_TMP="/srv/activemq/instance1/tmp"
```
Control the instance
```
/etc/init.d/activemq-instance1 start|stop|restart|console|....
```
> **Hint**
> 
> If you are using multiple instances you can only add the main instance to the automatic system start using ("update-rc.d" or "chkconfig") because the LSB Header "Provides" needs to be uniq.

Testing and reporting interoperability problems for unix platforms
==================================================================

The activemq repo provides a testsuite to validate the proper operation of the unix shell script on different platforms.

*   Download the testsuite  
    Alternatively clone the sourcecode repo ([Source](source)) and pick the file `assembly/src/test/scripts/init-script-testsuite`
    ```
    cd /tmp
    wget -c "https://git-wip-us.apache.org/repos/asf?p=activemq.git;a=blob_plain;f=assembly/src/test/scripts/init-script-testsuite;hb=HEAD"
    ```
*   Execute the testsuite
    ```
    cd <Installation Dir of the ActiveMQ Script>
    ./activemq stop
    /tmp/init-script-testsuite 2>&1| tee /tmp/test-suite.log
    ```
*   Detailed debug information can be provided by the following procedure
    ```
    export PS4=' ${BASH_SOURCE}:${LINENO}(${FUNCNAME\[0\]}) '
    bash -x ./activemq <argument> 2>&1 |tee /tmp/detailed-debugging.log
    ```
*   Create a ticket and attach the logfiles to the ticket.
    

