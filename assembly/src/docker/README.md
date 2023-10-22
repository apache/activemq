<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->
# Apache ActiveMQ docker

## Installation

Install the most recent stable version of docker
https://docs.docker.com/installation/

Install the most recent stable version of docker-compose
https://docs.docker.com/compose/install/

If you want to build multi-platform (OS/Arch) Docker images, then you must install
[`buildx`](https://docs.docker.com/buildx/working-with-buildx/).
On macOS, an easy way to install `buildx` is to install [Docker Desktop Edge](https://docs.docker.com/docker-for-mac/edge-release-notes/).

## Build

Images are based on the Docker official [Eclipse Temurin 11 JRE](https://hub.docker.com/_/eclipse-temurin/tags?page=1&name=11-jre) image. If you want to
build the ActiveMQ image you have the following choices:

1. Create the docker image from a local distribution package
2. Create the docker image from an Apache ActiveMQ archive, for example (apache-activemq-5.18.1.tar.gz)
3. Create the docker image from a specific version of Apache ActiveMQ
4. Create the docker image from remote or local custom Apache ActiveMQ distribution

If you run `build.sh` without arguments then you could see how to usage this command.

```bash
Usage:
  build.sh --from-local-dist [--archive <archive>] [--image-name <image>] [--build-multi-platform <comma-separated platforms>]
  build.sh --from-release --activemq-version <x.x.x> [--image-name <image>] [--build-multi-platform <comma-separated platforms>]
  build.sh --help

  If the --image-name flag is not used the built image name will be 'activemq'.
  Check the supported build platforms; you can verify with this command: docker buildx ls
  The supported platforms (OS/Arch) depend on the build's base image, in this case [eclipse-temurin:11-jre](https://hub.docker.com/_/eclipse-temurin).
```

To create the docker image from local distribution) you can execute the command
below. Remember that before you can successfully run this command, you must build
the project (for example with the command `mvn clean install -DskipTests`).

```bash
./build.sh --from-local-dist
```

For create the docker image from the local dist version but with the archive,
you can execute the below command. Remember that before you can successfully run
this command.

```bash
./build.sh --from-local-dist --archive ~/path/to/apache-activemq-5.18.1.tar.gz
```

You can also specify the image name with the `--image-name` flag, for example
(replacing the version, image name, and targets as appropriate):

```bash
./build.sh --from-local-dist --archive ~/Downloads/apache-activemq-5.18.1.tar.gz --image-name myrepo/myamq:x.x.x
```

If you want to build the docker image for a specific version of ActiveMQ
you can run `build.sh` command in this way (replacing the version, image name,
and targets as appropriate):

```bash
./build.sh --from-release --activemq-version 5.18.1 --image-name myrepo/myamq:x.x.x
```

If you want to build the container for a specific version of ActiveMQ and
specific version of the platform, and push the image to the Docker Hub repository,
you can use this command (replacing the version, image name, and targets as appropriate):

```bash
./build.sh --from-release --activemq-version 5.18.1 --image-name myrepo/myamq:x.x.x \
 --build-multi-platform linux/arm64,linux/arm/v7,linux/amd64
```

Below is the output you should get from running the previous command.

```
Downloading apache-activemq-5.18.1.tar.gz from https://downloads.apache.org/activemq/5.18.1/
Checking if buildx installed...
Found buildx {github.com/docker/buildx v0.3.1-tp-docker 6db68d029599c6710a32aa7adcba8e5a344795a7} on your docker system
Starting build of the docker image for the platform linux/arm64,linux/arm/v7,linux/amd64
[+] Building 15.8s (16/16) FINISHED
...
```

## Run

* Run ActiveMQ

```
docker-compose run activemq activemq
```

or

```
docker run --name activemq activemq activemq
```

* Run ActiveMQ as a daemon

```
docker-compose up
```

or

```
docker run --name activemq
```

* Kill ActiveMQ

```
docker-compose kill
```

or

```
docker kill activemq
```

### Ports

* ActiveMQ web console on `8161`
* ActiveMQ tcp connector on `61616`
* ActiveMQ AMQP connector on `5672`
* ActiveMQ STOMP connector on `61613`
* ActiveMQ MQTT connector on `1883`
* ActiveMQ WS connector on `61614`

Edit the `docker-compose.yml` file to edit port settings.