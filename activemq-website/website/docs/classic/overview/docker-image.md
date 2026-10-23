---
title: "Using ActiveMQ with Docker"
slug: /docker-image
---

You can also get started with ActiveMQ Docker image in no time. The image is hosted [here](https://hub.docker.com/r/apache/activemq/tags)

Step 1. Pull the image
```
docker pull apache/activemq:latest
```

Step 2. Run the image
```
docker run -p 61616:61616 -p 8161:8161 apache/activemq:latest 
```
Note: Make sure you expose the correct port using `-p`. The command above only expose port 8161 for the web console and port 61616 for the Openwire connection
