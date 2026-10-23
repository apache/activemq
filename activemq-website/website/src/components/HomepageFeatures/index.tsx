/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import type {ReactNode} from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  color: string;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Protect your data & Balance your Load',
    color: 'var(--activemq-purple)',
    description: (
      <>
        ActiveMQ provides many advanced features including message load-balancing
        and high-availability for your data. Multiple connected "master" brokers can
        dynamically respond to consumer demand by moving messages between the nodes
        in the background. Brokers can also be paired together in a master-slave
        configuration so that if a master fails then the slave takes over ensuring
        clients can get to their important data and eliminating costly downtime.
      </>
    ),
  },
  {
    title: 'Easy Enterprise Integration Patterns',
    color: 'var(--activemq-pink)',
    description: (
      <>
        Enterprise Integration Patterns describe the various ways in which multiple
        applications generally interact and integrate with each other. Asynchronous
        messaging is at the heart of this integration, and ActiveMQ makes it easy to
        leverage these patterns via Apache Camel routes deployed directly on the broker.
      </>
    ),
  },
  {
    title: 'Flexible Deployment',
    color: 'var(--activemq-orange)',
    description: (
      <>
        ActiveMQ is most commonly deployed as a standalone process. This option
        isolates ActiveMQ from any particular application and provides maximum
        flexibility for resource allocation and management. However, ActiveMQ can be
        configured to have a very small footprint which makes it viable to <em>embed</em> it
        within your application. This option can provide an application with simple,
        powerful messaging semantics and also allow easy message exchange with other
        applications.
      </>
    ),
  },
];

function Feature({title, color, description}: FeatureItem) {
  return (
    <div className={clsx('col col--4')}>
      <div className={styles.featureCard}>
        <Heading as="h3" style={{color}}>{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
