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
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import {FontAwesomeIcon} from '@fortawesome/react-fontawesome';
import {faDownload, faInfoCircle, faBook} from '@fortawesome/free-solid-svg-icons';
import styles from './index.module.css';
import {
    nmsAmqpCurrentPrefixes,
    nmsAmqpReleases,
    nmsApiCurrentPrefixes,
    nmsApiReleases,
} from '@site/src/data/nmsReleases';
import {formatDate} from '@site/src/utils/formatDate';
import {currentReleases} from '@site/src/utils/releases';

type ComponentCard = {
    title: string;
    subtitle: string;
    description: ReactNode;
    moreUrl: string;
};

const componentCards: ComponentCard[] = [
    {
        title: 'Apache.NMS.AMQP',
        subtitle: 'AMQP 1.0 the ISO and OASIS Standard Messaging Protocol',
        description: (
            <>
                Apache.NMS.AMQP provides AMQP 1.0 connectivity with .NET Standard 2.0 support,
                enabling connectivity to ActiveMQ 5.x, ActiveMQ Artemis and any other AMQP 1.0
                compatible broker.
            </>
        ),
        moreUrl: '/components/nms/providers/amqp',
    },
    {
        title: 'Apache.NMS.ActiveMQ',
        subtitle: "ActiveMQ 5.x's native OpenWire protocol",
        description: (
            <>
                Apache.NMS.ActiveMQ provides OpenWire connectivity using ActiveMQ's native
                protocol, with .NET Framework support.
            </>
        ),
        moreUrl: '/components/nms/providers/activemq',
    },
];

type LatestRelease = {
    title: string;
    shortDescription?: string;
    releaseDate: string;
    url: string;
};

const apiRelease = currentReleases(nmsApiReleases, nmsApiCurrentPrefixes)[0];
const amqpRelease = currentReleases(nmsAmqpReleases, nmsAmqpCurrentPrefixes)[0];

const latestReleases: LatestRelease[] = [
    {
        title: `Apache.NMS ${apiRelease.version} Release`,
        shortDescription: apiRelease.shortDescription,
        releaseDate: formatDate(apiRelease.releaseDate, 'long'),
        url: '/components/nms/nms-api-downloads',
    },
    {
        title: `Apache.NMS.AMQP ${amqpRelease.version} Release`,
        shortDescription: amqpRelease.shortDescription,
        releaseDate: formatDate(amqpRelease.releaseDate, 'long'),
        url: `/components/nms/providers/amqp/downloads/${amqpRelease.slug}`,
    },
];

function ComponentSection({title, subtitle, description, moreUrl}: ComponentCard) {
    return (
        <section className={styles.component}>
            <div className="container">
                <div className={styles.componentCard}>
                    <h3>{title}</h3>
                    <h4>{subtitle}</h4>
                    <p>{description}</p>
                    <div className={styles.componentButtons}>
                        <Link className={clsx('button button--secondary button--md', styles.decoratedButtons)} to={moreUrl}>
                            Find out more <FontAwesomeIcon icon={faInfoCircle} />
                        </Link>
                    </div>
                </div>
            </div>
        </section>
    );
}

function ReleaseCard({title, shortDescription, releaseDate, url}: LatestRelease) {
    return (
        <div className={styles.releaseCard}>
            <h6><Link to={url}>{title}</Link></h6>
            <p>{shortDescription} <Link to={url}>...more</Link></p>
            <p className={styles.releaseDate}>{releaseDate}</p>
        </div>
    );
}

export default function NmsHome() {
    return (
        <Layout title="NMS Clients" description=".NET Messaging API">
            <header className={clsx('hero', styles.heroBanner)}>
                <div className="container">
                    <div className={styles.heroContent}>
                        <div className={styles.heroText}>
                            <h1 className="hero__title">NMS Clients</h1>
                            <p className={styles.heroTagline}>.NET Messaging API</p>
                            <div className={styles.componentButtons}>
                                <Link className="button button--secondary button--md" to="/components/nms/download">
                                    Download <FontAwesomeIcon icon={faDownload} />
                                </Link>
                                <Link className="button button--secondary button--md" to="/components/nms/documentation">
                                    Read the Docs <FontAwesomeIcon icon={faBook} />
                                </Link>
                            </div>
                        </div>
                        <div className={styles.heroLogo}>
                            <img src={useBaseUrl('/img/activemq_logo_white_vertical.png')} alt="Apache ActiveMQ" />
                        </div>
                    </div>
                </div>
            </header>
            <main>
                <section className={styles.description}>
                    <div className="container">
                        <h3>Simple Messaging API for .NET</h3>
                        <p>
                            The NMS API allows you to build .NET applications in C#, VB, or any other .NET
                            language, using a single API to connect to multiple different providers using a
                            JMS style API.
                        </p>
                    </div>
                </section>

                <section className={styles.latestReleases}>
                    <div className="container">
                        <div className={styles.releaseCards}>
                            {latestReleases.map((props, idx) => (
                                <ReleaseCard key={idx} {...props} />
                            ))}
                        </div>
                    </div>
                </section>

                {componentCards.map((props, idx) => (
                    <ComponentSection key={idx} {...props} />
                ))}
            </main>
        </Layout>
    );
}
