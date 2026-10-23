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

import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import {FontAwesomeIcon} from '@fortawesome/react-fontawesome';
import {faDownload, faInfoCircle, faBook} from '@fortawesome/free-solid-svg-icons';
import styles from './index.module.css';

const CURRENT_RELEASE = '3.9.5';

export default function CmsHome() {
    return (
        <Layout title="CMS Client" description="C++ Messaging API">
            <header className={clsx('hero', styles.heroBanner)}>
                <div className="container">
                    <div className={styles.heroContent}>
                        <div className={styles.heroText}>
                            <h1 className="hero__title">CMS Client</h1>
                            <p className={styles.heroTagline}>C++ Messaging API</p>
                            <div className={styles.componentButtons}>
                                <Link className="button button--secondary button--md" to="/components/cms/download">
                                    Download <FontAwesomeIcon icon={faDownload} />
                                </Link>
                                <Link className="button button--secondary button--md" to="/components/cms/documentation">
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
                        <p>
                            The CMS API is a JMS-like API for C++ for interfacing with Message Brokers
                            such as Apache ActiveMQ. CMS helps to make your C++ client code much neater
                            and easier to follow. To get a better feel for CMS try the API Reference.
                            ActiveMQ-CPP is a client only library, a message broker such as Apache
                            ActiveMQ is still needed for your clients to communicate.
                        </p>
                    </div>
                </section>

                <section className={styles.latestReleases}>
                    <div className="container">
                        <div className={styles.releaseCards}>
                            <div className={styles.releaseCard}>
                                <h6>
                                    <Link to={`/components/cms/download/${CURRENT_RELEASE.replace(/\./g, '')}-release`}>
                                        ActiveMQ-CPP {CURRENT_RELEASE} Release
                                    </Link>
                                </h6>
                                <p>
                                    The current stable release of the code.{' '}
                                    <Link to={`/components/cms/download/${CURRENT_RELEASE.replace(/\./g, '')}-release`}>
                                        ...more
                                    </Link>
                                </p>
                            </div>
                        </div>
                    </div>
                </section>
            </main>
        </Layout>
    );
}
