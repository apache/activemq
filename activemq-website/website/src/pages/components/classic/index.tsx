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
import {faDownload, faBook} from '@fortawesome/free-solid-svg-icons';
import styles from './index.module.css';
// The page body stays authored as Markdown; the leading underscore keeps the
// pages plugin from turning it into a route of its own.
import Content from './_content.md';
import MDXContent from '@theme/MDXContent';

export default function ClassicHome() {
    return (
        <Layout title="ActiveMQ Classic" description="The Tried and Trusted Open Source Message Broker">
            <header className={clsx('hero', styles.heroBanner)}>
                <div className="container">
                    <div className={styles.heroContent}>
                        <div className={styles.heroText}>
                            <h1 className="hero__title">ActiveMQ</h1>
                            <p className={styles.heroTagline}>The Tried and Trusted Open Source Message Broker</p>
                            <div className={styles.componentButtons}>
                                <Link className="button button--secondary button--md" to="/components/classic/download">
                                    Download <FontAwesomeIcon icon={faDownload} />
                                </Link>
                                <Link className="button button--secondary button--md" to="/components/classic/documentation">
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
                        <MDXContent>
                            <Content />
                        </MDXContent>
                    </div>
                </section>
            </main>
        </Layout>
    );
}
