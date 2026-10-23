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

import {existsSync} from 'node:fs';
import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const LEGACY_DOCUMENTATION = new Set<string>([
  'using-activemq-classic/configuring-brokers',
  'using-activemq-classic/configuring-transports/configuring-transports',
  'using-activemq-classic/examples',
  'using-activemq-classic/hello-world',
  'using-activemq-classic/initial-configuration',
  'using-activemq-classic/installation',
  'using-activemq-classic/performance-tuning',
  'using-activemq-classic/run-broker',
  'using-activemq-classic/topologies',
  'using-activemq-classic/web-samples',
  'using-activemq-classic/xml-configuration'
]);

function isLegacyDocumentation(sidebarItem): boolean {
  if (sidebarItem.type === 'category') {
    return sidebarItem.link?.type === 'doc'
      && sidebarItem.link.id === 'using-activemq-classic-4/index';
  }
  return sidebarItem.type === 'doc' && LEGACY_DOCUMENTATION.has(sidebarItem.id);
}

function removeLegacyDocumentation(sidebarItems) {
  return sidebarItems
    .filter((item) => !isLegacyDocumentation(item))
    .map((item) =>
      item.type === 'category'
        ? {
            ...item,
            link: item.link?.type === 'doc' && LEGACY_DOCUMENTATION.has(item.link.id) ? undefined : item.link,
            items: removeLegacyDocumentation(item.items),
          }
        : item,
    );
}

const config: Config = {
  title: 'Apache ActiveMQ',
  tagline: 'Flexible & Powerful Open Source Multi-Protocol Messaging',
  favicon: 'img/favicon.png',

  // Overridable so preview builds (e.g. the GitHub Pages workflow) can be served from another location.
  url: process.env.DOCUSAURUS_URL ?? 'https://activemq.apache.org/',
  baseUrl: process.env.DOCUSAURUS_BASE_URL ?? '/',
  // Matches the original Jekyll site: pages are served as /page rather than /page/.
  trailingSlash: false,

  onBrokenLinks: 'throw',
  onBrokenAnchors: 'throw',

  // The Pagefind index only exists after a production build. `pnpm run start:search`
  // copies it to .pagefind so the dev server can serve it at /pagefind.
  staticDirectories:
    process.env.NODE_ENV === 'development' && existsSync('.pagefind')
      ? ['static', '.pagefind']
      : ['static'],

  markdown: {
    // .md files are plain CommonMark, .mdx files may use JSX and imports.
    format: 'detect',
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        // Each component's docs are a docs plugin instance, declared in plugins.
        docs: false,
        blog: {
          routeBasePath: 'news',
          blogTitle: 'News',
          blogDescription: 'The latest Apache ActiveMQ releases, CVEs, and announcements',
          blogSidebarTitle: 'News',
          blogSidebarCount: 'ALL',
          showReadingTime: false,
          // The swizzled BlogListPage renders a single flat list with no paginator.
          postsPerPage: 'ALL',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  plugins: [
    [
      '@docusaurus/plugin-content-pages',
      {
        id: 'classic-releases',
        path: 'src/data/releases/classic',
        routeBasePath: 'components/classic/download',
      },
    ],
    // Documentation for each component is separately configured
    [
      '@docusaurus/plugin-content-docs',
      {
        id: 'classic',
        path: 'docs/classic',
        routeBasePath: 'components/classic/documentation',
        sidebarPath: './docs/classic/sidebars.ts',
        editUrl: 'https://github.com/apache/activemq-website/tree/main/',
        async sidebarItemsGenerator({defaultSidebarItemsGenerator, ...args}) {
          const items = await defaultSidebarItemsGenerator(args);
          return removeLegacyDocumentation(items);
        },
      },
    ],
    [
      '@docusaurus/plugin-content-docs',
      {
        id: 'nms',
        path: 'docs/nms',
        routeBasePath: 'components/nms',
        sidebarPath: './docs/nms/sidebars.ts',
        editUrl: 'https://github.com/apache/activemq-website/tree/main/',
      },
    ],
    [
      '@docusaurus/plugin-content-docs',
      {
        id: 'cms',
        path: 'docs/cms',
        routeBasePath: 'components/cms',
        sidebarPath: './docs/cms/sidebars.ts',
        editUrl: 'https://github.com/apache/activemq-website/tree/main/',
      },
    ],
    // Client-side search
    [
      'docusaurus-plugin-pagefind',
      {
        forceLanguage: 'en',
      },
    ],
  ],

  themeConfig: {
    image: 'img/activemq_logo_black.png',
    navbar: {
      logo: {
        alt: 'ActiveMQ Logo',
        src: 'img/activemq_logo_black_small.png',
      },
      items: [
        // Shown on docs pages only (see src/theme/SearchBar), centered by custom.css.
        {type: 'search', position: 'left', className: 'navbar-search--center'},
        {to: '/news', label: 'News', position: 'right'},
        {
          type: 'dropdown',
          label: 'Components',
          position: 'right',
          items: [
            {
              label: 'ActiveMQ',
              to: '/components/classic',
            },
            {
              label: 'NMS Clients',
              to: '/components/nms',
            },
            {
              label: 'CMS Client',
              to: '/components/cms',
            },
            {
              label: 'Artemis',
              href: 'https://artemis.apache.org',
            },
          ],
        },
        {
          type: 'dropdown',
          label: 'Community',
          position: 'right',
          items: [
            {
              label: 'Contact Us',
              to: '/contact',
            },
            {
              label: 'Contribute',
              to: '/contributing',
            },
            {
              label: 'Report Issues',
              to: '/issues',
            },
            {
              label: 'Get Support',
              to: '/support',
            },
          ],
        },
        {
          type: 'dropdown',
          label: 'ASF',
          position: 'right',
          items: [
            {
              label: 'The Apache Software Foundation',
              href: 'https://www.apache.org/',
            },
            {
              label: 'License',
              href: 'https://www.apache.org/licenses/',
            },
            {
              label: 'Sponsorship',
              href: 'https://www.apache.org/foundation/sponsorship.html',
            },
            {
              label: 'Thanks',
              href: 'https://www.apache.org/foundation/thanks.html',
            },
            {
              label: 'Security',
              to: '/security-advisories',
            },
            {
              label: 'Events',
              href: 'https://www.apache.org/events/current-event',
            },
            {
              label: 'PMC & Committers',
              href: 'https://people.apache.org/phonebook.html?pmc=activemq',
            },
            {
              label: 'Board Reports',
              href: 'https://whimsy.apache.org/board/minutes/ActiveMQ.html',
            },
            {
              label: 'Privacy Policy',
              href: 'https://privacy.apache.org/policies/privacy-policy-public.html',
            },
          ],
        },
        {
          href: 'https://github.com/apache/activemq',
          position: 'right',
          className: 'header-github-link',
          'aria-label': 'GitHub repository',
        },
      ],
    },
    footer: {
      links: [],
      logo: {
        width: 200,
        src: '/img/activemq_logo_white_vertical_small.png',
        href: '/',
        alt: 'Apache ActiveMQ',
      },
      copyright: `<a href="https://www.apache.org/foundation/marks/list/">Apache, ActiveMQ, Apache ActiveMQ</a>, the Apache logo, and the Apache ActiveMQ project logo are trademarks of The Apache Software Foundation. Copyright &copy; ${new Date().getFullYear()}, The Apache Software Foundation. Licensed under <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache License 2.0</a>.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['java', 'markup'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
