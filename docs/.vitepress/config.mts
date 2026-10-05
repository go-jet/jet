import { defineConfig } from 'vitepress'

// Project site (https://<user>.github.io/jet/) needs base '/jet/'.
// Override with DOCS_BASE (e.g. '/' for a custom domain or user/org site).
const base = process.env.DOCS_BASE ?? '/jet/'

export default defineConfig({
  title: 'Jet',
  description: 'Type safe SQL builder with code generation and automatic query result data mapping for Go.',
  base,
  lang: 'en-US',
  cleanUrls: true,
  lastUpdated: true,

  head: [
    ['link', { rel: 'icon', type: 'image/png', href: `${base}mascot.png` }],
  ],

  themeConfig: {
    logo: '/mascot.png',

    nav: [
      { text: 'Guide', link: '/guide/getting-started' },
      { text: 'Statements', link: '/statements/' },
      { text: 'FAQ', link: '/faq' },
      { text: 'pkg.go.dev', link: 'https://pkg.go.dev/github.com/go-jet/jet/v2' },
    ],

    sidebar: [
      {
        text: 'Guide',
        items: [
          { text: 'Getting Started', link: '/guide/getting-started' },
          { text: 'Generator', link: '/guide/generator' },
          { text: 'Model', link: '/guide/model' },
          { text: 'SQL Builder', link: '/guide/sql-builder' },
          { text: 'Expressions', link: '/guide/expressions' },
          { text: 'Query Result Mapping (QRM)', link: '/guide/qrm' },
        ],
      },
      {
        text: 'Statements',
        items: [
          { text: 'Overview', link: '/statements/' },
          { text: 'SELECT', link: '/statements/select' },
          { text: 'Sub-query', link: '/statements/subquery' },
          { text: 'SELECT_JSON', link: '/statements/select-json' },
          { text: 'INSERT', link: '/statements/insert' },
          { text: 'UPDATE', link: '/statements/update' },
          { text: 'DELETE', link: '/statements/delete' },
          { text: 'LOCK', link: '/statements/lock' },
          { text: 'WITH', link: '/statements/with' },
          { text: 'VALUES', link: '/statements/values' },
        ],
      },
      {
        text: 'FAQ',
        link: '/faq',
      },
    ],

    outline: { level: [2, 3] },

    socialLinks: [
      { icon: 'github', link: 'https://github.com/go-jet/jet' },
    ],

    editLink: {
      pattern: 'https://github.com/go-jet/jet/edit/master/docs/:path',
      text: 'Edit this page on GitHub',
    },

    search: {
      provider: 'local',
    },

    footer: {
      message: 'Released under the Apache 2.0 License.',
      copyright: 'Copyright © go-jet contributors',
    },
  },

  markdown: {
    // Wiki content uses `golang` fences in places; alias to go for highlighting.
    languageAlias: { golang: 'go' },
  },
})
