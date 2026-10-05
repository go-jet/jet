---
layout: home

hero:
  name: Jet
  text: Type-safe SQL builder for Go
  tagline: Compile-time checked SQL, generated from your database schema, with results mapped into any struct composition. It is not an ORM.
  image:
    src: /mascot.png
    alt: Jet mascot
  actions:
    - theme: brand
      text: Get Started
      link: /guide/getting-started
    - theme: alt
      text: Statements
      link: /statements/
    - theme: alt
      text: GitHub
      link: https://github.com/go-jet/jet

features:
  - icon: 🛡️
    title: Type-safe SQL
    details: Tables, columns and enums are generated from your live schema. Invalid expressions fail at go build, not at runtime.
  - icon: 🧩
    title: Query Result Mapping
    details: Scan joined result sets straight into nested structs and slices. Grouping by primary key is automatic.
  - icon: 🗄️
    title: PostgreSQL, MySQL, SQLite
    details: Also CockroachDB and MariaDB. One builder API per dialect, generated SQL that looks like the SQL you would write by hand.
  - icon: ⚡
    title: Fast and explicit
    details: No reflection-driven magic in query building, optional prepared statement caching, and SELECT_JSON for large joined result sets.
---
