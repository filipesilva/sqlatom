# sqlatom

`sqlatom` is a Clojure and Babashka library that stores atoms in a SQLite database:

``` clojure
;; deps.edn
{:deps {io.github.filipesilva/sqlatom {:git/tag "v1.1.0" :git/sha "d0d3628"}}}
```

## Usage

``` clojure
(ns app
  (:require [filipesilva.sqlatom :as sqlatom]))

(defonce state (sqlatom/atom :state {}))
```

This will create a `sqlatom/atoms.db` in the project root if there isn't one yet, then initialize `:state` as `{}` if there is no value for it yet, or read the existing value for `:state`.

All atom operations are supported, with the following semantics:
- `swap!`, `compare-and-set!`, `swap-vals!` have transaction semantics and are safe to use between atoms/threads/processes/dialects
- `deref` will read from the database if the value has been updated since last read
- `add-watch` watchers see updates from other atoms only when reading/updating, and will not be called for unseen updates

Values are stored as edn, and use the readers for the current process.
You should add `sqlatom/` to your `.gitignore` unless you want to commit the state.


## Options

`sqlatom` supports the existing `:meta` and `:validator` [atom options](https://clojuredocs.org/clojure.core/atom).

You can also pass in a `:dir` option after the value to change it from the `sqlatom` default:

``` clojure
(sqlatom/atom :tmp-state {} :dir "/tmp/sqlatom")
```


## Helpers

Use `sqlatom/list` to get a list of all saved keys. This is useful for instrospection and maintenance (e.g. unit test fixtures).
You can then use `sqlatom/atom` to get their values, or `sqlatom/remove` to remove keys.

``` clojure
(sqlatom/list)              ;=> (:state)
@(sqlatom/atom :state nil)  ;=> {}
(sqlatom/remove :state)     ;=> nil
(sqlatom/list)              ;=> ()
```

Both `sqlatom/list` and `sqlatom/remove` support the `:dir` option.
Using an existing `sqlatom` that was removed will throw an an error, but resume working normally if you recreate it using the same key.


## Comparison with duratom

[duratom](https://github.com/jimpil/duratom) is a more established library for durable atoms. Here's how the two differ:

| | sqlatom | duratom |
|---|---|---|
| **Cross-process swap safety** | Yes, uses SQL compare-and-set with versioned rows | No, uses an in-memory lock, so concurrent processes can clobber each other |
| **Storage backends** | SQLite only | PostgreSQL, SQLite, S3, Redis, filesystem, file.io |
| **Consistency** | Strong, every read/write goes through the database | Eventual by default (async writes), optional sync mode |
| **Scope** | Minimal, atoms only, no configuration beyond `:dir` | Feature-rich, custom serializers, error handlers, sync/async modes, `duragent` |

Choose `sqlatom` if you need safe cross-process swaps with a simple API. Choose `duratom` if you need multiple storage backends or its additional features.


## Babashka

The following operations are not supported in Babashka:
- `add-watch`, `remove-watch`
- `set-validator!`, `get-validator`
- `meta`, `alter-meta!`, `reset-meta!`

This is being tracked in https://github.com/babashka/babashka/issues/1931

EDN size is limited to ~20mb in Babashaka.


## Performance

On my machine I measured 314ms for a `reset!` and 649ms for a `swap!` of 20mb worth of EDN from a Datascript backup.
