# sqlatom

`sqlatom` is a Clojure and Babashka library that stores atoms in a SQLite database:

``` clojure
;; deps.edn
{:deps {io.github.filipesilva/sqlatom {:git/tag "v1.2.0" :git/sha "dd30a20"}}}
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

Use `sqlatom/keys` to get the set of all saved keys. This is useful for instrospection and maintenance (e.g. unit test fixtures).
You can then use `sqlatom/atom` to get their values, or `sqlatom/remove` to remove keys.

``` clojure
@(sqlatom/atom :state {})   ;=> {}
(sqlatom/keys)              ;=> #{:state}
(sqlatom/remove :state)     ;=> nil
(sqlatom/keys)              ;=> #{}
```

Both `sqlatom/keys` and `sqlatom/remove` support the `:dir` option.
`sqlatom/list` is a deprecated alias for `sqlatom/keys` that returns a vector instead of a set.
Using an existing `sqlatom` that was removed will throw an an error, but resume working normally if you recreate it using the same key.


## Comparison with duratom and eve

[duratom](https://github.com/jimpil/duratom) is a more established library for durable atoms.
[eve](https://github.com/SeniorCareMarket/eve) provides shared-memory persistent data structures, including cross-process atoms backed by memory-mapped files.
Here's how they differ:

| | sqlatom | duratom | eve |
|---|---|---|---|
| **Cross-process swap safety** | Yes, uses SQL compare-and-set with versioned rows | No, uses an in-memory lock, so concurrent processes can clobber each other | Yes, uses lock-free compare-and-set on a root pointer in shared memory |
| **Storage backends** | SQLite only | PostgreSQL, SQLite, S3, Redis, filesystem, file.io | Custom memory-mapped files, or in-memory |
| **Consistency** | Strong, every read/write goes through the database | Eventual by default (async writes), optional sync mode | Strong, writes are visible to other processes immediately via shared memory |
| **Write cost** | Writes the whole value, ~80ms for 20mb | Writes the whole value | Copies only the changed path of the tree, ~1ms regardless of size |
| **Metadata** | Value metadata kept, including nested. Atom metadata on Clojure only | Value metadata kept on the top-level collection only. Atom metadata supported | Not supported, value metadata is dropped and atoms have no metadata |
| **Readers** | EDN with the process's `*data-readers*`, so custom tagged literals work | EDN with built-in readers for sorted collections and queues. Custom readers or serializers via `:rw` | None, a fixed set of types including symbols, UUIDs and dates. Records and sorted collections become plain maps and sets, other types throw on Clojure and become `nil` on ClojureScript |
| **Platforms** | Clojure, Babashka | Clojure | Clojure, Babashka, ClojureScript on Node.js and browser (in-memory only) |
| **Scope** | Minimal, atoms only, no configuration beyond `:dir` | Feature-rich, custom serializers, error handlers, sync/async modes, `duragent` | Large, persistent maps, vectors, sets, lists and more, with its own allocator and garbage collection. No watches or validators on persistent atoms |

Choose `sqlatom` if you need safe cross-process swaps with a simple API. Choose `duratom` if you need multiple storage backends or its additional features. Choose `eve` if you need fast writes to large values, or ClojureScript support.


## Babashka

The following operations are not supported in Babashka:
- `add-watch`, `remove-watch`
- `set-validator!`, `get-validator`
- `meta`, `alter-meta!`, `reset-meta!`

This is being tracked in https://github.com/babashka/babashka/issues/1931

In Babashka the following [limits](https://github.com/dakrone/cheshire/pull/219) apply:
- ~20mb printed edn size
- 1000 number length
- 1000 nesting limit


## Performance

On my machine, [criterium](https://github.com/hugoduncan/criterium) measured 82ms (±1ms) for a `reset!` and 81ms (±1ms) for a `swap!` of 20mb worth of EDN from a Datascript backup.
