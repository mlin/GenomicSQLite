# Language Bindings Guide

Thank you for considering a contribution to the language bindings available for the Genomics Extension!

## Overview

The extension is a C++11 library, and C/C++ programs compile against it in the usual way to invoke the routines outlined in the Programming Guide. But for other programming languages, another option should make it **unnecessary to use a C/C++ foreign-function interface**.

The key routines are also exposed as [custom SQL functions](https://www.sqlite.org/appfunc.html), which can be invoked by SELECT statements on any SQLite connection, once the extension has been loaded. New language bindings consist largely of ~one-liner functions that pass arguments through to `SELECT routine(arg1,arg2,...)` and return the result, usually a TEXT value. None are performance-sensitive, as long as developers use prepared, parameterized SQLite3 statements for GRI queries in loops.

Bindings should endeavor to integrate "naturally" with the host language and its existing SQLite3 bindings. For example, the object returned by the Open procedure should be an idiomatic SQLite3 connection object. Also, APIs should follow the host language's conventions for naming style and optional/keyword arguments.

 Our [Python module](https://github.com/mlin/GenomicSQLite/blob/main/bindings/python/genomicsqlite/__init__.py) can be followed as an illustrative example.

## Locating & loading the extension library

The module should first locate the extension shared-library file (e.g. `libgenomicsqlite.so` on Linux), but it doesn't actually load it; instead, it *tells SQLite to load it*. This can occur either during global module initialization or on the first connection attempt.

1. Unless the `GENOMICSQLITE_SYSTEM_LIBRARY` environment variable is truthy, prefer first a platform-appropriate library file shipped with the bindings, if you choose to do so (see Packaging below). For development, start with prebuilt binaries from [GitHub Releases](https://github.com/mlin/GenomicSQLite/releases).
2. If none is available, then use some appropriate helper function (e.g. Python's [`ctypes.util.find_library()`](https://docs.python.org/3/library/ctypes.html#finding-shared-libraries)) to look through the usual system paths for shared libraries. Or, just fall back to "libgenomicsqlite" to let SQLite look for it using `dlopen()`. 
3. Use the language SQLite3 bindings to open a connection to a [`:memory:` database](https://www.sqlite.org/inmemorydb.html), which will be used just for these initialization operations.
4. On the connection, [enable extension loading](https://www.sqlite.org/c3ref/enable_load_extension.html) and perform the equivalent of [`sqlite3_load_extension()`](https://www.sqlite.org/c3ref/load_extension.html) on the found library path.

The extension needs to be loaded only once per process: upon first loading, it [registers itself](https://www.sqlite.org/c3ref/auto_extension.html) to activate automatically on each new connection opened.

## GenomicSQLite Open

The Open method should "look like" the language's existing wrapper around `sqlite3_open()`, taking similar arguments and returning the same type of connection object. It uses two routines from the GenomicSQLite library, exposed as SQL functions, which help with activating the compression layer and tuning various settings.

Given a database filename, the method follows these steps:

#### 1. Generate connection string with `SELECT genomicsqlite_uri(dbfilename, config_json)`

This helper generates a text [URI](https://www.sqlite.org/uri.html) based on the given filename, which tells SQLite how to open it with the compression layer activated. Call it on your `:memory:` connection.

`config_json` is the text of a JSON object containing keys and values for the several tuning options shown in the Programming Guide. Any supplied settings are merged into a hard-coded default JSON, so it's only necessary to specify values that need to be overridden, and fine to pass `'{}'` if there are none. The Open method should allow the caller to supply these optional arguments in some linguistically natural way, then take care of formulating the JSON text.

You can access the hard-coded defaults with `SELECT genomicsqlite_default_config_json()`, which may be useful to determine the available keys. (For example, the Python bindings use this to distinguish the GenomicSQLite options from other optional arguments meant to be passed through to SQLite.) 

#### 2. Call `sqlite3_open()` using the connection string

Give the URI connection string to the normal SQLite open method. You must set the `SQLITE_OPEN_URI` flag or equivalent for this to work.

The Open method should pass other flags based on optional arguments in the same way as the existing SQLite3 wrapper. If the caller requested a read-only connection, Open can either set `SQLITE_OPEN_READONLY` or append `&mode=ro` to the connection string.

#### 3. Generate tuning script with `SELECT genomicsqlite_tuning_sql(config_json)`

Through the new connection, pass the same `config_json` described above to this helper. The helper doesn't actually do anything to the database; it merely generates text of an SQL script (semicolon-separated statements) for you to execute.

#### 4. Execute tuning script on the new connection

The existing SQLite3 bindings probably expose some method to imperatively execute the semicolon-separated SQL script in one shot.

#### 5. Return the connection object

At this point the connection is ready to go, and it should not be necessary to wrap it.

## Other routines

The other routines are much simpler. The binding for each just takes its required and optional arguments, passes them through to a `SELECT routine(...)` statement on the caller-supplied connection object, and returns the single text answer.

* `SELECT genomicsqlite_vacuum_into_sql(destfilename, config_json)`
* `SELECT create_genomic_range_index_sql(tableName, chromosome, beginPosition, endPosition[, floor])`: floor is an integer, others text.
* `SELECT genomic_range_rowids_sql(tableName[, qrid, qbeg, qend][, ceiling[, floor]])` ceiling and floor are integers.
* `SELECT put_reference_assembly_sql(assembly)`
* `SELECT put_reference_sequence_sql(name, length[, assembly, refget_id, meta_json, rid])` length and rid are integers.

Optional text arguments can default to NULL, and optional integers can default to -1.

The bindings for **Get Reference Sequences by Rid** just read the `_gri_refseq` table like,

```
SELECT 
    _gri_rid,
    gri_refseq_name, gri_refseq_length,
    gri_assembly, gri_refget_id,
    gri_refseq_meta_json
FROM _gri_refseq
```

and loads the results into some linguistically-natural data structure that'll provide quick lookup of those attributes by rid. Then, **Get Reference Sequences by Name** can simply call that and "invert" the results.

## Packaging

Our [GitHub Releases](https://github.com/mlin/GenomicSQLite/releases) supply prebuilt extension binaries intended to be compatible with most modern hosts. There are at least three packaging options:

1. **Bundle nothing:** require the end user to download the right library file (or build it themselves) and place it where the bindings and SQLite3 will be able to find them, as described above.

2. **Bundle our binaries:** ship our binaries inside your package and choose the right one to load at runtime. Remember, the only actual ABI linking occurs between the extension library and SQLite3 itself, so there's nothing to worry about compatibility between the library and the host language runtime.

3. **Source build:** You can specify the CMake-based source build procedure & its dependencies described in the [GitHub README](https://github.com/mlin/GenomicSQLite) however the host language's packaging system does it. 

## Documentation

The [MkDocs](https://www.mkdocs.org/) source Markdown for this documentation site is [in the GitHub repository](https://github.com/mlin/GenomicSQLite/tree/main/docs). Fork it & send us a pull request, adding appropriate examples to all the code tabs in the Installation & Programming Guide.
