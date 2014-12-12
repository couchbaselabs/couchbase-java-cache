couchbase-java-cache
====================

This is a work in progress JCache implementation for the Couchbase Java SDK 2.0.

See JSR-107 [specification](https://docs.google.com/document/d/1MZQstO9GJo_MUMy5iD5sxCrstunnQ1f85ekCng8LcqM/edit?usp=sharing), [api](https://github.com/jsr107/jsr107spec) and [reference implementation](https://github.com/jsr107/RI).

Basic operations are implemented for now.

## What's partially there / missing
 * Limited tracking of statistics
 * Expiration is managed by the underlying couchbase bucket
 * No support for listeners
 * No support for EntryProcessors
 * No locking, so the "atomic operations" of the specification should not yet be considered as such
 * For now, write-through is not implemented and read-through is only implemented in Get

## How to use
The CouchbaseCacheManager keeps a reference to a Couchbase Cluster under the wire. It will create Caches mapped to Couchbase Buckets.
By default, a "jcache" bucket (password "jcache") is used and expected. Cache data stored inside this bucket will by default have their key prefixed with the name of the cache and an underscore.

One can tune this per-cache mapping using the `CouchbaseConfiguration.builder()` builder.

## Obtaining a Cache, implementation-specific parameters
In JCache, a `CachingProvider` is resolved and used to obtain a `CacheManager`, in turn used to create `Cache`s. You will probably need to reconfigure the CachingProvider (in our implementation, a `CouchbaseCachingProvider`) to modify the cluster to be used.

At least, you should probably give the CachingProvider a bootstrap list of IP/hosts to connect to (instead of the localhost default). This can be done by calling `CouchbaseCachingProvider.setBootstrap(...)` prior to obtaining a CacheManager.

If you already use Couchbase in your application, you should reuse the `CouchbaseEnvironment` on the `CouchbaseCachingProvider`.

After that, obtain a CouchbaseCacheManager and create some Caches! A `CouchbaseConfiguration` tied to the same name as its corresponding `CouchbaseCache` is expected for that, and an `IllegalArgumentException` will be thrown if this is not the case.

## Tests, examples
`BasicCacheIntegrationTest` contains some tests and can serve as an example of creating caches and performing basic operations with them.

Integration tests use the following buckets : "default" and "jcache" (with "jcache" as password). Both should probably be flushed beforehand (at least remove "test" key).
