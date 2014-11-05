/**
 * Copyright (C) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.client.jcache;

import java.lang.ref.WeakReference;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;

import com.couchbase.client.jcache.spi.CouchbaseCachingProvider;

/**
 * The Couchbase implementation for the {@link CacheManager}.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @since 1.0
 */
public class CouchbaseCacheManager implements CacheManager {

    private final CouchbaseCachingProvider provider;
    private final URI uri;
    private final WeakReference<ClassLoader> classLoader;
    private final Properties properties;
    private final Map<String, Cache> caches;

    private volatile boolean isClosed;

    /**
     * Creates a new CouchbaseCacheManager.
     *
     * @param provider the caching provider used
     * @param uri the uri of the manager
     * @param classLoader the classloader associated with the manager
     * @param properties the properties used by the manager
     */
    public CouchbaseCacheManager(CouchbaseCachingProvider provider, URI uri, ClassLoader classLoader,
                                 Properties properties) {
        this.provider = provider;
        this.uri = uri;
        this.classLoader = new WeakReference<ClassLoader>(classLoader);
        this.properties = properties;
        this.caches = new HashMap<String, Cache>();
        // this.isClosed defaults to false
    }

    @Override
    public CachingProvider getCachingProvider() {
        return provider;
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader.get();
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    /**
     * Create a new Cache programmatically. Note that this implementation requires
     * a {@link CouchbaseConfiguration} as second parameter.
     *
     * @param cacheName the name of the new cache to be created
     * @param configuration the {@link CouchbaseConfiguration} used to configure the cache
     * @throws java.lang.IllegalArgumentException if the configuration is not of the expected type
     * @see javax.cache.CacheManager#createCache(String, javax.cache.configuration.Configuration)
     *
     */
    @Override
    public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C configuration)
            throws IllegalArgumentException {
        if (isClosed()) {
            throw new IllegalStateException("CacheManager closed");
        }
        if (cacheName == null) {
            throw new NullPointerException("Cache name must not be null");
        }
        if (configuration == null) {
            throw new NullPointerException("Configuration must not be null");
        }
        if (!(configuration instanceof CouchbaseConfiguration)) {
            throw new IllegalArgumentException("Configuration must be of type "
                    + CouchbaseConfiguration.class.getName());
        }

        CouchbaseConfiguration<K, V> couchbaseConfiguration = (CouchbaseConfiguration) configuration;

        synchronized (caches) {
            if (caches.containsKey(cacheName)) {
                throw new IllegalArgumentException("Cache " + cacheName + " already exist");
            } else {
                CouchbaseCache<K, V> cache = new CouchbaseCache<K, V>(this, cacheName, couchbaseConfiguration);
                caches.put(cacheName, cache);
                return cache;
            }
        }
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
        if (isClosed()) {
            throw new IllegalStateException("CacheManager closed");
        }
        if (keyType == null || valueType == null) {
            throw new NullPointerException("Key type and value type must be non-null");
        }

        synchronized (caches) {
            Cache<?, ?> cache = caches.get(cacheName);
            if (cache != null) {
                //TODO verify that if the cache exist, its configuration conforms to the asked types (else ClassCastException)
                return (Cache<K, V>) cache;
            } else {
                return null;
            }
        }
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName) {
        if (isClosed()) {
            throw new IllegalStateException("CacheManager closed");
        }

        synchronized (caches) {
            Cache<?, ?> cache = caches.get(cacheName);
            if (cache == null) {
                return null;
            } else {
                //TODO verify that cache configuration has key and value types to Object else IllegalArgumentException
                return (Cache<K, V>) cache;
            }
        }
    }

    @Override
    public Iterable<String> getCacheNames() {
        if (isClosed()) {
            throw new IllegalStateException("CacheManager closed");
        }

        synchronized (caches) {
            Set<String> currentNames = new HashSet<String>(caches.keySet());
            return Collections.unmodifiableSet(currentNames);
        }
    }

    @Override
    public synchronized void destroyCache(String cacheName) {
        if (isClosed()) {
            throw new IllegalStateException("CacheManager closed");
        }
        if (cacheName == null) {
            throw new NullPointerException("cacheName must not be null");
        }
    }

    @Override
    public void enableManagement(String cacheName, boolean enabled) {
        if (isClosed()) {
            throw new IllegalStateException("CacheManager closed");
        }
        if (cacheName == null) {
            throw new NullPointerException("cacheName must not be null");
        }
    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {
        if (isClosed()) {
            throw new IllegalStateException("CacheManager closed");
        }
        if (cacheName == null) {
            throw new NullPointerException("cacheName must not be null");
        }
    }

    @Override
    public void close() {
        if (isClosed()) {
            return;
        }

        this.isClosed = true;
        //signal the provider that this cachemanager is no longer active so that
        // future requests for a similar cachemanager don't return this one.
        provider.signalCacheManagerClosed(this.getClassLoader(), this.getURI());

        List<Cache> cachesToClose;
        synchronized (caches) {
            cachesToClose = new ArrayList(caches.values());
            caches.clear();
        }

        for (Cache cache : cachesToClose) {
            try {
                cache.close();
            } catch (Exception e) {
                //TODO replace with logger
                System.err.println("Error while closing the CacheManager");
                e.printStackTrace();
            }
        }
    }

    @Override
    public boolean isClosed() {
        return this.isClosed;
    }

    /**
     * Allows a cache to signal its manager it has been closed.
     *
     * @param cacheName the name of the closed cache
     */
    /*package*/ void signalCacheClosed(String cacheName) {
        if (cacheName == null) {
            return;
        }
        synchronized (caches) {
            caches.remove(cacheName);
        }
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(this.getClass())) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException("Cannot unwrap to " + clazz.getName());
    }
}