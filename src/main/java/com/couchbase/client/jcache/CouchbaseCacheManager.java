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
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

    @Override
    public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C configuration)
            throws IllegalArgumentException {
        if (isClosed()) {
            throw new IllegalStateException("CacheManager closed");
        }

        return null;
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
        if (isClosed()) {
            throw new IllegalStateException("CacheManager closed");
        }
        return null;
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName) {
        if (isClosed()) {
            throw new IllegalStateException("CacheManager closed");
        }
        return null;
    }

    @Override
    public Iterable<String> getCacheNames() {
        if (isClosed()) {
            throw new IllegalStateException("CacheManager closed");
        }
        return Collections.unmodifiableSet(caches.keySet());
    }

    @Override
    public synchronized void destroyCache(String cacheName) {
        if (isClosed()) {
            throw new IllegalStateException("CacheManager closed");
        }
    }

    @Override
    public void enableManagement(String cacheName, boolean enabled) {
        if (isClosed()) {
            throw new IllegalStateException("CacheManager closed");
        }
    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {
        if (isClosed()) {
            throw new IllegalStateException("CacheManager closed");
        }
    }

    @Override
    public void close() {
        if (isClosed()) {
            return;
        }

        this.isClosed = true;
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

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return (T) this;
        }
        throw new IllegalArgumentException("Not of class " + clazz.getName());
    }
}
