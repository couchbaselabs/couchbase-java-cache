/*
 * Copyright (c) 2014 Couchbase, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
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
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.jcache.spi.CouchbaseCachingProvider;

/**
 * The Couchbase implementation for the {@link CacheManager}.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @since 1.0
 */
public class CouchbaseCacheManager implements CacheManager {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(CouchbaseCacheManager.class);

    private final CouchbaseCachingProvider provider;
    private final URI uri;
    private final WeakReference<ClassLoader> classLoader;
    private final Properties properties;
    private final Map<String, Cache> caches;

    private volatile boolean isClosed;

    /* package protected*/
    final Cluster cluster;

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

        this.cluster = CouchbaseCluster.create(provider.getEnvironment());
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
        CouchbaseConfiguration<K, V> couchbaseConfiguration = (CouchbaseConfiguration<K, V>) configuration;

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
                //get the configuration
                Configuration<?, ?> configuration = cache.getConfiguration(CompleteConfiguration.class);
                //check the key type
                Class<?> actualKeyType = configuration.getKeyType();
                Class<?> actualValueType = configuration.getValueType();
                if (actualKeyType == null || !actualKeyType.equals(keyType)) {
                    throw new ClassCastException("Requested key type " + keyType.getName()
                        + " incompatible with " + cacheName + "'s key type: " + actualKeyType);
                }
                if (actualValueType == null || !actualValueType.equals(valueType)) {
                    throw new ClassCastException("Request value type " + valueType.getName()
                        + " incompatible with " + cacheName + "'s value type: " + actualValueType);
                }
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
                Configuration<?, ?> configuration = cache.getConfiguration(CompleteConfiguration.class);
                Class<?> actualKeyType = configuration.getKeyType();
                Class<?> actualValueType = configuration.getValueType();
                if (actualKeyType.equals(Object.class)
                        && actualValueType.equals(Object.class)) {
                    //no runtime type checking
                    return (Cache<K, V>) cache;
                } else {
                    throw new IllegalArgumentException("Cache " + cacheName + " was defined with runtime type checking"
                        + " as a Cache<" + actualKeyType.getName() + ", " + actualValueType.getName() + ">");
                }
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

        //close the cluster
        try {
            Boolean disconnected = this.cluster.disconnect();
            if (!disconnected) {
                LOGGER.warn("Cluster disconnect failed (returned false)");
            }
        } catch (Exception e) {
            LOGGER.error("Cluster disconnect failed", e);
        }

        //signal the provider that this cachemanager is no longer active so that
        // future requests for a similar cachemanager don't return this one.
        provider.signalCacheManagerClosed(this.getClassLoader(), this.getURI());

        List<Cache> cachesToClose;
        synchronized (caches) {
            cachesToClose = new ArrayList<Cache>(caches.values());
            caches.clear();
        }

        for (Cache cache : cachesToClose) {
            try {
                cache.close();
            } catch (Exception e) {
                LOGGER.error("Error while closing a managed Cache", e);
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

    /*package*/ Cluster getCluster() {
        return cluster;
    }
}