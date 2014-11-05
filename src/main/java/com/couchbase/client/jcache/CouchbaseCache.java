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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.java.Bucket;

/**
 * The Couchbase implementation of a @{link Cache}.
 *
 * @author Simon Baslé
 * @since 1.0
 */
public class CouchbaseCache<K, V> implements Cache<K, V> {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(CouchbaseCache.class);

    private final CouchbaseCacheManager cacheManager;
    private final String name;

    private final ExpiryPolicy expiryPolicy;
    private final CouchbaseConfiguration<K, V> configuration;
    private final CacheLoader<K, V> cacheLoader;
    private final CacheWriter<K, V> cacheWriter;

    private volatile boolean isClosed;

    private final Bucket bucket;
    private final String keyPrefix;

    /*package*/ <T extends CouchbaseCacheManager> CouchbaseCache(T cacheManager, String name,
            CouchbaseConfiguration<K, V> conf) {
        this.cacheManager = cacheManager;
        this.name = name;
        //make a local copy of the configuration for this cache
        this.configuration = new CouchbaseConfiguration<K, V>(conf);

        if (this.configuration.getCacheLoaderFactory() != null) {
            this.cacheLoader = this.configuration.getCacheLoaderFactory().create();
        } else {
            this.cacheLoader = null;
        }
        if (this.configuration.getCacheWriterFactory() != null) {
            this.cacheWriter = (CacheWriter<K, V>) this.configuration.getCacheWriterFactory().create();
        } else {
            this.cacheWriter = null;
        }

        this.expiryPolicy = this.configuration.getExpiryPolicyFactory().create();

        //TODO create management and statistics MXBeans here

        this.isClosed = false;

        if (configuration.isManagementEnabled()) {
            setManagementEnabled(true);
        }
        if (configuration.isStatisticsEnabled()) {
            setStatisticsEnabled(true);
        }

        this.keyPrefix = configuration.getCachePrefix();
        this.bucket = cacheManager.getCluster().openBucket(configuration.getBucketName(),
                configuration.getBucketPassword());
    }

    /**
     * Allows to enable/disable statistics via JMX.
     * This will also update the configuration.
     *
     * @param enabled the new status of statistics
     */
    private void setStatisticsEnabled(boolean enabled) {
        //TODO : register/deregister the MXBean
        this.configuration.setStatisticsEnabled(enabled);
    }

    /**
     * Allows to enable/disable managemnet via JMX.
     * This will also update the configuration.
     *
     * @param enabled
     */
    private void setManagementEnabled(boolean enabled) {
        //TODO : register/deregister the MXBean
        this.configuration.setManagementEnabled(enabled);
    }

    @Override
    public V get(K key) {
        checkOpen();

        return null;
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        checkOpen();

        return null;
    }

    @Override
    public boolean containsKey(K key) {
        checkOpen();

        return false;
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        checkOpen();

    }

    @Override
    public void put(K key, V value) {
        checkOpen();


    }

    @Override
    public V getAndPut(K key, V value) {
        checkOpen();

        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        checkOpen();


    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        checkOpen();

        return false;
    }

    @Override
    public boolean remove(K key) {
        checkOpen();

        return false;
    }

    @Override
    public boolean remove(K key, V oldValue) {
        checkOpen();

        return false;
    }

    @Override
    public V getAndRemove(K key) {
        checkOpen();

        return null;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        checkOpen();

        return false;
    }

    @Override
    public boolean replace(K key, V value) {
        checkOpen();

        return false;
    }

    @Override
    public V getAndReplace(K key, V value) {
        checkOpen();

        return null;
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        checkOpen();


    }

    @Override
    public void removeAll() {
        checkOpen();


    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor,
            Object... arguments) throws EntryProcessorException {
        checkOpen();

        return null;
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor,
            Object... arguments) {
        checkOpen();

        return null;
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {

    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {

    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        checkOpen();

        return null;
    }


    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        if (clazz.isInstance(this.configuration)) {
            return clazz.cast(this.configuration);
        }
        throw new IllegalArgumentException("Configuration class " + clazz.getName() + " not supported");
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(this.getClass())) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException("Cannot unwrap to " + clazz.getName());
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public CacheManager getCacheManager() {
        return this.cacheManager;
    }

    @Override
    public void clear() {
        checkOpen();

    }

    @Override
    public synchronized void close() {
        if (!isClosed) {
            this.isClosed = true;

            //disable statistics and management
            setStatisticsEnabled(false);
            setManagementEnabled(false);

            //close the configured CacheLoader
            if (cacheLoader instanceof Closeable) {
                try {
                    ((Closeable) cacheLoader).close();
                } catch (IOException e) {
                    Logger.getLogger(this.getName()).log(Level.WARNING, "Problem closing CacheLoader "
                            + cacheLoader.getClass(), e);
                }
            }

            //close the configured CacheWriter
            if (cacheWriter instanceof Closeable) {
                try {
                    ((Closeable) cacheWriter).close();
                } catch (IOException e) {
                    Logger.getLogger(this.getName()).log(Level.WARNING, "Problem closing CacheWriter "
                            + cacheWriter.getClass(), e);
                }
            }

            //close the configured ExpiryPolicy
            if (expiryPolicy instanceof Closeable) {
                try {
                    ((Closeable) expiryPolicy).close();
                } catch (IOException e) {
                    Logger.getLogger(this.getName()).log(Level.WARNING, "Problem closing ExpiryPolicy "
                            + expiryPolicy.getClass(), e);
                }
            }

            //signal the CacheManager that this cache is closed
            this.cacheManager.signalCacheClosed(getName());

            //close the corresponding bucket
            try {
                if (!this.bucket.close()) {
                    LOGGER.warn("Could not close bucket for cache " + getName() + " (returned false)");
                }
            } catch (Exception e) {
                LOGGER.error("Could not close bucket for cache " + getName(), e);
            }
            //TODO other cleanup operations needed to close the cache properly
        }
    }

    @Override
    public boolean isClosed() {
        return this.isClosed;
    }

    /**
     * Convenience method to check status of the cache and throw appropriate exception if it is closed
     */
    private void checkOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Cache " + name + " closed");
        }
    }
}
