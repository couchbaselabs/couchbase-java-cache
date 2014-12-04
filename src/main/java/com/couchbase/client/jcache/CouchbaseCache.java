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

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.Duration;
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
import com.couchbase.client.java.document.SerializableDocument;
import com.couchbase.client.java.view.AsyncViewResult;
import com.couchbase.client.java.view.AsyncViewRow;
import com.couchbase.client.java.view.DesignDocument;
import com.couchbase.client.java.view.View;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.jcache.management.CouchbaseCacheMxBean;
import com.couchbase.client.jcache.management.CouchbaseStatisticsMxBean;
import com.couchbase.client.jcache.management.ManagementUtil;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Actions;
import rx.functions.Func1;

/**
 * The Couchbase implementation of a @{link Cache}.
 *
 * @author Simon Baslé
 * @since 1.0
 */
public class CouchbaseCache<K, V> implements Cache<K, V> {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(CouchbaseCache.class);

    /** 30 days in seconds */
    private static final long MAX_TTL = 30 * 24 * 60 * 60;
    private static final int TTL_DONT_CHANGE = -2;
    private static final int TTL_EXPIRED = -1;
    private static final int TTL_NONE = 0;

    private final CouchbaseCacheManager cacheManager;
    private final String name;

    private final ExpiryPolicy expiryPolicy;
    private final CouchbaseConfiguration<K, V> configuration;
    private final CacheLoader<K, V> cacheLoader;
    private final CacheWriter<K, V> cacheWriter;
    private final CouchbaseCacheMxBean cacheMxBean;
    private final CouchbaseStatisticsMxBean statisticsMxBean;

    private volatile boolean isClosed;

    private final Bucket bucket;
    private final String keyPrefix;

    /* package scope*/
    <T extends CouchbaseCacheManager> CouchbaseCache(T cacheManager, String name,
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

        this.cacheMxBean = new CouchbaseCacheMxBean(this);
        this.statisticsMxBean = new CouchbaseStatisticsMxBean(this);

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
        if (enabled) {
            ManagementUtil.registerStatistics(this, this.statisticsMxBean);
        } else {
            ManagementUtil.unregisterStatistics(this);
        }
        this.configuration.setStatisticsEnabled(enabled);
    }

    /**
     * Checks the status of statistics.
     *
     * @return true if statistics are enabled in configuration, false otherwise
     */
    private boolean isStatisticsEnabled() {
        return configuration.isStatisticsEnabled();
    }

    /**
     * Allows to enable/disable managemnet via JMX.
     * This will also update the configuration.
     *
     * @param enabled the enabled flag value
     */
    private void setManagementEnabled(boolean enabled) {
        if (enabled) {
            ManagementUtil.registerConfiguration(this, this.cacheMxBean);
        } else {
            ManagementUtil.unregisterConfiguration(this);
        }
        this.configuration.setManagementEnabled(enabled);
    }

    /**
     * Checks the status of management.
     *
     * @return true if management is enabled in configuration, false otherwise
     */
    private boolean isManagementEnabled() {
        return configuration.isManagementEnabled();
    }

    @Override
    public V get(K key) {
        checkOpen();
        long start = (isStatisticsEnabled()) ? System.nanoTime() : 0;
        String cbKey = toInternalKey(key);
        V result = null;

        SerializableDocument doc = bucket.get(cbKey, SerializableDocument.class);
        if(doc != null) {
            if (isStatisticsEnabled()) {
                statisticsMxBean.increaseCacheHits(1L);
            }
            touchIfNeeded(cbKey);
            result = (V) doc.content();
        } else {
            if (isStatisticsEnabled()) {
                statisticsMxBean.increaseCacheMisses(1L);
            }
            if (cacheLoader != null && configuration.isReadThrough()) {
                V loaded = cacheLoader.load(key);
                if (loaded != null && (doc = createDocument(key, loaded, Operation.CREATION)) != null) {
                    bucket.insert(doc);
                    result = loaded;
                }
            }
        }
        if (isStatisticsEnabled()) {
            statisticsMxBean.addGetTimeNano(System.nanoTime() - start);
        }
        return result;
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        checkOpen();
        Map<K, V> result = new HashMap<K, V>(keys.size());
        for (K key : keys) {
            //TODO optimize for bulk loads?
            V value = get(key);
            if (value != null) {
                result.put(key, value);
            }
        }

        return result;
    }

    /**
     * {@inheritDoc}
     *
     * Note that this implementation attempts a load of the document from couchbase.
     * It is more efficient to directly attempt to retrieve the value with get than call containsKey then get in this
     * cache implementation, unless you don't want to trigger statistics and/or read-through (if activated).
     *
     * @param key the key to check for
     * @return true if key is present in cache, false otherwise
     */
    @Override
    public boolean containsKey(K key) {
        checkOpen();

        return bucket.get(String.valueOf(key)) != null;
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        checkOpen();

        throw new UnsupportedOperationException();
    }

    @Override
    public void put(K key, V value) {
        checkOpen();
        checkTypes(key, value);

        try {
            SerializableDocument doc = createDocument(key, value, Operation.CREATION);
            //Only do something if doc is not null (otherwise it means expiry was already set)
            if (doc != null) {
                bucket.upsert(doc);
                if (configuration.isStatisticsEnabled()) {
                    statisticsMxBean.increaseCachePuts(1L);
                }
            }
        } catch (Exception e) {
            throw new CacheException("Error during put of " + key, e);
        }

    }

    @Override
    public V getAndPut(K key, V value) {
        checkOpen();
        checkTypes(key, value);

        String internalKey = toInternalKey(key);
        SerializableDocument oldValue = bucket.get(internalKey, SerializableDocument.class);

        put(key, value);

        if (oldValue == null) {
            if (configuration.isStatisticsEnabled()) {
                statisticsMxBean.increaseCacheMisses(1L);
            }
            return null;
        } else {
            if (configuration.isStatisticsEnabled()) {
                statisticsMxBean.increaseCacheHits(1L);
            }
            return (V) oldValue.content();
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        checkOpen();
        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        checkOpen();
        checkTypes(key, value);
        String internalKey = toInternalKey(key);

        SerializableDocument oldDoc = bucket.get(internalKey, SerializableDocument.class);
        if (oldDoc != null) {
            if (isStatisticsEnabled()) {
                statisticsMxBean.increaseCacheHits(1L);
            }
            return false;
        } else {
            //TODO the key should be locked here and other instances of the cache should not be able to insert it
            if (isStatisticsEnabled()) {
                statisticsMxBean.increaseCacheMisses(1L);
            }
            try {
                SerializableDocument newDoc = createDocument(key, value, Operation.CREATION, oldDoc.cas());
                if (newDoc != null) {
                    bucket.insert(newDoc);
                    if (isStatisticsEnabled()) {
                        statisticsMxBean.increaseCachePuts(1L);
                    }
                    return true;
                } else {
                    return false;
                }
            } catch (Exception e) {
                throw new CacheException("Error during putIfAbsent of " + key);
            }
        }
    }

    @Override
    public boolean remove(K key) {
        checkOpen();
        if (key == null) {
            throw new NullPointerException("Removed key cannot be null");
        }
        String internalKey = toInternalKey(key);
        SerializableDocument oldDoc = bucket.getAndLock(internalKey, 10, SerializableDocument.class);
        if (oldDoc == null) {
            return false;
        } else {
            bucket.remove(oldDoc);
            if (isStatisticsEnabled()) {
                statisticsMxBean.increaseCacheRemovals(1L);
            }
            return true;
        }
    }

    @Override
    public boolean remove(K key, V oldValue) {
        checkOpen();

        throw new UnsupportedOperationException();
    }

    @Override
    public V getAndRemove(K key) {
        checkOpen();

        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        checkOpen();

        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(K key, V value) {
        checkOpen();

        throw new UnsupportedOperationException();
    }

    @Override
    public V getAndReplace(K key, V value) {
        checkOpen();

        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        if (keys == null) {
            throw new NullPointerException("Set of keys cannot be null");
        }
        checkOpen();
        for (K key : keys) {
            remove(key);
        }
    }

    @Override
    public void removeAll() {
        checkOpen();
        internalClear(new Action1<SerializableDocument>() {
                    @Override
                    public void call(SerializableDocument serializableDocument) {
                        //TODO notify CacheEntryRemovedListeners here
                    }
                });
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor,
            Object... arguments) throws EntryProcessorException {
        checkOpen();

        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor,
            Object... arguments) {
        checkOpen();

        throw new UnsupportedOperationException();
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        checkOpen();

        return getAllKeys()
                .flatMap(new Func1<String, Observable<SerializableDocument>>() {
                    @Override
                    public Observable<SerializableDocument> call(String id) {
                        return bucket.async().get(id, SerializableDocument.class);
                    }
                })
                .map(new Func1<SerializableDocument, Entry<K, V>>() {
                    @Override
                    public Entry<K, V> call(SerializableDocument serializableDocument) {
                        return new CouchbaseCacheEntry(serializableDocument.id(), serializableDocument.content());
                    }
                })
                .toBlocking()
                .getIterator();
    }

    private String[] checkAndGetViewInfo() {
        String expectedDesignDoc = configuration.getAllViewDesignDoc();
        String expectedViewName = configuration.getAllViewName(getName());
        Exception cause = null;

        try {
            DesignDocument designDoc = bucket.bucketManager().getDesignDocument(expectedDesignDoc);
            if (designDoc == null) {
                cause = new NullPointerException("Design document " + expectedDesignDoc + " does not exist");
            } else {
                for (View view : designDoc.views()) {
                    if (view.name() == expectedViewName) {
                        return new String[] { expectedDesignDoc, expectedViewName };
                    }
                }
            }
        } catch (Exception e) {
            cause = e;
        }
        throw new CacheException("Cannot find view " + expectedDesignDoc + "/" + expectedViewName
                + " for cache " + getName() + ",did you create it?", cause);
    }

    private Observable<String> getAllKeys() {
        String[] viewInfo = checkAndGetViewInfo();

        return bucket.async()
                     .query(ViewQuery.from(viewInfo[0], viewInfo[1]))
                     .flatMap(new Func1<AsyncViewResult, Observable<AsyncViewRow>>() {
                         @Override
                         public Observable<AsyncViewRow> call(AsyncViewResult asyncViewResult) {
                             return asyncViewResult.rows();
                         }
                     })
                     .map(new Func1<AsyncViewRow, String>() {
                         @Override
                         public String call(AsyncViewRow asyncViewRow) {
                             return asyncViewRow.id();
                         }
                     });
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
        try {
            internalClear(Actions.empty());
        } catch (Exception e) {
            throw new CacheException("Unable to clear", e);
        }
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
     * Convenience method to check status of the cache and throw appropriate exception if it is closed.
     */
    private void checkOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Cache " + name + " closed");
        }
    }

    private V internalGet(String cbKey) {
        SerializableDocument doc = bucket.get(cbKey, SerializableDocument.class);
        if (doc == null) {
            return null;
        }
        return (V) doc.content();
    }

    private void internalPut(String cbKey, V value) {
        SerializableDocument doc = SerializableDocument.create(cbKey, toInternalValue(value));
        bucket.upsert(doc);
    }


    private void internalClear(Action1<? super SerializableDocument> action) {
        getAllKeys()
                .flatMap(new Func1<String, Observable<SerializableDocument>>() {
                    @Override
                    public Observable<SerializableDocument> call(String id) {
                        return bucket.async().remove(id, SerializableDocument.class);
                    }
                })
                .toBlocking()
                .forEach(action);
    }

    private int getDurationCode(Operation op) {
        ExpiryPolicy policy = configuration.getExpiryPolicyFactory().create();
        Duration expiryDuration = Duration.ZERO;
        switch (op) {
            case CREATION:
                expiryDuration = policy.getExpiryForCreation();
                break;
            case UPDATE:
                expiryDuration = policy.getExpiryForUpdate();
                break;
            case ACCESS:
                expiryDuration = policy.getExpiryForAccess();
                break;
        }

        if (expiryDuration == null) {
            return TTL_DONT_CHANGE;
        } else if (expiryDuration.isZero()) {
            return TTL_EXPIRED;
        } else if (expiryDuration.isEternal()) {
            return TTL_NONE;
        } else {
            long longExpiry = expiryDuration.getTimeUnit().toSeconds(expiryDuration.getDurationAmount());
            if (longExpiry <= MAX_TTL) {
                return (int) longExpiry;
            } else {
                throw new IllegalArgumentException("Explicit " + op.name()
                        + " expiry must be less than 30 days (30 * 24 * 60 * 60 = " + MAX_TTL + "sec)");
            }
        }
    }

    private void touchIfNeeded(String docId) {
        int ttlOrCode = getDurationCode(Operation.ACCESS);
        if (ttlOrCode >= 0) {
            bucket.touch(docId, ttlOrCode);
        }
    }

    /**
     * Depending on the operation, produces a SerializableDocument with correct TTL and a CAS of 0.
     *
     * @param key the key for the document
     * @param value the value to store
     * @param op the operation being performed
     * @return the {@link SerializableDocument} to be persisted, or null if the {@link ExpiryPolicy}
     *  indicates a TTL already expired
     * @throws IllegalArgumentException when the {@link ExpiryPolicy} produces a TTL > 30 days
     */
    private SerializableDocument createDocument(K key, V value, Operation op) {
        return createDocument(key, value, op, 0L);
    }

    /**
     * Depending on the operation, produces a SerializableDocument with correct TTL and CAS.
     *
     * @param key the key for the document
     * @param value the value to store
     * @param op the operation being performed
     * @param cas the cas of the document (or 0 if none needed)
     * @return the {@link SerializableDocument} to be persisted, or null if the {@link ExpiryPolicy}
     *  indicates a TTL already expired
     * @throws IllegalArgumentException when the {@link ExpiryPolicy} produces a TTL > 30 days
     */
    private SerializableDocument createDocument(K key, V value, Operation op, long cas) {
        String cbKey = toInternalKey(key);
        Serializable cbValue = toInternalValue(value);
        int ttlOrCode = getDurationCode(op);
        switch (ttlOrCode) {
            case TTL_DONT_CHANGE:
                return SerializableDocument.create(cbKey, cbValue, cas);
            case TTL_NONE:
                return SerializableDocument.create(cbKey, cbValue, cas);
            case TTL_EXPIRED:
                return null;
            default:
                if (ttlOrCode < 0) {
                    throw new IllegalArgumentException("Unknown ttl code " + ttlOrCode);
                } else {
                    return SerializableDocument.create(cbKey, ttlOrCode, cbValue, cas);
                }
        }
    }

    protected String toInternalKey(K key) {
        if (key == null) {
            throw new NullPointerException("Keys must not be null");
        }
        return keyPrefix + String.valueOf(key);
    }

    private Serializable toInternalValue(V value) {
        if (value instanceof Serializable) {
            return (Serializable) value;
        } else {
            throw new ClassCastException("This cache can only accept Serializable values");
        }
    }

    private void checkTypes(K key, V value) {
        Class keyType = key.getClass();
        Class valueType = value.getClass();

        Class confKeyType = configuration.getKeyType();
        Class confValueType = configuration.getValueType();

        if (confKeyType != Object.class && !confKeyType.isAssignableFrom(keyType)) {
            throw new ClassCastException("Keys are required to be of type " + confKeyType.getName());
        }
        if (confValueType != Object.class && !confValueType.isAssignableFrom(valueType)) {
            throw new ClassCastException("Values are required to be of type " + confValueType.getName());
        }

    }

    private static enum Operation {
        CREATION,
        UPDATE,
        ACCESS
    }
}
