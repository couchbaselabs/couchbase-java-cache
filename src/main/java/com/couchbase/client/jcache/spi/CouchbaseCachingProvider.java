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
package com.couchbase.client.jcache.spi;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;

import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.jcache.CouchbaseCacheManager;

/**
 * The Couchbase implementation of a {@link CachingProvider}.
 * This implementation does not support {@link OptionalFeature optional features} of the JSR-107.
 *
 * @author Simon Baslé
 * @since 1.0
 */
public class CouchbaseCachingProvider implements CachingProvider {

    private WeakHashMap<ClassLoader, Map<URI, CacheManager>> managersByClassLoader;
    private CouchbaseEnvironment env;

    public CouchbaseCachingProvider(CouchbaseEnvironment environment) {
        this.managersByClassLoader = new WeakHashMap<ClassLoader, Map<URI, CacheManager>>();
        //TODO validate this way to pass a full-fledged ClusterEnvironment
        this.env = environment;
    }

    public CouchbaseCachingProvider() {
        this(DefaultCouchbaseEnvironment.create());
    }

    @Override
    public ClassLoader getDefaultClassLoader() {
        return this.getClass().getClassLoader();
    }

    @Override
    public URI getDefaultURI() {
        try {
            return new URI(this.getClass().getName());
        } catch (URISyntaxException e) {
            throw new CacheException("Unable to create default URI for Couchbase JCache implementation");
        }
    }

    @Override
    public Properties getDefaultProperties() {
        return new Properties();
    }

    public CouchbaseEnvironment getEnvironment() {
        return this.env;
    }

    /**
     * Replaces the cluster environment to be used by cacheManagers with the one given. Will shutdown the current one
     * and {@link #close() close all} open cacheManagers beforehand.
     *
     * @param env the new {@link CouchbaseEnvironment} to be used
     */
    public void setEnvironment(CouchbaseEnvironment env) {
        this.close();
        env.shutdown();
        this.env = env;
    }

    @Override
    public synchronized CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        URI managerUri = (uri == null) ? getDefaultURI() : uri;
        ClassLoader managerClassLoader = (classLoader == null) ? getDefaultClassLoader() : classLoader;
        Properties managerProperties = (properties == null) ? getDefaultProperties() : properties;

        Map<URI, CacheManager> byUri = managersByClassLoader.get(managerClassLoader);
        if (byUri == null) {
            byUri = new HashMap<URI, CacheManager>();
            managersByClassLoader.put(managerClassLoader, byUri);
        }


        if (byUri.containsKey(managerUri)) {
            return byUri.get(managerUri);
        } else {
            //construct the CacheManager, store and return it
            CacheManager manager = new CouchbaseCacheManager(this, managerUri, managerClassLoader, managerProperties);
            byUri.put(managerUri, manager);
            return manager;
        }
    }

    @Override
    public synchronized CacheManager getCacheManager(URI uri, ClassLoader classLoader) {
        return getCacheManager(uri, classLoader, null);
    }

    @Override
    public synchronized CacheManager getCacheManager() {
        return getCacheManager(null, null, null);
    }

    @Override
    public synchronized void close() {
        WeakHashMap<ClassLoader, Map<URI, CacheManager>> oldHandles = managersByClassLoader;
        managersByClassLoader = new WeakHashMap<ClassLoader, Map<URI, CacheManager>>();

        for (Map<URI, CacheManager> managerMap : oldHandles.values()) {
            for (CacheManager manager : managerMap.values()) {
                manager.close();
            }
        }
    }

    @Override
    public synchronized void close(ClassLoader classLoader) {
        ClassLoader managerClassLoader = (classLoader == null) ? getDefaultClassLoader() : classLoader;

        Map<URI, CacheManager> managerMap = managersByClassLoader.remove(managerClassLoader);
        if (managerMap != null) {
            for (CacheManager cacheManager : managerMap.values()) {
                cacheManager.close();
            }
        }
    }

    @Override
    public synchronized void close(URI uri, ClassLoader classLoader) {
        URI managerUri = (uri == null) ? getDefaultURI() : uri;
        ClassLoader managerClassLoader = (classLoader == null) ? getDefaultClassLoader() : classLoader;

        Map<URI, CacheManager> managerMap = managersByClassLoader.get(managerClassLoader);
        if (managerMap != null) {
            CacheManager removed = managerMap.remove(managerUri);
            if (removed != null) {
                removed.close();
            }

            if (managerMap.isEmpty()) {
                managersByClassLoader.remove(managerMap);
            }
        }
    }

    @Override
    public boolean isSupported(OptionalFeature optionalFeature) {
        switch (optionalFeature) {
            case STORE_BY_REFERENCE:
                return false;
            default:
                return false;
        }
    }

    /**
     * This method allows to signal the {@link CachingProvider} that a {@link CacheManager}
     * identified by a given URI and ClassLoader has been closed externally. The manager
     * should no longer be tracked by this provider if it was (otherwise this method does
     * nothing).
     *
     * This method does not close the CacheManager.
     *
     * @param classLoader the classLoader identifying the closed manager
     * @param uri the uri identifying the closed manager
     */
    public synchronized void signalCacheManagerClosed(ClassLoader classLoader, URI uri) {
        URI managerUri = (uri == null) ? getDefaultURI() : uri;
        ClassLoader managerClassLoader = (classLoader == null) ? getDefaultClassLoader() : classLoader;

        Map<URI, CacheManager> managers = managersByClassLoader.get(managerClassLoader);
        if (managers != null) {
            managers.remove(managerUri);

            if (managers.isEmpty()) {
                managersByClassLoader.remove(managerClassLoader);
            }
        }

    }
}
