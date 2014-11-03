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

    public CouchbaseCachingProvider() {
        this.managersByClassLoader = new WeakHashMap<ClassLoader, Map<URI, CacheManager>>();
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
}
