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
package com.couchbase.client.jcache.management;

import javax.cache.management.CacheMXBean;

import com.couchbase.client.jcache.CouchbaseCache;
import com.couchbase.client.jcache.CouchbaseConfiguration;

/**
 * A management bean for {@link CouchbaseCache cache}. It provides configuration information.
 * It does not allow mutation of configuration or mutation of the cache.
 *
 * Each cache's management object must be registered with an ObjectName that is unique and has
 * the following type and attributes:
 * Type: <code>javax.cache:type=CacheConfiguration</code>
 *
 * Required Attributes:
 *  - CacheManager: the URI of the CacheManager
 *  - Cache: the name of the Cache
 *
 * @author Simon Baslé
 * @since 1.0
 */
public class CouchbaseCacheMxBean implements CacheMXBean {

    //TODO weak reference here?
    private final CouchbaseConfiguration configuration;

    public CouchbaseCacheMxBean(CouchbaseCache<?, ?> cache) {
        this.configuration = cache.getConfiguration(CouchbaseConfiguration.class);
    }

    @Override
    public String getKeyType() {
        return configuration.getKeyType().getName();
    }

    @Override
    public String getValueType() {
        return configuration.getValueType().getName();
    }

    @Override
    public boolean isReadThrough() {
        return configuration.isReadThrough();
    }

    @Override
    public boolean isWriteThrough() {
        return configuration.isWriteThrough();
    }

    @Override
    public boolean isStoreByValue() {
        return configuration.isStoreByValue();
    }

    @Override
    public boolean isStatisticsEnabled() {
        return configuration.isStatisticsEnabled();
    }

    @Override
    public boolean isManagementEnabled() {
        return configuration.isManagementEnabled();
    }
}
