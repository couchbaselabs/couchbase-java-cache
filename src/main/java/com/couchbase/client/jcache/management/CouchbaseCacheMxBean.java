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
