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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.cache.Cache;

import com.couchbase.client.java.document.SerializableDocument;
import com.couchbase.client.java.error.TranscodingException;
import com.couchbase.client.jcache.spi.CouchbaseCachingProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration test of initializing cachingProvider, creating caches and doing basic configuration and
 * operations on them.
 *
 * @author Simon Baslé
 * @since 1.0
 */
public class BasicCacheIntegrationTest {

    private static CouchbaseCachingProvider cachingProvider;
    private static CouchbaseCacheManager cacheManager;
    private static CouchbaseCache<String, String> dedicatedCache;

    @BeforeClass
    public static void init() {
        //here is where you'd get the environment
        cachingProvider = new CouchbaseCachingProvider();
        //here is where we get the cluster
        cacheManager = (CouchbaseCacheManager) cachingProvider.getCacheManager();

        //here we create a dedicated cache for most of the tests
        CouchbaseConfiguration<String, String> cbConfig = new CouchbaseConfiguration.Builder<String, String>()
                .defaultBase()
                .useBucket("jcache", "jcache")
                .build();
        dedicatedCache = (CouchbaseCache<String, String>) cacheManager.createCache("dedicatedCache", cbConfig);
    }

    @Test
    public void shouldPersistInDefaultBucketWithCorrectPrefix() {
        final String cacheName = "cacheA";

        CouchbaseConfiguration<String, String> cbConfig = new CouchbaseConfiguration.Builder<String, String>()
                .defaultBase()
                .useDefaultBucket()
                .shared(cacheName + "_")
                .build();

        CouchbaseCache<String, String> cacheA = (CouchbaseCache<String, String>) cacheManager.createCache(cacheName, cbConfig);
        cacheA.put("test", "testValue");

        String realKey = cacheName + "_test";
        assertEquals("default", cacheA.bucket.name());
        assertNull(cacheA.bucket.get("test"));
        assertNull(cacheA.bucket.get("test", SerializableDocument.class));
        try {
            cacheA.bucket.get(realKey);
            fail("did not expect to get this as a JsonDocument");
        } catch (TranscodingException e) {
            //EXPECTED
        } catch (Exception e) {
            fail(e.toString());
        }
        assertNotNull(cacheA.bucket.get(realKey, SerializableDocument.class));
        assertTrue(cacheA.bucket.get(realKey, SerializableDocument.class).content() instanceof String);
    }

    @Test
    public void shouldPersistInDedicatedBucketWithoutPrefix() {
        final String cacheName = "cacheB";
        final String bucketName = "jcache";
        CouchbaseConfiguration<String, String> cbConfig = new CouchbaseConfiguration.Builder<String, String>()
                .defaultBase()
                .useBucket(bucketName, "jcache")
                .build();

        CouchbaseCache<String, String> cacheB = (CouchbaseCache<String, String>) cacheManager.createCache(cacheName,
                cbConfig);
        cacheB.put("test", "testValue");

        assertEquals(bucketName, cacheB.bucket.name());
        try {
            cacheB.bucket.get("test");
            fail("did expect a transcodingException here");
        } catch (TranscodingException e) {
            //EXPECTED
        } catch (Exception e) {
            fail(e.toString());
        }
        assertNotNull(cacheB.bucket.get("test", SerializableDocument.class));
        assertTrue(cacheB.bucket.get("test", SerializableDocument.class).content() instanceof String);
    }



    @Test
    public void shouldCorrectlyRemove() {
        Cache<Object, Object> cache = cacheManager.getCache("dedicatedCache");
        cache.put("removeMe", "testValue");
        assertTrue(cache.containsKey("removeMe"));
        cache.remove("removeMe");
        assertFalse(cache.containsKey("removeMe"));
    }

    @AfterClass
    public static void tearDown() {
        cachingProvider.close();
    }
}
