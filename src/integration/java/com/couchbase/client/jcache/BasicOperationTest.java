/*
 * Copyright (c) 2015 Couchbase, Inc. All rights reserved.
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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.SerializableDocument;
import com.couchbase.client.jcache.spi.CouchbaseCachingProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class BasicOperationTest {

    private static final String CLUSTER_LOGIN = "Administrator";
    private static final String CLUSTER_PWD = "password";
    private static final String CACHE_NAME = "dedicatedCache";

    private static final String TEST_KEY = "test";
    private static final String TEST_VALUE = "this is a test";

    private static CouchbaseCachingProvider cachingProvider;
    private static CouchbaseCacheManager cacheManager;
    private static CouchbaseCache<String, String> dedicatedCache;
    private static KeyConverter<String> keyConverter;

    @BeforeClass
    public static void init() {
        //here is where you'd get the environment
        cachingProvider = new CouchbaseCachingProvider();
        //here is where we get the cluster
        cacheManager = (CouchbaseCacheManager) cachingProvider.getCacheManager();

        ClusterManager manager = cacheManager.cluster.clusterManager(CLUSTER_LOGIN, CLUSTER_PWD);
        boolean hasBucket = manager.hasBucket(CACHE_NAME);
        if (!hasBucket) {
            manager.insertBucket(DefaultBucketSettings.builder()
                                                      .name(CACHE_NAME)
                                                      .enableFlush(true)
                                                      .quota(100)
                                                      .build());
        }

        //here we create a dedicated cache for most of the tests
        CouchbaseConfiguration<String, String> cbConfig = new CouchbaseConfiguration.Builder<String, String>(
                CACHE_NAME, KeyConverter.STRING_KEY_CONVERTER)
                .useDedicatedBucket(CACHE_NAME, "")
                .build();
        dedicatedCache = (CouchbaseCache<String, String>) cacheManager.createCache(CACHE_NAME, cbConfig);
        keyConverter = dedicatedCache.keyConverter();

        //prepare data
        if (hasBucket) {
            dedicatedCache.bucket.bucketManager().flush();
        }
        dedicatedCache.bucket.insert(SerializableDocument.create(keyConverter.asString(TEST_KEY), TEST_VALUE));
        dedicatedCache.bucket.insert(SerializableDocument.create(keyConverter.asString(TEST_KEY) + "2", TEST_VALUE + "2"));
        dedicatedCache.bucket.insert(SerializableDocument.create(keyConverter.asString("remove"), TEST_VALUE));
        dedicatedCache.bucket.insert(SerializableDocument.create(keyConverter.asString("oldremove"), TEST_VALUE));
        dedicatedCache.bucket.insert(SerializableDocument.create(keyConverter.asString("getremove"), TEST_VALUE));
        dedicatedCache.bucket.insert(SerializableDocument.create(keyConverter.asString("itremove"), TEST_VALUE));
        dedicatedCache.bucket.insert(SerializableDocument.create(keyConverter.asString("replace"), TEST_VALUE));
        dedicatedCache.bucket.insert(SerializableDocument.create(keyConverter.asString("oldreplace"), TEST_VALUE));
        dedicatedCache.bucket.insert(SerializableDocument.create(keyConverter.asString("getreplace"), TEST_VALUE));

        //TODO prepare view
    }

    @AfterClass
    public static void cleanup() {
        dedicatedCache.close();
        cacheManager.close();
        cachingProvider.close();
    }

    private String cbKey(String cacheKey) {
        return keyConverter.asString(cacheKey);
    }

    private SerializableDocument assertDocument(String expected, String key) {
        SerializableDocument doc = dedicatedCache.bucket.get(cbKey(key), SerializableDocument.class);

        assertNotNull(doc);
        assertNotNull(doc.content());
        assertEquals(expected, doc.content());

        return doc;
    }

    @Test
    public void testGet() throws Exception {
        String value = dedicatedCache.get(TEST_KEY);

        assertNotNull(value);
        assertEquals(TEST_VALUE, value);
    }

    @Test
    public void testGetAll() throws Exception {
        String goodKey1 = TEST_KEY;
        String goodKey2 = TEST_KEY + "2";
        String badKey = TEST_KEY + "3";
        Set<String> keysToGet = new HashSet<String>(Arrays.asList(goodKey1, goodKey2, badKey));

        Map<String, String> values = dedicatedCache.getAll(keysToGet);

        assertNotNull(values);
        assertEquals(2, values.size());
        assertNotNull(values.get(goodKey1));
        assertEquals(TEST_VALUE, values.get(goodKey1));
        assertNotNull(values.get(goodKey2));
        assertEquals(TEST_VALUE + "2", values.get(goodKey2));
        assertNull(values.get(badKey));
    }

    @Test
    public void testContainsKey() throws Exception {
        assertTrue(dedicatedCache.containsKey(TEST_KEY));
    }

    @Test
    public void testPut() throws Exception {
        dedicatedCache.put("testPut", "putTest");

        assertDocument("putTest", "testPut");
    }

    @Test
    public void testGetAndPut() throws Exception {
        String old = dedicatedCache.getAndPut("testGetAndPut", "getAndPutTest");
        SerializableDocument doc = dedicatedCache.bucket.get(cbKey("testGetAndPut"), SerializableDocument.class);

        assertNotNull(doc);
        assertNotNull(doc.content());
        assertEquals("getAndPutTest", doc.content());
        assertNull(old);

        old = dedicatedCache.getAndPut("testGetAndPut", "getAndPutTest2");
        doc = dedicatedCache.bucket.get(cbKey("testGetAndPut"), SerializableDocument.class);

        assertEquals("getAndPutTest", old);
        assertNotNull(doc);
        assertEquals("getAndPutTest2", doc.content());

    }

    @Test
    public void testPutAll() throws Exception {
        Map<String, String> toPut = new HashMap<String, String>(3);
        toPut.put("k1", "v1");
        toPut.put("k2", "v2");
        toPut.put("k3", "v3");

        dedicatedCache.putAll(toPut);

        assertDocument("v1", "k1");
        assertDocument("v2", "k2");
        assertDocument("v3", "k3");

        toPut.clear();
        toPut.put("k1", "v1b");
        toPut.put("k2", "v2b");
        dedicatedCache.putAll(toPut);

        assertDocument("v1b", "k1");
        assertDocument("v2b", "k2");
        assertDocument("v3", "k3");
    }

    @Test
    public void testPutIfAbsent() throws Exception {
        boolean result = dedicatedCache.putIfAbsent(TEST_KEY, "test");

        assertFalse(result);
        assertDocument(TEST_VALUE, TEST_KEY);

        result = dedicatedCache.putIfAbsent("testPutIfAbsent", "test");

        assertTrue(result);
        assertDocument("test", "testPutIfAbsent");
    }

    @Test
    public void testRemove() throws Exception {
        boolean result = dedicatedCache.remove("remove");

        assertTrue(result);
        assertNull(dedicatedCache.bucket.get(cbKey("remove"), SerializableDocument.class));
    }

    @Test
    public void testRemoveIfOldValueMatches() throws Exception {
        boolean result = dedicatedCache.remove("oldremove", "not");

        assertFalse(result);
        assertDocument(TEST_VALUE, "oldremove");

        result = dedicatedCache.remove("oldremove", TEST_VALUE);

        assertTrue(result);
        assertNull(dedicatedCache.bucket.get(cbKey("oldremove"), SerializableDocument.class));
    }

    @Test
    public void testGetAndRemove() throws Exception {
        String oldValue = dedicatedCache.getAndRemove("no");

        assertNull(oldValue);

        assertTrue(dedicatedCache.containsKey("getremove"));
        oldValue = dedicatedCache.getAndRemove("getremove");

        assertNotNull(oldValue);
        assertEquals(TEST_VALUE, oldValue);
        assertFalse(dedicatedCache.containsKey("getremove"));

    }

    @Test
    public void testReplace() throws Exception {
        boolean result = dedicatedCache.replace("no", "replaced");

        assertFalse(result);
        assertNull(dedicatedCache.bucket.get(cbKey("no")));
        assertDocument(TEST_VALUE, "replace");

        result = dedicatedCache.replace("replace", "replaced");

        assertTrue(result);
        assertDocument("replaced", "replace");
    }

    @Test
    public void testReplaceWhenOldValueMatches() throws Exception {
        boolean result = dedicatedCache.replace("no", TEST_VALUE, "replaced");

        assertFalse(result);
        assertNull(dedicatedCache.bucket.get(cbKey("no")));
        assertDocument(TEST_VALUE, "oldreplace");

        result = dedicatedCache.replace("oldreplace", "nope", "replaced");
        assertFalse(result);
        assertDocument(TEST_VALUE, "oldreplace");

        result = dedicatedCache.replace("oldreplace", TEST_VALUE, "replaced");
        assertTrue(result);
        assertDocument("replaced", "oldreplace");
    }

    @Test
    public void testGetAndReplace() throws Exception {
        String oldValue = dedicatedCache.getAndReplace("no", "replaced");

        assertNull(oldValue);

        oldValue = dedicatedCache.getAndReplace("getreplace", "replaced");
        assertEquals(TEST_VALUE, oldValue);
        assertDocument("replaced", "getreplace");
    }

    @Test
    public void testRemoveAllFromSet() throws Exception {
        dedicatedCache.bucket.insert(SerializableDocument.create(cbKey("rk1"), TEST_VALUE));
        dedicatedCache.bucket.insert(SerializableDocument.create(cbKey("rk2"), TEST_VALUE));
        Set<String> toRemove = new HashSet<String>(Arrays.asList("rk1", "rk2", "rk3"));

        dedicatedCache.removeAll(toRemove);

        assertNull(dedicatedCache.bucket.get("rk1"));
        assertNull(dedicatedCache.bucket.get("rk2"));
        assertNull(dedicatedCache.bucket.get("rk3"));
    }

    @Ignore //TODO replace with true test when view created
    @Test
    public void testRemoveAll() throws Exception {
        dedicatedCache.removeAll();
    }

    @Ignore //TODO replace with true test when view created
    @Test
    public void testIterator() throws Exception {
        dedicatedCache.iterator();
    }

    @Ignore //TODO replace with true test when view created
    @Test
    public void testClear() throws Exception {
        dedicatedCache.clear();
    }
}