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
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

/**
 * Unit tests on the {@link CouchbaseConfiguration} and building the configuration.
 *
 * @author Simon Baslé
 * @since 1.0
 */
public class CouchbaseConfigurationTest {

    private static final String CACHE = "myCache";

    @Test(expected = NullPointerException.class)
    public void shouldNullPointerOnNullCacheName() {
        CouchbaseConfiguration.builder(null, KeyConverter.STRING_KEY_CONVERTER).build();
    }

    @Test(expected = NullPointerException.class)
    public void shouldNullPointerOnNullKeyConverter() {
        CouchbaseConfiguration.builder("test", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldIllegalArgumentOnEmptyCacheName() {
        CouchbaseConfiguration.builder("", KeyConverter.STRING_KEY_CONVERTER).build();
    }

    @Test
    public void shouldHaveSaneDefaults() {
        CouchbaseConfiguration conf = CouchbaseConfiguration.builder(CACHE, KeyConverter.STRING_KEY_CONVERTER).build();

        assertEquals(CouchbaseConfiguration.DEFAULT_BUCKET_NAME, conf.getBucketName());
        assertEquals(CouchbaseConfiguration.DEFAULT_BUCKET_PASSWORD, conf.getBucketPassword());
        assertEquals(CACHE + "_", conf.getCachePrefix());
        assertEquals(CouchbaseConfiguration.DEFAULT_VIEWALL_DESIGNDOC, conf.getAllViewDesignDoc());
        assertEquals(CACHE, conf.getAllViewName());
        assertEquals(KeyConverter.STRING_KEY_CONVERTER, conf.getKeyConverter());
    }

    @Test
    public void shouldHaveEmptyPrefixIfDedicatedBucket() {
        CouchbaseConfiguration conf = CouchbaseConfiguration.builder(CACHE, KeyConverter.STRING_KEY_CONVERTER)
                .withPrefix("ignored")
                .useDedicatedBucket("toto", "")
                .build();
        assertNotNull(conf.getCachePrefix());
        assertEquals("", conf.getCachePrefix());
    }

    @Test
    public void shouldHaveNameAsDefaultPrefixIfSharedBucket() {
        CouchbaseConfiguration conf = CouchbaseConfiguration.builder(CACHE, KeyConverter.STRING_KEY_CONVERTER)
                .useSharedBucket("toto", "")
                .build();

        assertNotNull(conf.getCachePrefix());
        assertEquals(CACHE + "_", conf.getCachePrefix());
    }

    @Test
    public void shouldHaveCustomPrefixIfSharedBucketAndPrefixSet() {
        CouchbaseConfiguration conf = CouchbaseConfiguration.builder(CACHE, KeyConverter.STRING_KEY_CONVERTER)
                .useSharedBucket("toto", "")
                .withPrefix("tata")
                .build();

        assertNotNull(conf.getCachePrefix());
        assertEquals("tata", conf.getCachePrefix());
    }

}
