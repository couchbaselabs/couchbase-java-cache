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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeoutException;

import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.jcache.spi.CouchbaseCachingProvider;
import org.junit.Test;

/**
 * Integration tests related to the connection to a Couchbase Cluster.
 *
 * @author Simon Baslé
 * @since 1.0
 */
public class ClusterConfigurationTest {

    @Test
    public void shouldConnectToDefaultCluster() {
        CouchbaseCachingProvider defaultProvider = new CouchbaseCachingProvider();
        assertNull(defaultProvider.getEnvironment());
        assertNotNull(defaultProvider.getBoostrap());
        try {
            CouchbaseCacheManager manager = (CouchbaseCacheManager) defaultProvider.getCacheManager();
            assertNotNull(manager);
            assertNotNull(manager.cluster);
        } finally {
            defaultProvider.close();
        }
    }

    @Test
    public void shouldNotConnectToBadCustomCluster() {
        final String badIp = "192.168.123.123";
        CouchbaseCachingProvider defaultProvider = new CouchbaseCachingProvider();
        defaultProvider.setBootstrap(badIp);
        defaultProvider.setEnvironment(DefaultCouchbaseEnvironment.builder().connectTimeout(1).build());

        assertEquals(1, defaultProvider.getBoostrap().size());
        assertNotNull(defaultProvider.getEnvironment());
        assertEquals(badIp,  defaultProvider.getBoostrap().get(0));
        try {
            CouchbaseCacheManager manager = (CouchbaseCacheManager) defaultProvider.getCacheManager();
            manager.cluster.openBucket();
            fail();
        } catch (RuntimeException e) {
            assertNotNull(e.getCause());
            assertTrue(e.getCause() instanceof TimeoutException);
        } finally {
            defaultProvider.close();
        }
    }

}
