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

import java.util.Set;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.management.CacheMXBean;
import javax.cache.management.CacheStatisticsMXBean;
import javax.management.MalformedObjectNameException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import com.couchbase.client.jcache.CouchbaseCache;

/**
 * Utility class for dealing with management and statistics MXBeans.
 *
 * @author Simon Baslé
 * @since 1.0
 */
public final class ManagementUtil {

    private ManagementUtil() {
        //NO-OP
    }

    /**  The type of management bean. */
    private static enum ManagementType {
        /** Cache Statistics type. */
        Statistics,
        /** Cache Configuration type. */
        Configuration
    }

    /** The mBean server in which to register beans. */
    private static MBeanServer mBeanServer = MBeanServerFactory.createMBeanServer();

    /**
     * Register the {@link CacheStatisticsMXBean} for the given cache.
     *
     * @param cache the cache for which to register statistics
     * @param mxBean the statistics MXBean
     */
    public static void registerStatistics(CouchbaseCache<?, ?> cache, CacheStatisticsMXBean mxBean) {
        registerBean(cache, mxBean, ManagementType.Statistics);
    }

    /**
     * Register the configuration {@link CacheMXBean} for the given cache.
     *
     * @param cache the cache for which to register configuration
     * @param mxBean the configuration MXBean
     */
    public static void registerConfiguration(CouchbaseCache<?, ?> cache, CacheMXBean mxBean) {
        registerBean(cache, mxBean, ManagementType.Configuration);
    }

    /**
     * Un-register the statistics MXBean for the given cache.
     *
     * @param cache the cache for which to un-register statistics
     */
    public static void unregisterStatistics(CouchbaseCache<?, ?> cache) {
        unregisterBean(cache, ManagementType.Statistics);
    }

    /**
     * Un-register the configuration MXBean for the given cache.
     *
     * @param cache the cache for which to un-register configuration
     */
    public static void unregisterConfiguration(CouchbaseCache<?, ?> cache) {
        unregisterBean(cache, ManagementType.Configuration);
    }

    private static void registerBean(CouchbaseCache<?, ?> cache, Object mxBean, ManagementType mxBeanType) {
        ObjectName name = calculateObjectName(cache, mxBeanType);
        try {
            if (!isRegistered(name)) {
                mBeanServer.registerMBean(mxBean, name);
            }
        } catch (Exception e) {
            throw new CacheException("Error registering cache MXBeans " + name
                    + ", error was: " + e.getMessage(), e);
        }
    }

    private static void unregisterBean(CouchbaseCache<?, ?> cache, ManagementType mxBeanType) {
        ObjectName objectName = calculateObjectName(cache, mxBeanType);
        Set<ObjectName> registeredObjectNames = mBeanServer.queryNames(objectName, null);

        for (ObjectName registeredObjectName : registeredObjectNames) {
            try {
                mBeanServer.unregisterMBean(registeredObjectName);
            } catch (Exception e) {
                throw new CacheException("Error unregistering " + mxBeanType + " MXBean "
                        + registeredObjectName + " . Error was " + e.getMessage(), e);
            }
        }
    }

    private static boolean isRegistered(ObjectName name) {
        Set<ObjectName> registeredObjectNames = mBeanServer.queryNames(name, null);
        return !registeredObjectNames.isEmpty();
    }

    /**
     * Cache object names use the scheme
     * <code>javax.cache:type=Cache&lt;Statistics|Configuration&gt;,CacheManager=&lt;cacheManagerName&gt;,name=&lt;cacheName&gt;</code>
     */
    private static ObjectName calculateObjectName(Cache cache, ManagementType type) {
        String cacheManagerName = sanitize(cache.getCacheManager().getURI().toString());
        String cacheName = sanitize(cache.getName());

        try {
            return new ObjectName("javax.cache:type=Cache" + type + ",CacheManager="
                    + cacheManagerName + ",Cache=" + cacheName);
        } catch (MalformedObjectNameException e) {
            throw new CacheException("Illegal ObjectName for Management Bean. "
                    + "CacheManager=[" + cacheManagerName + "], Cache=[" + cacheName + "]", e);
        }
    }


    /**
     * Sanitize characters from string to produce a valid ObjectName.
     *
     * @param string input string
     * @return A valid JMX ObjectName attribute value.
     */
    private static String sanitize(String string) {
        return string == null ? "" : string.replaceAll(",|:|=|\n", ".");
    }
}
