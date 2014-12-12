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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.MutableConfiguration;

import com.couchbase.client.java.Bucket;

/**
 * A {@link CompleteConfiguration} with Couchbase-specific elements like associated bucket, etc...
 *
 * @author Simon Baslé
 * @since 1.0
 */
public class CouchbaseConfiguration<K, V> extends MutableConfiguration<K, V> implements Serializable {

    public static final long serialVersionUID = 1L;

    public static final String DEFAULT_BUCKET_NAME = "default";
    public static final String DEFAULT_BUCKET_PASSWORD = "";
    public static final String DEFAULT_CACHE_PREFIX = "";
    public static final String DEFAULT_VIEWALL_DESIGNDOC = "jcache";

    private final String bucketName;
    private final String bucketPassword;
    private final String cachePrefix;
    private final String viewAllDesignDoc;
    private final Map<String, String> viewAllViews;
    private final String cacheName;

    private CouchbaseConfiguration(String cacheName, CompleteConfiguration<K, V> configuration,
            String bucketName, String bucketPassword, String cachePrefix,
            String viewAllDesignDoc, Map<String, String> viewAllViews) {
        super(configuration);
        this.cacheName = cacheName;
        this.bucketName = bucketName;
        this.bucketPassword = bucketPassword;
        this.cachePrefix = cachePrefix;
        this.viewAllDesignDoc = viewAllDesignDoc;
        this.viewAllViews = viewAllViews;
    }

    private CouchbaseConfiguration(String cacheName, String bucketName, String bucketPassword,
            String cachePrefix, String viewAllDesignDoc, Map<String, String> viewAllViews) {
        super();
        this.cacheName = cacheName;
        this.bucketName = bucketName;
        this.bucketPassword = bucketPassword;
        this.cachePrefix = cachePrefix;
        this.viewAllDesignDoc = viewAllDesignDoc;
        this.viewAllViews = viewAllViews;
    }

    /**
     * Copy constructor for a {@link CouchbaseConfiguration}.
     *
     * @param configuration the configuration to copy
     */
    /*package*/ CouchbaseConfiguration(CouchbaseConfiguration<K, V> configuration) {
        super(configuration);
        this.cacheName = configuration.getCacheName();
        this.bucketName = configuration.bucketName;
        this.bucketPassword = configuration.bucketPassword;
        this.cachePrefix = configuration.cachePrefix;
        this.viewAllDesignDoc = configuration.viewAllDesignDoc;
        this.viewAllViews = new HashMap<String, String>(configuration.viewAllViews);
    }

    /**
     * Returns the name of the bucket in which the cached data will be stored.
     */
    public String getBucketName() {
        return bucketName;
    }

    /**
     * Returns  the password for the bucket.
     *
     * @see #getBucketName()
     */
    public String getBucketPassword() {
        return bucketPassword;
    }

    /**
     * In case multiple caches store data in the same bucket, a prefix is needed
     * to distinguish keys of each cache.
     *
     * @return the prefix for keys or the empty String if bucket is not shared
     */
    public String getCachePrefix() {
        return cachePrefix;
    }

    /**
     * In order to be able to list all elements present in cache, a view must
     * be created for each cache. These view are expected to be created by the
     * user. Standard name for a view is the cache's name, but one can change
     * this when creating the configuration.
     *
     * This method returns the view name for a given cache.
     *
     * @param cacheName the cache for which we want a view name
     * @return the corresponding view name
     * @see #getAllViewDesignDoc
     */
    public String getAllViewName(String cacheName) {
        String allViewName = this.viewAllViews.get(cacheName);
        if (allViewName != null) {
            return allViewName;
        }
        return cacheName;
    }

    /**
     * In order to list all elements present in cache, a view must
     * be created for each cache. These view are expected to be created by the
     * user. They must all be under the same design document, as returned by
     * this method.
     *
     * @return the name of the design document containing the views for managed caches.
     */
    public String getAllViewDesignDoc() {
        return this.viewAllDesignDoc;
    }

    /**
     * The name of the cache this configuration will be used to create.
     *
     * @return the name of the cache.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * Creates and return a {@link Builder} for creating configuration for a {@link CouchbaseCache} with the given name.
     *
     * @param name the name of the cache to be created via the produced configuration.
     * @return a new builder.
     */
    public static <K, V> Builder<K, V> builder(String name) {
        return new Builder(name);
    }

    /**
     * Builder for creating {@link CouchbaseConfiguration CouchbaseConfigurations}.
     */
    public static final class Builder<K, V> {

        private CompleteConfiguration<K, V> base;
        private String bucketName;
        private String bucketPassword;
        private String cachePrefix;
        private String viewAllDesignDoc;
        private final String cacheName;

        private final Map<String, String> viewAllViews = new HashMap<String, String>();

        protected Builder(String cacheName) {
            this.cacheName = cacheName;
        }

        /**
         * Defines what the name of the cache to be built from this configuration will be.
         *
         * @return the name of the cache.
         */
        public String getCacheName() {
            return this.cacheName;
        }

        /**
         * Copy a given {@link CompleteConfiguration} as the base
         * for this {@link CouchbaseConfiguration}.
         *
         * @param base the configuration to use as a base
         * @return this {@link Builder} for chaining calls
         */
        public Builder from(CompleteConfiguration<K, V> base) {
            this.base = base;
            return this;
        }

        /**
         * Use a default configuration as the base for this {@link CouchbaseConfiguration}.
         *
         * @return this {@link Builder} for chaining calls
         */
        public Builder defaultBase() {
            this.base = null;
            return this;
        }

        /**
         * Instruct this {@link CouchbaseConfiguration} to use default bucket
         * information to connect to the underlying {@link Bucket}.
         *
         * @return this {@link Builder} for chaining calls
         * @see #DEFAULT_BUCKET_NAME
         * @see #DEFAULT_BUCKET_PASSWORD
         */
        public Builder useDefaultBucket() {
            this.bucketName = DEFAULT_BUCKET_NAME;
            this.bucketPassword = DEFAULT_BUCKET_PASSWORD;
            return this;
        }

        /**
         * Instruct this {@link CouchbaseConfiguration} to use given
         * information to connect to the underlying {@link Bucket}.
         *
         * @param name the name of the bucket
         * @param password the password for the bucket
         * @return this {@link Builder} for chaining calls
         */
        public Builder useBucket(String name, String password) {
            this.bucketName = name;
            this.bucketPassword = password;
            return this;
        }

        /**
         * Indicate that the underlying {@link Bucket} can be shared
         * between several caches and that keys should be prefixed by
         * <i>prefix</i> for the cache associated to this configuration.
         *
         * @param prefix the prefix to use for keys in the associated cache
         * @return this {@link Builder} for chaining calls
         */
        public Builder shared(String prefix) {
            this.cachePrefix = prefix;
            return this;
        }

        /**
         * Indicate that the underlying {@link Bucket} is not shared
         * between several caches but only used to back up the one
         * cache associated with this configuration. As such there is
         * no need to add a prefix to keys.
         *
         * @return this {@link Builder} for chaining calls
         */
        public Builder dedicated() {
            this.cachePrefix = DEFAULT_CACHE_PREFIX;
            return this;
        }

        /**
         * Indicates that the views that can enumerate the content of
         * each cache are found under design document <i>designDoc</i>.
         *
         * Defaults to {@link CouchbaseConfiguration#DEFAULT_VIEWALL_DESIGNDOC}.
         *
         * @param designDoc the name of the design document
         * @return this {@link Builder} for chaining calls
         */
        public Builder viewAllDesignDoc(String designDoc) {
            this.viewAllDesignDoc = designDoc;
            return this;
        }

        /**
         * Indicates which view should be queried when attempting to enumerate the
         * values in cache <i>cacheName</i>.
         *
         * Default is to use the cacheName as view name.
         *
         * @param cacheName the name of the cache for which to configure a view
         * @param viewAllViewName the name of the view to be used for this cache
         * @return this {@link Builder} for chaining calls
         */
        public Builder viewAllForCache(String cacheName, String viewAllViewName) {
            this.viewAllViews.put(cacheName, viewAllViewName);
            return this;
        }

        /**
         * Create the appropriate {@link CouchbaseConfiguration} from this {@link Builder}.
         *
         * @return the built CouchbaseConfiguration
         */
        public CouchbaseConfiguration<K, V> build() {
            CouchbaseConfiguration config;
            if (viewAllDesignDoc == null) {
                viewAllDesignDoc = DEFAULT_VIEWALL_DESIGNDOC;
            }

            if (base == null) {
                config = new CouchbaseConfiguration<K, V>(cacheName, bucketName, bucketPassword, cachePrefix,
                        viewAllDesignDoc, viewAllViews);
            } else {
                config = new CouchbaseConfiguration<K, V>(cacheName, base, bucketName, bucketPassword, cachePrefix,
                        viewAllDesignDoc, viewAllViews);
            }
            return config;
        }
    }
}
