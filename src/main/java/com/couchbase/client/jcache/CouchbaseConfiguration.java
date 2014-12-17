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

    public static final String DEFAULT_BUCKET_NAME = "jcache";
    public static final String DEFAULT_BUCKET_PASSWORD = "jcache";
    public static final String DEFAULT_VIEWALL_DESIGNDOC = "jcache";

    private final String bucketName;
    private final String bucketPassword;
    private final String cachePrefix;
    private final String viewAllDesignDoc;
    private final String viewAllViewName;
    private final String cacheName;

    private CouchbaseConfiguration(String cacheName, CompleteConfiguration<K, V> configuration,
            String bucketName, String bucketPassword, String cachePrefix,
            String viewAllDesignDoc, String viewAllViewName) {
        super(configuration);
        this.cacheName = cacheName;
        this.bucketName = bucketName;
        this.bucketPassword = bucketPassword;
        this.cachePrefix = cachePrefix;
        this.viewAllDesignDoc = viewAllDesignDoc;
        this.viewAllViewName = viewAllViewName;
    }

    private CouchbaseConfiguration(String cacheName, String bucketName, String bucketPassword,
            String cachePrefix, String viewAllDesignDoc, String viewAllViewName) {
        super();
        this.cacheName = cacheName;
        this.bucketName = bucketName;
        this.bucketPassword = bucketPassword;
        this.cachePrefix = cachePrefix;
        this.viewAllDesignDoc = viewAllDesignDoc;
        this.viewAllViewName = viewAllViewName;
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
        this.viewAllViewName = configuration.viewAllViewName;
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
     * This method returns the view name for the cache associated.
     *
     * @return the view name for this cache
     * @see #getAllViewDesignDoc
     */
    public String getAllViewName() {
        return this.viewAllViewName;
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
     * The default is to use a shared bucket {@value CouchbaseConfiguration#DEFAULT_BUCKET_NAME}, with the name of the
     * cache followed by an underscore as the prefix for keys in said bucket. To enumerate all values in the cache, a
     * view is used, defaults to {@value CouchbaseConfiguration#DEFAULT_VIEWALL_DESIGNDOC} for the design document and
     * the name of the cache for the view.
     *
     * @param name the name of the cache to be created via the produced configuration.
     * @return a new builder.
     * @throws NullPointerException if the given cache name is null
     * @throws IllegalArgumentException if the given cache name is empty
     */
    public static <K, V> Builder<K, V> builder(String name) {
        if (name == null) {
            throw new NullPointerException("Null cache name not allowed");
        }
        if (name.isEmpty()) {
            throw new IllegalArgumentException("Empty cache name not allowed");
        }
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
        private String viewAllViewName;
        private final String cacheName;

        protected Builder(String cacheName) {
            this.cacheName = cacheName;
            this.bucketName = CouchbaseConfiguration.DEFAULT_BUCKET_NAME;
            this.bucketPassword = CouchbaseConfiguration.DEFAULT_BUCKET_PASSWORD;
            this.cachePrefix = cacheName + "_";
            this.viewAllDesignDoc = CouchbaseConfiguration.DEFAULT_VIEWALL_DESIGNDOC;
            this.viewAllViewName = cacheName;
        }

        /**
         * Copy a given {@link CompleteConfiguration} as the base
         * for this {@link CouchbaseConfiguration}.
         *
         * @param base the configuration to use as a base
         * @return this {@link Builder} for chaining calls
         */
        public Builder useBase(CompleteConfiguration<K, V> base) {
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
         * Instruct this {@link CouchbaseConfiguration} to use a dedicated bucket
         * as the underlying {@link Bucket}. Note that this reverts to using no
         * prefix for the keys, call {@link #withPrefix} <b>after</b> calling this method
         * to change that.
         */
        public Builder useDedicatedBucket(String bucketName, String bucketPassword) {
            this.bucketName = bucketName;
            this.bucketPassword = bucketPassword;
            this.cachePrefix = "";
            return this;
        }

        /**
         * Instruct this {@link CouchbaseConfiguration} to use default bucket
         * information to connect to the underlying {@link Bucket}.
         *
         * Since default bucket for JCache can be used by several caches, the
         * default prefix will be used, which is the name of the cache followed
         * by an underscore, unless you call {@link #withPrefix}
         *
         * @return this {@link Builder} for chaining calls
         * @see #DEFAULT_BUCKET_NAME
         * @see #DEFAULT_BUCKET_PASSWORD
         */
        public Builder useDefaultSharedBucket() {
            this.bucketName = DEFAULT_BUCKET_NAME;
            this.bucketPassword = DEFAULT_BUCKET_PASSWORD;
            return this;
        }

        /**
         * Instruct this {@link CouchbaseConfiguration} to use given
         * information to connect to the underlying {@link Bucket}.
         *
         * Since default bucket for JCache can be used by several caches, the
         * default prefix will be used, which is the name of the cache followed
         * by an underscore, unless you call {@link #withPrefix}
         *
         * @param name the name of the bucket
         * @param password the password for the bucket
         * @return this {@link Builder} for chaining calls
         */
        public Builder useSharedBucket(String name, String password) {
            this.bucketName = name;
            this.bucketPassword = password;
            return this;
        }

        /**
         * Indicate that the underlying {@link Bucket}'s keys should be
         * prefixed by <i>prefix</i> for the cache associated to this configuration.
         *
         * @param prefix the prefix to use for keys in the associated cache
         * @return this {@link Builder} for chaining calls
         */
        public Builder withPrefix(String prefix) {
            this.cachePrefix = (prefix == null) ? "" : prefix;
            return this;
        }

        /**
         * Indicates that the view that can enumerate the content of
         * this cache is found under design document <i>designDoc</i>,
         * view <i>viewName</i>.
         *
         * Defaults to {@link CouchbaseConfiguration#DEFAULT_VIEWALL_DESIGNDOC}
         * and the cacheName as viewName.
         *
         * @param designDoc the name of the design document
         * @param viewName the name of the view
         * @return this {@link Builder} for chaining calls
         */
        public Builder viewAll(String designDoc, String viewName) {
            this.viewAllDesignDoc = designDoc == null ? CouchbaseConfiguration.DEFAULT_VIEWALL_DESIGNDOC : designDoc;
            this.viewAllViewName = viewName == null ? this.cacheName : viewName;
            return this;
        }

        /**
         * Indicates that the view that can enumerate the content of
         * this cache is found under design document <i>designDoc</i>.
         *
         * Keeps the current view name (which is the cacheName by default).
         *
         * @param designDoc the name of the design document
         * @return this {@link Builder} for chaining calls
         */
        public Builder viewAllDesignDoc(String designDoc) {
            return this.viewAll(designDoc, this.viewAllViewName);
        }

        /**
         * Indicates that the view that can enumerate the content of
         * this cache is found under name <i>viewName</i>.
         *
         * Keeps the current view design document (which is
         * {@link CouchbaseConfiguration#DEFAULT_VIEWALL_DESIGNDOC} by default).
         *
         * @param viewName the name of the view
         * @return this {@link Builder} for chaining calls
         */
        public Builder viewAllName(String viewName) {
            return this.viewAll(this.viewAllDesignDoc, viewName);
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
                        viewAllDesignDoc, viewAllViewName);
            } else {
                config = new CouchbaseConfiguration<K, V>(cacheName, base, bucketName, bucketPassword, cachePrefix,
                        viewAllDesignDoc, viewAllViewName);
            }
            return config;
        }
    }
}
