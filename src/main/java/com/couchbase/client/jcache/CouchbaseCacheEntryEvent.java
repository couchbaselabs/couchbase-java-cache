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

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.EventType;

/**
 * Couchbase implementation of a {@link CacheEntryEvent}.
 *
 * @author Simon Baslé
 * @since 1.0
 */
public class CouchbaseCacheEntryEvent<K, V> extends CacheEntryEvent<K, V> {

    private final K key;
    private final V value;
    private final V oldValue;

    /**
     * Construct a new event with old value.
     *
     * @param eventType the type of the event.
     * @param key the key impacted by the event.
     * @param value the value corresponding to the key after the event.
     * @param oldValue the previous value corresponding to the key prior to the event (or null if not applicable).
     * @param source the cache in which the event happened.
     */
    public CouchbaseCacheEntryEvent(EventType eventType, K key, V value, V oldValue, CouchbaseCache source) {
        super(source, eventType);
        this.key = key;
        this.value = value;
        this.oldValue = oldValue;
    }

    /**
     * Construct a new event without an old value.
     *
     * @param eventType the type of the event.
     * @param key the key impacted by the event.
     * @param value the value corresponding to the key after the event.
     * @param source the cache in which the event happened.
     */
    public CouchbaseCacheEntryEvent(EventType eventType, K key, V value, CouchbaseCache source) {
        this(eventType, key, value, null, source);
    }

    @Override
    public V getOldValue() {
        return this.oldValue;
    }

    @Override
    public boolean isOldValueAvailable() {
        return this.oldValue != null;
    }

    @Override
    public K getKey() {
        return this.key;
    }

    @Override
    public V getValue() {
        return this.value;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz != null && clazz.isInstance(this)) {
            return (T) this;
        } else {
            throw new IllegalArgumentException("The class " + clazz + " is unknown to this implementation");
        }
    }
}
