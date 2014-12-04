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

import javax.cache.Cache.Entry;

/**
 * The Couchbase implementation of a {@link Entry Cache Entry}.
 *
 * @author Simon Baslé
 * @since 1.0
 */
public class CouchbaseCacheEntry<K, V> implements Entry<K, V> {

    private final K id;
    private final V content;

    public CouchbaseCacheEntry(K id, V content) {
        this.id = id;
        this.content = content;
    }

    @Override
    public K getKey() {
        return id;
    }

    @Override
    public V getValue() {
        return content;
    }

    @Override
    public <T> T unwrap(Class<T> aClass) {
        if (aClass != null && aClass.isInstance(this)) {
            return (T) this;
        } else {
            throw new IllegalArgumentException("Class " + aClass + " is unknown to this implementation");
        }
    }
}
