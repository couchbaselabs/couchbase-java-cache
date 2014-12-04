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
