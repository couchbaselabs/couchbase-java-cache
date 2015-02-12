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

import java.io.Serializable;

/**
 * An interface used to convert a key of type K to and from a {@link String} in order to map to Couchbase's
 * representation of keys.
 *
 * @author Simon Baslé
 * @since 1.0
 */
public interface KeyConverter<K> extends Serializable {

    /**
     * A very simple implementation of a {@link KeyConverter} for keys of type {@link String}.
     * This implementation just uses the original String.
     */
    public static final KeyConverter<String> STRING_KEY_CONVERTER = new KeyConverter<String>() {
        @Override
        public String asString(String key) {
            return key;
        }

        @Override
        public String fromString(String internalKey) {
            return internalKey;
        }
    };

    /**
     * Transforms the key K into a {@link String} representation. The inverse transformation must be possible
     * by calling {@link #fromString(String)}.
     *
     * @param key the key to stringify.
     * @return the key in String form.
     */
    public String asString(K key);

    /**
     * Transforms a key in {@link String} form back to its K form. This is the inverse transformation of
     * {@link #asString(Object)}.
     *
     * @param internalKey the key in internal String form.
     * @return the key in K form.
     */
    public K fromString(String internalKey);
}
