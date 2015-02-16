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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.cache.Cache;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.SerializableDocument;
import rx.Notification;
import rx.Observable;
import rx.Subscriber;
import rx.exceptions.Exceptions;

/**
 * An iterator over a {@link CouchbaseCache}, that uses a stream of all documents in the cache,
 * the cache's {@link KeyConverter} and the underlying {@link Bucket} to notably implement {@link #remove()}.
 *
 * @author Simon Baslé
 * @since 1.0
 */
class CouchbaseCacheIterator<K, V> implements Iterator<Cache.Entry<K, V>> {

    private final Bucket bucket;
    private final BlockingQueue<Notification<? extends SerializableDocument>> notifications =
            new LinkedBlockingQueue<Notification<? extends SerializableDocument>>();
    private final KeyConverter<K> keyConverter;

    private SerializableDocument current;
    private Notification<? extends SerializableDocument> next;

    public CouchbaseCacheIterator(Bucket bucket, KeyConverter<K> keyConverter,
            Observable<SerializableDocument> stream) {
        this.bucket = bucket;
        this.keyConverter = keyConverter;

        stream.materialize().subscribe(new Subscriber<Notification<? extends SerializableDocument>>() {
            @Override
            public void onCompleted() {
            }

            @Override
            public void onError(Throwable e) {
                notifications.offer(Notification.<SerializableDocument>createOnError(e));
            }

            @Override
            public void onNext(Notification<? extends SerializableDocument> args) {
                notifications.offer(args);
            }
        });
    }

    @Override
    public boolean hasNext() {
        if (next == null) {
            next = take();
        }
        if (next.isOnError()) {
            throw Exceptions.propagate(next.getThrowable());
        }
        return !next.isOnCompleted();
    }

    @Override
    public Cache.Entry<K, V> next() {
        if (hasNext()) {
            current = next.getValue();
            next = null;
            return new CouchbaseCacheEntry(keyConverter.fromString(current.id()), current.content());
        }
        throw new NoSuchElementException();
    }

    @Override
    public void remove() {
        if (current != null) {
            bucket.remove(current);
            current = null;
        } else {
            throw new IllegalStateException("remove() must be called at most once after a call to next()");
        }
    }

    private Notification<? extends SerializableDocument> take() {
        try {
            return notifications.take();
        } catch (InterruptedException e) {
            throw Exceptions.propagate(e);
        }
    }
}
