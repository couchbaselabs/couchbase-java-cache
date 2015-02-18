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

import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.SerializableDocument;
import rx.Notification;
import rx.Observable;
import rx.Subscriber;
import rx.exceptions.Exceptions;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;

/**
 * An iterator over a stream of documents in a {@link CouchbaseCache}, usually all documents, that uses
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
    private final Action1<Tuple2<Long, SerializableDocument>> onRemoveAction;

    private SerializableDocument current;
    private Notification<? extends SerializableDocument> next;


    private static final Func2<Long, SerializableDocument, Tuple2<Long, SerializableDocument>> timeAndDocZipFunction =
            new Func2<Long, SerializableDocument, Tuple2<Long, SerializableDocument>>() {
                @Override
                public Tuple2<Long, SerializableDocument> call(Long aLong, SerializableDocument serializableDocument) {
                    return Tuple.create(aLong, serializableDocument);
                }
            };

    /**
     * Iterator constructor without side effects (stats, event notification), mainly useful for tests.
     *
     * @param bucket the bucket on which to remove.
     * @param keyConverter the {@link KeyConverter} to use to translate to/from document keys vs domain keys.
     * @param stream the stream of documents IDs to iterate over.
     */
    protected CouchbaseCacheIterator(Bucket bucket, KeyConverter<K> keyConverter,
            Observable<String> stream) {
        this(bucket, keyConverter, stream, null, null);
    }

    /**
     * Iterator constructor that allows to hook side effects (stats, event notification) on the iteration and removal.
     *
     * @param bucket the bucket on which to remove.
     * @param keyConverter the {@link KeyConverter} to use to translate to/from document keys vs domain keys.
     * @param stream the stream of document IDs to iterate over.
     * @param onEachAction the hook to be called each time an element is pulled ({@link #next()}).
     * @param onRemoveAction the hook to be called each time an element is removed ({@link #remove()}).
     */
    public CouchbaseCacheIterator(Bucket bucket, KeyConverter<K> keyConverter,
            Observable<String> stream,
            TimeAndDocHook onEachAction,
            TimeAndDocHook onRemoveAction) {
        this.bucket = bucket;
        this.keyConverter = keyConverter;

        if (onEachAction == null) {
            onEachAction = EMPTY;
        }
        if (onRemoveAction == null) {
            this.onRemoveAction = EMPTY;
        } else {
            this.onRemoveAction = onRemoveAction;
        }

        stream
                //record the starting time before actually getting the value
                .flatMap(new Func1<String, Observable<Tuple2<Long, SerializableDocument>>>() {
                    @Override
                    public Observable<Tuple2<Long, SerializableDocument>> call(String id) {
                        return Observable.zip(
                                Observable.just(System.nanoTime()),
                                CouchbaseCacheIterator.this.bucket.async().get(id, SerializableDocument.class),
                                timeAndDocZipFunction);
                    }
                })
                //call hook with start time and document
                .doOnNext(onEachAction)
                //simplify back to just the document
                .map(new Func1<Tuple2<Long,SerializableDocument>, SerializableDocument>() {
                    @Override
                    public SerializableDocument call(Tuple2<Long, SerializableDocument> timeAndDoc) {
                        return timeAndDoc.value2();
                    }
                })
                //materialize a feed of notifications out of it
                .materialize()
                //push the notifications into a queue
                .subscribe(new Subscriber<Notification<? extends SerializableDocument>>() {
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
            return new CouchbaseCacheEntry<K, V>(keyConverter.fromString(current.id()), (V) current.content());
        }
        throw new NoSuchElementException();
    }

    @Override
    public void remove() {
        if (current != null) {
            //record the remove starting time and do the remove, then call the remove hook
            Observable.zip(
                    Observable.just(System.nanoTime()),
                    bucket.async().remove(current),
                    timeAndDocZipFunction
            ).subscribe(onRemoveAction);
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

    public static interface TimeAndDocHook extends Action1<Tuple2<Long, SerializableDocument>> { }

    public static final TimeAndDocHook EMPTY = new TimeAndDocHook() {
        @Override
        public void call(Tuple2<Long, SerializableDocument> longSerializableDocumentTuple2) {
            //NO-OP
        }
    };
}
