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

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.cache.Cache;

import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.SerializableDocument;
import com.couchbase.client.jcache.CouchbaseCacheIterator.TimeAndDocHook;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import rx.Observable;
import rx.Subscriber;

public class CouchbaseCacheIteratorTest {

    private static List<String> keyList;
    private static List<Double> valueList;
    private static Bucket mockBucket;

    private static final KeyConverter<Integer> CONVERTER = new KeyConverter<Integer>() {
        @Override
        public String asString(Integer key) {
            return "s" + key;
        }

        @Override
        public Integer fromString(String internalKey) {
            return Integer.valueOf(internalKey.replaceFirst("s", ""));
        }
    };

    @BeforeClass
    public static void init() {
        keyList = new ArrayList<String>(10);
        valueList = new ArrayList<Double>(10);
        for (int i = 0; i < 10; i++) {
            Double value = i + 0.4d;
            valueList.add(value);
            keyList.add(CONVERTER.asString(i));
        }

        mockBucket = mock(Bucket.class);
        AsyncBucket mockAsyncBucket = mock(AsyncBucket.class);
        when(mockBucket.async()).thenReturn(mockAsyncBucket);

        when(mockAsyncBucket.get(anyString(), eq(SerializableDocument.class))).then(new Answer<Observable>() {
            @Override
            public Observable answer(InvocationOnMock invocation) throws Throwable {
                String id = (String) invocation.getArguments()[0];
                Double value = CONVERTER.fromString(id) + 0.4d;
                return Observable.just(SerializableDocument.create(id, value));
            }
        });

        when(mockAsyncBucket.remove(any(SerializableDocument.class))).then(new Answer<Observable>() {
            @Override
            public Observable answer(InvocationOnMock invocation) throws Throwable {
                return Observable.just(invocation.getArguments()[0]);
            }
        });

    }

    @Test
    public void shouldIterateFullyAndRemove() {
        final List<SerializableDocument> removed = new ArrayList<SerializableDocument>(10);
        final List<SerializableDocument> visited = new ArrayList<SerializableDocument>(10);
        List<Double> extractedValues = new ArrayList<Double>(10);

        Observable<String> ids = Observable.from(keyList);
        TimeAndDocHook onRemoveAction = new TimeAndDocHook() {
            @Override
            public void call(Tuple2<Long, SerializableDocument> timeAndDoc) {
                removed.add(timeAndDoc.value2());
            }
        };
        TimeAndDocHook onEachAction = new TimeAndDocHook() {
            @Override
            public void call(Tuple2<Long, SerializableDocument> timeAndDoc) {
                visited.add(timeAndDoc.value2());
            }
        };

        CouchbaseCacheIterator<Integer, Double> iterator = new CouchbaseCacheIterator<Integer, Double>(
                mockBucket, CONVERTER, ids, onEachAction, onRemoveAction);

        while(iterator.hasNext()) {
            Cache.Entry<Integer, Double> entry = iterator.next();
            assertNotNull(entry);
            assertNotNull(entry.getKey());
            assertNotNull(entry.getValue());

            extractedValues.add(entry.getValue());
            iterator.remove();
        }

        assertEquals(10, extractedValues.size());
        assertEquals(10, removed.size());
        assertEquals(10, visited.size());

        for (int i = 0; i < 10; i++) {
            Double extractedValue = extractedValues.get(i);
            String expectedKey = "s" + i;
            SerializableDocument removedDoc = removed.get(i);
            SerializableDocument visitedDoc = visited.get(i);

            assertEquals(i + 0.4d, extractedValue, 0d);
            assertEquals(expectedKey, visitedDoc.id());
            assertEquals(extractedValue, visitedDoc.content());
            assertEquals(expectedKey, removedDoc.id());
            assertEquals(extractedValue, removedDoc.content());
        }
    }

    @Test
    public void shouldPropagateExceptionWhenExceptionThrownInIterator() {
        final Observable<String> docs = Observable.create(
                new Observable.OnSubscribe<String>() {
                    @Override
                    public void call(Subscriber<? super String> subscriber) {
                        subscriber.onNext(keyList.get(0));
                        subscriber.onNext(keyList.get(1));
                        subscriber.onError(new IllegalStateException());
                    }
                }
        );

        CouchbaseCacheIterator<Integer, Double> iterator = new CouchbaseCacheIterator<Integer, Double>(
                mockBucket, CONVERTER, docs);

        List<Double> extractedValues = new ArrayList<Double>(10);
        int countBeforeNext = 0;
        try {
            while (iterator.hasNext()) {
                countBeforeNext++;
                Cache.Entry<Integer, Double> entry = iterator.next();
                extractedValues.add(entry.getValue());
            }
        } catch (IllegalStateException e) {
            //expected
        }

        assertEquals(2, extractedValues.size());
        assertEquals(2, countBeforeNext);
        assertEquals(valueList.get(0), extractedValues.get(0));
        assertEquals(valueList.get(1), extractedValues.get(1));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldIllegalStateExceptionIfRemovedCalledBeforeNext() {
        CouchbaseCacheIterator<Integer, Double> iterator = new CouchbaseCacheIterator<Integer, Double>(
                mockBucket, CONVERTER, Observable.just(CONVERTER.asString(0)));

        assertTrue(iterator.hasNext());
        //trigger IllegalStateException
        iterator.remove();
    }

    @Test
    public void shouldIllegalStateExceptionIfRemovedCalledTwice() {
        CouchbaseCacheIterator<Integer, Double> iterator = new CouchbaseCacheIterator<Integer, Double>(
                mockBucket, CONVERTER, Observable.just(CONVERTER.asString(0)));

        assertTrue(iterator.hasNext());
        assertEquals(0.4d, iterator.next().getValue(), 0d);

        //don't trigger exception on first remove
        iterator.remove();
        //trigger IllegalArgumentException
        try {
            iterator.remove();
            fail("second remove in a row should fail");
        } catch (IllegalStateException e) {
            //expected
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void shouldNoSuchElementExceptionIfNextCalledOneExtraTime() {
        CouchbaseCacheIterator<Integer, Double> iterator = new CouchbaseCacheIterator<Integer, Double>(
                mockBucket, CONVERTER,Observable.just(CONVERTER.asString(0)));

        assertTrue(iterator.hasNext());
        assertEquals(0.4d, iterator.next().getValue(), 0d);
        assertFalse(iterator.hasNext());
        //trigger NoSuchElementException
        iterator.next();
    }
}