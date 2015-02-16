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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.cache.Cache;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.SerializableDocument;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import rx.Observable;
import rx.Subscriber;

public class CouchbaseCacheIteratorTest {

    private static List<SerializableDocument> docsList;
    private static ArrayList<Double> valueList;

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
        docsList = new ArrayList<SerializableDocument>(10);
        valueList = new ArrayList<Double>(10);
        for (int i = 0; i < 10; i++) {
            Double value = new Double(i + 0.4d);
            valueList.add(value);
            docsList.add(SerializableDocument.create(CONVERTER.asString(i), value));
        }
    }

    @Test
    public void shouldIterateFully() {
        final List<String> removedKeys = new ArrayList<String>(10);
        Observable<SerializableDocument> docs = Observable.from(docsList);
        Bucket bucket = mock(Bucket.class);
        when(bucket.remove(any(SerializableDocument.class))).thenAnswer(new Answer<SerializableDocument>() {
            @Override
            public SerializableDocument answer(InvocationOnMock invocation) throws Throwable {
                SerializableDocument doc = (SerializableDocument) invocation.getArguments()[0];
                removedKeys.add(doc.id());
                return doc;
            }
        });

        CouchbaseCacheIterator<Integer, Double> iterator = new CouchbaseCacheIterator<Integer, Double>(
                bucket, CONVERTER, docs);

        List<Double> extractedValues = new ArrayList<Double>(10);
        while(iterator.hasNext()) {
            Cache.Entry<Integer, Double> entry = iterator.next();
            assertNotNull(entry);
            assertNotNull(entry.getKey());
            assertNotNull(entry.getValue());

            extractedValues.add(entry.getValue());
            iterator.remove();
        }

        assertEquals(10, extractedValues.size());
        assertEquals(10, removedKeys.size());

        for (int i = 0; i < 10; i++) {
            Double extractedValue = extractedValues.get(i);
            String removedKey = removedKeys.get(i);

            assertEquals(i + 0.4d, extractedValue, 0d);
            assertEquals("s" + i, removedKey);
        }
    }

    @Test
    public void shouldPropagateExceptionWhenExceptionThrownInIterator() {
        Bucket bucket = mock(Bucket.class);
        final Observable<SerializableDocument> docs = Observable.create(
                new Observable.OnSubscribe<SerializableDocument>() {
                    @Override
                    public void call(Subscriber<? super SerializableDocument> subscriber) {
                        subscriber.onNext(docsList.get(0));
                        subscriber.onNext(docsList.get(1));
                        subscriber.onError(new IllegalStateException());
                    }
                }
        );

        CouchbaseCacheIterator<Integer, Double> iterator = new CouchbaseCacheIterator<Integer, Double>(
                bucket, CONVERTER, docs);

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
        assertEquals(docsList.get(0).content(), extractedValues.get(0));
        assertEquals(docsList.get(1).content(), extractedValues.get(1));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldIllegalStateExceptionIfRemovedCalledBeforeNext() {
        Bucket bucket = mock(Bucket.class);
        when(bucket.remove(any(Document.class))).thenReturn(null); //ignore remove itself

        CouchbaseCacheIterator iterator = new CouchbaseCacheIterator(bucket, CONVERTER,
                Observable.just(SerializableDocument.create(CONVERTER.asString(0), "test")));

        assertTrue(iterator.hasNext());
        //trigger IllegalArgumentException
        iterator.remove();
    }

    @Test
    public void shouldIllegalStateExceptionIfRemovedCalledTwice() {
        Bucket bucket = mock(Bucket.class);
        when(bucket.remove(any(Document.class))).thenReturn(null); //ignore remove itself

        CouchbaseCacheIterator iterator = new CouchbaseCacheIterator(bucket, CONVERTER,
                Observable.just(SerializableDocument.create(CONVERTER.asString(0), "test")));

        assertTrue(iterator.hasNext());
        assertEquals("test", iterator.next().getValue());

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
        Bucket bucket = mock(Bucket.class);
        when(bucket.remove(any(Document.class))).thenReturn(null); //ignore remove itself

        CouchbaseCacheIterator iterator = new CouchbaseCacheIterator(bucket, CONVERTER,
                Observable.just(SerializableDocument.create(CONVERTER.asString(0), "test")));

        assertTrue(iterator.hasNext());
        assertEquals("test", iterator.next().getValue());
        assertFalse(iterator.hasNext());
        //trigger NoSuchElementException
        iterator.next();
    }
}