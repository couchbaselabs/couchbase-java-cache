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
package com.couchbase.client.jcache.management;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.cache.Cache;
import javax.cache.management.CacheStatisticsMXBean;

import com.couchbase.client.jcache.CouchbaseCache;

/**
 * Couchbase Cache statistics.
 * <p>
 * Statistics are accumulated from the time a cache is created. They can be reset
 * to zero using {@link #clear}.
 * <p>
 * There are no defined consistency semantics for statistics. Refer to the
 * implementation for precise semantics.
 * <p>
 * Each cache's statistics object must be registered with an ObjectName that is
 * unique and has the following type and attributes:
 * <p>
 * Type:
 * <code>javax.cache:type=CacheStatistics</code>
 * <p>
 * Required Attributes:
 * <ul>
 * <li>CacheManager the URI of the CacheManager
 * <li>Cache the name of the Cache
 * </ul>
 *
 * @author Simon Baslé
 * @since 1.0
 * @see javax.cache.management.CacheStatisticsMXBean
 */
public class CouchbaseStatisticsMxBean implements CacheStatisticsMXBean, Serializable {

    private static final long serialVersionUID = 1L;

    //TODO need to keep the cache at all?
    private transient Cache<?, ?> cache;

    private final AtomicLong cacheRemovals = new AtomicLong();
    private final AtomicLong cacheExpiries = new AtomicLong();
    private final AtomicLong cachePuts = new AtomicLong();
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();
    private final AtomicLong cacheEvictions = new AtomicLong();
    private final AtomicLong cachePutTimeTakenNanos = new AtomicLong();
    private final AtomicLong cacheGetTimeTakenNanos = new AtomicLong();
    private final AtomicLong cacheRemoveTimeTakenNanos = new AtomicLong();

    /**
     * Constructs a cache statistics object.
     *
     * @param cache the associated cache
     */
    public CouchbaseStatisticsMxBean(CouchbaseCache<?, ?> cache) {
        this.cache = cache;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Statistics will also automatically be cleared if internal counters overflow.
     */
    @Override
    public void clear() {
        cachePuts.set(0);
        cacheMisses.set(0);
        cacheRemovals.set(0);
        cacheExpiries.set(0);
        cacheHits.set(0);
        cacheEvictions.set(0);
        cacheGetTimeTakenNanos.set(0);
        cachePutTimeTakenNanos.set(0);
        cacheRemoveTimeTakenNanos.set(0);
    }

    @Override
    public long getCacheHits() {
        return cacheHits.longValue();
    }

    @Override
    public float getCacheHitPercentage() {
        Long hits = getCacheHits();
        if (hits == 0) {
            return 0;
        }
        return (float) hits / getCacheGets() * 100.0f;
    }

    @Override
    public long getCacheMisses() {
        return cacheMisses.longValue();
    }

    @Override
    public float getCacheMissPercentage() {
        Long misses = getCacheMisses();
        if (misses == 0) {
            return 0;
        }
        return (float) misses / getCacheGets() * 100.0f;
    }

    @Override
    public long getCacheGets() {
        return getCacheHits() + getCacheMisses();
    }

    @Override
    public long getCachePuts() {
        return cachePuts.longValue();
    }

    @Override
    public long getCacheRemovals() {
        return cacheRemovals.longValue();
    }

    @Override
    public long getCacheEvictions() {
        return cacheEvictions.longValue();
    }

    @Override
    public float getAverageGetTime() {
        if (cacheGetTimeTakenNanos.longValue() == 0 || getCacheGets() == 0) {
            return 0;
        }
        return (cacheGetTimeTakenNanos.longValue() / getCacheGets()) / TimeUnit.MICROSECONDS.toNanos(1L);
    }

    @Override
    public float getAveragePutTime() {
        if (cachePutTimeTakenNanos.longValue() == 0 || getCacheGets() == 0) {
            return 0;
        }
        return (cachePutTimeTakenNanos.longValue() / getCacheGets()) / TimeUnit.MICROSECONDS.toNanos(1L);
    }

    @Override
    public float getAverageRemoveTime() {
        if (cacheRemoveTimeTakenNanos.longValue() == 0 || getCacheGets() == 0) {
            return 0;
        }
        return (cacheRemoveTimeTakenNanos.longValue() / getCacheGets()) / TimeUnit.MICROSECONDS.toNanos(1L);
    }

    //* METHODS TO INCREMENT COUNTERS */

    /**
     * Increase the cache removals by the number specified.
     *
     * @param number the number to increase the counter by
     */
    public void increaseCacheRemovals(long number) {
        cacheRemovals.getAndAdd(number);
    }

    /**
     * Increase the expiries by the number specified.
     *
     * @param number the number to increase the counter by
     */
    public void increaseCacheExpiries(long number) {
        cacheExpiries.getAndAdd(number);
    }

    /**
     * Increase the cache puts by the number specified.
     *
     * @param number the number to increase the counter by
     */
    public void increaseCachePuts(long number) {
        cachePuts.getAndAdd(number);
    }

    /**
     * Increase the cache hits by the number specified.
     *
     * @param number the number to increase the counter by
     */
    public void increaseCacheHits(long number) {
        cacheHits.getAndAdd(number);
    }

    /**
     * Increase the cache misses by the number specified.
     *
     * @param number the number to increase the counter by
     */
    public void increaseCacheMisses(long number) {
        cacheMisses.getAndAdd(number);
    }

    /**
     * Increase the cache evictions by the number specified.
     *
     * @param number the number to increase the counter by
     */
    public void increaseCacheEvictions(long number) {
        cacheEvictions.getAndAdd(number);
    }

    /**
     * Increment the get time accumulator.
     *
     * @param duration the time taken in nanoseconds
     */
    public void addGetTimeNano(long duration) {
        if (cacheGetTimeTakenNanos.get() <= Long.MAX_VALUE - duration) {
            cacheGetTimeTakenNanos.addAndGet(duration);
        } else {
            //counter full. Just reset.
            clear();
            cacheGetTimeTakenNanos.set(duration);
        }
    }


    /**
     * Increment the put time accumulator.
     *
     * @param duration the time taken in nanoseconds
     */
    public void addPutTimeNano(long duration) {
        if (cachePutTimeTakenNanos.get() <= Long.MAX_VALUE - duration) {
            cachePutTimeTakenNanos.addAndGet(duration);
        } else {
            //counter full. Just reset.
            clear();
            cachePutTimeTakenNanos.set(duration);
        }
    }

    /**
     * Increment the remove time accumulator.
     *
     * @param duration the time taken in nanoseconds
     */
    public void addRemoveTimeNano(long duration) {
        if (cacheRemoveTimeTakenNanos.get() <= Long.MAX_VALUE - duration) {
            cacheRemoveTimeTakenNanos.addAndGet(duration);
        } else {
            //counter full. Just reset.
            clear();
            cacheRemoveTimeTakenNanos.set(duration);
        }
    }
}
