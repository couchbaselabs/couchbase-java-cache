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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;

/**
 * This class manages registration of event listeners (through their {@link CacheEntryListenerConfiguration}). It also
 * manages dispatching of events. First events are queued using {@link #queueEvent(CacheEntryEvent)}. Once all events
 * have been prepared, {@link #dispatch()} is called to notify the adequate listeners.
 * <p>
 * The event concrete class for this implementation is {@link CouchbaseCacheEntryEvent}.
 *
 * @author Simon Baslé
 * @since 1.0
 */
public class CacheEventManager<K, V> {

    private Set<ListenerEntry<K, V>> entries = new CopyOnWriteArraySet<ListenerEntry<K, V>>();
    private Map<Class<? extends CacheEntryListener>, List<CacheEntryEvent<K, V>>> eventQueues =
            new ConcurrentHashMap<Class<? extends CacheEntryListener>, List<CacheEntryEvent<K, V>>>();

    /**
     * Requests that the specified event be prepared for dispatching to their specific type of listeners.
     *
     * @param event         the event to be dispatched
     */
    public void queueEvent(CacheEntryEvent<K, V> event) {
        if (event == null) {
            throw new NullPointerException("event can't be null");
        }

        Class<? extends CacheEntryListener> listenerClass;
        switch (event.getEventType()) {
            case CREATED:
                listenerClass = CacheEntryCreatedListener.class;
                break;
            case UPDATED:
                listenerClass = CacheEntryUpdatedListener.class;
                break;
            case REMOVED:
                listenerClass = CacheEntryRemovedListener.class;
                break;
            case EXPIRED:
                listenerClass = CacheEntryExpiredListener.class;
                break;
            default:
                throw new IllegalArgumentException("Unknown event type " + event.getEventType());
        }

        List<CacheEntryEvent<K, V>> queue;
        synchronized (this) {
            queue = eventQueues.get(listenerClass);
            if (queue == null) {
                queue = new ArrayList<CacheEntryEvent<K, V>>();
                eventQueues.put(listenerClass, queue);
            }
        }

        queue.add(event);
    }

    private Iterable<CacheEntryEvent<K, V>> filterEvents(ListenerEntry<K, V> listenerEntry,
            List<CacheEntryEvent<K, V>> allEvents) {
        CacheEntryEventFilter filter = listenerEntry.getFilter();
        List<CacheEntryEvent<K, V>> filteredEvents;
        if (filter == null) {
            filteredEvents = allEvents;
        } else {
            filteredEvents = new ArrayList<CacheEntryEvent<K, V>>(allEvents.size());
            for (CacheEntryEvent<K, V> event : allEvents) {
                if (filter.evaluate(event)) {
                    filteredEvents.add(event);
                }
            }
        }
        return filteredEvents;
    }

    protected void dispatchForCreate() {
        List<CacheEntryEvent<K, V>> events = eventQueues.get(CacheEntryCreatedListener.class);
        if (events == null) {
            return;
        }

        for (ListenerEntry entry: entries) {
            if (entry.getListener() instanceof CacheEntryCreatedListener) {
                ((CacheEntryCreatedListener) entry.getListener()).onCreated(filterEvents(entry, events));
            }
        }
    }

    protected void dispatchForUpdate() {
        List<CacheEntryEvent<K, V>> events = eventQueues.get(CacheEntryUpdatedListener.class);
        if (events == null) {
            return;
        }

        for (ListenerEntry entry: entries) {
            if (entry.getListener() instanceof CacheEntryUpdatedListener) {
                ((CacheEntryUpdatedListener) entry.getListener()).onUpdated(filterEvents(entry, events));
            }
        }
    }

    protected void dispatchForRemove() {
        List<CacheEntryEvent<K, V>> events = eventQueues.get(CacheEntryRemovedListener.class);
        if (events == null) {
            return;
        }

        for (ListenerEntry entry: entries) {
            if (entry.getListener() instanceof CacheEntryRemovedListener) {
                ((CacheEntryRemovedListener) entry.getListener()).onRemoved(filterEvents(entry, events));
            }
        }
    }

    protected void dispatchForExpiry() {
        List<CacheEntryEvent<K, V>> events = eventQueues.get(CacheEntryExpiredListener.class);
        if (events == null) {
            return;
        }

        for (ListenerEntry entry: entries) {
            if (entry.getListener() instanceof CacheEntryExpiredListener) {
                ((CacheEntryExpiredListener) entry.getListener()).onExpired(filterEvents(entry, events));
            }
        }
    }

    /**
     * Dispatches the queued events to the registered listeners.
     */
    public void dispatch() {
        try {
            dispatchForExpiry();
            dispatchForCreate();
            dispatchForUpdate();
            dispatchForRemove();
        } catch (Exception e) {
            if (e instanceof CacheEntryListenerException) {
                throw (CacheEntryListenerException) e;
            } else {
                throw new CacheEntryListenerException("Exception on listener execution", e);
            }
        }
    }

    /**
     * Utility method to create a {@link CouchbaseCacheEntryEvent}, {@link #queueEvent(CacheEntryEvent) queue} it and
     * {@link #dispatch() dispatch} it one call.
     *
     * @param type the type of the event to create.
     * @param key the key impacted by the event.
     * @param value the value corresponding to the key after the event.
     * @param oldValueOrNull the old value before the event or null if not applicable.
     * @param source the cache in which the event happened.
     */
    public void queueAndDispatch(EventType type, K key, V value, V oldValueOrNull, CouchbaseCache source) {
        queueEvent(new CouchbaseCacheEntryEvent<K, V>(type, key, value, oldValueOrNull, source));
        dispatch();
    }

    /**
     * Register a new listener using the given configuration.
     *
     * @param config the configuration for the listener.
     */
    public void addListener(CacheEntryListenerConfiguration<K, V> config) {
        ListenerEntry<K, V> entry = new ListenerEntry<K, V>(config);
        this.entries.add(entry);
    }

    /**
     * Deregister a listener that was created using the given configuration.
     *
     * @param config The configuration used to register and create the listener.
     */
    public void removeListener(CacheEntryListenerConfiguration<K, V> config) {
        ListenerEntry<K, V> toRemove = new ListenerEntry<K, V>(config);
        entries.remove(toRemove);
    }

    protected static final class ListenerEntry<K, V> {

        private final CacheEntryListener<? super K, ? super V> listener;
        private final CacheEntryEventFilter<? super K, ? super V> filter;
        private final boolean isOldValueRequired;
        private final boolean isSynchronous;

        public ListenerEntry(CacheEntryListenerConfiguration<? super K, ? super V> configuration) {
            this.listener = configuration.getCacheEntryListenerFactory().create();
            this.filter = configuration.getCacheEntryEventFilterFactory() == null
                    ? null
                    : configuration.getCacheEntryEventFilterFactory().create();
            this.isOldValueRequired = configuration.isOldValueRequired();
            this.isSynchronous = configuration.isSynchronous();
        }

        public CacheEntryListener<? super K, ? super V> getListener() {
            return listener;
        }

        public CacheEntryEventFilter<? super K, ? super V> getFilter() {
            return filter;
        }

        public boolean isOldValueRequired() {
            return isOldValueRequired;
        }

        public boolean isSynchronous() {
            return isSynchronous;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ListenerEntry that = (ListenerEntry) o;

            if (isOldValueRequired != that.isOldValueRequired) {
                return false;
            }
            if (isSynchronous != that.isSynchronous) {
                return false;
            }
            if (filter != null ? !filter.equals(that.filter) : that.filter != null) {
                return false;
            }

            return listener.equals(that.listener);
        }

        @Override
        public int hashCode() {
            int result = listener.hashCode();
            result = 31 * result + (filter != null ? filter.hashCode() : 0);
            result = 31 * result + (isOldValueRequired ? 1 : 0);
            result = 31 * result + (isSynchronous ? 1 : 0);
            return result;
        }
    }
}
