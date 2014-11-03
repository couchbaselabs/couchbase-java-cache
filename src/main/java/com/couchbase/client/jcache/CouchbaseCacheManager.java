package com.couchbase.client.jcache;

import java.net.URI;
import java.util.Properties;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;

import com.couchbase.client.jcache.spi.CouchbaseCachingProvider;

/**
 * The Couchbase implementation for the {@link CacheManager}.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @since 1.0
 */
public class CouchbaseCacheManager implements CacheManager {

    private final CouchbaseCachingProvider provider;
    private final URI uri;
    private final ClassLoader classLoader;
    private final Properties properties;

    /**
     * Creates a new CouchbaseCacheManager.
     *
     * @param provider the caching provider used
     * @param uri the uri of the manager
     * @param classLoader the classloader associated with the manager
     * @param properties the properties used by the manager
     */
    public CouchbaseCacheManager(CouchbaseCachingProvider provider, URI uri, ClassLoader classLoader,
                                 Properties properties) {
        this.provider = provider;
        this.uri = uri;
        this.classLoader = classLoader;
        this.properties = properties;
    }

    @Override
    public CachingProvider getCachingProvider() {
        return provider;
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    @Override
    public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C configuration)
            throws IllegalArgumentException {
        return null;
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
        return null;
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName) {
        return null;
    }

    @Override
    public Iterable<String> getCacheNames() {
        return null;
    }

    @Override
    public void destroyCache(String cacheName) {

    }

    @Override
    public void enableManagement(String cacheName, boolean enabled) {

    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {

    }

    @Override
    public void close() {

    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return (T) this;
        }
        throw new IllegalArgumentException("Not of class " + clazz.getName());
    }
}
