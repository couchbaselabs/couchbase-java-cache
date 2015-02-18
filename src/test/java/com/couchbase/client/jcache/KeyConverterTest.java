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

import org.junit.Test;

public class KeyConverterTest {

    private static final KeyConverter<Double> DOUBLE_KEY_CONVERTER = new KeyConverter<Double>() {
        @Override
        public String asString(Double key) {
            return String.valueOf(key);
        }

        @Override
        public Double fromString(String internalKey) {
            return new Double(internalKey);
        }
    };

    @Test
    public void testStringKeyConverter() {
        String input = "input";

        String converted = KeyConverter.STRING_KEY_CONVERTER.asString(input);
        String convertedBack = KeyConverter.STRING_KEY_CONVERTER.fromString(converted);

        assertEquals(input, converted);
        assertEquals(input, convertedBack);
    }

    @Test
    public void testBidiConversion() {
        final Double input = 123.45;

        String converted = DOUBLE_KEY_CONVERTER.asString(input);
        Double convertedBack = DOUBLE_KEY_CONVERTER.fromString(converted);

        assertEquals("123.45", converted);
        assertEquals(input, convertedBack);
    }

    @Test
    public void testPrefixedConversionPrepends() {
        Double input = 123.45;
        KeyConverter<Double> prefixedConverter = new KeyConverter.PrefixedKeyConverter<Double>(
                DOUBLE_KEY_CONVERTER, "test-");

        String converted = prefixedConverter.asString(input);

        assertEquals("test-123.45", converted);
    }

    @Test
    public void testPrefixedConversionBackRemovesPrefix() {
        String prefixedConverted = "test-123.45";
        KeyConverter<Double> prefixedConverter = new KeyConverter.PrefixedKeyConverter<Double>(
                DOUBLE_KEY_CONVERTER, "test-");

        Double convertedBack = prefixedConverter.fromString(prefixedConverted);

        assertNotNull(convertedBack);
        assertEquals(123.45, convertedBack, 0d);
    }
}