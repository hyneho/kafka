/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.metadata.authorizer.trie;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StringInserterTest {
    @Test
    public void noWildcardPatternTests() {
        StringInserter underTest = new StringInserter("HelloWorld");
        assertEquals("HelloWorld", underTest.getFragment());
        assertFalse(underTest.isWildcard());
        underTest.advance(underTest.getFragment().length());
        assertTrue(underTest.isEmpty());
    }

    @Test
    public void questionWildcardPatternTest() {
        StringInserter underTest = new StringInserter("Hell?World");
        assertEquals("Hell", underTest.getFragment());
        assertFalse(underTest.isWildcard());
        underTest.advance(underTest.getFragment().length());
        assertFalse(underTest.isEmpty());

        assertEquals("?", underTest.getFragment());
        assertTrue(underTest.isWildcard());
        underTest.advance(underTest.getFragment().length());
        assertFalse(underTest.isEmpty());

        assertEquals("World", underTest.getFragment());
        assertFalse(underTest.isWildcard());
        underTest.advance(underTest.getFragment().length());
        assertTrue(underTest.isEmpty());
    }

    @Test
    public void splatWildcardPatternTest() {
        StringInserter underTest = new StringInserter("Hell*rld");
        assertEquals("Hell", underTest.getFragment());
        assertFalse(underTest.isWildcard());
        underTest.advance(underTest.getFragment().length());
        assertFalse(underTest.isEmpty());

        assertEquals("*", underTest.getFragment());
        assertTrue(underTest.isWildcard());
        underTest.advance(underTest.getFragment().length());
        assertFalse(underTest.isEmpty());

        assertEquals("rld", underTest.getFragment());
        assertFalse(underTest.isWildcard());
        underTest.advance(underTest.getFragment().length());
        assertTrue(underTest.isEmpty());
    }

}
