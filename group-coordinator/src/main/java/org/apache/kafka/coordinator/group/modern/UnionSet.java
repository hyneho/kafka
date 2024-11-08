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
package org.apache.kafka.coordinator.group.modern;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

public class UnionSet<T> implements Set<T> {
    private final Set<T> largeSet;
    private final Set<T> smallSet;
    private int size = -1;

    public UnionSet(Set<T> s1, Set<T> s2) {
        Objects.requireNonNull(s1);
        Objects.requireNonNull(s2);

        if (s1.size() > s2.size()) {
            largeSet = s1;
            smallSet = s2;
        } else {
            largeSet = s2;
            smallSet = s1;
        }
    }

    @Override
    public int size() {
        if (size == -1) {
            size = largeSet.size();
            for (T item : smallSet) {
                if (!largeSet.contains(item)) {
                    size++;
                }
            }
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        return largeSet.isEmpty() && smallSet.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return largeSet.contains(o) || smallSet.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            private final Iterator<T> largeSetIterator = largeSet.iterator();
            private final Iterator<T> smallSetIterator = smallSet.iterator();
            private T next = null;

            @Override
            public boolean hasNext() {
                if (next != null) return true;
                if (largeSetIterator.hasNext()) {
                    next = largeSetIterator.next();
                    return true;
                }
                while (smallSetIterator.hasNext()) {
                    next = smallSetIterator.next();
                    if (!largeSet.contains(next)) {
                        return true;
                    }
                }
                next = null;
                return false;
            }

            @Override
            public T next() {
                if (!hasNext()) throw new NoSuchElementException();
                T result = next;
                next = null;
                return result;
            }
        };
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(T t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (!contains(o)) return false;
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UnionSet<?> unionSet = (UnionSet<?>) o;

        if (!Objects.equals(largeSet, unionSet.largeSet)) return false;
        return Objects.equals(smallSet, unionSet.smallSet);
    }

    @Override
    public int hashCode() {
        int result = largeSet != null ? largeSet.hashCode() : 0;
        result = 31 * result + (smallSet != null ? smallSet.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "UnionSet(" +
            "largeSet=" + largeSet +
            ", smallSet=" + smallSet +
            ')';
    }
}
