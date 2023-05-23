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
package org.apache.kafka.connect.runtime.isolation;

/**
 * Helper for having {@code Plugins} use a given classloader within a try-with-resources statement.
 * See {@link Plugins#withClassLoader(ClassLoader)}.
 */
public class LoaderSwap implements AutoCloseable {

    private final ClassLoader savedLoader;

    // package-local, intended only for internal use.
    static LoaderSwap use(ClassLoader loader) {
        ClassLoader savedLoader = compareAndSwapLoaders(loader);
        try {
            return new LoaderSwap(savedLoader);
        } catch (Throwable t) {
            compareAndSwapLoaders(savedLoader);
            throw t;
        }
    }

    private LoaderSwap(ClassLoader savedLoader) {
        this.savedLoader = savedLoader;
    }


    @Override
    public void close() {
        compareAndSwapLoaders(savedLoader);
    }

    private static ClassLoader compareAndSwapLoaders(ClassLoader loader) {
        ClassLoader current = Thread.currentThread().getContextClassLoader();
        if (!current.equals(loader)) {
            Thread.currentThread().setContextClassLoader(loader);
        }
        return current;
    }

}
