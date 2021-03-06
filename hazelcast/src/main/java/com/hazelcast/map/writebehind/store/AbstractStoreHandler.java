/*
* Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.hazelcast.map.writebehind.store;

import com.hazelcast.map.MapStoreWrapper;

import java.util.Map;

/**
 * TODO add a batching upper limit.
 * @param <T> Type of entry.
 */
abstract class AbstractStoreHandler<T> implements StoreHandler<T> {

    protected StoreHandler<T> successorHandler;

    protected final MapStoreWrapper mapStoreWrapper;

    protected AbstractStoreHandler(MapStoreWrapper storeWrapper) {
        this.mapStoreWrapper = storeWrapper;
    }

    abstract boolean processSingle(Object key, Object value);

    abstract boolean processBatch(Map map);

    @Override
    public boolean single(Object key, Object value) {
        final boolean result = processSingle(key, value);
        if (result) {
            return true;
        }
        if (successorHandler == null) {
            return result;
        }
        return successorHandler.single(key, value);
    }

    @Override
    public boolean batch(Map map) {
        final boolean result = processBatch(map);
        if (result) {
            return true;
        }
        if (successorHandler == null) {
            return result;
        }
        return successorHandler.batch(map);
    }

    @Override
    public void setSuccessorHandler(StoreHandler<T> successorHandler) {
        this.successorHandler = successorHandler;
    }

}
