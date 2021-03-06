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

package com.hazelcast.map.record;

import com.hazelcast.nio.serialization.Data;

class DataRecordWithStats extends AbstractRecordWithStats<Data> {

    protected Data value;

    public DataRecordWithStats(Data keyData, Data value) {
        super(keyData);
        this.value = value;
    }

    public DataRecordWithStats() {
    }

    /**
    * Get record size in bytes.
    * */
    @Override
    public long getCost() {
        long cost = super.getCost();
        final int objectReferenceInBytes = 4;
        // add value size.
        cost += objectReferenceInBytes + (value == null ? 0L : value.getHeapCost());
        return cost;
    }

    public Data getValue() {
        return value;
    }

    public void setValue(Data o) {
        value = o;
    }

    public void invalidate() {
        value = null;
    }
}
