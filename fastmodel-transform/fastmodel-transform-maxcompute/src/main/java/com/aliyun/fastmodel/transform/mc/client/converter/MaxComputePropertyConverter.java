/*
 * Copyright 2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.transform.mc.client.converter;

import java.util.Map;
import java.util.function.Function;

import com.aliyun.fastmodel.transform.api.client.converter.BasePropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.ClusterKey;
import com.aliyun.fastmodel.transform.mc.client.property.AcidDataRetainHours;
import com.aliyun.fastmodel.transform.mc.client.property.AcidIncrementalQueryOutOfTimeRangeEnabled;
import com.aliyun.fastmodel.transform.mc.client.property.AcidTransactional;
import com.aliyun.fastmodel.transform.mc.client.property.AcidWriteBucketNum;
import com.aliyun.fastmodel.transform.mc.client.property.LifeCycle;
import com.aliyun.fastmodel.transform.mc.format.MaxComputePropertyKey;
import com.google.common.collect.Maps;

/**
 * converter
 *
 * @author panguanjing
 * @date 2022/8/5
 */
public class MaxComputePropertyConverter extends BasePropertyConverter {

    private final Map<String, Function<String, BaseClientProperty>> map = Maps.newHashMap();

    public MaxComputePropertyConverter() {
        initFunctionMap();
    }

    private void initFunctionMap() {
        this.map.put(MaxComputePropertyKey.LIFE_CYCLE.getValue(), value -> {
            LifeCycle lifeCycle = new LifeCycle();
            lifeCycle.setValueString(value);
            return lifeCycle;
        });
        this.map.put(MaxComputePropertyKey.TRANSACTIONAL.getValue(), value -> {
            AcidTransactional transactional = new AcidTransactional();
            transactional.setValueString(value);
            return transactional;
        });

        this.map.put(MaxComputePropertyKey.WRITE_BUCKET_NUM.getValue(), value -> {
            AcidWriteBucketNum writeBucketNum = new AcidWriteBucketNum();
            writeBucketNum.setValueString(value);
            return writeBucketNum;
        });

        this.map.put(MaxComputePropertyKey.ACID_DATA_RETAIN_HOURS.getValue(), value -> {
            AcidDataRetainHours dataRetainHours = new AcidDataRetainHours();
            dataRetainHours.setValueString(value);
            return dataRetainHours;
        });

        this.map.put(MaxComputePropertyKey.ACID_INCREMENTAL_QUERY_OUT_OF_TIME_RANGE_ENABLED.getValue(), value -> {
            AcidIncrementalQueryOutOfTimeRangeEnabled timeRangeEnabled = new AcidIncrementalQueryOutOfTimeRangeEnabled();
            timeRangeEnabled.setValueString(value);
            return timeRangeEnabled;
        });

        this.map.put(MaxComputePropertyKey.TABLE_FORMAT_VERSION.getValue(), value -> {
            StringProperty stringProperty = new StringProperty();
            stringProperty.setKey(MaxComputePropertyKey.TABLE_FORMAT_VERSION.getValue());
            stringProperty.setValueString(value);
            return stringProperty;
        });

        this.map.put(ClusterKey.CLUSTERING_KEY, value -> {
            ClusterKey clusterKey = new ClusterKey();
            clusterKey.setValueString(value);
            return clusterKey;
        });
    }

    @Override
    protected Map<String, Function<String, BaseClientProperty>> getFunctionMap() {
        return map;
    }
}
