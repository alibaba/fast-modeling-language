/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.converter;

import java.util.Map;
import java.util.function.Function;

import com.aliyun.fastmodel.transform.api.client.converter.BasePropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.hologres.client.property.BinLogTTL;
import com.aliyun.fastmodel.transform.hologres.client.property.BitMapColumn;
import com.aliyun.fastmodel.transform.hologres.client.property.ClusterKey;
import com.aliyun.fastmodel.transform.hologres.client.property.DictEncodingColumn;
import com.aliyun.fastmodel.transform.hologres.client.property.DistributionKey;
import com.aliyun.fastmodel.transform.hologres.client.property.EnableBinLogLevel;
import com.aliyun.fastmodel.transform.hologres.client.property.EventTimeColumn;
import com.aliyun.fastmodel.transform.hologres.client.property.HoloPropertyKey;
import com.aliyun.fastmodel.transform.hologres.client.property.SegmentKey;
import com.aliyun.fastmodel.transform.hologres.client.property.TableGroup;
import com.aliyun.fastmodel.transform.hologres.client.property.TableOrientation;
import com.aliyun.fastmodel.transform.hologres.client.property.TimeToLiveSeconds;
import com.google.common.collect.Maps;

/**
 * converter
 *
 * @author panguanjing
 * @date 2022/6/29
 */
public class HologresPropertyConverter extends BasePropertyConverter {

    private static final HologresPropertyConverter INSTANCE = new HologresPropertyConverter();

    private final Map<String, Function<String, BaseClientProperty>> PROPERTY_FUNCTION_MAP = Maps.newHashMap();

    private HologresPropertyConverter() {
        init();
    }

    public static HologresPropertyConverter getInstance() {
        return INSTANCE;
    }

    public void init() {
        PROPERTY_FUNCTION_MAP.put(BitMapColumn.BITMAP_COLUMN, value -> {
            BitMapColumn bitMapColumn = new BitMapColumn();
            bitMapColumn.setValueString(value);
            return bitMapColumn;
        });

        PROPERTY_FUNCTION_MAP.put(ClusterKey.CLUSTERING_KEY, value -> {
            ClusterKey clusterKeys = new ClusterKey();
            clusterKeys.setValueString(value);
            return clusterKeys;
        });

        PROPERTY_FUNCTION_MAP.put(DictEncodingColumn.DICTIONARY_ENCODING_COLUMN, value -> {
            DictEncodingColumn encodingColumns = new DictEncodingColumn();
            encodingColumns.setValueString(value);
            return encodingColumns;
        });

        PROPERTY_FUNCTION_MAP.put(DistributionKey.DISTRIBUTION_KEY, value -> {
            DistributionKey distributionKey = new DistributionKey();
            distributionKey.setValueString(value);
            return distributionKey;
        });

        PROPERTY_FUNCTION_MAP.put(EventTimeColumn.EVENT_TIME_COLUMN, value -> {
            EventTimeColumn eventTimeColumn = new EventTimeColumn();
            eventTimeColumn.setValueString(value);
            return eventTimeColumn;
        });

        PROPERTY_FUNCTION_MAP.put(TableGroup.TABLE_GROUP, value -> {
            TableGroup group = new TableGroup();
            group.setValueString(value);
            return group;
        });

        PROPERTY_FUNCTION_MAP.put(TableOrientation.ORIENTATION, value -> {
            TableOrientation tableOrientation = new TableOrientation();
            tableOrientation.setValueString(value);
            return tableOrientation;
        });

        PROPERTY_FUNCTION_MAP.put(EnableBinLogLevel.ENABLE_BINLOG, value -> {
            EnableBinLogLevel enableBingLogLevel = new EnableBinLogLevel();
            enableBingLogLevel.setValueString(value);
            return enableBingLogLevel;
        });

        PROPERTY_FUNCTION_MAP.put(HoloPropertyKey.BINLOG_TTL.getValue(), value -> {
            BinLogTTL ttl = new BinLogTTL();
            ttl.setValueString(value);
            return ttl;
        });

        PROPERTY_FUNCTION_MAP.put(TimeToLiveSeconds.TIME_TO_LIVE_IN_SECONDS, value -> {
            TimeToLiveSeconds timeToLiveSeconds = new TimeToLiveSeconds();
            timeToLiveSeconds.setValueString(value);
            return timeToLiveSeconds;
        });

        PROPERTY_FUNCTION_MAP.put(SegmentKey.SEGMENT_KEY, value -> {
            SegmentKey segmentKey = new SegmentKey();
            segmentKey.setValueString(value);
            return segmentKey;
        });
    }

    /**
     * init
     */
    @Override
    protected Map<String, Function<String, BaseClientProperty>> getFunctionMap() {
        return PROPERTY_FUNCTION_MAP;
    }

    public boolean isValidProperty(String key) {
        return PROPERTY_FUNCTION_MAP.containsKey(key.toLowerCase());
    }
}
