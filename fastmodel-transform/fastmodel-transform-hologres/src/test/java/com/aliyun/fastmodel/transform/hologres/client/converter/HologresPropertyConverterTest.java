/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.hologres.client.converter;

import java.util.Locale;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.hologres.client.property.BinLogTTL;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.ClusterKey;
import com.aliyun.fastmodel.transform.hologres.client.property.EnableBinLogLevel;
import com.aliyun.fastmodel.transform.hologres.client.property.EnableBinLogLevel.BinLogLevel;
import com.aliyun.fastmodel.transform.hologres.client.property.SegmentKey;
import com.aliyun.fastmodel.transform.hologres.client.property.TimeToLiveSeconds;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/6/29
 */
public class HologresPropertyConverterTest {

    HologresPropertyConverter hologresPropertyConverter = HologresPropertyConverter.getInstance();

    @Test
    public void get() {
        BaseClientProperty baseClientProperty = hologresPropertyConverter.create(BinLogTTL.BINLOG_TTL, "100");
        BinLogTTL bingLogTTL = (BinLogTTL)baseClientProperty;
        assertEquals(bingLogTTL.getValue(), new Long(100L));
    }

    @Test
    public void testGetBingLogLevel() {
        BaseClientProperty baseClientProperty = hologresPropertyConverter.create(EnableBinLogLevel.ENABLE_BINLOG, "none");
        EnableBinLogLevel bingLogTTL = (EnableBinLogLevel)baseClientProperty;
        assertEquals(bingLogTTL.getValue(), BinLogLevel.NONE);
    }

    @Test
    public void testClusterKey() {
        BaseClientProperty baseClientProperty = hologresPropertyConverter.create(ClusterKey.CLUSTERING_KEY, "a,b");
        ClusterKey bingLogTTL = (ClusterKey)baseClientProperty;
        assertEquals(bingLogTTL.getValue().size(), 2);
    }

    @Test
    public void testClusterKeyUpper() {
        BaseClientProperty baseClientProperty = hologresPropertyConverter.create(ClusterKey.CLUSTERING_KEY.toUpperCase(Locale.ROOT), "a,b");
        ClusterKey bingLogTTL = (ClusterKey)baseClientProperty;
        assertEquals(bingLogTTL.getValue().size(), 2);
    }

    @Test
    public void testSegment() {
        BaseClientProperty baseClientProperty = hologresPropertyConverter.create(SegmentKey.SEGMENT_KEY, "a,b");
        SegmentKey segmentKey = (SegmentKey)baseClientProperty;
        assertEquals(segmentKey.getValue().size(), 2);
    }

    @Test
    public void testIsValidProperty() {
        boolean validProperty = hologresPropertyConverter.isValidProperty(TimeToLiveSeconds.TIME_TO_LIVE_IN_SECONDS);
        assertTrue(validProperty);
        boolean timeToLiveInSeconds = hologresPropertyConverter.isValidProperty("TIME_TO_LIVE_IN_SECONDS");
        assertTrue(timeToLiveInSeconds);
    }
}