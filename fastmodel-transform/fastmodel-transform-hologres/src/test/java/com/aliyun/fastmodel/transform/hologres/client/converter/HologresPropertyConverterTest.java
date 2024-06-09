/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.converter;

import java.util.Locale;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.hologres.client.property.BinLogTTL;
import com.aliyun.fastmodel.transform.hologres.client.property.ClusterKey;
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