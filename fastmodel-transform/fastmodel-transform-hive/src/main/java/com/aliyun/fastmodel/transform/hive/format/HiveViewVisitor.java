/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hive.format;

import com.aliyun.fastmodel.transform.api.format.BaseSampleDataProvider;
import com.aliyun.fastmodel.transform.api.format.BaseViewVisitor;
import com.aliyun.fastmodel.transform.hive.context.HiveTransformContext;

/**
 * hive view visitor
 *
 * @author panguanjing
 * @date 2022/8/8
 */
public class HiveViewVisitor extends BaseViewVisitor<HiveTransformContext> {
    public HiveViewVisitor(HiveTransformContext context) {
        super(context);
    }

    private final HiveSampleDataProvider hiveSampleDataProvider = new HiveSampleDataProvider();

    @Override
    protected BaseSampleDataProvider getProvider() {
        return hiveSampleDataProvider;
    }
}
