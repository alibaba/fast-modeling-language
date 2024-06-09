/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.property;

import java.util.Collections;
import java.util.List;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

/**
 * key
 *
 * @author panguanjing
 * @date 2022/6/7
 */
public class DistributionKey extends BaseClientProperty<List<String>> {

    public static final String DISTRIBUTION_KEY = HoloPropertyKey.DISTRIBUTION_KEY.getValue();

    public DistributionKey() {
        this.setKey(DISTRIBUTION_KEY);
    }

    @Override
    public String valueString() {
        return Joiner.on(",").join(this.getValue());
    }

    @Override
    public void setValueString(String value) {
        if (StringUtils.isBlank(value)) {
            this.setValue(Collections.emptyList());
            return;
        }
        List<String> list = Splitter.on(",").splitToList(value);
        this.setValue(list);
    }

    @Override
    public List<String> toColumnList() {
        return getValue();
    }
}
