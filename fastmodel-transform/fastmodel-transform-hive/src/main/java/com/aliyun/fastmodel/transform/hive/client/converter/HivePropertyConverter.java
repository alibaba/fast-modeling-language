/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hive.client.converter;

import java.util.Map;
import java.util.function.Function;

import com.aliyun.fastmodel.transform.api.client.converter.BasePropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.hive.client.property.FieldsTerminated;
import com.aliyun.fastmodel.transform.hive.client.property.LinesTerminated;
import com.aliyun.fastmodel.transform.hive.client.property.StorageFormat;
import com.aliyun.fastmodel.transform.hive.format.HivePropertyKey;
import com.google.common.collect.Maps;

/**
 * converter
 *
 * @author panguanjing
 * @date 2022/8/5
 */
public class HivePropertyConverter extends BasePropertyConverter {

    private final Map<String, Function<String, BaseClientProperty>> map = Maps.newHashMap();

    public HivePropertyConverter() {
        initFunctionMap();
    }

    private void initFunctionMap() {
        this.map.put(HivePropertyKey.STORAGE_FORMAT.getValue(), value -> {
            StorageFormat storageFormat = new StorageFormat();
            storageFormat.setValueString(value);
            return storageFormat;
        });
        this.map.put(HivePropertyKey.FIELDS_TERMINATED.getValue(), value -> {
            FieldsTerminated fieldsTerminated = new FieldsTerminated();
            fieldsTerminated.setValueString(value);
            return fieldsTerminated;
        });
        this.map.put(HivePropertyKey.LINES_TERMINATED.getValue(), value -> {
            LinesTerminated linesTerminated = new LinesTerminated();
            linesTerminated.setValueString(value);
            return linesTerminated;
        });
    }

    @Override
    protected Map<String, Function<String, BaseClientProperty>> getFunctionMap() {
        return map;
    }

    @Override
    protected boolean returnDefaultWhenNotExist() {
        return false;
    }
}
