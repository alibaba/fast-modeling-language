/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.property;

import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.google.common.base.Joiner;

/**
 * DictEncodingColumns
 *
 * @author panguanjing
 * @date 2022/6/7
 */
public class DictEncodingColumn extends BaseClientProperty<List<ColumnStatus>> {

    /**
     * columns
     */
    public static final String DICTIONARY_ENCODING_COLUMN = "dictionary_encoding_columns";

    public DictEncodingColumn() {
        this.setKey(DICTIONARY_ENCODING_COLUMN);
    }

    @Override
    public String valueString() {
        List<String> list = this.getValue().stream().map(
            s -> new StringJoiner(":").add(s.getColumnName()).add(s.getStatus().getValue()).toString()
        ).collect(Collectors.toList());
        return Joiner.on(",").join(list);
    }

    @Override
    public void setValueString(String value) {
        List<ColumnStatus> of = ColumnStatus.of(value, Status.AUTO);
        this.setValue(of);
    }

    @Override
    public List<String> toColumnList() {
        List<ColumnStatus> value = this.getValue();
        if (value == null || value.isEmpty()) {
            return super.toColumnList();
        }
        return value.stream().map(ColumnStatus::getColumnName).collect(Collectors.toList());
    }
}
