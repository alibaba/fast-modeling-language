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
 * bit map column
 *
 * @author panguanjing
 * @date 2022/6/13
 */
public class BitMapColumn extends BaseClientProperty<List<ColumnStatus>> {

    public static final String BITMAP_COLUMN = "bitmap_columns";

    public BitMapColumn() {
        this.setKey(BITMAP_COLUMN);
    }

    @Override
    public String valueString() {
        List<String> list = this.getValue().stream().map(
            s -> {
                StringJoiner stringJoiner = new StringJoiner(":");
                if (s.getStatus() != null) {
                    return stringJoiner.add(s.getColumnName()).add(s.getStatus().getValue()).toString();
                } else {
                    return s.getColumnName();
                }
            }
        ).collect(Collectors.toList());
        return Joiner.on(",").join(list);
    }

    @Override
    public void setValueString(String value) {
        List<ColumnStatus> columnStatuses = ColumnStatus.of(value, null);
        this.setValue(columnStatuses);
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
