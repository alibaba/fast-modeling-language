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
import java.util.stream.Collectors;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * cluster keys
 *
 * @author panguanjing
 * @date 2022/6/13
 */
public class ClusterKey extends BaseClientProperty<List<ColumnOrder>> {

    public static final String CLUSTERING_KEY = HoloPropertyKey.CLUSTERING_KEY.getValue();

    public ClusterKey() {
        this.setKey(CLUSTERING_KEY);
    }

    @Override
    public String valueString() {
        return this.getValue().stream().map(
            c -> {
                if (c.getOrder() != null) {
                    return c.getColumnName() + ":" + c.getOrder().getCode();
                } else {
                    return c.getColumnName();
                }
            }
        ).collect(Collectors.joining(","));
    }

    @Override
    public void setValueString(String value) {
        if (StringUtils.isBlank(value)) {
            this.setValue(Lists.newArrayList());
        }
        List<ColumnOrder> columnOrders = ColumnOrder.of(value);
        this.setValue(columnOrders);
    }

    @Override
    public List<String> toColumnList() {
        List<ColumnOrder> value1 = getValue();
        if (CollectionUtils.isEmpty(value1)) {
            return Collections.emptyList();
        }
        return value1.stream().map(ColumnOrder::getColumnName).collect(Collectors.toList());
    }

    @Override
    public void setColumnList(List<String> columnList) {
        if (columnList == null || columnList.isEmpty()) {
            return;
        }
        List<ColumnOrder> columnOrders = this.getValue();
        if (columnOrders == null) {
            columnOrders = Lists.newArrayList();
            for (String c : columnList) {
                ColumnOrder columnStatus = ColumnOrder.builder().columnName(c).build();
                columnOrders.add(columnStatus);
            }
            this.setValue(columnOrders);
            return;
        }
        for (int i = 0; i < columnList.size(); i++) {
            ColumnOrder columnOrder = columnOrders.get(i);
            String newColumn = columnList.get(i);
            columnOrder.setColumnName(newColumn);
        }
    }
}
