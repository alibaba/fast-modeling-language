/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.impl.table;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.google.common.collect.ImmutableList;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/8/30
 */
public class PropertiesCompare implements TableElementCompare {
    @Override
    public List<BaseStatement> compareTableElement(CreateTable before, CreateTable after) {
        List<Property> properties = before.getProperties();
        List<Property> afterProperties = after.getProperties();
        boolean isBeforeEmpty = before.isPropertyEmpty();
        boolean isAfterEmpty = after.isPropertyEmpty();
        if (isBeforeEmpty && isAfterEmpty) {
            return ImmutableList.of();
        }
        //如果原来为空，目标不是空，那么直接设置
        if (isBeforeEmpty && !isAfterEmpty) {
            return ImmutableList.of(new SetTableProperties(after.getQualifiedName(), afterProperties));
        }
        if (isAfterEmpty) {
            return ImmutableList.of(
                new UnSetTableProperties(after.getQualifiedName(), properties.stream().map(Property::getName).collect(
                    Collectors.toList())));
        } else {
            List<Property> unSet = new ArrayList<>();
            List<String> collect = afterProperties.stream().map(Property::getName).collect(Collectors.toList());
            for (Property b : properties) {
                if (!collect.contains(b.getName())) {
                    unSet.add(b);
                }
            }
            List<Property> setProp = new ArrayList<>();
            for (Property ap : afterProperties) {
                if (properties.contains(ap)) {
                    continue;
                } else {
                    setProp.add(ap);
                }
            }
            List<BaseStatement> list = new ArrayList<>();
            if (!unSet.isEmpty()) {
                list.add(
                    new UnSetTableProperties(after.getQualifiedName(), unSet.stream().map(Property::getName).collect(
                        Collectors.toList())));
            }

            if (!setProp.isEmpty()) {
                list.add(new SetTableProperties(after.getQualifiedName(), setProp));
            }
            return list;
        }
    }
}
