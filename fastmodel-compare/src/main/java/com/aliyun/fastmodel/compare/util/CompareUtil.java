/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.util;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.util.PropertyUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * isChange
 *
 * @author panguanjing
 * @date 2021/9/18
 */
public class CompareUtil {

    public static final int NOT_VALID_POSITION = -1;

    /**
     * 集合的判断
     *
     * @param before
     * @param after
     * @param <T>
     * @return
     */
    public static <T> boolean isChangeCollection(List<T> before, List<T> after) {
        Set<T> beforeSet = before == null ? Sets.newHashSet() : Sets.newHashSet(before);
        Set<T> afterSet = after == null ? Sets.newHashSet() : Sets.newHashSet(after);
        return !Sets.difference(beforeSet, afterSet).isEmpty();
    }

    /**
     * get position for special colName
     *
     * @param columnDefines
     * @param oldColName
     * @return
     */
    public static int getPosition(List<ColumnDefinition> columnDefines, Identifier oldColName) {
        if (oldColName == null || columnDefines == null) {
            return NOT_VALID_POSITION;
        }
        for (int i = 0; i < columnDefines.size(); i++) {
            ColumnDefinition columnDefinition = columnDefines.get(i);
            if (Objects.equals(columnDefinition.getColName(), oldColName)) {
                return i;
            }
        }
        return NOT_VALID_POSITION;
    }

    /**
     * merge modify to src
     * example. src : a,b,c, modify: d => a,b,c,d
     * @param src
     * @param modify
     * @return
     */
    public static List<Property> merge(List<Property> src, List<Property> modify) {
        //if modify is null then return src
        if (modify == null) {
            return src;
        }
        //if src is null return modify
        if (src == null) {
            return modify;
        }
        Map<String, String> map = PropertyUtil.toMap(modify);
        List<Property> all = Lists.newArrayList();
        for (Property property : src) {
            String name = property.getName();
            if (map.containsKey(name)) {
                Property newProperty = new Property(name, map.get(name));
                all.add(newProperty);
                map.remove(name);
            } else {
                all.add(property);
            }
        }
        if (!map.isEmpty()) {
            for (String k : map.keySet()) {
                String value = map.get(k);
                Property property = new Property(k, value);
                all.add(property);
            }
        }
        return all;
    }
}
