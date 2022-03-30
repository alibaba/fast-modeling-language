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

package com.aliyun.fastmodel.compare;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.collect.Sets;

/**
 * isChange
 *
 * @author panguanjing
 * @date 2021/9/18
 */
public class CompareUtil {

    /**
     * 集合的判断
     *
     * @param before
     * @param after
     * @param <T>
     * @return
     */
    public static <T> boolean isChangeCollection(List<T> before, List<T> after) {
        boolean beforeIsNull = before == null;
        boolean afterIsNull = after == null;
        if (beforeIsNull && afterIsNull) {
            return false;
        }
        if (beforeIsNull && after.isEmpty()) {
            return false;
        }
        boolean beforeIsEmpty = before.isEmpty();
        if (beforeIsEmpty && afterIsNull) {
            return false;
        }
        if (beforeIsEmpty && after.isEmpty()) {
            return false;
        }
        List<Identifier> differences = new ArrayList(
            Sets.difference(Sets.newHashSet(before), Sets.newHashSet(after)));
        return !differences.isEmpty();
    }
}
