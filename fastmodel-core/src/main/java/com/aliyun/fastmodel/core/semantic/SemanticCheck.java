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

package com.aliyun.fastmodel.core.semantic;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.aliyun.fastmodel.core.exception.SemanticException;

/**
 * semantic check
 *
 * @author panguanjing
 * @date 2020/9/17
 */
public interface SemanticCheck<T> {

    /**
     * check statement
     *
     * @param baseStatement 创建的语句
     * @throws SemanticException 语义检查
     */
    void check(T baseStatement) throws SemanticException;

    /**
     * 是否为空
     *
     * @param colNames 列名集合
     * @return true/false
     */
    default boolean isEmpty(List<String> colNames) {
        return colNames == null || colNames.isEmpty();
    }

    /**
     * 是否包含
     *
     * @param sets     集合列表名
     * @param colNames 列名
     * @return 不包含的
     */
    default List<String> notContains(Set<String> sets, List<String> colNames) {
        List<String> notContains = new ArrayList<>();
        for (String col : colNames) {
            boolean contain = sets.contains(col.toLowerCase());
            if (!contain) {
                notContains.add(col);
            }
        }
        return notContains;
    }
}
