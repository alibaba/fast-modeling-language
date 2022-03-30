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

/**
 * 语义的错误码
 *
 * @author panguanjing
 * @date 2020/10/15
 */
public enum SemanticErrorCode {
    /**
     * 表含有相同的列
     */
    TABLE_HAVE_SAME_COLUMN,

    /**
     * 表的约束的名必须唯一
     */
    TABLE_CONSTRAINT_MUST_UNIQUE,

    /**
     * 主键的约束只能有一个
     */
    TABLE_PRIMARY_KEY_MUST_ONE,

    /**
     * 约束的列名，必须存在
     */
    TABLE_CONSTRAINT_COL_MUST_EXIST,

    /**
     * 约束引用的个数, 与列的个数不相等
     */
    TABLE_CONSTRAINT_REF_SIZE_MUST_EQUAL;
}
