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

package com.aliyun.fastmodel.core.tree.statement.constants;

/**
 * 列属性中，FML中默认的属性名
 *
 * @author panguanjing
 * @date 2021/5/31
 */
public enum ColumnPropertyDefaultKey {
    /**
     * 关联码表
     */
    code_table,

    /**
     * 分区表达式
     */
    partition_spec,

    /**
     * uuid
     */
    uuid;
}
