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

package com.aliyun.fastmodel.transform.api.client.dto.table;

import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * table client config
 *
 * @author panguanjing
 * @date 2022/6/6
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableConfig {

    /**
     * 方言名
     */
    private DialectMeta dialectMeta;
    /**
     * 默认忽略大小写
     */
    private boolean caseSensitive;

    /**
     * 是否先删除原有的表
     * 默认不生成
     */
    private boolean dropIfExist;

    /**
     * 生成的sql是否需要增加分号
     */
    @Default
    private boolean appendSemicolon = true;

    /**
     * 是否合并alterTable的操作
     */
    private boolean mergeAlterTableOperations;

    /**
     * 是否过滤属性
     * 是否对用户传入的属性进行过滤，去掉自定义属性
     */
    @Default
    private boolean filterProperties = true;

}
