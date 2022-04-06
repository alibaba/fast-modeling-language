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

package com.aliyun.fastmodel.transform.graph.domain.table;

import com.aliyun.fastmodel.core.tree.statement.constants.TableType;
import com.aliyun.fastmodel.core.tree.statement.table.type.ITableType;
import com.aliyun.fastmodel.transform.graph.domain.VertexType;

/**
 * 表的顶点类型
 *
 * @author panguanjing
 * @date 2021/12/24
 */
public enum TableVertexType implements VertexType {
    /**
     * dim table
     */
    DIM,
    /**
     * fact table
     */
    FACT,
    /**
     * dws table
     */
    DWS,

    /**
     * ads table
     */
    ADS;

    @Override
    public String getName() {
        return name();
    }

    public static TableVertexType formTableType(ITableType tableType) {
        TableVertexType[] tableVertexTypes = TableVertexType.values();
        for (TableVertexType tableVertexType : tableVertexTypes) {
            if (tableVertexType.getName().equalsIgnoreCase(tableType.getCode())) {
                return tableVertexType;
            }
        }
        return null;
    }
}
