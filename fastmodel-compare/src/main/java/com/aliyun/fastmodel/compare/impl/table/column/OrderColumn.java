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

package com.aliyun.fastmodel.compare.impl.table.column;

import java.util.Objects;

import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 含有位置的列的信息
 *
 * @author panguanjing
 * @date 2022/1/26
 */
@Getter
@Setter
@ToString
public class OrderColumn {
    /**
     * 列的位置
     * zero-based
     */
    private final Integer position;

    /**
     * 列名
     */
    private final ColumnDefinition columnDefinition;

    public OrderColumn(Integer position, ColumnDefinition columnDefinition) {
        this.position = position;
        this.columnDefinition = columnDefinition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {return true;}
        if (o == null || getClass() != o.getClass()) {return false;}
        OrderColumn that = (OrderColumn)o;
        return position.equals(that.position) && columnDefinition.getColName().equals(that.columnDefinition.getColName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(position, columnDefinition.getColName());
    }
}
