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

package com.aliyun.fastmodel.transform.api.context;

import java.util.List;
import java.util.Map;

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.ToString;

/**
 * TransformContext
 *
 * @author panguanjing
 * @date 2020/10/16
 */
@ToString
@Getter
public class ReverseContext {

    public enum ReverseTargetStrategy {
        /**
         * DDL模式，比如：create、alter等语句
         */
        DDL,
        /**
         * 脚本模式，比如import、ref
         */
        SCRIPT;
    }

    /**
     * 逆向时设置的目标表类型
     */
    private final TableDetailType reverseTableType;

    /**
     * 扩展信息
     */
    private final List<Property> properties;

    /**
     * columns
     */
    private final Map<Identifier, ColumnDefinition> columnProperties;

    /**
     * 关系转换策略, FMl不仅提供了DDL模式的处理，还提供了Script的语句
     * 比如关系的转换的策略, 默认提供的是DDL模式，但在一些设计的场景，需要转换为Script模式进行处理。
     */
    private final ReverseTargetStrategy reverseRelationStrategy;

    /**
     * 逆向时，有些时候需要将多个语句合并到一个语句里。
     */
    private final boolean merge;

    /**
     * 使用Builder进行传递
     *
     * @param tBuilder
     * @param <T>
     */
    protected <T extends Builder<T>> ReverseContext(Builder tBuilder) {
        Preconditions.checkNotNull(tBuilder);
        reverseTableType = tBuilder.reverseTableType;
        properties = tBuilder.properties;
        columnProperties = tBuilder.columnProperties;
        merge = tBuilder.merge;
        this.reverseRelationStrategy = tBuilder.reverseTargetStrategy;
    }

    /**
     * 初始化builder
     *
     * @return Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder<T extends Builder<T>> {

        /**
         * 逆向表的类型
         */
        private TableDetailType reverseTableType = TableDetailType.NORMAL_DIM;

        /**
         * 自定义属性
         */
        private List<Property> properties = Lists.newArrayListWithCapacity(8);

        /**
         * 列的属性转换
         */
        private Map<Identifier, ColumnDefinition> columnProperties = Maps.newHashMapWithExpectedSize(16);

        /**
         * 关系转换的目标策略
         */
        private ReverseTargetStrategy reverseTargetStrategy = ReverseTargetStrategy.DDL;

        private boolean merge;

        /**
         * 是否需要合并
         *
         * @param merge
         * @return
         */
        public T merge(boolean merge) {
            this.merge = merge;
            return (T)this;
        }

        public T reverseTableType(TableDetailType tableDetailType) {
            reverseTableType = tableDetailType;
            return (T)this;
        }

        public T properties(List<Property> properties) {
            this.properties = properties;
            return (T)this;
        }

        public T property(Property property) {
            properties.add(property);
            return (T)this;
        }

        public T columnProperty(ColumnDefinition columnDefinition) {
            columnProperties.put(columnDefinition.getColName(), columnDefinition);
            return (T)this;
        }

        public T columnsProperty(Map<Identifier, ColumnDefinition> columnProperties) {
            this.columnProperties = columnProperties;
            return (T)this;
        }

        public T reverseTargetStrategy(ReverseTargetStrategy reverseTargetStrategy) {
            this.reverseTargetStrategy = reverseTargetStrategy;
            return (T)this;
        }

        public ReverseContext build() {
            return new ReverseContext(this);
        }

    }

}
