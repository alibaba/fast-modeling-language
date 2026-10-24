/*
 * Copyright 2022 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc.format;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.transform.api.context.setting.ViewSetting;
import com.aliyun.fastmodel.transform.api.format.BaseSampleDataProvider;
import com.aliyun.fastmodel.transform.api.format.BaseViewVisitor;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.util.MaxComputeSampleUtil;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * 针对create materialize view的visitor
 *
 * @author panguanjing
 * @date 2022/5/25
 */
public class MaxComputeMaterializedViewVisitor extends BaseViewVisitor<MaxComputeContext> {

    private final MaxComputeSampleUtil maxComputeSampleUtil = new MaxComputeSampleUtil();

    public MaxComputeMaterializedViewVisitor(MaxComputeContext maxComputeContext) {
        super(maxComputeContext);
    }

    @Override
    public Boolean visitCreateTable(CreateTable createTable, Integer indent) {
        builder.append("CREATE MATERIALIZED VIEW ");
        if (createTable.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        builder.append(getCode(createTable.getQualifiedName()));
        String s = formatLifecycle(createTable);
        builder.append(s);
        String elementIndent = indentString(indent + 1);
        boolean columnNotEmpty = !createTable.isColumnEmpty();
        List<ColumnDefinition> allColumnDefinition = Lists.newArrayList();
        if (columnNotEmpty) {
            allColumnDefinition.addAll(createTable.getColumnDefines());
        }
        boolean partitionEmpty = createTable.isPartitionEmpty();
        if (!partitionEmpty) {
            allColumnDefinition.addAll(createTable.getPartitionedBy().getColumnDefinitions());
        }
        if (CollectionUtils.isNotEmpty(allColumnDefinition)) {
            builder.append(newLine("("));
            builder.append(formatColumnList(allColumnDefinition, elementIndent));
            builder.append(newLine(")"));
        }

        if (!partitionEmpty) {
            builder.append(formatPartitionOn(createTable.getPartitionedBy().getColumnDefinitions()));
        }
        if (!isEndNewLine(builder.toString())) {
            builder.append("\n");
        }
        builder.append("AS ");
        ViewSetting transformToView = context.getViewSetting();
        if (transformToView.isUseQueryCode()) {
            builder.append(transformToView.getQueryCode());
        } else {
            Query query = transformToView.getQuery();
            if (query != null) {
                process(query, indent);
            } else {
                if (!allColumnDefinition.isEmpty()) {
                    query = sampleQuery(allColumnDefinition);
                    process(query, indent);
                }
            }
        }
        return !allColumnDefinition.isEmpty();
    }

    private String formatLifecycle(CreateTable createTable) {
        ///优先从fml的模型对象中，获取lifecycle对象
        if (createTable.isPropertyEmpty()) {
            return StringUtils.EMPTY;
        }
        List<Property> properties = createTable.getProperties();
        Optional<Property> first = properties.stream().filter(
            x -> x.getName().equalsIgnoreCase(MaxComputePropertyKey.LIFE_CYCLE.getValue())).findFirst();
        return first.map(property -> " LIFECYCLE " + property.getValue()).orElse(StringUtils.EMPTY);
    }

    private String formatPartitionOn(List<ColumnDefinition> columnDefinitions) {
        return "PARTITIONED ON (" + columnDefinitions.stream().map(c -> c.getColName().getValue()).collect(Collectors.joining(",")) + ")";
    }

    @Override
    public Boolean visitDropTable(DropTable dropTable, Integer context) {
        builder.append("DROP MATERIALIZED VIEW ");
        if (dropTable.isExists()) {
            builder.append("IF EXISTS ");
        }
        builder.append(getCode(dropTable.getQualifiedName()));
        return true;
    }

    @Override
    protected BaseSampleDataProvider getProvider() {
        return maxComputeSampleUtil;
    }
}
