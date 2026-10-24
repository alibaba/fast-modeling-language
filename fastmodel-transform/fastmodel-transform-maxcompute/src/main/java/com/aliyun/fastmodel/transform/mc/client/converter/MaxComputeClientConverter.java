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

package com.aliyun.fastmodel.transform.mc.client.converter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.select.order.SortItem;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.converter.BaseClientConverter;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.extension.client.constraint.ExtensionClientConstraintType;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.ClusterKey;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.ColumnOrder;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.AutoPartitionedBy;
import com.aliyun.fastmodel.transform.mc.client.property.AutoPartitionedByClient;
import com.aliyun.fastmodel.transform.mc.client.property.AutoPartitionedByProperty;
import com.aliyun.fastmodel.transform.mc.client.property.LifeCycle;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.format.MaxComputePropertyKey;
import com.aliyun.fastmodel.transform.mc.parser.MaxComputeLanguageParser;
import com.aliyun.fastmodel.transform.mc.parser.tree.constraint.ClusteredByConstraint;
import com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName;
import com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeGenericDataType;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * maxcompute client converter
 *
 * @author panguanjing
 * @date 2022/8/5
 */
public class MaxComputeClientConverter extends BaseClientConverter<MaxComputeContext> {
    private final MaxComputePropertyConverter maxComputePropertyConverter;
    private final MaxComputeLanguageParser maxComputeLanguageParser;

    public MaxComputeClientConverter() {
        maxComputePropertyConverter = new MaxComputePropertyConverter();
        maxComputeLanguageParser = new MaxComputeLanguageParser();
    }

    @Override
    public PropertyConverter getPropertyConverter() {
        return maxComputePropertyConverter;
    }

    @Override
    protected PartitionedBy toPartitionedBy(Table table, List<Column> columns) {
        List<BaseClientProperty> properties = table.getProperties();
        // 查找是否存在AUTO_PARTITIONED_BY
        if (properties == null) {
            return super.toPartitionedBy(table, columns);
        }
        Optional<BaseClientProperty> first = properties.stream().filter(
            p -> StringUtils.equalsIgnoreCase(MaxComputePropertyKey.AUTO_PARTITIONED_BY.getValue(), p.getKey())
        ).findFirst();
        if (first.isPresent()) {
            AutoPartitionedByClient value = (AutoPartitionedByClient)first.get().getValue();
            return new AutoPartitionedBy(
                List.of(),
                maxComputeLanguageParser.parseExpression(value.getExpression()),
                new Identifier(value.getAlias())
            );
        } else {
            return super.toPartitionedBy(table, columns);
        }
    }

    @Override
    protected List<BaseClientProperty> toBaseClientProperty(CreateTable createTable) {
        List<BaseClientProperty> clientProperties = super.toBaseClientProperty(createTable);
        PartitionedBy partitionedBy = createTable.getPartitionedBy();
        if (partitionedBy instanceof AutoPartitionedBy) {
            AutoPartitionedBy autoPartitionedBy = (AutoPartitionedBy)partitionedBy;
            // 将autoPartitionedBy转为AutoPartitionedByProperty
            AutoPartitionedByClient autoPartitionedByClient = new AutoPartitionedByClient(
                autoPartitionedBy.getExpression().toString(),
                autoPartitionedBy.getAlias().getValue()
            );
            AutoPartitionedByProperty autoPartitionedByProperty = new AutoPartitionedByProperty();
            autoPartitionedByProperty.setValue(autoPartitionedByClient);
            clientProperties.add(autoPartitionedByProperty);
        }
        return clientProperties;
    }

    /**
     * 如果是三层模型，那么必须是a.b.c三层定义
     * 如果是二层模型，那么传入的a.b, 那么只会将a认为是database
     *
     * @param createTable
     * @param transformContext
     * @return
     */
    @Override
    protected String toSchema(CreateTable createTable, MaxComputeContext transformContext) {
        QualifiedName qualifiedName = createTable.getQualifiedName();
        if (!qualifiedName.isJoinPath()) {
            return transformContext.getSchema();
        }
        boolean isSecondSchema = qualifiedName.getOriginalParts().size() == SECOND_INDEX;
        if (isSecondSchema) {
            return transformContext.getSchema();
        }
        boolean isThirdSchema = qualifiedName.getOriginalParts().size() == THIRD_INDEX;
        if (isThirdSchema) {
            return qualifiedName.getParts().get(1);
        }
        return transformContext.getSchema();
    }

    @Override
    public Long toLifeCycleSeconds(CreateTable createTable) {
        List<Property> properties = createTable.getProperties();
        Optional<Property> first = properties.stream().filter(
            p -> StringUtils.equalsIgnoreCase(MaxComputePropertyKey.LIFE_CYCLE.getValue(), p.getName())).findFirst();
        if (!first.isPresent()) {
            return null;
        }
        LifeCycle lifeCycle = new LifeCycle();
        lifeCycle.setValueString(first.get().getValue());
        return lifeCycle.toSeconds();
    }

    @Override
    public Boolean isExternal(CreateTable createTable) {
        List<Property> properties = createTable.getProperties();
        Optional<Property> first = properties.stream().filter(
            p -> StringUtils.equalsIgnoreCase(MaxComputePropertyKey.EXTERNAL.getValue(), p.getName())).findFirst();
        return first.filter(property -> BooleanUtils.toBoolean(property.getValue())).isPresent();
    }

    @Override
    protected List<Property> toProperty(Table table, List<BaseClientProperty> properties) {
        List<Property> list = super.toProperty(table, properties);

        Long lifeCycleSeconds = table.getLifecycleSeconds();
        if (lifeCycleSeconds != null) {
            list.add(new Property(MaxComputePropertyKey.LIFE_CYCLE.getValue(), String.valueOf(LifeCycle.toLifeCycle(lifeCycleSeconds))));
        }

        boolean external = table.isExternal();
        if (external) {
            list.add(new Property(MaxComputePropertyKey.EXTERNAL.getValue(), String.valueOf(true)));
        }

        return list;
    }

    @Override
    protected List<ColumnDefinition> toColumnDefinition(Table table, List<Column> columns) {
        // mc是columns分开的处理
        if (columns == null) {
            return Lists.newArrayList();
        }
        return columns.stream().filter(column -> !column.isPartitionKey()).map(c -> this.toColumnDefinition(table, c)).collect(Collectors.toList());
    }

    @Override
    public LanguageParser getLanguageParser() {
        return maxComputeLanguageParser;
    }

    @Override
    public BaseDataType getDataType(Column column) {
        String dataTypeName = column.getDataType();
        if (StringUtils.isBlank(dataTypeName)) {
            throw new IllegalArgumentException("dataType name can't be null:" + column.getName());
        }
        IDataTypeName byValue = null;
        // complex data use parse dataType
        ReverseContext context = ReverseContext.builder().build();
        try {
            byValue = MaxComputeDataTypeName.getByValue(dataTypeName);
        } catch (Exception e) {
            try {
                return maxComputeLanguageParser.parseDataType(dataTypeName, context);
            } catch (ParseException parseException) {
                throw new IllegalArgumentException("not support dataTypeName with:" + dataTypeName);
            }
        }
        Dimension dimension = byValue.getDimension();
        if (dimension == null || dimension == Dimension.ZERO) {
            return DataTypeUtil.simpleType(byValue);
        }
        if (dimension == Dimension.ONE) {
            boolean isValidLength = column.getLength() != null && column.getLength() > 0;
            if (isValidLength) {
                NumericParameter numericParameter = new NumericParameter(String.valueOf(column.getLength()));
                return new MaxComputeGenericDataType(byValue, numericParameter);
            }
        }
        if (dimension == Dimension.TWO) {
            if (column.getPrecision() != null) {
                NumericParameter numericParameter = new NumericParameter(String.valueOf(column.getPrecision()));
                if (column.getScale() == null) {
                    return DataTypeUtil.simpleType(byValue, numericParameter);
                }
                NumericParameter scale = new NumericParameter(String.valueOf(column.getScale()));
                return new MaxComputeGenericDataType(byValue, numericParameter, scale);
            }
        }

        try {
            return maxComputeLanguageParser.parseDataType(dataTypeName, context);
        } catch (ParseException e) {
            throw new IllegalArgumentException("not support dataTypeName with:" + dataTypeName);
        }
    }

    @Override
    public List<Constraint> toOutlineConstraint(CreateTable createTable) {
        List<BaseConstraint> constraints = createTable.getConstraintStatements();
        if (CollectionUtils.isEmpty(constraints)) {
            return Collections.emptyList();
        }

        List<Constraint> resultList = new ArrayList<>();
        Optional<BaseConstraint> baseConstraint = constraints.stream()
            .filter(constraint -> constraint instanceof ClusteredByConstraint).findAny();
        baseConstraint.ifPresent(constraint -> {
            Constraint constraintDto = new Constraint();
            constraintDto.setType(ExtensionClientConstraintType.CLUSTER_BY);
            ClusteredByConstraint clusteredByConstraint = (ClusteredByConstraint)constraint;
            if (CollectionUtils.isNotEmpty(clusteredByConstraint.getColumns())) {
                List<String> columns = clusteredByConstraint.getColumns().stream()
                    .map(Identifier::getValue).collect(Collectors.toList());
                constraintDto.setColumns(columns);
            }

            List<BaseClientProperty> properties = new ArrayList<>();
            StringProperty property = new StringProperty();
            property.setKey(MaxComputePropertyKey.CLUSTERED_BY.getValue());
            property.setValue(clusteredByConstraint.getClusterType().name());
            properties.add(property);

            if (CollectionUtils.isNotEmpty(clusteredByConstraint.getSortColumns())) {
                String sort = clusteredByConstraint.getSortColumns().stream()
                    .map(sortItem -> sortItem.getSortKey().getOrigin() + ":" + sortItem.getOrdering().getCode())
                    .collect(Collectors.joining(","));
                ClusterKey clusterKey = new ClusterKey();
                clusterKey.setValueString(sort);
                properties.add(clusterKey);
            }
            if (clusteredByConstraint.getBucketNum() != null) {
                StringProperty bucket = new StringProperty();
                bucket.setKey(MaxComputePropertyKey.NUMBER_OF_BUCKETS.getValue());
                bucket.setValue(clusteredByConstraint.getBucketNum().getValue().toString());
                properties.add(bucket);
            }
            constraintDto.setProperties(properties);
            resultList.add(constraintDto);
        });

        resultList.addAll(super.toOutlineConstraint(createTable));
        return resultList;
    }

    @Override
    public void setOutlineConstraint(Constraint c, List<BaseConstraint> constraintList) {
        if (ExtensionClientConstraintType.CLUSTER_BY == c.getType()) {
            List<Identifier> columns = null;
            ClusteredByConstraint.ClusterType clusterType = null;
            List<SortItem> sortColumns = null;
            LongLiteral bucketNum = null;
            if (CollectionUtils.isNotEmpty(c.getColumns())) {
                columns = c.getColumns().stream().map(Identifier::new).collect(Collectors.toList());
            }

            if (CollectionUtils.isNotEmpty(c.getProperties())) {
                List<BaseClientProperty> properties = c.getProperties();
                Optional<BaseClientProperty> clusteredByType = properties.stream().filter(
                    property -> StringUtils.equalsIgnoreCase(MaxComputePropertyKey.CLUSTERED_BY.getValue(), property.getKey())).findAny();
                if (clusteredByType.isPresent()) {
                    clusterType = ClusteredByConstraint.ClusterType.valueOf(((String)clusteredByType.get().getValue()).toUpperCase());
                }
                Optional<BaseClientProperty> clusteringKey = properties.stream().filter(
                    property -> StringUtils.equalsIgnoreCase(ClusterKey.CLUSTERING_KEY, property.getKey())).findAny();
                if (clusteringKey.isPresent()) {
                    ClusterKey clusterKey = (ClusterKey)clusteringKey.get();
                    List<ColumnOrder> columnOrders = clusterKey.getValue();
                    sortColumns = columnOrders.stream().map(columnOrder -> {
                        StringLiteral columnName = new StringLiteral(columnOrder.getColumnName());
                        return new SortItem(columnName, columnOrder.getOrder(), null);
                    }).collect(Collectors.toList());
                }
                Optional<BaseClientProperty> bucket = properties.stream().filter(
                    property -> StringUtils.equalsIgnoreCase(MaxComputePropertyKey.NUMBER_OF_BUCKETS.getValue(), property.getKey())).findAny();
                if (bucket.isPresent()) {
                    String value = (String)bucket.get().getValue();
                    bucketNum = new LongLiteral(value);
                }
            }

            ClusteredByConstraint clusteredByConstraint = new ClusteredByConstraint(columns, clusterType, sortColumns, bucketNum);
            constraintList.add(clusteredByConstraint);
        } else {
            super.setOutlineConstraint(c, constraintList);
        }
    }
}
