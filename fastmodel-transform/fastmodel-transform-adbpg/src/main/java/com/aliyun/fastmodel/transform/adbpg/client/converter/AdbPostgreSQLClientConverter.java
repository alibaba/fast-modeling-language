/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.adbpg.client.converter;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.transform.adbpg.context.AdbPostgreSQLTransformContext;
import com.aliyun.fastmodel.transform.adbpg.format.AdbPostgreSQLOutVisitor;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLLanguageParser;
import com.aliyun.fastmodel.transform.adbpg.parser.tree.AdbPostgreSQLPartitionBy;
import com.aliyun.fastmodel.transform.adbpg.parser.tree.partition.desc.AdbPostgreSQLRangeElement;
import com.aliyun.fastmodel.transform.adbpg.parser.tree.partition.desc.PartitionIntervalExpression;
import com.aliyun.fastmodel.transform.adbpg.parser.tree.partition.desc.PartitionValueExpression;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.extension.client.constraint.DistributeClientConstraint;
import com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.PartitionElementClient;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.SubPartitionClient;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.SubPartitionElementClient;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.list.ListPartitionClient;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.list.ListPartitionElementClient;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.list.ListPartitionProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.list.SubListPartitionClient;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.list.SubListPartitionElementClient;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.list.SubListTemplatePartitionClient;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.range.RangePartitionClient;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.range.RangePartitionElementClient;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.range.RangePartitionProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.range.SubRangePartitionClient;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.range.SubRangePartitionElementClient;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.range.SubRangePartitionElementClient.RangeExpression;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.range.SubRangePartitionElementClient.RangeIntervalExpression;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.range.SubRangeTemplatePartitionClient;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeNonKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.PartitionByType;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.PartitionDesc;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.BaseSubPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.SubListPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.SubRangePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.BasePartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.BaseSubPartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.ListPartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.RangePartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.SubListPartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.SubPartitionList;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.SubRangePartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.template.SubListTemplatePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.template.SubRangeTemplatePartition;
import com.aliyun.fastmodel.transform.postgresql.client.converter.PostgreSQLClientConverter;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype.PostgreSQLDataTypeName;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * converter
 *
 * @author panguanjing
 * @date 2024/10/18
 */
public class AdbPostgreSQLClientConverter extends PostgreSQLClientConverter<AdbPostgreSQLTransformContext> {
    @Override
    public IDataTypeName getDataTypeName(String dataTypeName) {
        return PostgreSQLDataTypeName.getByValue(dataTypeName);
    }

    @Override
    public String getRaw(Node node) {
        AdbPostgreSQLOutVisitor adbPostgreSQLOutVisitor = new AdbPostgreSQLOutVisitor(AdbPostgreSQLTransformContext.builder().build());
        node.accept(adbPostgreSQLOutVisitor, 0);
        return adbPostgreSQLOutVisitor.getBuilder().toString();
    }

    @Override
    public List<BaseClientProperty> toBaseClientProperty(CreateTable createTable) {
        List<BaseClientProperty> baseClientProperties = super.toBaseClientProperty(createTable);
        PartitionedBy partitionedBy = createTable.getPartitionedBy();
        if (partitionedBy instanceof AdbPostgreSQLPartitionBy) {
            AdbPostgreSQLPartitionBy partitionBy = (AdbPostgreSQLPartitionBy)partitionedBy;
            PartitionByType partitionByType = partitionBy.getPartitionByType();
            if (partitionByType == PartitionByType.RANGE) {
                RangePartitionProperty property = toRangePartitionProperty(partitionBy);
                baseClientProperties.add(property);
            } else if (partitionByType == PartitionByType.LIST) {
                ListPartitionProperty property = toListPartitionProperty(partitionBy);
                baseClientProperties.add(property);
            }
        }
        return baseClientProperties;
    }

    @Override
    protected PartitionedBy toPartitionedBy(Table table, List<Column> columns) {
        List<BaseClientProperty> properties = table.getProperties();
        if (properties == null) {
            return null;
        }
        Optional<BaseClientProperty> first = properties.stream().filter(p -> {
            return StringUtils.equalsIgnoreCase(p.getKey(), ExtensionPropertyKey.TABLE_PARTITION.getValue());
        }).findFirst();
        if (first.isEmpty()) {
            return null;
        }
        BaseClientProperty baseClientProperty = first.get();
        if (baseClientProperty instanceof ListPartitionProperty) {
            ListPartitionProperty hashPartitionProperty = (ListPartitionProperty)baseClientProperty;
            return toListPartitionBy(hashPartitionProperty);
        }
        if (baseClientProperty instanceof RangePartitionProperty) {
            RangePartitionProperty rangePartitionProperty = (RangePartitionProperty)baseClientProperty;
            return toRangePartitionBy(rangePartitionProperty);
        }
        return null;
    }

    private PartitionedBy toRangePartitionBy(RangePartitionProperty rangePartitionProperty) {
        RangePartitionClient value = rangePartitionProperty.getValue();
        List<ColumnDefinition> columns;
        columns = value.getColumns().stream().map(c -> ColumnDefinition.builder().colName(new Identifier(c)).build()).collect(Collectors.toList());
        List<BaseSubPartition> subPartitions = null;
        if (value.getSubPartitionClients() != null) {
            subPartitions = value.getSubPartitionClients().stream().map(this::toBaseSubPartition).collect(Collectors.toList());
        }
        List<BasePartitionElement> partitionElements = null;
        if (value.getPartitionElementClients() != null) {
            partitionElements = value.getPartitionElementClients().stream().map(this::toBasePartitionElement).collect(Collectors.toList());
        }
        return new AdbPostgreSQLPartitionBy(columns, PartitionByType.RANGE, subPartitions, partitionElements);
    }

    private PartitionedBy toListPartitionBy(ListPartitionProperty listPartitionProperty) {
        List<ColumnDefinition> columns;
        ListPartitionClient value = listPartitionProperty.getValue();
        columns = value.getColumns().stream().map(c -> ColumnDefinition.builder().colName(new Identifier(c)).build()).collect(Collectors.toList());
        List<BaseSubPartition> subPartitions = null;
        if (value.getSubPartitionClients() != null) {
            subPartitions = value.getSubPartitionClients().stream().map(this::toBaseSubPartition).collect(Collectors.toList());
        }
        List<BasePartitionElement> partitionElements = null;
        if (value.getPartitionElementClients() != null) {
            partitionElements = value.getPartitionElementClients().stream().map(this::toBasePartitionElement).collect(Collectors.toList());
        }
        return new AdbPostgreSQLPartitionBy(columns, PartitionByType.LIST, subPartitions, partitionElements);
    }

    private BasePartitionElement toBasePartitionElement(PartitionElementClient partitionElementClient) {
        if (partitionElementClient instanceof ListPartitionElementClient) {
            ListPartitionElementClient c = (ListPartitionElementClient)partitionElementClient;
            QualifiedName qualifiedName = null;
            if (c.getQualifiedName() != null) {
                qualifiedName = QualifiedName.of(c.getQualifiedName());
            }
            List<BaseExpression> expressionList = null;
            if (c.getExpressionList() != null) {
                expressionList = c.getExpressionList().stream().map(e -> (BaseExpression)getLanguageParser().parseExpression(e)).collect(
                    Collectors.toList());
            }
            SubPartitionList subPartitionList = null;
            if (c.getSubPartitionElementClients() != null) {
                List<BaseSubPartitionElement> subPartitionElementList = null;
                subPartitionElementList = c.getSubPartitionElementClients().stream().map(this::toBaseSubPartitionElement).collect(
                    Collectors.toList());
                subPartitionList = new SubPartitionList(subPartitionElementList);
            }
            return new ListPartitionElement(qualifiedName, c.getDefaultExpr(), expressionList, null, null, subPartitionList);
        } else if (partitionElementClient instanceof RangePartitionElementClient) {
            RangePartitionElementClient c = (RangePartitionElementClient)partitionElementClient;
            QualifiedName qualifiedName = null;
            if (c.getQualifiedName() != null) {
                qualifiedName = QualifiedName.of(c.getQualifiedName());
            }
            SubPartitionList subPartitionList = null;
            if (c.getSubPartitionElementClients() != null) {
                List<BaseSubPartitionElement> subPartitionElementList = null;
                subPartitionElementList = c.getSubPartitionElementClients().stream().map(this::toBaseSubPartitionElement).collect(
                    Collectors.toList());
                subPartitionList = new SubPartitionList(subPartitionElementList);
            }
            PartitionDesc partitionDesc = new AdbPostgreSQLRangeElement(
                toValueExpression(c.getStart()),
                toValueExpression(c.getEnd()),
                toIntervalExpression(c.getEvery())
            );
            return new RangePartitionElement(qualifiedName, null, partitionDesc, subPartitionList);
        }
        return null;
    }

    @Override
    protected List<Constraint> toOutlineConstraint(CreateTable createTable) {
        List<Constraint> outlineConstraint = super.toOutlineConstraint(createTable);
        if (createTable.isConstraintEmpty()) {
            return outlineConstraint;
        }
        List<BaseConstraint> constraintStatements = createTable.getConstraintStatements();
        if (outlineConstraint == null || outlineConstraint.isEmpty()) {
            outlineConstraint = Lists.newArrayList();
        }
        if (!constraintStatements.isEmpty()) {
            for (BaseConstraint baseConstraint : constraintStatements) {
                convertConstraint(baseConstraint, outlineConstraint);
            }
        }
        return outlineConstraint;
    }

    private void convertConstraint(BaseConstraint baseConstraint, List<Constraint> outlineConstraint) {
        if (!(baseConstraint instanceof DistributeNonKeyConstraint)) {
            return;
        }
        DistributeNonKeyConstraint distributeConstraint = (DistributeNonKeyConstraint)baseConstraint;
        DistributeClientConstraint starRocksDistributeConstraint = toDistribute(distributeConstraint);
        outlineConstraint.add(starRocksDistributeConstraint);
    }

    private BaseSubPartitionElement toBaseSubPartitionElement(SubPartitionElementClient s) {
        if (s instanceof SubListPartitionElementClient) {
            SubListPartitionElementClient subListPartitionElementClient = (SubListPartitionElementClient)s;
            QualifiedName qualifiedName = QualifiedName.of(subListPartitionElementClient.getQualifiedName());
            List<BaseExpression> expressionList = null;
            if (subListPartitionElementClient.getExpressionList() != null) {
                expressionList = subListPartitionElementClient.getExpressionList().stream().map(
                    e -> (BaseExpression)getLanguageParser().parseExpression(e)).collect(Collectors.toList());
            }
            return new SubListPartitionElement(qualifiedName, subListPartitionElementClient.getDefaultListExpr(), expressionList, null);
        } else if (s instanceof SubRangePartitionElementClient) {
            SubRangePartitionElementClient subRangePartitionElementClient = (SubRangePartitionElementClient)s;
            PartitionValueExpression start = toValueExpression(subRangePartitionElementClient.getStart());
            PartitionValueExpression end = toValueExpression(subRangePartitionElementClient.getEnd());
            PartitionIntervalExpression every = toIntervalExpression(subRangePartitionElementClient.getEvery());
            PartitionDesc singleRangePartition = new AdbPostgreSQLRangeElement(start, end, every);
            String nameValue = subRangePartitionElementClient.getName();
            QualifiedName name = null;
            if (nameValue != null) {
                name = QualifiedName.of(nameValue);
            }
            return new SubRangePartitionElement(singleRangePartition, name);
        }
        return null;
    }

    /**
     * @param every
     * @return
     */
    private PartitionIntervalExpression toIntervalExpression(RangeIntervalExpression every) {
        if (every == null) {
            return null;
        }
        BaseDataType baseDataType = null;
        if (every.getDataType() != null) {
            baseDataType = getLanguageParser().parseDataType(every.getDataType(), ReverseContext.builder().build());
        }
        BaseExpression baseExpression = null;
        if (every.getExpression() != null) {
            baseExpression = (BaseExpression)getLanguageParser().parseExpression(every.getExpression());
        }
        StringLiteral stringLiteral = null;
        if (every.getInterval() != null) {
            stringLiteral = new StringLiteral(every.getInterval());
        }
        return new PartitionIntervalExpression(baseDataType, baseExpression, stringLiteral);
    }

    private PartitionValueExpression toValueExpression(RangeExpression start) {
        if (start == null) {
            return null;
        }
        BaseDataType baseDataType = null;
        if (start.getDataType() != null) {
            baseDataType = getLanguageParser().parseDataType(start.getDataType(), ReverseContext.builder().build());
        }
        BaseExpression baseExpression = null;
        if (start.getExpression() != null) {
            baseExpression = (BaseExpression)getLanguageParser().parseExpression(start.getExpression());
        }
        return new PartitionValueExpression(baseDataType, baseExpression);
    }

    private BaseSubPartition toBaseSubPartition(SubPartitionClient s) {
        if (s instanceof SubListPartitionClient) {
            SubListPartitionClient subListPartitionClient = (SubListPartitionClient)s;
            BaseExpression expression = null;
            if (subListPartitionClient.getExpression() != null) {
                expression = (BaseExpression)getLanguageParser().parseExpression(subListPartitionClient.getExpression());
            }
            List<Identifier> columnList = null;
            if (subListPartitionClient.getColumnList() != null) {
                columnList = subListPartitionClient.getColumnList().stream().map(Identifier::new).collect(Collectors.toList());
            }
            return new SubListPartition(expression, columnList);
        }
        if (s instanceof SubRangePartitionClient) {
            SubRangePartitionClient subRangePartitionClient = (SubRangePartitionClient)s;
            BaseExpression expression = null;
            if (subRangePartitionClient.getExpression() != null) {
                expression = (BaseExpression)getLanguageParser().parseExpression(subRangePartitionClient.getExpression());
            }
            List<Identifier> columnList = null;
            if (subRangePartitionClient.getColumnList() != null) {
                columnList = subRangePartitionClient.getColumnList().stream().map(Identifier::new).collect(Collectors.toList());
            }
            List<BasePartitionElement> singleRangePartitionList = null;
            if (subRangePartitionClient.getSingleRangePartitionList() != null) {
                singleRangePartitionList = subRangePartitionClient.getSingleRangePartitionList().stream().map(this::toRangePartitionElement).collect(
                    Collectors.toList());
            }
            return new SubRangePartition(expression, columnList, singleRangePartitionList);
        }

        if (s instanceof SubListTemplatePartitionClient) {
            SubListTemplatePartitionClient subListTemplatePartitionClient = (SubListTemplatePartitionClient)s;
            BaseExpression expression = null;
            if (subListTemplatePartitionClient.getExpression() != null) {
                expression = (BaseExpression)getLanguageParser().parseExpression(subListTemplatePartitionClient.getExpression());
            }
            List<Identifier> columnList = null;
            if (subListTemplatePartitionClient.getColumnList() != null) {
                columnList = subListTemplatePartitionClient.getColumnList().stream().map(Identifier::new).collect(Collectors.toList());
            }
            SubPartitionList subPartitionList = null;
            if (subListTemplatePartitionClient.getSubPartitionElementClients() != null) {
                List<BaseSubPartitionElement> subPartitionElements = subListTemplatePartitionClient.getSubPartitionElementClients()
                    .stream()
                    .map(this::toBaseSubPartitionElement)
                    .collect(Collectors.toList());
                subPartitionList = new SubPartitionList(subPartitionElements);
            }
            return new SubListTemplatePartition(expression, columnList, subPartitionList);
        }

        if (s instanceof SubRangeTemplatePartitionClient) {
            SubRangeTemplatePartitionClient subRangeTemplatePartitionClient = (SubRangeTemplatePartitionClient)s;
            BaseExpression expression = null;
            if (subRangeTemplatePartitionClient.getExpression() != null) {
                expression = (BaseExpression)getLanguageParser().parseExpression(subRangeTemplatePartitionClient.getExpression());
            }
            List<Identifier> columnList = null;
            if (subRangeTemplatePartitionClient.getColumnList() != null) {
                columnList = subRangeTemplatePartitionClient.getColumnList().stream().map(Identifier::new).collect(Collectors.toList());
            }
            SubPartitionList subPartitionList = null;
            if (subRangeTemplatePartitionClient.getSubPartitionElementClients() != null) {
                List<BaseSubPartitionElement> subPartitionElements = subRangeTemplatePartitionClient.getSubPartitionElementClients()
                    .stream()
                    .map(this::toBaseSubPartitionElement)
                    .collect(Collectors.toList());
                subPartitionList = new SubPartitionList(subPartitionElements);
            }
            return new SubRangeTemplatePartition(expression, columnList, subPartitionList);
        }
        return null;
    }

    private RangePartitionElement toRangePartitionElement(PartitionElementClient rangePartitionElementClient) {
        RangePartitionElementClient m = (RangePartitionElementClient)rangePartitionElementClient;
        QualifiedName qualifiedName = null;
        if (m.getQualifiedName() != null) {
            qualifiedName = QualifiedName.of(m.getQualifiedName());
        }
        PartitionValueExpression start = toValueExpression(m.getStart());
        PartitionValueExpression end = toValueExpression(m.getEnd());
        PartitionIntervalExpression every = toIntervalExpression(m.getEvery());
        PartitionDesc singleRangePartition = new AdbPostgreSQLRangeElement(start, end, every);
        SubPartitionList subPartitionList = null;
        if (m.getSubPartitionElementClients() != null) {
            List<BaseSubPartitionElement> subPartitionElements = m.getSubPartitionElementClients().stream().map(this::toBaseSubPartitionElement)
                .collect(Collectors.toList());
            subPartitionList = new SubPartitionList(subPartitionElements);
        }
        return new RangePartitionElement(qualifiedName, null, singleRangePartition, subPartitionList);
    }

    private ListPartitionProperty toListPartitionProperty(AdbPostgreSQLPartitionBy partitionBy) {
        ListPartitionProperty property = new ListPartitionProperty();
        ListPartitionClient value = new ListPartitionClient();
        List<BaseSubPartition> subPartitions = partitionBy.getSubPartitions();
        List<BasePartitionElement> partitionElements = partitionBy.getPartitionElements();
        List<SubPartitionClient> subPartitionClientList = Lists.newArrayList();
        for (BaseSubPartition subPartition : subPartitions) {
            SubPartitionClient subPartitionClient = toSubPartitionClient(subPartition);
            subPartitionClientList.add(subPartitionClient);
        }
        List<ListPartitionElementClient> partitionElementClientList = Lists.newArrayList();
        for (BasePartitionElement partitionElement : partitionElements) {
            ListPartitionElement listPartitionElement = (ListPartitionElement)partitionElement;
            ListPartitionElementClient listPartitionElementClient = toListPartitionElementClient(listPartitionElement);
            partitionElementClientList.add(listPartitionElementClient);
        }
        //id list
        List<Identifier> identifierList = partitionBy.getColumnDefinitions().stream().map(ColumnDefinition::getColName).collect(Collectors.toList());
        value.setColumns(ParserHelper.getColumnList(identifierList));
        value.setPartitionElementClients(partitionElementClientList);
        value.setSubPartitionClients(subPartitionClientList);
        property.setValue(value);
        return property;
    }

    private ListPartitionElementClient toListPartitionElementClient(ListPartitionElement listPartitionElement) {
        List<SubPartitionElementClient> subPartitionElements = null;
        if (listPartitionElement.getSubPartitionList() != null) {
            subPartitionElements = listPartitionElement.getSubPartitionList().getSubPartitionElementList().stream().map(
                this::toSubPartitionElementClient).collect(Collectors.toList());
        }
        return ListPartitionElementClient.builder().defaultExpr(BooleanUtils.isTrue(listPartitionElement.getDefaultExpr())).qualifiedName(
            listPartitionElement.getQualifiedName().toString()).expressionList(
            listPartitionElement.getExpressionList().stream().map(this::getRaw).collect(Collectors.toList())).subPartitionElementClients(
            subPartitionElements).build();
    }

    private SubPartitionElementClient toSubPartitionElementClient(BaseSubPartitionElement c) {
        if (c instanceof SubListPartitionElement) {
            SubListPartitionElement subListPartitionElement = (SubListPartitionElement)c;
            List<BaseExpression> expressionList = subListPartitionElement.getExpressionList();
            List<String> collect = null;
            if (expressionList != null) {
                collect = expressionList.stream().map(this::getRaw).collect(Collectors.toList());
            }
            QualifiedName qualifiedName = subListPartitionElement.getQualifiedName();
            return SubListPartitionElementClient.builder()
                .defaultListExpr(BooleanUtils.isTrue(subListPartitionElement.getDefaultListExpr()))
                .expressionList(collect)
                .qualifiedName(qualifiedName != null ? qualifiedName.toString() : null)
                .build();
        }
        if (c instanceof SubRangePartitionElement) {
            SubRangePartitionElement subRangePartitionElement = (SubRangePartitionElement)c;
            PartitionDesc singleRangePartition = subRangePartitionElement.getSingleRangePartition();
            if (!(singleRangePartition instanceof AdbPostgreSQLRangeElement)) {
                return null;
            }
            AdbPostgreSQLRangeElement adbPostgreSQLRangeElement = (AdbPostgreSQLRangeElement)singleRangePartition;
            RangeExpression start = getRangeExpression(adbPostgreSQLRangeElement.getStart());
            RangeExpression end = getRangeExpression(adbPostgreSQLRangeElement.getEnd());
            RangeIntervalExpression every = getRangeIntervalExpression(adbPostgreSQLRangeElement.getEvery());
            QualifiedName name = subRangePartitionElement.getName();
            return SubRangePartitionElementClient.builder()
                .name(name != null ? name.toString() : null)
                .start(start)
                .end(end)
                .every(every)
                .build();
        }
        return null;
    }

    private RangeIntervalExpression getRangeIntervalExpression(PartitionIntervalExpression every) {
        if (every == null) {
            return null;
        }
        BaseDataType baseDataType = every.getBaseDataType();
        String dataType = null;
        if (baseDataType != null) {
            dataType = formatExpression(baseDataType);
        }
        String expression = getRaw(every.getBaseExpression());
        StringLiteral stringLiteral = every.getStringLiteral();
        return RangeIntervalExpression.builder().dataType(dataType).expression(expression).interval(
            stringLiteral != null ? stringLiteral.getValue() : null).build();
    }

    private RangeExpression getRangeExpression(PartitionValueExpression baseExpression) {
        if (baseExpression == null) {
            return null;
        }
        BaseDataType baseDataType = baseExpression.getBaseDataType();
        String dataType = null;
        if (baseDataType != null) {
            dataType = formatExpression(baseDataType);
        }
        String expression = getRaw(baseExpression.getBaseExpression());
        return RangeExpression.builder().dataType(dataType).expression(expression).build();
    }

    private SubPartitionClient toSubPartitionClient(BaseSubPartition subPartition) {
        if (subPartition instanceof SubListPartition) {
            SubListPartition subListPartition = (SubListPartition)subPartition;
            return SubListPartitionClient.builder()
                .expression(getRaw(subListPartition.getExpression()))
                .columnList(ParserHelper.getColumnList(subListPartition.getColumnList()))
                .build();
        } else if (subPartition instanceof SubRangePartition) {
            SubRangePartition subRangePartition = (SubRangePartition)subPartition;
            SubRangePartitionClient subPartitionClient = SubRangePartitionClient.builder()
                .expression(getRaw(subRangePartition.getExpression()))
                .columnList(ParserHelper.getColumnList(subRangePartition.getColumnList()))
                .build();
            List<BasePartitionElement> singleRangePartitionList = subRangePartition.getSingleRangePartitionList();
            if (singleRangePartitionList != null) {
                List<PartitionElementClient> partitionList = toRangePartitionElementClient(singleRangePartitionList);
                subPartitionClient.setSingleRangePartitionList(partitionList);
            }
            return subPartitionClient;
        } else if (subPartition instanceof SubListTemplatePartition) {
            SubListTemplatePartition subListTemplatePartition = (SubListTemplatePartition)subPartition;
            List<String> columns = null;
            if (subListTemplatePartition.getColumnList() != null) {
                columns = subListTemplatePartition.getColumnList().stream().map(this::getRaw).collect(Collectors.toList());
            }
            List<SubPartitionElementClient> clients = null;
            if (subListTemplatePartition.getSubPartitionList() != null) {
                clients = subListTemplatePartition.getSubPartitionList()
                    .getSubPartitionElementList()
                    .stream()
                    .map(this::toSubPartitionElementClient)
                    .collect(Collectors.toList());
            }
            return SubListTemplatePartitionClient.builder()
                .subPartitionElementClients(clients)
                .columnList(columns)
                .expression(getRaw(subListTemplatePartition.getExpression()))
                .build();
        } else if (subPartition instanceof SubRangeTemplatePartition) {
            SubRangeTemplatePartition subRangeTemplatePartition = (SubRangeTemplatePartition)subPartition;
            String expression = null;
            if (subRangeTemplatePartition.getExpression() != null) {
                expression = formatExpression(subRangeTemplatePartition.getExpression());
            }
            List<String> columns = null;
            if (subRangeTemplatePartition.getColumnList() != null) {
                columns = subRangeTemplatePartition.getColumnList().stream().map(this::getRaw).collect(Collectors.toList());
            }
            List<SubPartitionElementClient> list = null;
            if (subRangeTemplatePartition.getSubPartitionList() != null) {
                list = subRangeTemplatePartition.getSubPartitionList()
                    .getSubPartitionElementList()
                    .stream()
                    .map(this::toSubPartitionElementClient)
                    .collect(Collectors.toList());
            }
            return SubRangeTemplatePartitionClient.builder()
                .expression(expression)
                .columnList(columns)
                .subPartitionElementClients(list)
                .build();
        }
        return null;
    }

    private List<PartitionElementClient> toRangePartitionElementClient(List<BasePartitionElement> singleRangePartitionList) {
        return singleRangePartitionList.stream().map(s -> {
            if (s instanceof RangePartitionElement) {
                return getRangePartitionElementClient((RangePartitionElement)s);
            } else if (s instanceof ListPartitionElement) {
                return getListPartitionElementClient((ListPartitionElement)s);
            }
            return null;
        }).collect(Collectors.toList());
    }

    private ListPartitionElementClient getListPartitionElementClient(ListPartitionElement s) {
        List<BaseExpression> expressionList1 = s.getExpressionList();
        List<String> expressionList = null;
        if (expressionList1 != null) {
            expressionList = expressionList1.stream().map(this::getRaw).collect(Collectors.toList());
        }
        List<SubPartitionElementClient> subPartitionElementList = null;
        if (s.getSubPartitionList() != null) {
            subPartitionElementList = s.getSubPartitionList()
                .getSubPartitionElementList()
                .stream()
                .map(this::toSubPartitionElementClient)
                .collect(Collectors.toList());
        }
        String name = null;
        if (s.getQualifiedName() != null) {
            name = s.getQualifiedName().toString();
        }
        return ListPartitionElementClient.builder()
            .qualifiedName(name)
            .expressionList(expressionList)
            .defaultExpr(s.getDefaultExpr())
            .subPartitionElementClients(subPartitionElementList)
            .build();
    }

    private RangePartitionElementClient getRangePartitionElementClient(RangePartitionElement s) {

        RangePartitionElementClient rangePartitionElementClient = new RangePartitionElementClient();
        List<SubPartitionElementClient> rangeSubPartitionElementsClient = null;
        if (s.getSubPartitionList() != null) {
            rangeSubPartitionElementsClient = s.getSubPartitionList().getSubPartitionElementList().stream().map(this::toSubPartitionElementClient)
                .collect(Collectors.toList());
        }
        rangePartitionElementClient.setSubPartitionElementClients(rangeSubPartitionElementsClient);
        if (s.getQualifiedName() != null) {
            rangePartitionElementClient.setQualifiedName(s.getQualifiedName().toString());
        }
        PartitionDesc singleRangePartition = s.getSingleRangePartition();
        AdbPostgreSQLRangeElement adbPostgreSQLRangeElement = (AdbPostgreSQLRangeElement)singleRangePartition;
        RangeExpression start = getRangeExpression(adbPostgreSQLRangeElement.getStart());
        RangeExpression end = getRangeExpression(adbPostgreSQLRangeElement.getEnd());
        RangeIntervalExpression every = getRangeIntervalExpression(adbPostgreSQLRangeElement.getEvery());

        //set start end every
        rangePartitionElementClient.setStart(start);
        rangePartitionElementClient.setEnd(end);
        rangePartitionElementClient.setEvery(every);
        return rangePartitionElementClient;
    }

    private RangePartitionProperty toRangePartitionProperty(AdbPostgreSQLPartitionBy partitionBy) {
        RangePartitionProperty rangePartitionProperty = new RangePartitionProperty();
        List<String> columns = null;
        if (partitionBy.getColumnDefinitions() != null) {
            columns = partitionBy.getColumnDefinitions().stream().map(c -> c.getColName().getValue()).collect(Collectors.toList());
        }
        List<SubPartitionClient> subPartitionClient = null;
        if (partitionBy.getSubPartitions() != null) {
            subPartitionClient = partitionBy.getSubPartitions().stream().map(this::toSubPartitionClient).collect(Collectors.toList());
        }
        List<PartitionElementClient> rangePartitionElementClients = null;
        if (partitionBy.getPartitionElements() != null) {
            rangePartitionElementClients = toRangePartitionElementClient(partitionBy.getPartitionElements());
        }
        RangePartitionClient client = RangePartitionClient.builder().columns(columns).subPartitionClients(subPartitionClient)
            .partitionElementClients(rangePartitionElementClients).build();
        rangePartitionProperty.setValue(client);
        return rangePartitionProperty;
    }

    @Override
    public LanguageParser getLanguageParser() {
        return new AdbPostgreSQLLanguageParser();
    }

    @Override
    public PropertyConverter getPropertyConverter() {
        return new AdbPostgreSQLPropertyConverter();
    }

}
