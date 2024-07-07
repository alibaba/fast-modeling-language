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

package com.aliyun.fastmodel.transform.adbmysql.client.converter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.transform.adbmysql.context.AdbMysqlTransformContext;
import com.aliyun.fastmodel.transform.adbmysql.format.AdbMysqlOutVisitor;
import com.aliyun.fastmodel.transform.adbmysql.format.AdbMysqlPropertyKey;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlLanguageParser;
import com.aliyun.fastmodel.transform.adbmysql.parser.tree.AdbMysqlDataTypeName;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.extension.client.constraint.DistributeClientConstraint;
import com.aliyun.fastmodel.transform.api.extension.client.converter.ExtensionClientConverter;
import com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.TimeExpressionPartitionProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.TimeExpressionClientPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.ExpressionPartitionBy;
import com.aliyun.fastmodel.transform.api.util.PropertyKeyUtil;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * adb mysql client converter
 *
 * @author panguanjing
 * @date 2024/3/24
 */
public class AdbMysqlClientConverter extends ExtensionClientConverter<AdbMysqlTransformContext> {

    private final AdbMysqlLanguageParser adbMysqlLanguageParser = new AdbMysqlLanguageParser();

    private final AdbMysqlPropertyConverter adbMysqlPropertyConverter = new AdbMysqlPropertyConverter();

    @Override
    public LanguageParser getLanguageParser() {
        return adbMysqlLanguageParser;
    }

    @Override
    public PropertyConverter getPropertyConverter() {
        return adbMysqlPropertyConverter;
    }

    @Override
    public IDataTypeName getDataTypeName(String dataTypeName) {
        return AdbMysqlDataTypeName.getByValue(dataTypeName);
    }

    @Override
    public String getRaw(Node node) {
        AdbMysqlOutVisitor dorisOutVisitor = new AdbMysqlOutVisitor(AdbMysqlTransformContext.builder().build());
        node.accept(dorisOutVisitor, 0);
        return dorisOutVisitor.getBuilder().toString();
    }

    /**
     * adb mysql的分区函数是列再第一位
     *
     * @param baseClientProperty 属性信息
     * @return {@see FunctionCall}
     */
    protected FunctionCall getFunctionCall(TimeExpressionPartitionProperty baseClientProperty) {
        TimeExpressionClientPartition timeExpressionClientPartition = baseClientProperty.getValue();
        List<BaseExpression> arguments = new ArrayList<>();
        if (StringUtils.isNotBlank(timeExpressionClientPartition.getColumn())) {
            arguments.add(new TableOrColumn(QualifiedName.of(timeExpressionClientPartition.getColumn())));
        }
        if (StringUtils.isNotBlank(timeExpressionClientPartition.getTimeUnit())) {
            arguments.add(new StringLiteral(timeExpressionClientPartition.getTimeUnit()));
        }
        return new FunctionCall(QualifiedName.of(timeExpressionClientPartition.getFuncName()),
            false, arguments);
    }

    /**
     * adb mysql的分区函数，列是第一位
     *
     * @param expressionPartitionBy
     * @return
     */
    protected TimeExpressionPartitionProperty getTimeExpressionPartitionProperty(ExpressionPartitionBy expressionPartitionBy) {
        TimeExpressionClientPartition timeExpressionClientPartition = new TimeExpressionClientPartition();
        FunctionCall functionCall = expressionPartitionBy.getFunctionCall();
        timeExpressionClientPartition.setFuncName(functionCall.getFuncName().getFirst());
        StringLiteral timeUnitArg = expressionPartitionBy.getTimeUnitArg(1);
        if (timeUnitArg != null) {
            timeExpressionClientPartition.setTimeUnit(timeUnitArg.getValue());
        }
        TableOrColumn tableOrColumn = expressionPartitionBy.getColumn(0);
        timeExpressionClientPartition.setColumn(tableOrColumn.getQualifiedName().toString());
        TimeExpressionPartitionProperty timeExpressionPartitionProperty = new TimeExpressionPartitionProperty();
        timeExpressionPartitionProperty.setValue(timeExpressionClientPartition);
        return timeExpressionPartitionProperty;
    }

    protected List<Constraint> toOutlineConstraint(CreateTable createTable) {
        List<Constraint> outlineConstraint = super.toOutlineConstraint(createTable);
        if (createTable.isConstraintEmpty()) {
            return outlineConstraint;
        }
        if (outlineConstraint == null || outlineConstraint.isEmpty()) {
            outlineConstraint = Lists.newArrayList();
        }
        List<BaseConstraint> constraintStatements = createTable.getConstraintStatements();
        if (!constraintStatements.isEmpty()) {
            for (BaseConstraint baseConstraint : constraintStatements) {
                convertConstraint(baseConstraint, outlineConstraint);
            }
        }
        return outlineConstraint;
    }

    private void convertConstraint(BaseConstraint baseConstraint, List<Constraint> outlineConstraint) {
        if (baseConstraint instanceof DistributeConstraint) {
            DistributeConstraint distributeConstraint = (DistributeConstraint)baseConstraint;
            DistributeClientConstraint starRocksDistributeConstraint = toDistributeClientConstraint(distributeConstraint);
            outlineConstraint.add(starRocksDistributeConstraint);
            return;
        }
    }

    private DistributeClientConstraint toDistributeClientConstraint(DistributeConstraint distributeConstraint) {
        DistributeClientConstraint clientConstraint = new DistributeClientConstraint();
        clientConstraint.setBucket(distributeConstraint.getBucket());
        if (CollectionUtils.isNotEmpty(distributeConstraint.getColumns())) {
            clientConstraint.setColumns(
                distributeConstraint.getColumns().stream().map(Identifier::getValue).collect(Collectors.toList()));
        }
        clientConstraint.setRandom(distributeConstraint.getRandom());
        return clientConstraint;
    }

    @Override
    protected List<Property> toProperty(Table table, List<BaseClientProperty> properties) {
        List<Property> list = super.toProperty(table, properties);
        Long lifeCycleSeconds = table.getLifecycleSeconds();
        if (lifeCycleSeconds != null) {
            list.add(new Property(AdbMysqlPropertyKey.LIFE_CYCLE.getValue(), String.valueOf(PropertyKeyUtil.toLifeCycle(lifeCycleSeconds))));
        }
        return list;
    }

    /**
     * to lifecycle
     *
     * @param createTable
     * @return
     */
    protected Long toLifeCycleSeconds(CreateTable createTable) {
        List<Property> properties = createTable.getProperties();
        Optional<Property> first = properties.stream().filter(
            p -> StringUtils.equalsIgnoreCase(ExtensionPropertyKey.LIFE_CYCLE.getValue(), p.getName())).findFirst();
        if (!first.isPresent()) {
            return null;
        }
        String value = first.get().getValue();
        return PropertyKeyUtil.toSeconds(Long.parseLong(value));
    }
}
