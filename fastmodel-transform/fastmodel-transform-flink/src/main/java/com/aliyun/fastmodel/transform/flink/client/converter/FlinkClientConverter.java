package com.aliyun.fastmodel.transform.flink.client.converter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnPropertyDefaultKey;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.core.tree.util.PropertyUtil;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.extension.client.constraint.ExtensionClientConstraintType;
import com.aliyun.fastmodel.transform.api.extension.client.converter.ExtensionClientConverter;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.WaterMarkConstraint;
import com.aliyun.fastmodel.transform.flink.client.property.FlinkPropertyKey;
import com.aliyun.fastmodel.transform.flink.context.FlinkTransformContext;
import com.aliyun.fastmodel.transform.flink.format.FlinkColumnPropertyKey;
import com.aliyun.fastmodel.transform.flink.format.FlinkOutVisitor;
import com.aliyun.fastmodel.transform.flink.parser.FlinkLanguageParser;
import com.aliyun.fastmodel.transform.flink.parser.tree.datatype.FlinkDataTypeName;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author 子梁
 * @date 2024/5/15
 */
public class FlinkClientConverter extends ExtensionClientConverter<FlinkTransformContext> {

    private final FlinkLanguageParser flinkLanguageParser;

    private final FlinkPropertyConverter flinkPropertyConverter;

    public FlinkClientConverter() {
        flinkLanguageParser = new FlinkLanguageParser();
        flinkPropertyConverter = new FlinkPropertyConverter();
    }

    @Override
    public Column getColumn(ColumnDefinition c, boolean partitionKey, Integer partitionKeyIndex) {
        Map<String, String> propertyMap = PropertyUtil.toMap(c.getColumnProperties());
        if (propertyMap.containsKey(FlinkColumnPropertyKey.COMPUTED.getValue())
            && BooleanUtils.toBoolean(propertyMap.get(FlinkColumnPropertyKey.COMPUTED.getValue()))) {
            return getComputedColumn(c, partitionKey, partitionKeyIndex);
        } else if (propertyMap.containsKey(FlinkColumnPropertyKey.METADATA.getValue())
            && BooleanUtils.toBoolean(propertyMap.get(FlinkColumnPropertyKey.METADATA.getValue()))) {
            return getMetaDataColumn(c, partitionKey, partitionKeyIndex);
        } else {
            return super.getColumn(c, partitionKey, partitionKeyIndex);
        }
    }

    @Override
    public ColumnDefinition toColumnDefinition(Table table, Column c) {
        List<BaseClientProperty> columnProperties = c.getProperties();
        if (CollectionUtils.isEmpty(columnProperties)) {
            return super.toColumnDefinition(table, c);
        }

        Optional<BaseClientProperty> computedColumnProperty = columnProperties.stream()
            .filter(columnProperty -> StringUtils.equalsIgnoreCase(FlinkColumnPropertyKey.COMPUTED.getValue(), columnProperty.getKey()))
            .findAny();
        if (computedColumnProperty.isPresent()) {
            return toComputedColumnDefinition(table, c);
        } else {
            return super.toColumnDefinition(table, c);
        }
    }

    @Override
    public List<Constraint> toOutlineConstraint(CreateTable createTable) {
        List<BaseConstraint> constraints = createTable.getConstraintStatements();
        if (CollectionUtils.isEmpty(constraints)) {
            return Collections.emptyList();
        }

        List<Constraint> resultList = new ArrayList<>();
        Optional<BaseConstraint> waterMarkConstraintOpt = constraints.stream()
            .filter(constraint -> constraint instanceof WaterMarkConstraint).findAny();
        waterMarkConstraintOpt.ifPresent(constraint -> {
            Constraint constraintDto = new Constraint();
            constraintDto.setType(ExtensionClientConstraintType.WATERMARK);
            WaterMarkConstraint waterMarkConstraint = (WaterMarkConstraint)constraint;
            String column = waterMarkConstraint.getColumn().getValue();
            constraintDto.setColumns(Lists.newArrayList(column));

            StringProperty property = new StringProperty();
            property.setKey(FlinkPropertyKey.WATERMARK_EXPRESSION.getValue());
            property.setValue(waterMarkConstraint.getExpression().getOrigin());
            constraintDto.setProperties(Lists.newArrayList(property));

            resultList.add(constraintDto);
        });

        resultList.addAll(super.toOutlineConstraint(createTable));
        return resultList;
    }

    @Override
    public LanguageParser getLanguageParser() {
        return this.flinkLanguageParser;
    }

    @Override
    public PropertyConverter getPropertyConverter() {
        return this.flinkPropertyConverter;
    }

    @Override
    public IDataTypeName getDataTypeName(String dataTypeName) {
        return FlinkDataTypeName.getByValue(dataTypeName);
    }

    @Override
    public String getRaw(Node node) {
        FlinkOutVisitor flinkOutVisitor = new FlinkOutVisitor(FlinkTransformContext.builder().build());
        node.accept(flinkOutVisitor, 0);
        return flinkOutVisitor.getBuilder().toString();
    }

    @Override
    protected void setOutlineConstraint(Constraint c, List<BaseConstraint> constraintList) {
        if (ExtensionClientConstraintType.WATERMARK == c.getType()) {
            Identifier constraintName = StringUtils.isBlank(c.getName()) ? IdentifierUtil.sysIdentifier() : new Identifier(c.getName());
            Identifier column = new Identifier(c.getColumns().get(0));
            BaseClientProperty property = c.getProperties().get(0);
            StringLiteral expression = new StringLiteral(String.valueOf(property.getValue()));
            WaterMarkConstraint waterMarkConstraint = new WaterMarkConstraint(constraintName, column, expression);
            constraintList.add(waterMarkConstraint);
        } else {
            super.setOutlineConstraint(c, constraintList);
        }
    }

    @Override
    public PartitionedBy toPartition(List<ColumnDefinition> columnDefinitionList, List<BaseClientProperty> properties) {
        return new PartitionedBy(columnDefinitionList);
    }

    @Override
    public String toSchema(CreateTable createTable, FlinkTransformContext transformContext) {
        return null;
    }

    /**
     * get catalog
     *
     * @param createTable
     * @param transformContext
     * @return
     */
    @Override
    public String toCatalog(CreateTable createTable, FlinkTransformContext transformContext) {
        QualifiedName qualifiedName = createTable.getQualifiedName();
        boolean isThirdSchema = qualifiedName.isJoinPath() && qualifiedName.getOriginalParts().size() == THIRD_INDEX;
        if (isThirdSchema) {
            return qualifiedName.getFirst();
        }
        return transformContext.getCatalog();
    }

    /**
     * get database
     *
     * @param createTable
     * @param database
     * @return
     */
    @Override
    public String toDatabase(CreateTable createTable, String database) {
        QualifiedName qualifiedName = createTable.getQualifiedName();
        if (!qualifiedName.isJoinPath()) {
            return database;
        }
        boolean isSecondSchema = qualifiedName.getOriginalParts().size() == SECOND_INDEX;
        if (isSecondSchema) {
            return qualifiedName.getFirst();
        }
        boolean isThirdSchema = qualifiedName.getOriginalParts().size() == THIRD_INDEX;
        if (isThirdSchema) {
            return qualifiedName.getParts().get(1);
        }
        return database;
    }

    private Column getComputedColumn(ColumnDefinition c, boolean partitionKey, Integer partitionKeyIndex) {
        Column column = Column.builder()
            .id(c.getColName().getValue())
            .name(c.getColName().getValue())
            .comment(c.getCommentValue())
            .nullable(BooleanUtils.isNotTrue(c.getNotNull()))
            .primaryKey(BooleanUtils.isTrue(c.getPrimary()))
            .partitionKey(partitionKey)
            .defaultValue(toDefaultBaseValue(c.getDefaultValue()))
            .partitionKeyIndex(partitionKeyIndex)
            .build();

        List<Property> columnProperties = c.getColumnProperties();
        if (CollectionUtils.isEmpty(columnProperties)) {
            return column;
        }
        List<BaseClientProperty> baseClientProperties = Lists.newArrayList();
        for (Property property : columnProperties) {
            BaseClientProperty baseClientProperty = getPropertyConverter().create(property.getName(), getOrigin(property.getValueLiteral()));
            baseClientProperties.add(baseClientProperty);
        }
        column.setProperties(baseClientProperties);
        return column;
    }

    private Column getMetaDataColumn(ColumnDefinition c, boolean partitionKey, Integer partitionKeyIndex) {
        Column column = super.getColumn(c, partitionKey, partitionKeyIndex);

        List<Property> columnProperties = c.getColumnProperties();
        if (CollectionUtils.isEmpty(columnProperties)) {
            return column;
        }
        List<BaseClientProperty> baseClientProperties = Lists.newArrayList();
        for (Property property : columnProperties) {
            BaseClientProperty baseClientProperty = getPropertyConverter().create(property.getName(), getOrigin(property.getValueLiteral()));
            baseClientProperties.add(baseClientProperty);
        }
        column.setProperties(baseClientProperties);
        return column;
    }

    private ColumnDefinition toComputedColumnDefinition(Table table, Column c) {
        String id = c.getId();
        List<Property> all = Lists.newArrayList();
        Property property = null;
        if (StringUtils.isNotBlank(id)) {
            property = new Property(ColumnPropertyDefaultKey.uuid.name(), id);
            all.add(property);
        }
        List<BaseClientProperty> properties = c.getProperties();
        if (properties != null) {
            List<Property> columnProperty = toProperty(table, properties);
            all.addAll(columnProperty);
        }
        return ColumnDefinition.builder()
            .colName(new Identifier(c.getName()))
            .comment(new Comment(c.getComment()))
            .notNull(BooleanUtils.isFalse(c.isNullable()))
            .properties(all)
            .build();
    }

    private String getOrigin(BaseLiteral baseLiteral) {
        return StripUtils.strip(baseLiteral.getOrigin());
    }
}
