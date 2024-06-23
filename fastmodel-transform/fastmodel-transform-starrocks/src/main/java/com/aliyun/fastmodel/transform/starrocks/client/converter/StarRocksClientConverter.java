package com.aliyun.fastmodel.transform.starrocks.client.converter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.extension.client.constraint.ClientConstraintType;
import com.aliyun.fastmodel.transform.api.extension.client.constraint.DistributeClientConstraint;
import com.aliyun.fastmodel.transform.api.extension.client.converter.ExtensionClientConverter;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.AggregateKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.DuplicateKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.OrderByConstraint;
import com.aliyun.fastmodel.transform.starrocks.context.StarRocksContext;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksOutVisitor;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksLanguageParser;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.datatype.StarRocksDataTypeName;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;

/**
 * StarRocksClientConverter
 *
 * @author panguanjing
 * @date 2023/9/16
 */
public class StarRocksClientConverter extends ExtensionClientConverter<StarRocksContext> {

    private final StarRocksLanguageParser starRocksLanguageParser = new StarRocksLanguageParser();

    private final StarRocksPropertyConverter starRocksPropertyConverter = new StarRocksPropertyConverter();

    @Override
    public PropertyConverter getPropertyConverter() {
        return starRocksPropertyConverter;
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
        if (baseConstraint instanceof DistributeConstraint) {
            DistributeConstraint distributeConstraint = (DistributeConstraint)baseConstraint;
            DistributeClientConstraint starRocksDistributeConstraint = toStarRocksDistributeConstraint(distributeConstraint);
            outlineConstraint.add(starRocksDistributeConstraint);
            return;
        }
        if (baseConstraint instanceof OrderByConstraint) {
            OrderByConstraint orderByConstraint = (OrderByConstraint)baseConstraint;
            Constraint constraint = toOrderClientConstraint(orderByConstraint);
            outlineConstraint.add(constraint);
            return;
        }

        if (baseConstraint instanceof AggregateKeyConstraint) {
            AggregateKeyConstraint aggregateKeyConstraint = (AggregateKeyConstraint)baseConstraint;
            Constraint aggreKeyConstraint = toAggregateClientConstraint(aggregateKeyConstraint);
            outlineConstraint.add(aggreKeyConstraint);
        }

        if (baseConstraint instanceof DuplicateKeyConstraint) {
            DuplicateKeyConstraint duplicateKeyConstraint = (DuplicateKeyConstraint)baseConstraint;
            Constraint duplicateClientConstraint = toDuplicateClientConstraint(duplicateKeyConstraint);
            outlineConstraint.add(duplicateClientConstraint);
        }
    }

    private Constraint toDuplicateClientConstraint(DuplicateKeyConstraint aggregateKeyConstraint) {
        List<String> collect = aggregateKeyConstraint.getColumns().stream().map(Identifier::getValue).collect(Collectors.toList());
        return Constraint.builder().name(aggregateKeyConstraint.getName().getValue())
            .columns(collect)
            .type(ClientConstraintType.DUPLICATE_KEY)
            .build();
    }

    private Constraint toAggregateClientConstraint(AggregateKeyConstraint aggregateKeyConstraint) {
        List<String> collect = aggregateKeyConstraint.getColumns().stream().map(Identifier::getValue).collect(Collectors.toList());
        return Constraint.builder().name(aggregateKeyConstraint.getName().getValue())
            .columns(collect).type(ClientConstraintType.AGGREGATE_KEY)
            .build();
    }

    private Constraint toOrderClientConstraint(OrderByConstraint orderByConstraint) {
        List<String> collect = orderByConstraint.getColumns().stream().map(Identifier::getValue).collect(Collectors.toList());
        return Constraint.builder().name(orderByConstraint.getName().getValue())
            .columns(collect)
            .type(ClientConstraintType.ORDER_BY)
            .build();
    }

    private DistributeClientConstraint toStarRocksDistributeConstraint(DistributeConstraint distributeConstraint) {
        DistributeClientConstraint starRocksDistributeConstraint = new DistributeClientConstraint();
        starRocksDistributeConstraint.setBucket(distributeConstraint.getBucket());
        if (CollectionUtils.isNotEmpty(distributeConstraint.getColumns())) {
            starRocksDistributeConstraint.setColumns(
                distributeConstraint.getColumns().stream().map(Identifier::getValue).collect(Collectors.toList()));
        }
        starRocksDistributeConstraint.setRandom(distributeConstraint.getRandom());
        return starRocksDistributeConstraint;
    }

    @Override
    protected Column getColumn(ColumnDefinition c, boolean partitionKey, Integer partitionKeyIndex) {
        Column column = super.getColumn(c, partitionKey, partitionKeyIndex);
        List<Property> columnProperties = c.getColumnProperties();
        if (CollectionUtils.isEmpty(columnProperties)) {
            return column;
        }
        List<BaseClientProperty> baseClientProperties = Lists.newArrayList();
        for (Property property : columnProperties) {
            BaseClientProperty baseClientProperty = getPropertyConverter().create(property.getName(), property.getValue());
            baseClientProperties.add(baseClientProperty);
        }
        column.setProperties(baseClientProperties);
        return column;
    }

    @Override
    public IDataTypeName getDataTypeName(String dataTypeName) {
        return StarRocksDataTypeName.getByValue(dataTypeName);
    }

    @Override
    public LanguageParser getLanguageParser() {
        return this.starRocksLanguageParser;
    }

    @Override
    public String getRaw(Node node) {
        StarRocksOutVisitor starRocksOutVisitor = new StarRocksOutVisitor(StarRocksContext.builder().build());
        node.accept(starRocksOutVisitor, 0);
        return starRocksOutVisitor.getBuilder().toString();
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
    protected String toSchema(CreateTable createTable, StarRocksContext transformContext) {
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

    /**
     * get database
     *
     * @param createTable
     * @param database
     * @return
     */
    protected String toDatabase(CreateTable createTable, String database) {
        QualifiedName qualifiedName = createTable.getQualifiedName();
        boolean isThirdSchema = qualifiedName.isJoinPath() && qualifiedName.getOriginalParts().size() == THIRD_INDEX;
        if (isThirdSchema) {
            return qualifiedName.getFirst();
        }
        boolean isSecond = qualifiedName.isJoinPath() && qualifiedName.getOriginalParts().size() == SECOND_INDEX;
        if (isSecond) {
            return qualifiedName.getFirst();
        }
        return database;
    }

    protected BaseConstraint setPrimaryConstraintColumns(List<Column> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return null;
        }
        List<Column> primaryKeyColumns = columns.stream().filter(
            c -> BooleanUtils.isTrue(c.isPrimaryKey())
        ).collect(Collectors.toList());
        BaseConstraint baseConstraint = null;
        if (primaryKeyColumns.size() > 0) {
            List<Identifier> list = new ArrayList<>();
            for (Column column : primaryKeyColumns) {
                list.add(new Identifier(column.getName()));
                column.setPrimaryKey(false);
                column.setNullable(false);
            }
            baseConstraint = new PrimaryConstraint(IdentifierUtil.sysIdentifier(), list);
        }
        return baseConstraint;
    }

}
