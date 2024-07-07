package com.aliyun.fastmodel.transform.oceanbase.client.converter;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexColumnName;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexExpr;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexSortKey;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.extension.client.constraint.UniqueKeyExprClientConstraint;
import com.aliyun.fastmodel.transform.api.extension.client.converter.ExtensionClientConverter;
import com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.BaseClientPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.PartitionClientValue;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.SingleRangeClientPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.UniqueKeyExprConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.SingleRangePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.PartitionValue;
import com.aliyun.fastmodel.transform.oceanbase.client.converter.partition.OceanBasePartitionClientConverter;
import com.aliyun.fastmodel.transform.oceanbase.client.converter.partition.impl.OceanBasePartitionClientConverterImpl;
import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.hash.HashPartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.hash.HashPartitionElementClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.hash.HashPartitionProperty;
import com.aliyun.fastmodel.transform.oceanbase.client.property.hash.SubHashPartitionElementClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.key.KeyPartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.key.KeyPartitionProperty;
import com.aliyun.fastmodel.transform.oceanbase.client.property.list.ListPartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.list.ListPartitionElementClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.list.ListPartitionProperty;
import com.aliyun.fastmodel.transform.oceanbase.client.property.list.SubListPartitionElementClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.range.RangePartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.range.RangePartitionElementClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.range.RangePartitionProperty;
import com.aliyun.fastmodel.transform.oceanbase.client.property.range.SubRangePartitionElementClient;
import com.aliyun.fastmodel.transform.oceanbase.context.OceanBaseContext;
import com.aliyun.fastmodel.transform.oceanbase.format.OceanBaseMysqlOutVisitor;
import com.aliyun.fastmodel.transform.oceanbase.format.OceanBasePropertyKey;
import com.aliyun.fastmodel.transform.oceanbase.parser.OceanBaseMysqlLanguageParser;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.OceanBaseMysqlDataTypeName;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBaseHashPartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBaseKeyPartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBaseListPartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBaseRangePartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.BaseSubPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.HashPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.ListPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.RangePartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubPartitionList;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubRangePartitionElement;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * oceanbase mysql client converter
 *
 * @author panguanjing
 * @date 2024/2/5
 */
public class OceanBaseMysqlClientConverter extends ExtensionClientConverter<OceanBaseContext> {

    private final OceanBaseMysqlLanguageParser oceanBaseMysqlLanguageParser;

    private final OceanBaseMysqlPropertyConverter oceanBaseMysqlPropertyConverter;

    private final OceanBasePartitionClientConverter oceanBasePartitionClientConverter;

    public OceanBaseMysqlClientConverter() {
        oceanBaseMysqlLanguageParser = new OceanBaseMysqlLanguageParser();
        oceanBaseMysqlPropertyConverter = new OceanBaseMysqlPropertyConverter();
        oceanBasePartitionClientConverter = new OceanBasePartitionClientConverterImpl(this);
    }

    @Override
    public PropertyConverter getPropertyConverter() {
        return oceanBaseMysqlPropertyConverter;
    }

    @Override
    public IDataTypeName getDataTypeName(String dataTypeName) {
        return OceanBaseMysqlDataTypeName.getByValue(dataTypeName);
    }

    @Override
    public LanguageParser getLanguageParser() {
        return oceanBaseMysqlLanguageParser;
    }

    @Override
    public String getRaw(Node node) {
        if (node == null) {
            return null;
        }
        OceanBaseMysqlOutVisitor mysqlOutVisitor = new OceanBaseMysqlOutVisitor(OceanBaseContext.builder().build());
        node.accept(mysqlOutVisitor, 0);
        return mysqlOutVisitor.getBuilder().toString();
    }

    @Override
    public List<BaseClientProperty> toBaseClientProperty(CreateTable createTable) {
        List<BaseClientProperty> list = super.toBaseClientProperty(createTable);
        PartitionedBy partitionedBy = createTable.getPartitionedBy();
        if (partitionedBy instanceof OceanBaseHashPartitionBy) {
            OceanBaseHashPartitionBy oceanBaseHashPartitionBy = (OceanBaseHashPartitionBy)partitionedBy;
            HashPartitionProperty hashPartitionProperty = toHashPartitionProperty(oceanBaseHashPartitionBy);
            list.add(hashPartitionProperty);
        } else if (partitionedBy instanceof OceanBaseRangePartitionBy) {
            OceanBaseRangePartitionBy oceanBaseRangePartitionBy = (OceanBaseRangePartitionBy)partitionedBy;
            RangePartitionProperty rangePartitionProperty = toRangePartitionProperty(oceanBaseRangePartitionBy);
            list.add(rangePartitionProperty);
        } else if (partitionedBy instanceof OceanBaseKeyPartitionBy) {
            OceanBaseKeyPartitionBy oceanBaseKeyPartitionBy = (OceanBaseKeyPartitionBy)partitionedBy;
            KeyPartitionProperty keyPartitionProperty = toKeyPartitionProperty(oceanBaseKeyPartitionBy);
            list.add(keyPartitionProperty);
        } else if (partitionedBy instanceof OceanBaseListPartitionBy) {
            OceanBaseListPartitionBy oceanBaseListPartitionBy = (OceanBaseListPartitionBy)partitionedBy;
            ListPartitionProperty listPartitionProperty = toListPartitionProperty(oceanBaseListPartitionBy);
            list.add(listPartitionProperty);
        }
        return list;
    }

    private ListPartitionProperty toListPartitionProperty(OceanBaseListPartitionBy oceanBaseListPartitionBy) {
        ListPartitionProperty listPartitionProperty = new ListPartitionProperty();
        ListPartitionClient client = new ListPartitionClient();
        LongLiteral partitionCount = oceanBaseListPartitionBy.getPartitionCount();
        if (partitionCount != null) {
            client.setPartitionCount(partitionCount.getValue());
        }
        BaseExpression baseExpression = oceanBaseListPartitionBy.getBaseExpression();
        if (baseExpression != null) {
            client.setExpression(getRaw(baseExpression));
        }
        if (oceanBaseListPartitionBy.getSubPartition() != null) {
            SubPartitionClient subPartitionClient = toSubPartitionClient(oceanBaseListPartitionBy.getSubPartition());
            client.setSubPartitionClient(subPartitionClient);
        }
        List<ListPartitionElementClient> partitionElementClientList = null;
        if (oceanBaseListPartitionBy.getPartitionElementList() != null) {
            partitionElementClientList = oceanBaseListPartitionBy.getPartitionElementList().stream()
                .map(c -> {
                    ListPartitionElementClient listPartitionElementClient = toListPartitionElementClient(c);
                    return listPartitionElementClient;
                }).collect(Collectors.toList());
            client.setPartitionElementClientList(partitionElementClientList);
        }
        List<String> columns = null;
        if (oceanBaseListPartitionBy.getColumnDefinitions() != null) {
            columns = oceanBaseListPartitionBy.getColumnDefinitions().stream()
                .map(c -> c.getColName().getValue())
                .collect(Collectors.toList());
        }
        client.setColumns(columns);
        listPartitionProperty.setValue(client);
        return listPartitionProperty;
    }

    private ListPartitionElementClient toListPartitionElementClient(ListPartitionElement c) {
        List<String> expressionList = null;
        QualifiedName qualifiedName = c.getQualifiedName();
        String name = null;
        if (qualifiedName != null) {
            name = qualifiedName.toString();
        }
        String engine = null;
        if (c.getExpressionList() != null) {
            expressionList = c.getExpressionList().stream()
                .map(x -> getRaw(x)).collect(Collectors.toList());
        }
        if (c.getProperty() != null) {
            engine = c.getProperty().getValue();
        }
        Long num = null;
        if (c.getNum() != null) {
            num = c.getNum().getValue();
        }
        List<SubListPartitionElementClient> listSubPartitionElementClients = null;
        if (c.getSubPartitionList() != null) {
            List<SubPartitionElement> subPartitionElementList = c.getSubPartitionList().getSubPartitionElementList();
            listSubPartitionElementClients = subPartitionElementList.stream()
                .map(element -> {
                    return toListSubPartitionElementClient(element);
                }).collect(Collectors.toList());
        }
        return ListPartitionElementClient.builder()
            .expressionList(expressionList)
            .engine(engine)
            .num(num)
            .qualifiedName(name)
            .subPartitionElementList(listSubPartitionElementClients)
            .defaultExpr(c.getDefaultExpr())
            .build();

    }

    private SubListPartitionElementClient toListSubPartitionElementClient(SubPartitionElement c) {
        return (SubListPartitionElementClient)oceanBasePartitionClientConverter.toSubPartitionElementClient(c);
    }

    private KeyPartitionProperty toKeyPartitionProperty(OceanBaseKeyPartitionBy oceanBaseKeyPartitionBy) {
        if (oceanBaseKeyPartitionBy == null) {
            return null;
        }
        KeyPartitionProperty keyPartitionProperty = new KeyPartitionProperty();
        KeyPartitionClient keyPartitionClient = new KeyPartitionClient();
        LongLiteral partitionCount = oceanBaseKeyPartitionBy.getPartitionCount();
        if (partitionCount != null) {
            keyPartitionClient.setPartitionCount(partitionCount.getValue());
        }
        List<ColumnDefinition> columnDefinitions = oceanBaseKeyPartitionBy.getColumnDefinitions();
        List<String> columnList = null;
        if (columnDefinitions != null) {
            columnList = columnDefinitions.stream().map(c -> c.getColName().getValue()).collect(Collectors.toList());
            keyPartitionClient.setColumnList(columnList);
        }
        BaseSubPartition subPartition = oceanBaseKeyPartitionBy.getSubPartition();
        SubPartitionClient subPartitionClient = toSubPartitionClient(subPartition);
        keyPartitionClient.setSubPartitionClient(subPartitionClient);

        if (oceanBaseKeyPartitionBy.getHashPartitionElements() != null) {
            List<HashPartitionElementClient> partitionElementClientList = toPartitionElementClientList(
                oceanBaseKeyPartitionBy.getHashPartitionElements());
            keyPartitionClient.setPartitionElementClientList(partitionElementClientList);
        }

        //set value
        keyPartitionProperty.setValue(keyPartitionClient);
        return keyPartitionProperty;
    }

    private RangePartitionProperty toRangePartitionProperty(OceanBaseRangePartitionBy oceanBaseRangePartitionBy) {
        RangePartitionProperty rangePartitionProperty = new RangePartitionProperty();
        RangePartitionClient client = new RangePartitionClient();

        //columns
        List<String> columns = null;
        if (oceanBaseRangePartitionBy.getColumnDefinitions() != null) {
            columns = oceanBaseRangePartitionBy.getColumnDefinitions().stream().map(
                c -> c.getColName().getValue()
            ).collect(Collectors.toList());
        }
        client.setColumns(columns);

        //partition count
        Long partitionCount = null;
        if (oceanBaseRangePartitionBy.getPartitionCount() != null) {
            partitionCount = oceanBaseRangePartitionBy.getPartitionCount().getValue();
        }
        client.setPartitionCount(partitionCount);

        //expression
        BaseExpression baseExpression = oceanBaseRangePartitionBy.getBaseExpression();
        if (baseExpression != null) {
            client.setBaseExpression(getRaw(baseExpression));
        }

        //partition client
        SubPartitionClient rangeSubPartitionClient = toSubPartitionClient(oceanBaseRangePartitionBy.getSubPartition());
        client.setRangeSubPartitionClient(rangeSubPartitionClient);

        //RangePartitionElementClient
        List<RangePartitionElementClient> rangePartitionElementClients = toRangePartitionElementClient(
            oceanBaseRangePartitionBy.getSingleRangePartitionList());
        client.setRangePartitionElementClients(rangePartitionElementClients);
        //set value
        rangePartitionProperty.setValue(client);
        return rangePartitionProperty;
    }

    private SubPartitionClient toSubPartitionClient(BaseSubPartition subPartition) {
        return oceanBasePartitionClientConverter.toSubPartitionClient(subPartition);
    }

    public List<SubRangePartitionElementClient> toRangeSubPartitionElementClients(SubPartitionList subPartitionList) {
        if (CollectionUtils.isEmpty(subPartitionList.getSubPartitionElementList())) {
            return null;
        }
        List<SubPartitionElement> subPartitionElementList = subPartitionList.getSubPartitionElementList();
        return subPartitionElementList.stream().map(
            p -> (SubRangePartitionElementClient)oceanBasePartitionClientConverter.toSubPartitionElementClient((SubRangePartitionElement)p)
        ).collect(Collectors.toList());
    }

    public SingleRangeClientPartition toSingleRangeClientPartition(SingleRangePartition singleRangePartition) {
        LinkedHashMap<String, String> properties = null;
        List<Property> propertyList = singleRangePartition.getPropertyList();
        if (propertyList != null) {
            properties = Maps.newLinkedHashMap();
            for (Property p : propertyList) {
                properties.put(p.getName(), p.getValue());
            }
        }
        BaseClientPartitionKey partitionKey = null;
        if (singleRangePartition.getPartitionKey() != null) {
            partitionKey = toClientPartitionKey(singleRangePartition.getPartitionKey());
        }
        SingleRangeClientPartition singleRangeClientPartition = SingleRangeClientPartition.builder()
            .name(singleRangePartition.getName().getValue())
            .partitionKey(partitionKey)
            .properties(properties)
            .build();
        return singleRangeClientPartition;
    }

    private List<RangePartitionElementClient> toRangePartitionElementClient(List<RangePartitionElement> singleRangePartitionList) {
        if (singleRangePartitionList == null) {
            return null;
        }
        return singleRangePartitionList.stream().map(
            p -> geRangetPartitionElementClient(p)
        ).collect(Collectors.toList());
    }

    private RangePartitionElementClient geRangetPartitionElementClient(RangePartitionElement p) {
        Long id = null;
        if (p.getIdCount() != null) {
            id = p.getIdCount().getValue();
        }
        SingleRangeClientPartition singleRangeClientPartition = null;
        if (p.getSingleRangePartition() != null) {
            singleRangeClientPartition = toSingleRangeClientPartition(p.getSingleRangePartition());
        }
        List<SubRangePartitionElementClient> rangeSubPartitionElements = null;
        if (p.getSubPartitionList() != null) {
            rangeSubPartitionElements = toRangeSubPartitionElementClients(p.getSubPartitionList());
        }
        RangePartitionElementClient rangePartitionElementClient = RangePartitionElementClient.builder()
            .id(id)
            .singleRangeClientPartition(singleRangeClientPartition)
            .rangeSubPartitionElementClients(rangeSubPartitionElements)
            .build();
        return rangePartitionElementClient;
    }

    @Override
    protected PartitionedBy toPartitionedBy(Table table, List<Column> columns) {
        PartitionedBy partitionedBy = super.toPartitionedBy(table, columns);
        if (partitionedBy != null) {
            return partitionedBy;
        }
        List<BaseClientProperty> properties = table.getProperties();
        if (properties == null) {
            return null;
        }
        Optional<BaseClientProperty> first = properties.stream().filter(p -> {
            return StringUtils.equalsIgnoreCase(p.getKey(), OceanBasePropertyKey.PARTITION.getValue());
        }).findFirst();
        if (first.isEmpty()) {
            return null;
        }
        BaseClientProperty baseClientProperty = first.get();
        if (baseClientProperty instanceof HashPartitionProperty) {
            HashPartitionProperty hashPartitionProperty = (HashPartitionProperty)baseClientProperty;
            return toHashPartitionBy(hashPartitionProperty);
        }
        if (baseClientProperty instanceof RangePartitionProperty) {
            RangePartitionProperty rangePartitionProperty = (RangePartitionProperty)baseClientProperty;
            return toRangePartitionBy(rangePartitionProperty);
        }
        if (baseClientProperty instanceof ListPartitionProperty) {
            ListPartitionProperty listPartitionProperty = (ListPartitionProperty)baseClientProperty;
            return toListPartitionBy(listPartitionProperty);
        }
        if (baseClientProperty instanceof KeyPartitionProperty) {
            KeyPartitionProperty keyPartitionProperty = (KeyPartitionProperty)baseClientProperty;
            return toKeyPartitionBy(keyPartitionProperty);
        }
        return null;
    }

    private PartitionedBy toKeyPartitionBy(KeyPartitionProperty keyPartitionProperty) {
        if (keyPartitionProperty == null) {
            return null;
        }
        KeyPartitionClient value = keyPartitionProperty.getValue();
        OceanBaseKeyPartitionBy oceanBaseKeyPartitionBy = new OceanBaseKeyPartitionBy(
            getColumnDefinitions(value.getColumnList()),
            getLongLiteral(value.getPartitionCount()),
            getSubPartition(value.getSubPartitionClient()),
            getHashPartitionElements(value.getPartitionElementClientList())
        );
        return oceanBaseKeyPartitionBy;
    }

    private PartitionedBy toListPartitionBy(ListPartitionProperty listPartitionProperty) {
        if (listPartitionProperty == null) {
            return null;
        }
        ListPartitionClient value = listPartitionProperty.getValue();
        List<ListPartitionElement> partitionElementList = null;
        if (value.getPartitionElementClientList() != null) {
            partitionElementList = value.getPartitionElementClientList().stream()
                .map(e -> {
                    return getListPartitionElement(e);
                }).collect(Collectors.toList());
        }
        OceanBaseListPartitionBy oceanBaseListPartitionBy = new OceanBaseListPartitionBy(
            getColumnDefinitions(value.getColumns()),
            (BaseExpression)getLanguageParser().parseExpression(value.getExpression()),
            getLongLiteral(value.getPartitionCount()),
            getSubPartition(value.getSubPartitionClient()),
            partitionElementList
        );
        return oceanBaseListPartitionBy;
    }

    private ListPartitionElement getListPartitionElement(ListPartitionElementClient e) {
        QualifiedName qualifiedName = QualifiedName.of(e.getQualifiedName());
        List<BaseExpression> expressionList = null;
        if (e.getExpressionList() != null) {
            expressionList = e.getExpressionList().stream()
                .map(expr -> (BaseExpression)getLanguageParser().parseExpression(expr)).collect(Collectors.toList());
        }
        LongLiteral num = null;
        if (e.getNum() != null) {
            num = new LongLiteral(e.getNum().toString());
        }
        Property property = null;
        if (e.getEngine() != null) {
            property = new Property(ExtensionPropertyKey.TABLE_ENGINE.getValue(), e.getEngine());
        }
        SubPartitionList subPartitionList = null;
        if (e.getSubPartitionElementList() != null) {
            List<SubPartitionElement> subPartitionElementList = e.getSubPartitionElementList()
                .stream().map(
                    ele -> oceanBasePartitionClientConverter.getSubPartitionElement(ele)
                ).collect(Collectors.toList());
            subPartitionList = new SubPartitionList(subPartitionElementList);
        }
        return new ListPartitionElement(qualifiedName, e.getDefaultExpr(), expressionList, num, property, subPartitionList);
    }

    private PartitionedBy toRangePartitionBy(RangePartitionProperty rangePartitionProperty) {
        if (rangePartitionProperty == null) {
            return null;
        }
        RangePartitionClient value = rangePartitionProperty.getValue();
        if (value == null) {
            return null;
        }
        List<ColumnDefinition> columnDefine = getColumnDefinitions(value.getColumns());
        LongLiteral partitionCount = getLongLiteral(value.getPartitionCount());
        BaseExpression baseExpression = null;
        if (value.getBaseExpression() != null) {
            baseExpression = (BaseExpression)getLanguageParser().parseExpression(value.getBaseExpression());
        }
        BaseSubPartition subPartition = null;
        SubPartitionClient rangeSubPartitionClient = value.getRangeSubPartitionClient();
        if (rangeSubPartitionClient != null) {
            subPartition = getSubPartition(rangeSubPartitionClient);
        }
        List<RangePartitionElement> singleRangePartitionList = null;
        if (value.getRangePartitionElementClients() != null) {
            singleRangePartitionList = value.getRangePartitionElementClients().stream().map(
                c -> {
                    return toRangePartitionElement(c);
                }
            ).collect(Collectors.toList());
        }
        return new OceanBaseRangePartitionBy(columnDefine, partitionCount, subPartition, baseExpression, singleRangePartitionList);
    }

    public LongLiteral getLongLiteral(Long value) {
        LongLiteral partitionCount = null;
        if (value != null) {
            partitionCount = new LongLiteral(String.valueOf(value));
        }
        return partitionCount;
    }

    private List<ColumnDefinition> getColumnDefinitions(List<String> columns) {
        List<ColumnDefinition> columnDefine = null;
        if (columns != null) {
            columnDefine = columns.stream().map(
                c -> {
                    return ColumnDefinition.builder().colName(new Identifier(c)).build();
                }
            ).collect(Collectors.toList());
        }
        return columnDefine;
    }

    private BaseSubPartition getSubPartition(SubPartitionClient subPartitionClient) {
        return oceanBasePartitionClientConverter.getSubPartition(subPartitionClient);
    }

    public RangePartitionElement toRangePartitionElement(RangePartitionElementClient s) {
        LongLiteral idCount = getLongLiteral(s.getId());
        SingleRangePartition singleRangePartition = null;
        if (s.getSingleRangeClientPartition() != null) {
            singleRangePartition = (SingleRangePartition)getPartitionDesc(s.getSingleRangeClientPartition());
        }
        SubPartitionList subPartitionList = null;
        if (s.getRangeSubPartitionElementClients() != null) {
            List<SubPartitionElement> subPartitionElementList = s.getRangeSubPartitionElementClients().stream()
                .map(elementClient -> {
                    return oceanBasePartitionClientConverter.getSubPartitionElement(elementClient);
                }).collect(Collectors.toList());
            subPartitionList = new SubPartitionList(subPartitionElementList);
        }
        RangePartitionElement rangePartitionElement = new RangePartitionElement(
            idCount, singleRangePartition, subPartitionList
        );
        return rangePartitionElement;
    }

    private PartitionedBy toHashPartitionBy(HashPartitionProperty hashPartitionProperty) {
        LongLiteral i = null;
        HashPartitionClient value = hashPartitionProperty.getValue();
        if (value.getPartitionCount() != null) {
            i = new LongLiteral(String.valueOf(value.getPartitionCount()));
        }
        BaseExpression expression = null;
        if (value.getExpression() != null) {
            expression = (BaseExpression)getLanguageParser().parseExpression(value.getExpression());
        }
        SubPartitionClient subPartitionClient = value.getSubPartitionClient();
        BaseSubPartition subPartition = getSubPartition(subPartitionClient);
        List<HashPartitionElement> hashPartitionElements = getHashPartitionElements(value.getPartitionElementClientList());
        OceanBaseHashPartitionBy oceanBaseHashPartitionBy = new OceanBaseHashPartitionBy(
            i, expression, subPartition, hashPartitionElements
        );
        return oceanBaseHashPartitionBy;
    }

    private List<HashPartitionElement> getHashPartitionElements(List<HashPartitionElementClient> value) {
        List<HashPartitionElement> hashPartitionElements = null;
        if (value == null) {
            return null;
        }
        hashPartitionElements = value.stream().map(
            p -> getHashPartitionElement(p)
        ).collect(Collectors.toList());
        return hashPartitionElements;
    }

    private HashPartitionElement getHashPartitionElement(HashPartitionElementClient p) {
        QualifiedName qualifiedName = QualifiedName.of(p.getQualifiedName());
        LongLiteral num = getLongLiteral(p.getNum());
        Property property = null;
        if (p.getProperty() != null) {
            property = new Property(p.getProperty().getKey(), p.getProperty().getValue());
        }
        SubPartitionList subPartitionList = null;
        if (p.getSubPartitionList() != null) {
            List<SubPartitionElement> subPartitionElementList = p.getSubPartitionList().stream()
                .map(s -> {
                    return oceanBasePartitionClientConverter.getSubPartitionElement(s);
                }).collect(Collectors.toList());
            subPartitionList = new SubPartitionList(subPartitionElementList);
        }
        return new HashPartitionElement(qualifiedName, num, property, subPartitionList);
    }

    private HashPartitionProperty toHashPartitionProperty(OceanBaseHashPartitionBy oceanBaseHashPartitionBy) {
        HashPartitionProperty property = new HashPartitionProperty();
        LongLiteral partitionCount = oceanBaseHashPartitionBy.getPartitionCount();
        Long value = partitionCount != null ? partitionCount.getValue() : null;
        HashPartitionClient client = HashPartitionClient.builder()
            .expression(StripUtils.strip(getRaw(oceanBaseHashPartitionBy.getExpression())))
            .partitionCount(value)
            .subPartitionClient(toSubPartitionClient(oceanBaseHashPartitionBy.getSubPartition()))
            .partitionElementClientList(toPartitionElementClientList(oceanBaseHashPartitionBy.getHashPartitionElements()))
            .build();

        property.setValue(client);
        return property;
    }

    private List<HashPartitionElementClient> toPartitionElementClientList(List<HashPartitionElement> hashPartitionElements) {
        if (hashPartitionElements == null || hashPartitionElements.isEmpty()) {
            return null;
        }
        return hashPartitionElements.stream().map(e -> getHashPartitionElementClient(e)).collect(Collectors.toList());
    }

    private HashPartitionElementClient getHashPartitionElementClient(HashPartitionElement e) {
        StringProperty property = getStringProperty(e.getProperty());
        List<SubHashPartitionElementClient> list = null;
        if (e.getSubPartitionList() != null) {
            list = e.getSubPartitionList().getSubPartitionElementList().stream().map(
                element -> {
                    return (SubHashPartitionElementClient)oceanBasePartitionClientConverter.toSubPartitionElementClient(element);
                }
            ).collect(Collectors.toList());
        }
        return HashPartitionElementClient.builder()
            .num(e.getNum() == null ? null : e.getNum().getValue())
            .qualifiedName(e.getQualifiedName().toString())
            .property(property)
            .subPartitionList(list)
            .build();
    }

    @Override
    protected PartitionValue getPartitionValue(PartitionClientValue partitionClientValue) {
        String value = partitionClientValue.getValue();
        BaseExpression baseExpression = null;
        if (StringUtils.isNotBlank(value)) {
            baseExpression = (BaseExpression)getLanguageParser().parseExpression(value);
        }
        return new PartitionValue(partitionClientValue.isMaxValue(), baseExpression);
    }

    public StringProperty getStringProperty(Property e) {
        if (e == null) {
            return null;
        }
        StringProperty property = new StringProperty();
        property.setKey(e.getName());
        property.setValueString(e.getValue());
        return property;
    }

    @Override
    protected String formatExpression(BaseExpression baseExpression) {
        OceanBaseMysqlOutVisitor mysqlOutVisitor = new OceanBaseMysqlOutVisitor(OceanBaseContext.builder().build());
        baseExpression.accept(mysqlOutVisitor, 0);
        return mysqlOutVisitor.getBuilder().toString();
    }

    @Override
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
        if (!(baseConstraint instanceof UniqueKeyExprConstraint)) {
            return;
        }
        UniqueKeyExprConstraint u = (UniqueKeyExprConstraint)baseConstraint;
        List<IndexSortKey> indexSortKeys = u.getIndexSortKeys();
        UniqueKeyExprClientConstraint uniqueKeyExprClientConstraint = new UniqueKeyExprClientConstraint();
        List<String> column = indexSortKeys.stream()
            .filter(i -> {
                return i instanceof IndexColumnName;
            }).map(i -> {
                IndexColumnName indexColumnName = (IndexColumnName)i;
                return indexColumnName.getColumnName().getValue();
            }).collect(Collectors.toList());

        uniqueKeyExprClientConstraint.setColumns(column);

        List<String> expression = indexSortKeys.stream()
            .filter(i -> {
                return i instanceof IndexExpr;
            }).map(i -> {
                IndexExpr indexColumnName = (IndexExpr)i;
                return formatExpression(indexColumnName.getExpression());
            }).collect(Collectors.toList());
        uniqueKeyExprClientConstraint.setExpression(expression);
        outlineConstraint.add(uniqueKeyExprClientConstraint);
    }

    public List<String> getColumnList(List<Identifier> columnList) {
        if (columnList == null) {
            return null;
        }
        return columnList.stream()
            .map(Identifier::getValue)
            .collect(Collectors.toList());
    }
}
