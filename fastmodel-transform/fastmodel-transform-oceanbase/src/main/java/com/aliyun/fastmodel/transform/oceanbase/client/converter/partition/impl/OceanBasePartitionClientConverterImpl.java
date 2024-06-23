package com.aliyun.fastmodel.transform.oceanbase.client.converter.partition.impl;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.SingleRangeClientPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.SingleRangePartition;
import com.aliyun.fastmodel.transform.oceanbase.client.converter.OceanBaseMysqlClientConverter;
import com.aliyun.fastmodel.transform.oceanbase.client.converter.partition.OceanBasePartitionClientConverter;
import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionElementClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.hash.SubHashPartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.hash.SubHashPartitionElementClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.hash.SubHashTemplatePartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.key.SubKeyPartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.key.SubKeyTemplatePartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.list.SubListPartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.list.SubListPartitionElementClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.list.SubListTemplatePartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.range.RangePartitionElementClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.range.SubRangePartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.range.SubRangePartitionElementClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.range.SubRangeTemplatePartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.BaseSubPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.SubHashPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.SubKeyPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.SubListPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.SubRangePartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.RangePartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubHashPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubListPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubPartitionList;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubRangePartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.template.SubHashTemplatePartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.template.SubKeyTemplatePartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.template.SubListTemplatePartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.template.SubRangeTemplatePartition;

/**
 * OceanBasePartitionClientConverterImpl
 *
 * @author panguanjing
 * @date 2024/2/28
 */
public class OceanBasePartitionClientConverterImpl implements OceanBasePartitionClientConverter {

    private final OceanBaseMysqlClientConverter extensionClientConverter;

    public OceanBasePartitionClientConverterImpl(OceanBaseMysqlClientConverter extensionClientConverter) {
        this.extensionClientConverter = extensionClientConverter;
    }

    @Override
    public SubPartitionClient toSubPartitionClient(BaseSubPartition subPartition) {
        List<String> columnLists = null;
        String expression = null;
        List<RangePartitionElementClient> singleRangePartitionList = null;
        if (subPartition instanceof SubRangePartition) {
            return getSubRangePartitionClient((SubRangePartition)subPartition, columnLists, expression, singleRangePartitionList);
        } else if (subPartition instanceof SubKeyPartition) {
            return getSubKeyPartitionClient((SubKeyPartition)subPartition);
        } else if (subPartition instanceof SubHashPartition) {
            return getSubHashPartitionClient((SubHashPartition)subPartition);
        } else if (subPartition instanceof SubListPartition) {
            return getSubListPartitionClient((SubListPartition)subPartition);
        } else if (subPartition instanceof SubRangeTemplatePartition) {
            return getSubRangeTemplatePartitionClient((SubRangeTemplatePartition)subPartition);
        } else if (subPartition instanceof SubKeyTemplatePartition) {
            return getSubKeyTemplatePartitionClient((SubKeyTemplatePartition)subPartition);
        } else if (subPartition instanceof SubHashTemplatePartition) {
            return getSubHashTemplatePartitionClient((SubHashTemplatePartition)subPartition);
        } else if (subPartition instanceof SubListTemplatePartition) {
            return getSubListTemplatePartitionClient((SubListTemplatePartition)subPartition);
        }
        return null;
    }

    private SubPartitionClient getSubListTemplatePartitionClient(SubListTemplatePartition subPartition) {
        List<SubPartitionElementClient> clientList = getSubPartitionElementClientList(subPartition.getSubPartitionList());
        return SubListTemplatePartitionClient.builder()
            .expression(extensionClientConverter.getRaw(subPartition.getExpression()))
            .columnList(extensionClientConverter.getColumnList(subPartition.getColumnList()))
            .subPartitionElementClientList(clientList)
            .build();
    }

    private List<SubPartitionElementClient> getSubPartitionElementClientList(SubPartitionList subPartitionList) {
        List<SubPartitionElement> subPartitionElementList = null;
        List<SubPartitionElementClient> clientList = null;
        if (subPartitionList != null) {
            subPartitionElementList = subPartitionList.getSubPartitionElementList();
            clientList = subPartitionElementList.stream()
                .map(p -> toSubPartitionElementClient(p)).collect(Collectors.toList());
        }
        return clientList;
    }

    private SubPartitionClient getSubHashTemplatePartitionClient(SubHashTemplatePartition subPartition) {
        String expression = null;
        List<SubPartitionElementClient> list = null;
        if (subPartition.getExpression() != null) {
            expression = extensionClientConverter.getRaw(subPartition.getExpression());
        }
        if (subPartition.getSubPartitionList() != null) {
            list = getSubPartitionElementClientList(subPartition.getSubPartitionList());
        }
        return SubHashTemplatePartitionClient.builder()
            .expression(expression)
            .subPartitionElementClientList(list)
            .build();
    }

    private SubPartitionClient getSubKeyTemplatePartitionClient(SubKeyTemplatePartition subPartition) {
        List<String> columnList = extensionClientConverter.getColumnList(subPartition.getColumnList());
        List<SubPartitionElementClient> subPartitionElementClient = getSubPartitionElementClientList(subPartition.getSubPartitionList());
        return SubKeyTemplatePartitionClient.builder()
            .columnList(columnList)
            .subPartitionElementClients(subPartitionElementClient)
            .build();
    }

    private SubPartitionClient getSubRangeTemplatePartitionClient(SubRangeTemplatePartition subPartition) {
        List<String> columnList = extensionClientConverter.getColumnList(subPartition.getColumnList());
        String expression = null;
        if (subPartition.getExpression() != null) {
            expression = extensionClientConverter.getRaw(subPartition.getExpression());
        }
        List<SubPartitionElementClient> elementList = getSubPartitionElementClientList(subPartition.getSubPartitionList());
        return SubRangeTemplatePartitionClient.builder()
            .columnList(columnList)
            .expression(expression)
            .subPartitionElementClientList(elementList)
            .build();
    }

    private SubListPartitionClient getSubListPartitionClient(SubListPartition subPartition) {
        SubListPartition subListPartition = subPartition;
        List<Identifier> columnList = subListPartition.getColumnList();
        List<String> list = null;
        if (columnList != null) {
            list = columnList.stream().map(Identifier::getValue).collect(Collectors.toList());
        }
        String subListExpression = null;
        if (subListPartition.getExpression() != null) {
            subListExpression = extensionClientConverter.getRaw(subListPartition.getExpression());
        }
        return SubListPartitionClient.builder()
            .columnList(list)
            .expression(subListExpression)
            .build();
    }

    private SubHashPartitionClient getSubHashPartitionClient(SubHashPartition subPartition) {
        SubHashPartition subHashPartition = subPartition;
        return SubHashPartitionClient.builder()
            .expression(StripUtils.strip(extensionClientConverter.getRaw(subHashPartition.getExpression())))
            .subpartitionCount(subHashPartition.getSubpartitionCount() != null ? subHashPartition.getSubpartitionCount().getValue() : null)
            .build();
    }

    private static SubKeyPartitionClient getSubKeyPartitionClient(SubKeyPartition subPartition) {
        List<String> columnLists;
        SubKeyPartition subKeyPartition = subPartition;
        List<Identifier> columnList = subKeyPartition.getColumnList();
        columnLists = columnList.stream().map(Identifier::getValue).collect(Collectors.toList());
        Long subPartitionCount = null;
        if (subKeyPartition.getSubpartitionCount() != null) {
            subPartitionCount = subKeyPartition.getSubpartitionCount().getValue();
        }
        return new SubKeyPartitionClient(subPartitionCount, columnLists);
    }

    private SubRangePartitionClient getSubRangePartitionClient(SubRangePartition subPartition, List<String> columnLists, String expression,
        List<RangePartitionElementClient> singleRangePartitionList) {
        SubRangePartition subRangePartition = subPartition;
        List<Identifier> columnList = subRangePartition.getColumnList();
        if (columnList != null) {
            columnLists = columnList.stream().map(Identifier::getValue).collect(Collectors.toList());
        }
        if (subRangePartition.getExpression() != null) {
            expression = extensionClientConverter.getRaw(subRangePartition.getExpression());
        }
        if (subRangePartition.getSingleRangePartitionList() != null) {
            singleRangePartitionList = subRangePartition.getSingleRangePartitionList()
                .stream()
                .map(c -> getRangePartitionElementClient(c)).collect(Collectors.toList());
        }
        return SubRangePartitionClient.builder()
            .columnList(columnLists)
            .expression(expression)
            .singleRangePartitionList(singleRangePartitionList)
            .build();
    }

    private RangePartitionElementClient getRangePartitionElementClient(RangePartitionElement c) {
        Long id = null;
        if (c.getIdCount() != null) {
            id = c.getIdCount().getValue();
        }
        SingleRangeClientPartition singleRangePartition = null;
        if (c.getSingleRangePartition() != null) {
            singleRangePartition = extensionClientConverter.toSingleRangeClientPartition(c.getSingleRangePartition());
        }
        List<SubRangePartitionElementClient> rangeSubPartitionElementClients = null;
        if (c.getSubPartitionList() != null) {
            rangeSubPartitionElementClients = extensionClientConverter.toRangeSubPartitionElementClients(c.getSubPartitionList());
        }
        return new RangePartitionElementClient(id, singleRangePartition, rangeSubPartitionElementClients);
    }

    @Override
    public BaseSubPartition getSubPartition(SubPartitionClient subPartitionClient) {
        //sub range
        if (subPartitionClient instanceof SubRangePartitionClient) {
            return getSubRangePartition((SubRangePartitionClient)subPartitionClient);
        }
        //sub range template
        if (subPartitionClient instanceof SubRangeTemplatePartitionClient) {
            return getSubRangeTemplatePartition((SubRangeTemplatePartitionClient)subPartitionClient);
        }

        //sub key
        if (subPartitionClient instanceof SubKeyPartitionClient) {
            return getSubKeyPartition((SubKeyPartitionClient)subPartitionClient);
        }

        if (subPartitionClient instanceof SubKeyTemplatePartitionClient) {
            return getSubKeyTemplatePartition((SubKeyTemplatePartitionClient)subPartitionClient);
        }

        // sub hash
        if (subPartitionClient instanceof SubHashPartitionClient) {
            return getSubHashPartition((SubHashPartitionClient)subPartitionClient);
        }

        //sub hash template
        if (subPartitionClient instanceof SubHashTemplatePartitionClient) {
            return getSubHashTemplatePartition((SubHashTemplatePartitionClient)subPartitionClient);
        }

        //sub list
        if (subPartitionClient instanceof SubListPartitionClient) {
            return getSubListPartition((SubListPartitionClient)subPartitionClient);
        }

        //sub list template
        if (subPartitionClient instanceof SubListTemplatePartitionClient) {
            return getSubListTemplatePartition((SubListTemplatePartitionClient)subPartitionClient);
        }

        return null;
    }

    private BaseSubPartition getSubHashTemplatePartition(SubHashTemplatePartitionClient subPartitionClient) {
        BaseExpression expression = getExpression(subPartitionClient.getExpression());
        SubPartitionList subPartitionList = getSubPartitionList(subPartitionClient.getSubPartitionElementClientList());
        return new SubHashTemplatePartition(expression, subPartitionList);
    }

    private BaseSubPartition getSubListTemplatePartition(SubListTemplatePartitionClient subPartitionClient) {
        BaseExpression expression = getExpression(subPartitionClient.getExpression());
        List<Identifier> columnList = getIdentifiers(subPartitionClient.getColumnList());
        SubPartitionList subPartitionList = getSubPartitionList(subPartitionClient.getSubPartitionElementClientList());
        return new SubListTemplatePartition(expression, columnList, subPartitionList);
    }

    private BaseSubPartition getSubKeyTemplatePartition(SubKeyTemplatePartitionClient subPartitionClient) {
        List<Identifier> columnList = getIdentifiers(subPartitionClient.getColumnList());
        SubPartitionList subPartitionList = getSubPartitionList(subPartitionClient.getSubPartitionElementClients());
        return new SubKeyTemplatePartition(columnList, subPartitionList);
    }

    private BaseSubPartition getSubRangeTemplatePartition(SubRangeTemplatePartitionClient subPartitionClient) {
        BaseExpression expression = getExpression(subPartitionClient.getExpression());
        List<Identifier> columnList = getIdentifiers(subPartitionClient.getColumnList());
        SubPartitionList subPartitionList = getSubPartitionList(subPartitionClient.getSubPartitionElementClientList());
        return new SubRangeTemplatePartition(expression, columnList, subPartitionList);
    }

    private static List<Identifier> getIdentifiers(List<String> subPartitionClient) {
        List<Identifier> columnList = null;
        if (subPartitionClient != null) {
            columnList = subPartitionClient.stream()
                .map(Identifier::new)
                .collect(Collectors.toList());
        }
        return columnList;
    }

    private SubPartitionList getSubPartitionList(List<SubPartitionElementClient> subPartitionElementClients) {
        SubPartitionList subPartitionList = null;
        if (subPartitionElementClients != null) {
            List<SubPartitionElement> subPartitionElementList = subPartitionElementClients.stream()
                .map(c -> getSubPartitionElement(c)).collect(Collectors.toList());
            subPartitionList = new SubPartitionList(subPartitionElementList);
        }
        return subPartitionList;
    }

    private BaseExpression getExpression(String subPartitionClient) {
        BaseExpression expression = null;
        if (subPartitionClient != null) {
            expression = (BaseExpression)extensionClientConverter.getLanguageParser().parseExpression(subPartitionClient);
        }
        return expression;
    }

    @Override
    public SubPartitionElementClient toSubPartitionElementClient(SubPartitionElement subPartitionElement) {
        if (subPartitionElement == null) {
            return null;
        }
        if (subPartitionElement instanceof SubListPartitionElement) {
            return toListSubPartitionElementClient((SubListPartitionElement)subPartitionElement);
        }
        if (subPartitionElement instanceof SubHashPartitionElement) {
            return getSubHashPartitionElementClient((SubHashPartitionElement)subPartitionElement);
        }
        if (subPartitionElement instanceof SubRangePartitionElement) {
            return getSubRangePartitionElementClient((SubRangePartitionElement)subPartitionElement);
        }
        return null;
    }

    private SubRangePartitionElementClient getSubRangePartitionElementClient(SubRangePartitionElement p) {
        SubRangePartitionElement subPartitionElement = p;
        SingleRangeClientPartition singleRangeClientPartition = extensionClientConverter.toSingleRangeClientPartition(
            subPartitionElement.getSingleRangePartition());
        SubRangePartitionElementClient client = SubRangePartitionElementClient.builder()
            .singleRangeClientPartition(singleRangeClientPartition)
            .build();
        return client;
    }

    private SubHashPartitionElementClient getSubHashPartitionElementClient(SubHashPartitionElement element) {
        SubHashPartitionElement hashSubPartitionElement = element;
        StringProperty stringProperty = extensionClientConverter.getStringProperty(hashSubPartitionElement.getProperty());
        return SubHashPartitionElementClient.builder()
            .qualifiedName(hashSubPartitionElement.getQualifiedName().toString())
            .stringProperty(stringProperty)
            .build();
    }

    private SubListPartitionElementClient toListSubPartitionElementClient(SubListPartitionElement listSubPartitionElement) {
        String engine = null;
        if (listSubPartitionElement.getProperty() != null) {
            engine = listSubPartitionElement.getProperty().getValue();
        }
        List<String> expressionList = null;
        if (listSubPartitionElement.getExpressionList() != null) {
            expressionList = listSubPartitionElement.getExpressionList().stream()
                .map(e -> extensionClientConverter.getRaw(e)).collect(Collectors.toList());
        }
        return SubListPartitionElementClient.builder()
            .defaultListExpr(listSubPartitionElement.getDefaultListExpr())
            .qualifiedName(listSubPartitionElement.getQualifiedName().toString())
            .expressionList(expressionList)
            .engine(engine)
            .build();
    }

    @Override
    public SubPartitionElement getSubPartitionElement(SubPartitionElementClient subPartitionElementClient) {
        if (subPartitionElementClient instanceof SubListPartitionElementClient) {
            return getSubListPartitionElement((SubListPartitionElementClient)subPartitionElementClient);
        }

        if (subPartitionElementClient instanceof SubRangePartitionElementClient) {
            return getSubRangePartitionElement((SubRangePartitionElementClient)subPartitionElementClient);
        }

        if (subPartitionElementClient instanceof SubHashPartitionElementClient) {
            return getSubHashPartitionElement((SubHashPartitionElementClient)subPartitionElementClient);
        }

        return null;
    }

    private SubRangePartitionElement getSubRangePartitionElement(SubRangePartitionElementClient elementClient) {
        SingleRangeClientPartition singleRangeClientPartition = elementClient.getSingleRangeClientPartition();
        SingleRangePartition subSingleRangePartition = (SingleRangePartition)extensionClientConverter.getPartitionDesc(singleRangeClientPartition);
        SubRangePartitionElement rangeSubPartitionElement = new SubRangePartitionElement(
            subSingleRangePartition
        );
        return rangeSubPartitionElement;
    }

    private SubHashPartitionElement getSubHashPartitionElement(SubHashPartitionElementClient s) {
        Property property1 = null;
        if (s.getStringProperty() != null) {
            property1 = new Property(s.getStringProperty().getKey(), s.getStringProperty().getValue());
        }
        return new SubHashPartitionElement(QualifiedName.of(s.getQualifiedName()), property1);
    }

    private SubListPartitionElement getSubListPartitionElement(SubListPartitionElementClient ele) {
        Property property2 = null;
        if (ele.getEngine() != null) {
            property2 = new Property(ExtensionPropertyKey.TABLE_ENGINE.getValue(), ele.getEngine());
        }
        List<BaseExpression> expressionList2 = null;
        if (ele.getExpressionList() != null) {
            expressionList2 = ele.getExpressionList().stream().map(
                expr -> {
                    return (BaseExpression)extensionClientConverter.getLanguageParser().parseExpression(expr);
                }
            ).collect(Collectors.toList());
        }
        return new SubListPartitionElement(QualifiedName.of(ele.getQualifiedName()), ele.getDefaultListExpr(), expressionList2, property2);
    }

    private SubListPartition getSubListPartition(SubListPartitionClient subPartitionClient) {
        BaseExpression expression = getExpression(subPartitionClient.getExpression());
        List<Identifier> columnList = getIdentifiers(subPartitionClient.getColumnList());
        return new SubListPartition(expression, columnList);
    }

    private SubHashPartition getSubHashPartition(SubHashPartitionClient subPartitionClient) {
        SubHashPartitionClient subHashPartitionClient = subPartitionClient;
        BaseExpression expression = getExpression(subHashPartitionClient.getExpression());
        LongLiteral count = null;
        if (subHashPartitionClient.getSubpartitionCount() != null) {
            count = new LongLiteral(String.valueOf(subHashPartitionClient.getSubpartitionCount()));
        }
        return new SubHashPartition(expression, count);
    }

    private SubKeyPartition getSubKeyPartition(SubKeyPartitionClient subPartitionClient) {
        SubKeyPartitionClient subKeyPartitionClient = subPartitionClient;
        List<Identifier> collect = subKeyPartitionClient.getColumnList().stream()
            .map(Identifier::new)
            .collect(Collectors.toList());

        Long subPartitionCount = subKeyPartitionClient.getSubPartitionCount();
        LongLiteral longLiteral = extensionClientConverter.getLongLiteral(subPartitionCount);
        return new SubKeyPartition(collect, longLiteral);
    }

    private BaseSubPartition getSubRangePartition(SubRangePartitionClient subPartitionClient) {
        BaseSubPartition subPartition;
        SubRangePartitionClient subRangePartitionClient = subPartitionClient;
        String expression = subRangePartitionClient.getExpression();
        BaseExpression subExpression = getExpression(expression);
        List<Identifier> subColumnList = null;
        if (subRangePartitionClient.getColumnList() != null) {
            subColumnList = subRangePartitionClient.getColumnList().stream().map(
                c -> new Identifier(c)
            ).collect(Collectors.toList());
        }
        List<RangePartitionElementClient> singleRangePartitionList = subRangePartitionClient.getSingleRangePartitionList();
        List<RangePartitionElement> singlePartitionElementList = null;
        if (singleRangePartitionList != null) {
            singlePartitionElementList = singleRangePartitionList.stream()
                .map(s -> extensionClientConverter.toRangePartitionElement(s)).collect(Collectors.toList());
        }
        subPartition = new SubRangePartition(subExpression, subColumnList, singlePartitionElementList);
        return subPartition;
    }

}
