package com.aliyun.fastmodel.transform.oceanbase.parser.visitor;

import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.OceanBaseMysqlCharDataType;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.OceanBaseMysqlGenericDataType;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBaseHashPartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBaseKeyPartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBaseListPartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBasePartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBaseRangePartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.BaseSubPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.SubHashPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.SubKeyPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.SubListPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.SubRangePartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.HashPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.ListPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.PartitionElement;
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
 * Desc:
 *
 * @author panguanjing
 * @date 2024/1/20
 */
public interface OceanBaseMysqlAstVisitor<R, C> extends ExtensionAstVisitor<R, C> {

    default R visitOceanBaseMysqlGenericDataType(OceanBaseMysqlGenericDataType oceanBaseMysqlGenericDataType, C context) {
        return visitGenericDataType(oceanBaseMysqlGenericDataType, context);
    }

    default R visitSubHashPartition(SubHashPartition subHashPartition, C context) {
        return visitBaseSubPartition(subHashPartition, context);
    }

    default R visitBaseSubPartition(BaseSubPartition baseSubPartition, C context) {
        return visitNode(baseSubPartition, context);
    }

    default R visitSubKeyPartition(SubKeyPartition subKeyPartition, C context) {
        return visitBaseSubPartition(subKeyPartition, context);
    }

    default R visitSubRangePartition(SubRangePartition subRangePartition, C context) {
        return visitBaseSubPartition(subRangePartition, context);
    }

    default R visitSubListPartition(SubListPartition subListPartition, C context) {
        return visitBaseSubPartition(subListPartition, context);
    }

    default R visitOceanBaseHashPartitionBy(OceanBaseHashPartitionBy oceanBaseHashPartitionBy, C context) {
        return visitOceanBasePartitionBy(oceanBaseHashPartitionBy, context);
    }

    default R visitOceanBasePartitionBy(OceanBasePartitionBy oceanBasePartitionBy, C context) {
        return visitNode(oceanBasePartitionBy, context);
    }

    default R visitOceanBaseKeyPartitionBy(OceanBaseKeyPartitionBy oceanBaseKeyPartitionBy, C context) {
        return visitOceanBasePartitionBy(oceanBaseKeyPartitionBy, context);
    }

    default R visitOceanBaseRangePartitionBy(OceanBaseRangePartitionBy oceanBaseRangePartitionBy, C context) {
        return visitOceanBasePartitionBy(oceanBaseRangePartitionBy, context);
    }

    default R visitOceanBaseListPartitionBy(OceanBaseListPartitionBy oceanBaseListPartitionBy, C context) {
        return visitOceanBasePartitionBy(oceanBaseListPartitionBy, context);
    }

    default R visitRangePartitionElement(RangePartitionElement rangePartitionElement, C context) {
        return visitPartitionElement(rangePartitionElement, context);
    }

    default R visitPartitionElement(PartitionElement partitionElement, C context) {
        return visitNode(partitionElement, context);
    }

    default R visitOceanBaseCharDataType(OceanBaseMysqlCharDataType oceanBaseMysqlCharDataType, C context) {
        return visitDataType(oceanBaseMysqlCharDataType, context);
    }

    default R visitListPartitionElement(ListPartitionElement listPartitionElement, C context) {
        return visitPartitionElement(listPartitionElement, context);
    }

    default R visitHashPartitionElement(HashPartitionElement hashPartitionElement, C context) {
        return visitPartitionElement(hashPartitionElement, context);
    }

    default R visitSubPartitionList(SubPartitionList subPartitionList, C context) {
        return visitNode(subPartitionList, context);
    }

    default R visitRangeSubPartitionElement(SubRangePartitionElement rangeSubPartitionElement, C context) {
        return visitSubPartitionElement(rangeSubPartitionElement, context);
    }

    default R visitSubPartitionElement(SubPartitionElement subPartitionElement, C context) {
        return visitNode(subPartitionElement, context);
    }

    default R visitListSubPartitionElement(SubListPartitionElement listSubPartitionElement,
        C context) {
        return visitSubPartitionElement(listSubPartitionElement, context);
    }

    default R visitHashSubPartitionElement(SubHashPartitionElement hashSubPartitionElement,
        C context) {
        return visitSubPartitionElement(hashSubPartitionElement, context);
    }

    default R visitSubRangeTemplatePartition(SubRangeTemplatePartition subRangeTemplatePartition,
        C context) {
        return visitBaseSubPartition(subRangeTemplatePartition, context);
    }

    default R visitSubHashTemplatePartition(SubHashTemplatePartition subHashTemplatePartition,
        C context) {
        return visitBaseSubPartition(subHashTemplatePartition, context);
    }

    default R visitSubListTemplatePartition(SubListTemplatePartition subListTemplatePartition,
        C context) {
        return visitBaseSubPartition(subListTemplatePartition, context);
    }

    default R visitSubKeyTemplatePartition(SubKeyTemplatePartition subKeyTemplatePartition,
        C context) {
        return visitBaseSubPartition(subKeyTemplatePartition, context);
    }
}
