package com.aliyun.fastmodel.transform.api.extension.visitor;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.AggregateKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.CheckExpressionConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.ClusteredKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.DuplicateKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.ForeignKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.UniqueKeyExprConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.WaterMarkConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.ClusterNonKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeNonKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.OrderByNonKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.RollupItem;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.RollupNonKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.ExpressionPartitionBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.ListPartitionedBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.LogicalPartitionedBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.RangePartitionedBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.MultiItemListPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.MultiRangePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.PartitionDesc;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.SingleItemListPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.SingleRangePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.ArrayPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.LessThanPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.ListPartitionValue;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.PartitionKey;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.PartitionValue;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.BaseSubPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.SubHashPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.SubKeyPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.SubListPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.SubRangePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.BasePartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.BaseSubPartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.HashPartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.ListPartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.RangePartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.SubHashPartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.SubListPartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.SubPartitionList;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.SubRangePartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.template.SubHashTemplatePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.template.SubKeyTemplatePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.template.SubListTemplatePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.template.SubRangeTemplatePartition;

/**
 * 扩展的visitor
 *
 * @author panguanjing
 * @date 2024/1/21
 */
public interface ExtensionAstVisitor<R, C> extends IAstVisitor<R, C> {
    /**
     * visit array partition key
     *
     * @param arrayPartitionKey arrayPartitionKey
     * @param context           context
     * @return
     */
    default R visitArrayPartitionKey(ArrayPartitionKey arrayPartitionKey, C context) {
        return visitPartitionKey(arrayPartitionKey, context);
    }

    /**
     * visit partition key
     *
     * @param partitionKey
     * @param context
     * @return
     */
    default R visitPartitionKey(PartitionKey partitionKey, C context) {
        return visitNode(partitionKey, context);
    }

    /**
     * visitExpressionPartitionBy
     *
     * @param expressionPartitionedBy
     * @param context
     * @return
     */
    default R visitExpressionPartitionedBy(ExpressionPartitionBy expressionPartitionedBy, C context) {
        return visitNode(expressionPartitionedBy, context);
    }

    /**
     * single range partition
     *
     * @param singleRangePartition
     * @param context
     * @return
     */
    default R visitSingleRangePartition(SingleRangePartition singleRangePartition, C context) {
        return visitNode(singleRangePartition, context);
    }

    /**
     * multi range partition
     *
     * @param multiRangePartition
     * @param context
     * @return
     */
    default R visitMultiRangePartition(MultiRangePartition multiRangePartition, C context) {
        return visitNode(multiRangePartition, context);
    }

    /**
     * visit less than partition key
     *
     * @param lessThanPartitionKey
     * @param context
     * @return
     */
    default R visitLessThanPartitionKey(LessThanPartitionKey lessThanPartitionKey, C context) {
        return visitPartitionKey(lessThanPartitionKey, context);
    }

    /**
     * visit single item partition
     *
     * @param singleItemListPartition
     * @param context
     * @return
     */
    default R visitSingleItemListPartition(SingleItemListPartition singleItemListPartition, C context) {
        return visitNode(singleItemListPartition, context);
    }

    /**
     * visitListPartitionedBy
     *
     * @param listPartitionedBy
     * @param context
     * @return
     */
    default R visitListPartitionedBy(ListPartitionedBy listPartitionedBy, C context) {
        return visitNode(listPartitionedBy, context);
    }

    /**
     * visit list partition value
     *
     * @param listPartitionValue
     * @param context
     * @return
     */
    default R visitListPartitionValue(ListPartitionValue listPartitionValue, C context) {
        return visitNode(listPartitionValue, context);
    }

    /**
     * visit multiItemListPartition
     *
     * @param multiItemListPartition
     * @param context
     * @return
     */
    default R visitMultiItemListPartition(MultiItemListPartition multiItemListPartition, C context) {
        return visitNode(multiItemListPartition, context);
    }

    /**
     * visit partition desc
     *
     * @param partitionDesc
     * @param context
     * @return
     */
    default R visitPartitionDesc(PartitionDesc partitionDesc, C context) {
        return visitNode(partitionDesc, context);
    }

    /**
     * visit partition value
     *
     * @param partitionValue
     * @param context
     * @return
     */
    default R visitPartitionValue(PartitionValue partitionValue, C context) {
        return visitNode(partitionValue, context);
    }

    /**
     * visit starRocks partitioned by
     *
     * @param starRocksPartitionedBy
     * @param context
     * @return
     */
    default R visitRangePartitionedBy(RangePartitionedBy starRocksPartitionedBy, C context) {
        return visitNode(starRocksPartitionedBy, context);
    }

    /**
     * visitAggregateConstraint
     *
     * @param aggregateConstraint
     * @param context
     * @return
     */
    default R visitAggregateConstraint(AggregateKeyConstraint aggregateConstraint, C context) {
        return visitNode(aggregateConstraint, context);
    }

    /**
     * visitAggregateConstraint
     *
     * @param waterMarkConstraint
     * @param context
     * @return
     */
    default R visitWaterMarkConstraint(WaterMarkConstraint waterMarkConstraint, C context) {
        return visitNode(waterMarkConstraint, context);
    }

    /**
     * visit duplicate constraint
     *
     * @param duplicateConstraint
     * @param context
     * @return
     */
    default R visitDuplicateConstraint(DuplicateKeyConstraint duplicateConstraint, C context) {
        return visitNode(duplicateConstraint, context);
    }

    default R visitOrderByConstraint(OrderByNonKeyConstraint orderByConstraint, C context) {
        return visitNode(orderByConstraint, context);
    }

    default R visitDistributeKeyConstraint(DistributeNonKeyConstraint distributeKeyConstraint, C context) {
        return visitNode(distributeKeyConstraint, context);
    }

    default R visitRollupItem(RollupItem rollupItem, C context) {
        return visitNode(rollupItem, context);
    }

    default R visitRollupConstraint(RollupNonKeyConstraint rollupConstraint, C context) {
        return visitNode(rollupConstraint, context);
    }

    /**
     * visit cluster key constraint
     *
     * @param clusterKeyConstraint
     * @param context
     * @return
     */
    default R visitClusterKeyConstraint(ClusterNonKeyConstraint clusterKeyConstraint, C context) {
        return visitNode(clusterKeyConstraint, context);
    }

    default R visitCheckExpressionConstraint(CheckExpressionConstraint checkExpressionConstraint, C context) {
        return visitNode(checkExpressionConstraint, context);
    }

    default R visitForeignKeyConstraint(ForeignKeyConstraint foreignKeyConstraint, C context) {
        return visitNode(foreignKeyConstraint, context);
    }

    default R visitUniqueKeyExprConstraint(UniqueKeyExprConstraint uniqueKeyExprConstraint, C context) {
        return visitNode(uniqueKeyExprConstraint, context);
    }

    default R visitClusteredKeyConstraint(ClusteredKeyConstraint clusteredKeyConstraint, C context) {
        return visitNode(clusteredKeyConstraint, context);
    }

    /**
     * sub range partition
     *
     * @param subRangePartition
     * @param context
     * @return
     */
    default R visitSubRangePartition(SubRangePartition subRangePartition, C context) {
        return visitNode(subRangePartition, context);
    }

    /**
     * sub list partition
     *
     * @param subListPartition
     * @param context
     * @return
     */
    default R visitSubListPartition(SubListPartition subListPartition, C context) {
        return visitNode(subListPartition, context);
    }

    /**
     * sub key partition
     *
     * @param subKeyPartition
     * @param context
     * @return
     */
    default R visitSubKeyPartition(SubKeyPartition subKeyPartition, C context) {
        return visitNode(subKeyPartition, context);
    }

    /**
     * base sub partition
     *
     * @param baseSubPartition
     * @param context
     * @return
     */
    default R visitBaseSubPartition(BaseSubPartition baseSubPartition, C context) {
        return visitNode(baseSubPartition, context);
    }

    /**
     * visit sub Hash Partition
     *
     * @param subHashPartition
     * @param context
     * @return
     */
    default R visitSubHashPartition(SubHashPartition subHashPartition, C context) {
        return visitNode(subHashPartition, context);
    }

    /**
     * sub range partition element
     *
     * @param subRangePartitionElement
     * @param context
     * @return
     */
    default R visitRangeSubPartitionElement(SubRangePartitionElement subRangePartitionElement, C context) {
        return visitNode(subRangePartitionElement, context);
    }

    /**
     * visit sub hash template partition
     *
     * @param subHashTemplatePartition
     * @param context
     * @return
     */
    default R visitSubHashTemplatePartition(SubHashTemplatePartition subHashTemplatePartition, C context) {
        return visitNode(subHashTemplatePartition, context);
    }

    /**
     * sub range template partition
     *
     * @param subRangeTemplatePartition
     * @param context
     * @return
     */
    default R visitSubRangeTemplatePartition(SubRangeTemplatePartition subRangeTemplatePartition, C context) {
        return visitNode(subRangeTemplatePartition, context);
    }

    /**
     * sub list template partition
     *
     * @param subListTemplatePartition
     * @param context
     * @return
     */
    default R visitSubListTemplatePartition(SubListTemplatePartition subListTemplatePartition, C context) {
        return visitNode(subListTemplatePartition, context);
    }

    /**
     * subKeyTemplatePartition
     *
     * @param subKeyTemplatePartition
     * @param context
     * @return
     */
    default R visitSubKeyTemplatePartition(SubKeyTemplatePartition subKeyTemplatePartition, C context) {
        return visitNode(subKeyTemplatePartition, context);
    }

    /**
     * visit sub partition list
     *
     * @param subPartitionList
     * @param context
     * @return
     */
    default R visitSubPartitionList(SubPartitionList subPartitionList, C context) {
        return visitNode(subPartitionList, context);
    }

    /**
     * subHashPartitionElement
     *
     * @param subHashPartitionElement
     * @param context
     * @return
     */
    default R visitHashSubPartitionElement(SubHashPartitionElement subHashPartitionElement, C context) {
        return visitNode(subHashPartitionElement, context);
    }

    /**
     * rangePartitionElement
     *
     * @param rangePartitionElement
     * @param context
     * @return
     */
    default R visitRangePartitionElement(RangePartitionElement rangePartitionElement, C context) {
        return visitNode(rangePartitionElement, context);
    }

    /**
     * subListPartitionElement
     *
     * @param subListPartitionElement
     * @param context
     * @return
     */
    default R visitListSubPartitionElement(SubListPartitionElement subListPartitionElement, C context) {
        return visitNode(subListPartitionElement, context);
    }

    /**
     * partitionElement
     *
     * @param partitionElement
     * @param context
     * @return
     */
    default R visitPartitionElement(BasePartitionElement partitionElement, C context) {
        return visitNode(partitionElement, context);
    }

    /**
     * listPartitionElement
     *
     * @param listPartitionElement
     * @param context
     * @return
     */
    default R visitListPartitionElement(ListPartitionElement listPartitionElement, C context) {
        return visitNode(listPartitionElement, context);
    }

    /**
     * subPartitionElement
     *
     * @param subPartitionElement
     * @param context
     * @return
     */
    default R visitSubPartitionElement(BaseSubPartitionElement subPartitionElement, C context) {
        return visitNode(subPartitionElement, context);
    }

    /**
     * hashPartitionElement
     *
     * @param hashPartitionElement
     * @param context
     * @return
     */
    default R visitHashPartitionElement(HashPartitionElement hashPartitionElement, C context) {
        return visitNode(hashPartitionElement, context);
    }

    /**
     * visit logicalPartitionedBy
     * @param logicalPartitionedBy
     * @param context
     * @return
     */
    default R visitLogicPartitionedBy(LogicalPartitionedBy logicalPartitionedBy, C context) {
        return visitNode(logicalPartitionedBy, context);
    }
}
