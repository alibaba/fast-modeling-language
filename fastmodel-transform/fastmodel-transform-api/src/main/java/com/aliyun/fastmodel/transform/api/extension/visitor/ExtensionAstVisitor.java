package com.aliyun.fastmodel.transform.api.extension.visitor;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.AggregateKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.CheckExpressionConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.DuplicateKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.ForeignKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.UniqueKeyExprConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.ClusterKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.OrderByConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.RollupConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.RollupItem;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.ExpressionPartitionBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.ListPartitionedBy;
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
     * visit duplicate constraint
     *
     * @param duplicateConstraint
     * @param context
     * @return
     */
    default R visitDuplicateConstraint(DuplicateKeyConstraint duplicateConstraint, C context) {
        return visitNode(duplicateConstraint, context);
    }

    default R visitOrderByConstraint(OrderByConstraint orderByConstraint, C context) {
        return visitNode(orderByConstraint, context);
    }

    default R visitDistributeKeyConstraint(DistributeConstraint distributeKeyConstraint, C context) {
        return visitNode(distributeKeyConstraint, context);
    }

    default R visitRollupItem(RollupItem rollupItem, C context) {
        return visitNode(rollupItem, context);
    }

    default R visitRollupConstraint(RollupConstraint rollupConstraint, C context) {
        return visitNode(rollupConstraint, context);
    }

    default R visitClusterKeyConstraint(ClusterKeyConstraint clusterKeyConstraint, C context) {
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
}
