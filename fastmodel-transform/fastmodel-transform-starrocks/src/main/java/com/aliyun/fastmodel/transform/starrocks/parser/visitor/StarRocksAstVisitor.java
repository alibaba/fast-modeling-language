package com.aliyun.fastmodel.transform.starrocks.parser.visitor;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.AggregateKeyConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.DuplicateKeyConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.desc.DistributeConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.desc.OrderByConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.desc.RollupConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.desc.RollupItem;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.datatype.StarRocksGenericDataType;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ArrayPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ExpressionPartitionBy;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.LessThanPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ListPartitionValue;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ListPartitionedBy;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.MultiItemListPartition;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.MultiRangePartition;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionDesc;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionKey;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionValue;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.RangePartitionedBy;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.SingleItemListPartition;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.SingleRangePartition;

/**
 * star rocks visitor
 *
 * @author panguanjing
 * @date 2023/9/12
 */
public interface StarRocksAstVisitor<R, C> extends IAstVisitor<R, C> {

    /**
     * visit starRocks GenericDataType
     *
     * @param starRocksGenericDataType genericDataType
     * @param context                  context
     * @return R
     */
    default R visitStarRocksGenericDataType(StarRocksGenericDataType starRocksGenericDataType, C context) {
        return visitGenericDataType(starRocksGenericDataType, context);
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
        return visitPartitionDesc(singleItemListPartition, context);
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
     * visit multiItemListPartition
     *
     * @param multiItemListPartition
     * @param context
     * @return
     */
    default R visitMultiItemListPartition(MultiItemListPartition multiItemListPartition, C context) {
        return visitPartitionDesc(multiItemListPartition, context);
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
     * visit partition value
     *
     * @param partitionValue
     * @param context
     * @return
     */
    default R visitPartitionValue(PartitionValue partitionValue, C context) {
        return visitNode(partitionValue, context);
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
}
