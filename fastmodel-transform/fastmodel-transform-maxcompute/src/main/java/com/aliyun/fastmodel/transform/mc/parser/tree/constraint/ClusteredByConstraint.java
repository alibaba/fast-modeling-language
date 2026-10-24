package com.aliyun.fastmodel.transform.mc.parser.tree.constraint;

import java.util.List;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.statement.select.order.SortItem;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.ClusterNonKeyConstraint;
import lombok.Getter;

/**
 * clustered by constraint
 *
 * @author ziliang
 * @date 2024/7/12
 */
@Getter
public class ClusteredByConstraint extends ClusterNonKeyConstraint {

    private final ClusterType clusterType;

    private final List<SortItem> sortColumns;

    private final LongLiteral bucketNum;

    public ClusteredByConstraint(List<Identifier> columns, ClusterType clusterType, List<SortItem> sortColumns,
        LongLiteral bucketNum) {
        super(columns);
        this.sortColumns = sortColumns;
        this.clusterType = clusterType;
        this.bucketNum = bucketNum;
    }

    public enum ClusterType {

        /**
         * 聚簇类型
         */
        HASH,

        /**
         * 范围类型
         */
        RANGE;

    }

}
