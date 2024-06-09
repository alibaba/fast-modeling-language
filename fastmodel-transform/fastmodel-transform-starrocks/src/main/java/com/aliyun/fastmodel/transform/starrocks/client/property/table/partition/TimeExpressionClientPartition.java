package com.aliyun.fastmodel.transform.starrocks.client.property.table.partition;

import com.aliyun.fastmodel.core.tree.expr.literal.IntervalLiteral;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * expression partition
 *
 * @author 子梁
 * @date 2023/12/26
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TimeExpressionClientPartition {

    /**
     * time function name
     */
    private String funcName;

    /**
     * time unit
     */
    private String timeUnit;

    /**
     * interval
     */
    private IntervalLiteral interval;

}
