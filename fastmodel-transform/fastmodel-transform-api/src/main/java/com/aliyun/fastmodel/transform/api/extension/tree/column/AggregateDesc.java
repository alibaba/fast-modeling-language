package com.aliyun.fastmodel.transform.api.extension.tree.column;

/**
 * column agg desc
 *
 * @author panguanjing
 * @date 2023/9/16
 */
public enum AggregateDesc {
    /**
     * sum
     */
    SUM,
    /**
     * max
     */
    MAX,
    /**
     * min
     */
    MIN,
    /**
     * replace
     */
    REPLACE,
    /**
     * hll union
     */
    HLL_UNION,
    /**
     * bitmap union
     */
    BITMAP_UNION,
    /**
     * percentile union
     */
    PERCENTILE_UNION,
    /**
     * replace if not null
     */
    REPLACE_IF_NOT_NULL;
}
