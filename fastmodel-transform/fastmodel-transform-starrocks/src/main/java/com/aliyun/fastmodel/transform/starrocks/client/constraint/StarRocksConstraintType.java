package com.aliyun.fastmodel.transform.starrocks.client.constraint;

import com.aliyun.fastmodel.transform.api.client.dto.constraint.ConstraintType;
import lombok.Getter;

/**
 * StarRocksConstraintType
 *
 * @author panguanjing
 * @date 2023/12/13
 */
@Getter
public enum StarRocksConstraintType implements ConstraintType {

    AGGREGATE_KEY("aggregate_key"),

    DUPLICATE_KEY("duplicate_key"),
    /**
     * unique key
     */
    UNIQUE_KEY(com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType.UNIQUE.getCode()),
    /**
     * primary key
     */
    PRIMARY_KEY(com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType.PRIMARY_KEY.getCode()),

    /**
     * index
     */
    INDEX(com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType.INDEX.getCode()),
    /**
     * order by
     */
    ORDER_BY("orderBy"),

    /**
     * distribute key
     */
    DISTRIBUTE("distribute");

    private final String code;

    StarRocksConstraintType(String code) {this.code = code;}

    @Override
    public String getCode() {
        return code;
    }

    public static StarRocksConstraintType getByValue(String value) {
        for (StarRocksConstraintType starRocksConstraintType : StarRocksConstraintType.values()) {
            if (starRocksConstraintType.code.equalsIgnoreCase(value)) {
                return starRocksConstraintType;
            }
        }
        return null;
    }
}
