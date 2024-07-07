package com.aliyun.fastmodel.transform.api.extension.client.constraint;

import com.aliyun.fastmodel.transform.api.client.dto.constraint.ConstraintType;
import lombok.Getter;

/**
 * StarRocksConstraintType
 *
 * @author panguanjing
 * @date 2023/12/13
 */
@Getter
public enum ExtensionClientConstraintType implements ConstraintType {

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
     * order by
     */
    ORDER_BY("orderBy"),

    /**
     * distribute key
     */
    DISTRIBUTE("distribute");

    private final String code;

    ExtensionClientConstraintType(String code) {this.code = code;}

    @Override
    public String getCode() {
        return code;
    }

    public static ExtensionClientConstraintType getByValue(String value) {
        for (ExtensionClientConstraintType starRocksConstraintType : ExtensionClientConstraintType.values()) {
            if (starRocksConstraintType.code.equalsIgnoreCase(value)) {
                return starRocksConstraintType;
            }
        }
        return null;
    }
}
