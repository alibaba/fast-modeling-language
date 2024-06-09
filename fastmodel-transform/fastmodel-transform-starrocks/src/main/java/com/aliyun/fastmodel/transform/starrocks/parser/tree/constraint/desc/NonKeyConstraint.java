package com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.desc;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.CustomConstraint;

/**
 * 用于表达非key的constraint
 *
 * @author panguanjing
 * @date 2023/12/15
 */
public abstract class NonKeyConstraint extends CustomConstraint {

    public NonKeyConstraint(Identifier constraintName, Boolean enable, String customType) {
        super(constraintName, enable, customType);
    }
}
