package com.aliyun.fastmodel.transform.api.extension.client.constraint;

import java.util.List;

import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * unique key expr constraint
 *
 * @author panguanjing
 * @date 2024/2/19
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class UniqueKeyExprClientConstraint extends Constraint {

    /**
     * expressions
     */
    private List<String> expression;

    public UniqueKeyExprClientConstraint() {
        this.setType(ExtensionClientConstraintType.UNIQUE_KEY);
    }
}
