package com.aliyun.fastmodel.transform.api.extension.client.constraint;

import java.util.List;

import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import lombok.Data;

/**
 * unique key expr constraint
 *
 * @author panguanjing
 * @date 2024/2/19
 */
@Data
public class UniqueKeyExprClientConstraint extends Constraint {

    private List<String> expression;

    public UniqueKeyExprClientConstraint() {
        this.setType(ClientConstraintType.UNIQUE_KEY);
    }
}
