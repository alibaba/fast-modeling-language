package com.aliyun.fastmodel.transform.starrocks.client.constraint;

import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * DistributeClientConstraint
 *
 * @author panguanjing
 * @date 2023/12/15
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class StarRocksDistributeConstraint extends Constraint {
    /**
     * 是否random
     */
    private Boolean random;

    /**
     * bucket
     */
    private Integer bucket;

    public StarRocksDistributeConstraint() {
        this.setType(StarRocksConstraintType.DISTRIBUTE);
    }
}
