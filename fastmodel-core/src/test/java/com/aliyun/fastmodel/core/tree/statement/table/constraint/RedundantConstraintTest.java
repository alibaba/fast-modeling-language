/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.tree.statement.table.constraint;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * RedundantConstraintTest
 *
 * @author panguanjing
 * @date 2022/9/16
 */
public class RedundantConstraintTest {

    @Test
    public void testRedundant() {

        RedundantConstraint redundantConstraint = new RedundantConstraint(
            new Identifier("c1"),
            new Identifier("abc"),
            QualifiedName.of("p.t.$"),
            null
        );
        QualifiedName joinColumn = redundantConstraint.getJoinColumn();
        assertEquals(joinColumn.toString(), "p.t.$");
    }
}