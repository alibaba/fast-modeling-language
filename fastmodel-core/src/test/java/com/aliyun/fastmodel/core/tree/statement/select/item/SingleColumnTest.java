/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.tree.statement.select.item;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/7/23
 */
public class SingleColumnTest {

    @Test
    public void testToString() {
        SingleColumn singleColumn = new SingleColumn(
            null,
            new TableOrColumn(QualifiedName.of("abc")),
            new Identifier("b"),
            true
        );
        assertEquals(singleColumn.toString(), "abc AS b");
    }
}