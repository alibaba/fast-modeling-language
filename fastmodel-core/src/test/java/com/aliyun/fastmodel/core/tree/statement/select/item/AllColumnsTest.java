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
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * AllColumnsTest
 *
 * @author panguanjing
 * @date 2022/7/23
 */
public class AllColumnsTest {

    @Test
    public void testToString() {
        ImmutableList<Identifier> identifiers = ImmutableList.of(
            new Identifier("a1"),
            new Identifier("b1")
        );
        AllColumns allColumns = new AllColumns(null, new TableOrColumn(QualifiedName.of("a")), identifiers, true);
        assertEquals(allColumns.toString(), "a.* AS (a1, b1)");
    }

    @Test
    public void testToStringWithoutAs() {
        ImmutableList<Identifier> identifiers = ImmutableList.of(
            new Identifier("a1"),
            new Identifier("b1")
        );
        AllColumns allColumns = new AllColumns(null, new TableOrColumn(QualifiedName.of("a")), identifiers, false);
        assertEquals(allColumns.toString(), "a.* (a1, b1)");
    }
}