/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hive.builder;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hive.context.HiveTransformContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/8/8
 */
public class DefaultBuilderTest {
    DefaultBuilder defaultBuilder = new DefaultBuilder();

    @Test
    public void build() {
        BaseStatement source = new DropTable(QualifiedName.of("abc"));
        DialectNode build = defaultBuilder.build(source, HiveTransformContext.builder().build());
        assertEquals(build.getNode(), "DROP TABLE abc");
    }
}