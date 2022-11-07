/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mc.builder;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.transform.api.context.setting.ViewSetting;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/5/25
 */
public class DropViewBuilderTest {

    @Test
    public void testBuild() {
        ViewBuilder dropViewBuilder = new ViewBuilder();
        DropTable dropTable = new DropTable(QualifiedName.of("abc"));
        DialectNode build = dropViewBuilder.build(dropTable, MaxComputeContext.builder().transformToView(ViewSetting.builder()
            .transformToView(true).build()).build());
        assertEquals(build.getNode(), "DROP VIEW abc");
    }

    @Test
    public void testBuildIfExist() {
        ViewBuilder dropViewBuilder = new ViewBuilder();
        DropTable dropTable = new DropTable(QualifiedName.of("abc"), true);
        DialectNode build = dropViewBuilder.build(dropTable, MaxComputeContext.builder().transformToView(ViewSetting.builder()
            .transformToView(true).build()).build());
        assertEquals(build.getNode(), "DROP VIEW IF EXISTS abc");
    }

    @Test
    public void testIsMatch() {
        ViewBuilder dropViewBuilder = new ViewBuilder();
        DropTable dropTable = new DropTable(QualifiedName.of("abc"));
        MaxComputeContext build1 = MaxComputeContext.builder().transformToView(ViewSetting.builder()
            .transformToView(true).build()).build();
        boolean match = dropViewBuilder.isMatch(dropTable, build1);
        assertTrue(match);
    }
}