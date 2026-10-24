/*
 * Copyright 2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
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