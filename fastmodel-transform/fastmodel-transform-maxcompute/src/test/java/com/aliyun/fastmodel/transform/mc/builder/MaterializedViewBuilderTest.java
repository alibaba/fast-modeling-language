package com.aliyun.fastmodel.transform.mc.builder;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.context.setting.ViewSetting;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/12/10
 */
public class MaterializedViewBuilderTest {

    MaterializedViewBuilder materializeViewBuilder = new MaterializedViewBuilder();

    @Test
    public void build() {
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType("bigint", null))
                .build()
        );

        BaseStatement source = CreateTable.builder()
            .tableName(QualifiedName.of("t1"))
            .columns(columns)
            .build();
        TransformContext context = new MaxComputeContext(TransformContext.builder().build());
        DialectNode build = materializeViewBuilder.build(source, context);
        assertEquals(build.getNode(), "CREATE MATERIALIZED VIEW t1\n"
            + "(\n"
            + "   c1\n"
            + ")\n"
            + "AS SELECT 0\n"
            + "\n");
        context = new MaxComputeContext(TransformContext.builder().appendSemicolon(true).build());
        build = materializeViewBuilder.build(source, context);
        assertEquals(build.getNode(), "CREATE MATERIALIZED VIEW t1\n"
            + "(\n"
            + "   c1\n"
            + ")\n"
            + "AS SELECT 0\n"
            + "\n"
            + ";");
    }

    @Test
    public void isMatch() {
        DropTable dropTable = new DropTable(QualifiedName.of("abc"), true);
        boolean match = materializeViewBuilder.isMatch(dropTable, TransformContext.builder().transformToView(ViewSetting.builder()
            .transformToMaterializedView(true).build()).build());
        assertTrue(match);
    }

    @Test
    public void isMatchFalse() {
        DropTable dropTable = new DropTable(QualifiedName.of("abc"), true);
        boolean match = materializeViewBuilder.isMatch(dropTable, TransformContext.builder().transformToView(ViewSetting.builder()
            .transformToView(true).build()).build());
        assertFalse(match);
    }
}