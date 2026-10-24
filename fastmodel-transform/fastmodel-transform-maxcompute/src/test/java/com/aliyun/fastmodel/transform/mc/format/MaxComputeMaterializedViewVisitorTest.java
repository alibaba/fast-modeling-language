package com.aliyun.fastmodel.transform.mc.format;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/12/10
 */
public class MaxComputeMaterializedViewVisitorTest {

    MaxComputeMaterializedViewVisitor maxComputeMaterializedViewVisitor;

    @Before
    public void setUp() throws Exception {
        maxComputeMaterializedViewVisitor = new MaxComputeMaterializedViewVisitor(MaxComputeContext.builder().build());
    }

    @Test
    public void visitCreateTable() {
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .comment(new Comment("ccccc"))
                .dataType(DataTypeUtil.simpleType("bigint", null))
                .build(),
            ColumnDefinition.builder()
                .colName(new Identifier("c3"))
                .comment(new Comment("cccccddd"))
                .dataType(DataTypeUtil.simpleType("bigint", null))
                .build()
        );
        List<ColumnDefinition> columns2 = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("c2"))
                .dataType(DataTypeUtil.simpleType("bigint", null))
                .build()
        );
        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .partition(new PartitionedBy(columns2))
            .properties(ImmutableList.of(new Property(MaxComputePropertyKey.LIFE_CYCLE.getValue(), "2")))
            .build();
        Integer indent = 0;
        maxComputeMaterializedViewVisitor.visitCreateTable(createTable, indent);
        String s = maxComputeMaterializedViewVisitor.getBuilder().toString();
        assertEquals(s, "CREATE MATERIALIZED VIEW abc LIFECYCLE 2\n"
            + "(\n"
            + "   c1 COMMENT 'ccccc',\n"
            + "   c3 COMMENT 'cccccddd',\n"
            + "   c2\n"
            + ")\n"
            + "PARTITIONED ON (c2)\n"
            + "AS SELECT\n"
            + "  0\n"
            + ", 0\n"
            + ", 0\n"
            + "\n");
    }

    @Test
    public void visitDropTable() {
        DropTable dropTable = new DropTable(QualifiedName.of("a.b"));
        maxComputeMaterializedViewVisitor.visitDropTable(dropTable, 0);
        String result = maxComputeMaterializedViewVisitor.getBuilder().toString();
        assertEquals(result, "DROP MATERIALIZED VIEW b");
    }

    @Test
    public void visitDropTableIfExist() {
        DropTable dropTable = new DropTable(QualifiedName.of("a.b"), true);
        maxComputeMaterializedViewVisitor.visitDropTable(dropTable, 0);
        String result = maxComputeMaterializedViewVisitor.getBuilder().toString();
        assertEquals(result, "DROP MATERIALIZED VIEW IF EXISTS b");
    }
}