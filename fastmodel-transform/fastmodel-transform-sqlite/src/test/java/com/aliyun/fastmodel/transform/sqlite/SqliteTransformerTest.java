package com.aliyun.fastmodel.transform.sqlite;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.sqlite.context.SqliteContext;
import com.aliyun.fastmodel.transform.sqlite.datatype.Fml2SqliteDataTypeConverter;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/8/14
 */
public class SqliteTransformerTest {

    SqliteTransformer sqliteTransformer = new SqliteTransformer();

    @Test
    public void transform() {
        List<ColumnDefinition> columns = Lists.newArrayList();
        ColumnDefinition c = ColumnDefinition.builder()
            .colName(new Identifier("a"))
            .dataType(DataTypeUtil.simpleType("bigint", Lists.newArrayList()))
            .comment(new Comment("comment"))
            .build();
        ColumnDefinition stringColumn = ColumnDefinition.builder()
            .colName(new Identifier("stringColumn"))
            .dataType(DataTypeUtil.simpleType("string", Lists.newArrayList()))
            .comment(new Comment("comment"))
            .build();

        ColumnDefinition doubleColumn = ColumnDefinition.builder()
            .colName(new Identifier("d_column"))
            .dataType(DataTypeUtil.simpleType("double", Lists.newArrayList()))
            .comment(new Comment("comment"))
            .build();
        columns.add(c);
        columns.add(stringColumn);
        columns.add(doubleColumn);
        SqliteContext sqliteContext = SqliteContext.builder().dataTypeTransformer(new Fml2SqliteDataTypeConverter()).build();
        CreateTable createTable = CreateTable.builder().tableName(QualifiedName.of("ab")).columns(columns)
            .comment(new Comment("c")).build();
        DialectNode transform = sqliteTransformer.transform(createTable, sqliteContext);
        assertEquals("CREATE TABLE ab\n"
            + "(\n"
            + "   a            INTEGER,\n"
            + "   stringColumn TEXT,\n"
            + "   d_column     REAL\n"
            + ")", transform.getNode());
    }

    @Test
    public void testTransformWithDoubleQuote() {
        
    }
}