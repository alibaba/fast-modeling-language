package com.aliyun.fastmodel.transform.hologres;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/6/26
 */
public class HologresV2TransformerTest {

    HologresV2Transformer hologresV2Transformer = new HologresV2Transformer();

    @Test
    public void transform() {
        List<Property> properties = Lists.newArrayList();
        Property e = new Property("dictionary_encoding_columns", "c1:auto, C2:auto");
        properties.add(e);
        CreateDimTable dimTable = CreateDimTable.builder()
            .tableName(QualifiedName.of("a.b"))
            .properties(properties)
            .build();
        DialectNode transform = hologresV2Transformer.transform(dimTable, HologresTransformContext.builder().build());
        assertEquals("BEGIN;\n"
            + "CREATE TABLE b;\n"
            + "CALL SET_TABLE_PROPERTY('b', 'dictionary_encoding_columns', '\"c1\":auto,\" C2\":auto');\n"
            + "COMMIT;", transform.getNode());
    }

    @Test
    public void testChangeColumnRename() {
        ChangeCol changeCol = new ChangeCol(QualifiedName.of("abc"), new Identifier("bcd"),
            ColumnDefinition.builder()
                .colName(new Identifier("bde"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .build()
        );
        DialectNode transform = hologresV2Transformer.transform(changeCol, HologresTransformContext.builder().build());
        assertEquals(transform.getNode(), "BEGIN;\n"
            + "ALTER TABLE abc RENAME COLUMN bcd TO bde;\n"
            + "COMMIT;");
        assertTrue(transform.isExecutable());
    }
}