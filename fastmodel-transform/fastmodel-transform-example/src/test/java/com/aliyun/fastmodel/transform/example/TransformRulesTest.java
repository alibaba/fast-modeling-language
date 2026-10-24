/*
 * Copyright 2021-2023 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.example;

import java.util.Arrays;
import java.util.List;

import com.aliyun.fastmodel.compare.CompareNodeExecute;
import com.aliyun.fastmodel.compare.CompareStrategy;
import com.aliyun.fastmodel.converter.spi.BaseConverterFactory;
import com.aliyun.fastmodel.conveter.dqc.DefaultConvertContext;
import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.TransformerFactory;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.example.util.JsonFileIO;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * 定义下转换规则列表
 *
 * @author panguanjing
 * @date 2021/6/1
 */
public class TransformRulesTest {
    FastModelParser fastModelParser = FastModelParserFactory.getInstance().get();

    Transformer transformer = TransformerFactory.getInstance().get(DialectMeta.getMaxCompute());

    DefaultConvertContext context = new DefaultConvertContext();

    @Before
    public void setUp() throws Exception {
        CreateTable dim_shop = CreateTable.builder().tableName(QualifiedName.of("dim_shop")).
            constraints(
                ImmutableList.of(new PrimaryConstraint(new Identifier("c1"), ImmutableList.of(
                    new Identifier("pk")
                )))
            ).partition(null).columns(
                ImmutableList.of(ColumnDefinition.builder().colName(new Identifier("pk")).build())).build();
        context.setBeforeStatement(dim_shop);
        context.setAfterStatement(dim_shop);
    }

    @Test
    public void testTransform() {
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("id")).notNull(true).dataType(DataTypeUtil.simpleType(
                DataTypeEnums.BIGINT)).build()
        );
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(QualifiedName.of("dim_shop")).columns(
            columns).build();

        List<BaseStatement> convert = BaseConverterFactory.getInstance().convert(Arrays.asList(createDimTable), context);
        for (BaseStatement b : convert) {
            DialectNode transform = transformer.transform(b);
            System.out.println(transform.getNode());
        }
    }

    @Test
    public void testRuleTransform() {
        String fml = "CREATE DIM TABLE IF NOT EXISTS test_bu.dim_shop_test3 \n"
            + "(\n"
            + "   shop_code string NOT NULL COMMENT '门店code',\n"
            + "   shop_name string COMMENT '门店name',\n"
            + "   shop_type string COMMENT '门店类型',\n"
            + "   merchant_code bigint COMMENT '商家code',\n"
            + "   PRIMARY KEY(shop_code)\n"
            + ")\n"
            + " COMMENT '门店'\n"
            + " WITH ('type'='NORMAL','business_process'='test_bp')";
        List<BaseStatement> convert = BaseConverterFactory.getInstance().convert(
            Arrays.asList(fastModelParser.parseStatement(fml)), context);
        for (BaseStatement b : convert) {
            DialectNode transform = transformer.transform(b);
            assertNotNull(transform.getNode());
        }
    }

    @Test
    public void testAlterAddCol() {
        String fml = "ALTER DIM TABLE test_bu.dim_shop_test3 ADD COLUMNS (shop_code string NOT NULL COMMENT '门店code')";
        AddCols addCols = fastModelParser.parseStatement(fml);
        transform(addCols, "AddColNot");
    }

    @Test
    public void testAlterAddColPrimaryKey() {
        String fml
            = "ALTER DIM TABLE test_bu.dim_shop_test3 ADD COLUMNS (shop_code string PRIMARY KEY COMMENT '门店code')";
        AddCols addCols = fastModelParser.parseStatement(fml);
        transform(addCols, "AddColPrimary");
    }

    @Test
    public void testChangeColCodeTable() {
        String before = "create dim table dim_shop(a bigint with ('code_table'='a'));";
        String after = "create dim table dim_shop(a bigint);";
        BaseStatement before1 = fastModelParser.parseStatement(before);
        BaseStatement after1 = fastModelParser.parseStatement(after);
        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare(before1, after1,
            CompareStrategy.INCREMENTAL);
        DefaultConvertContext convertContext = new DefaultConvertContext();
        convertContext.setBeforeStatement(before1);
        convertContext.setAfterStatement(after1);
        List<BaseStatement> convert = BaseConverterFactory.getInstance().convert(compare, convertContext);
        Transformer transformer = TransformerFactory.getInstance().get(DialectMeta.getMaxCompute());
        for (BaseStatement b : convert) {
            DialectNode transform = transformer.transform(b);
            assertEquals(transform.getNode(), "{\n"
                + "\t\"dialectName\":\"MAXCOMPUTE\",\n"
                + "\t\"entityLevel\":\"SQL\",\n"
                + "\t\"matchExpression\":\"NOTAPARTITIONTABLE\",\n"
                + "\t\"ruleModelList\":[\n"
                + "\t\t{\n"
                + "\t\t\t\"action\":\"DELETE\",\n"
                + "\t\t\t\"blockType\":\"BLOCK_WEAK\",\n"
                + "\t\t\t\"checkerType\":\"FIX_STRATEGY_CHECK\",\n"
                + "\t\t\t\"expectValue\":\"0\",\n"
                + "\t\t\t\"oldProperty\":\"a\",\n"
                + "\t\t\t\"operator\":\"=\",\n"
                + "\t\t\t\"predictType\":\"NOT_DYNAMIC\",\n"
                + "\t\t\t\"property\":\"USER_DEFINED\",\n"
                + "\t\t\t\"propertyType\":\"user_defined\",\n"
                + "\t\t\t\"ruleName\":\"字段规则-标准代码-(a)\",\n"
                + "\t\t\t\"ruleType\":\"SLEF_CUSTOM\",\n"
                + "\t\t\t\"ruleValue\":\"SELECT count(*)\\nFROM\\n  dim_shop\\nWHERE NOT (a IN (SELECT code\\nFROM\\n  ``\\n))\\n\"\n"
                + "\t\t}\n"
                + "\t],\n"
                + "\t\"tableName\":\"dim_shop\"\n"
                + "}");
        }
    }

    private void transform(BaseStatement addCols, String expect) {
        Transformer transformer = TransformerFactory.getInstance().get(DialectMeta.getMaxCompute());
        List<BaseStatement> convert = BaseConverterFactory.getInstance().convert(Arrays.asList(addCols), context);
        for (BaseStatement s : convert) {
            if (s == null) {
                continue;
            }
            DialectNode transform = transformer.transform(s);
            System.out.println(transform.getNode());
            // assertEquals(transform.getNode(), JsonFileIO.getExpectValue(expect));
            assertNotNull(transform.getNode());
        }
    }

    private void transformWithoutConvert(BaseStatement baseStatement, String expect) {
        Transformer transformer = TransformerFactory.getInstance().get(DialectMeta.getMaxCompute());
        DialectNode transform = transformer.transform(baseStatement);
        System.out.println(transform.getNode());
        assertEquals(transform.getNode(), JsonFileIO.getExpectValue(expect));
    }

    @Test
    public void testAlterChange() {
        String fml = "ALTER DIM TABLE test_bu.dim_shop CHANGE COLUMN old_col new_col STRING NOT NULL";
        ChangeCol changeCol = fastModelParser.parseStatement(fml);
        transform(changeCol, "ChangeColNotNull");
    }

    @Test
    public void testAlterChangeDisableNotNull() {
        String fml = "ALTER DIM TABLE test_bu.dim_shop CHANGE COLUMN old_col new_col STRING NULL";
        ChangeCol changeCol = fastModelParser.parseStatement(fml);
        transform(changeCol, "ChangeColNull");
    }

    @Test
    public void testAlterChangeAddPrimaryKey() {
        String fml = "ALTER DIM TABLE test_bu.dim_shop CHANGE COLUMN old_col new_col STRING PRIMARY KEY";
        ChangeCol changeCol = fastModelParser.parseStatement(fml);
        transform(changeCol, "ChangeColPrimary");
    }

    @Test
    public void testAlterChangeAddConstraint() {
        String fml = "ALTER DIM TABLE dim_shop ADD CONSTRAINT c1 PRIMARY KEY(col1, col2)";
        AddConstraint addConstraint = fastModelParser.parseStatement(fml);
        transform(addConstraint, "AddPrimaryKeyConstraintMore");
    }

    @Test
    public void testAlterChangeAddConstraintOneCol() {
        String fml = "ALTER DIM TABLE dim_shop ADD CONSTRAINT c1 PRIMARY KEY(col1)";
        AddConstraint addConstraint = fastModelParser.parseStatement(fml);
        transform(addConstraint, "AddPrimaryKeyConstraintOne");
    }

    @Test
    public void testAlterChagneChangeColumn() {
        String fml = "ALTER TABLE dim_shop CHANGE COLUMN c1 c2 STRING";
        ChangeCol changeCol = fastModelParser.parseStatement(fml);
        transform(changeCol, "ChangeColName");
    }

    @Test
    public void testDropCol() {
        String fml = "ALTER DIM TABLE test_bu.dim_shop DROP COLUMN old_col";
        DropCol dropCol = fastModelParser.parseStatement(fml);
        transform(dropCol, "DropCol");
    }

    @Test
    public void testTransformAndCompare() {
        String before = "CREATE DIM TABLE IF NOT EXISTS test_bu.dim_shop_test3\n"
            + "(\n"
            + "  shop_code string not null COMMENT '门店code',\n"
            + "  shop_name string COMMENT '门店name',\n"
            + "  shop_type string COMMENT '门店类型',\n"
            + "  merchant_code bigint COMMENT '商家code',\n"
            + "  PRIMARY KEY (shop_code)\n"
            + ") COMMENT '门店' WITH('type' = 'NORMAL', 'business_process'='test_bp')";

        String after = "CREATE DIM TABLE IF NOT EXISTS test_bu.dim_shop_test3\n"
            + "(\n"
            + "  shop_code string COMMENT '门店code',\n"
            + "  shop_name string COMMENT '门店name',\n"
            + "  shop_type string COMMENT '门店类型',\n"
            + "  merchant_code bigint COMMENT '商家code',\n"
            + "  test_code string not null COMMENT '测试代码1',\n"
            + "  PRIMARY KEY (shop_code)\n"
            + ") COMMENT '门店' WITH('type' = 'NORMAL', 'business_process'='test_bp')";

        List<BaseStatement> list = CompareNodeExecute.getInstance().compare(fastModelParser.parseStatement(before),
            fastModelParser.parseStatement(after)
            , CompareStrategy.INCREMENTAL);
        assertEquals(
            "ALTER TABLE test_bu.dim_shop_test3 ADD COLUMNS\n"
                + "(\n"
                + "   test_code STRING NOT NULL COMMENT '测试代码1'\n"
                + ")",
            list.get(0).toString());
    }

    @Test
    public void testTransformAndCompareDuplicate() {
        String before = "CREATE DIM TABLE IF NOT EXISTS test_bu.dim_shop_test3\n"
            + "(\n"
            + "  shop_code string COMMENT '门店code',\n"
            + "  shop_name string COMMENT '门店name',\n"
            + "  shop_type string COMMENT '门店类型',\n"
            + "  merchant_code bigint COMMENT '商家code'\n"
            + ") COMMENT '门店' WITH('type' = 'NORMAL', 'business_process'='test_bp')";

        String after = "CREATE DIM TABLE IF NOT EXISTS test_bu.dim_shop_test3\n"
            + "(\n"
            + "  shop_code string NOT NULL COMMENT '门店code',\n"
            + "  shop_name string COMMENT '门店name',\n"
            + "  shop_type string COMMENT '门店类型',\n"
            + "  merchant_code bigint COMMENT '商家code',\n"
            + "  PRIMARY KEY (shop_code)\n"
            + ") COMMENT '门店' WITH('type' = 'NORMAL', 'business_process'='test_bp')";

        List<BaseStatement> list = CompareNodeExecute.getInstance().compare(fastModelParser.parseStatement(before),
            fastModelParser.parseStatement(after)
            , CompareStrategy.INCREMENTAL);
        transform(list.get(1), "addPrimaryKey");
    }

    @Test
    public void testAddNotNull() {
        String before = " CREATE DIM TABLE IF NOT EXISTS test_bu.dim_shop_test25 \n"
            + "(\n"
            + "   shop_code string COMMENT '门店code',\n"
            + "   shop_name string COMMENT '门店name',\n"
            + "   shop_type string COMMENT '门店类型',\n"
            + "   merchant_code bigint COMMENT '商家code',\n"
            + "   test_code string NOT NULL COMMENT '测试代码1',\n"
            + "   PRIMARY KEY(shop_code)\n"
            + ")\n"
            + " COMMENT '门店'\n"
            + " WITH ('type'='NORMAL','business_process'='test_bp')";

        String after = "CREATE DIM TABLE IF NOT EXISTS test_bu.dim_shop_test25 \n"
            + "(\n"
            + "   shop_code string COMMENT '门店code',\n"
            + "   shop_name string COMMENT '门店name',\n"
            + "   shop_type string COMMENT '门店类型',\n"
            + "   merchant_code bigint COMMENT '商家code',\n"
            + "   test_code string COMMENT '测试代码1',\n"
            + "   test_code13 string NOT NULL COMMENT '测试代码1',\n"
            + "   test_code14 string COMMENT '测试代码1',\n"
            + "   PRIMARY KEY(shop_code)\n"
            + ")\n"
            + " COMMENT '门店'\n"
            + " WITH ('type'='NORMAL','business_process'='test_bp')";

        List<BaseStatement> list = CompareNodeExecute.getInstance().compare(fastModelParser.parseStatement(before),
            fastModelParser.parseStatement(after)
            , CompareStrategy.INCREMENTAL);

        transform(list.get(0), "deleteNotNull");
        transform(list.get(1), "addNotNull");
    }

    @Test
    public void testConverter() {
        String fml = "CREATE DIM TABLE IF NOT EXISTS test_bu.dim_shop_test25 \n"
            + "(\n"
            + "   shop_code STRING ATTRIBUTE COMMENT '门店code',\n"
            + "   shop_name STRING ATTRIBUTE COMMENT '门店name',\n"
            + "   shop_type STRING ATTRIBUTE COMMENT '门店类型',\n"
            + "   merchant_code BIGINT ATTRIBUTE COMMENT '商家code',\n"
            + "   test_code STRING ATTRIBUTE NOT NULL COMMENT '测试代码1',\n"
            + "   PRIMARY KEY(shop_code)\n"
            + ")\n"
            + " COMMENT '门店'\n"
            + " WITH ('type'='NORMAL','business_process'='test_bp')\n";

        transform(fastModelParser.parseStatement(fml), "createTable");
    }

    @Test
    public void testConverterProcess() {
        String fml = "CREATE DIM TABLE IF NOT EXISTS test_bu.dim_shop_test25 \n"
            + "(\n"
            + "   shop_code STRING ATTRIBUTE COMMENT '门店code' WITH ('code_table'='code_test'),\n"
            + "   shop_name STRING ATTRIBUTE COMMENT '门店name',\n"
            + "   shop_type STRING ATTRIBUTE COMMENT '门店类型',\n"
            + "   merchant_code BIGINT ATTRIBUTE COMMENT '商家code'\n"
            + ")\n"
            + " COMMENT '门店'\n"
            + " WITH ('type'='NORMAL','business_process'='test_bp')\n";

        transform(fastModelParser.parseStatement(fml), "codeTable");
    }

    @Test
    public void testTransformRule() {
        String fml = "CREATE DQC_RULE sys_rules_dim_shop_test47 ON TABLE test_bu.dim_shop_test47\n"
            + "(\n"
            + "   CONSTRAINT `字段规则-非空-(shop_code)` CHECK(NULL_COUNT(shop_code) = 0) NOT ENFORCED,\n"
            + "   CONSTRAINT `字段规则-唯一-(shop_code)` CHECK(DUPLICATE_COUNT(shop_code) = 0) NOT ENFORCED\n"
            + ")";
        transformWithoutConvert(fastModelParser.parseStatement(fml), "createDqcRule");
    }

    @Test
    public void testTransformAndCoverter() {
        String before = "CREATE DIM TABLE IF NOT EXISTS test_bu.dim_shop_test3\n"
            + "(\n"
            + "  shop_code string COMMENT '门店code' with ('code_table'='bcd'),\n"
            + "  shop_name string COMMENT '门店name',\n"
            + "  shop_type string COMMENT '门店类型',\n"
            + "  merchant_code bigint COMMENT '商家code',\n"
            + "  PRIMARY KEY (shop_code)\n"
            + ") COMMENT '门店' WITH('type' = 'NORMAL', 'business_process'='test_bp')";

        String after = "CREATE DIM TABLE IF NOT EXISTS test_bu.dim_shop_test3\n"
            + "(\n"
            + "  shop_code string COMMENT '门店code' WITH('code_table'='abc'),\n"
            + "  shop_name string COMMENT '门店name',\n"
            + "  shop_type string COMMENT '门店类型',\n"
            + "  merchant_code bigint COMMENT '商家code',\n"
            + "  PRIMARY KEY (shop_code)\n"
            + ") COMMENT '门店' WITH('type' = 'NORMAL', 'business_process'='test_bp')";

        BaseStatement before1 = fastModelParser.parseStatement(before);
        BaseStatement after1 = fastModelParser.parseStatement(after);
        List<BaseStatement> list = CompareNodeExecute.getInstance().compare(before1,
            after1
            , CompareStrategy.INCREMENTAL);
        DefaultConvertContext context = new DefaultConvertContext();
        context.setBeforeStatement(before1);
        context.setAfterStatement(after1);
        List<BaseStatement> convert = BaseConverterFactory.getInstance().convert(list, context);
        for (BaseStatement b : convert) {
            DialectNode transform = transformer.transform(b);
            assertEquals(transform.getNode(), "{\n"
                + "\t\"dialectName\":\"MAXCOMPUTE\",\n"
                + "\t\"entityLevel\":\"SQL\",\n"
                + "\t\"matchExpression\":\"NOTAPARTITIONTABLE\",\n"
                + "\t\"ruleModelList\":[\n"
                + "\t\t{\n"
                + "\t\t\t\"action\":\"DELETE\",\n"
                + "\t\t\t\"blockType\":\"BLOCK_WEAK\",\n"
                + "\t\t\t\"checkerType\":\"FIX_STRATEGY_CHECK\",\n"
                + "\t\t\t\"expectValue\":\"0\",\n"
                + "\t\t\t\"oldProperty\":\"shop_code\",\n"
                + "\t\t\t\"operator\":\"=\",\n"
                + "\t\t\t\"predictType\":\"NOT_DYNAMIC\",\n"
                + "\t\t\t\"property\":\"USER_DEFINED\",\n"
                + "\t\t\t\"propertyType\":\"user_defined\",\n"
                + "\t\t\t\"ruleName\":\"字段规则-标准代码-(shop_code)\",\n"
                + "\t\t\t\"ruleType\":\"SLEF_CUSTOM\",\n"
                + "\t\t\t\"ruleValue\":\"SELECT count(*)\\nFROM\\n  dim_shop_test3\\nWHERE NOT (shop_code IN (SELECT code\\nFROM\\n  bcd\\n))\\n\"\n"
                + "\t\t},\n"
                + "\t\t{\n"
                + "\t\t\t\"action\":\"ADD\",\n"
                + "\t\t\t\"blockType\":\"BLOCK_WEAK\",\n"
                + "\t\t\t\"checkerType\":\"FIX_STRATEGY_CHECK\",\n"
                + "\t\t\t\"expectValue\":\"0\",\n"
                + "\t\t\t\"oldProperty\":\"shop_code\",\n"
                + "\t\t\t\"operator\":\"=\",\n"
                + "\t\t\t\"predictType\":\"NOT_DYNAMIC\",\n"
                + "\t\t\t\"property\":\"USER_DEFINED\",\n"
                + "\t\t\t\"propertyType\":\"user_defined\",\n"
                + "\t\t\t\"ruleName\":\"字段规则-标准代码-(shop_code)\",\n"
                + "\t\t\t\"ruleType\":\"SLEF_CUSTOM\",\n"
                + "\t\t\t\"ruleValue\":\"SELECT count(*)\\nFROM\\n  dim_shop_test3\\nWHERE NOT (shop_code IN (SELECT code\\nFROM\\n  abc\\n))\\n\"\n"
                + "\t\t}\n"
                + "\t],\n"
                + "\t\"tableName\":\"dim_shop_test3\"\n"
                + "}");
        }
    }
}
