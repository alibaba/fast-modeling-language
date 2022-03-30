/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
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

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/6/14
 */
public class TransformCompareColumnTest extends BaseTransformCaseTest {

    @Test
    public void testCompare() {

        String before = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION,\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION,\n"
            + "   CONSTRAINT PK PRIMARY KEY(ceshi4)\n"
            + ")";

        String after = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION,\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION NOT NULL,\n"
            + "   CONSTRAINT PK PRIMARY KEY(ceshi4)\n"
            + ")";

        List<BaseStatement> compare = starter.compare(before, after);
        assertEquals(compare.get(0).toString(), "ALTER TABLE dqctest3 CHANGE COLUMN ceshi5 ceshi5 STRING NOT NULL");
    }

    /**
     * 如果before是notNull
     */
    @Test
    public void testCompareBeforeNotNullAfterNull() {
        String before = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION,\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION NOT NULL,\n"
            + "   CONSTRAINT PK PRIMARY KEY(ceshi4)\n"
            + ")";

        String after = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION,\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION,\n"
            + "   CONSTRAINT PK PRIMARY KEY(ceshi4)\n"
            + ")";
        List<BaseStatement> compare = starter.compare(before, after);
        assertEquals(compare.get(0).toString(), "ALTER TABLE dqctest3 CHANGE COLUMN ceshi5 ceshi5 STRING NULL");

    }

    /**
     * 如果before是notNull
     */
    @Test
    public void testCompareBeforePrimaryAfterNull() {
        String before = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING CORRELATION PRIMARY KEY,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION,\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION"
            + ")";

        String after = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING CORRELATION,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION,\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION"
            + ")";
        List<BaseStatement> compare = starter.compare(before, after);
        assertEquals(2, compare.size());
    }

    /**
     *
     */
    @Test
    public void testCompareBeforeNullAfterCoreelation() {
        String before = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING PRIMARY KEY,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION,\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION"
            + ")";

        String after = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING CORRELATION PRIMARY KEY,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION,\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION"
            + ")";
        List<BaseStatement> compare = starter.compare(before, after);
        assertEquals(compare.get(0).toString(), "ALTER TABLE dqctest3 CHANGE COLUMN ceshi4 ceshi4 STRING CORRELATION");
    }

    /**
     *
     */
    @Test
    public void testCompareBeforeCoreelationAfterAttribute() {
        String before = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING CORRELATION PRIMARY KEY,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION,\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION"
            + ")";

        String after = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING PRIMARY KEY,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION,\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION"
            + ")";
        List<BaseStatement> compare = starter.compare(before, after);
        assertEquals(compare.get(0).toString(), "ALTER TABLE dqctest3 CHANGE COLUMN ceshi4 ceshi4 STRING");
    }

    /**
     *
     */
    @Test
    public void testCompareBeforeAttributeAfterAttribute() {
        String before = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING PRIMARY KEY,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION,\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION"
            + ")";

        String after = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING PRIMARY KEY,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION WITH ('dict'='dict_code'),\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION"
            + ")";
        List<BaseStatement> compare = starter.compare(before, after);
        assertEquals(compare.get(0).toString(),
            "ALTER TABLE dqctest3 CHANGE COLUMN ceshi2 ceshi2 STRING WITH ('dict'='dict_code')");
    }

    @Test
    public void testCompareBeforeAttributeAfterNoAttribute() {
        String before = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING PRIMARY KEY,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION WITH('dict'='dict_code'),\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION"
            + ")";

        String after = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING PRIMARY KEY,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION, \n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION"
            + ")";
        List<BaseStatement> compare = starter.compare(before, after);
        assertEquals(compare.get(0).toString(),
            "ALTER TABLE dqctest3 CHANGE COLUMN ceshi2 ceshi2 STRING WITH ('dict'='')");
    }

    @Test
    public void beforeCommentAfterWithoutComment() {
        String before = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING PRIMARY KEY,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION COMMENT 'comment',\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION"
            + ")";

        String after = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING PRIMARY KEY,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION, \n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION"
            + ")";
        List<BaseStatement> compare = starter.compare(before, after);
        assertEquals(compare.get(0).toString(),
            "ALTER TABLE dqctest3 CHANGE COLUMN ceshi2 ceshi2 STRING COMMENT ''");

    }

    @Test
    public void beforeNoCommentAfterComment() {
        String before = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING PRIMARY KEY,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION,\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION"
            + ")";

        String after = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING PRIMARY KEY,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION COMMENT 'comment', \n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION"
            + ")";
        List<BaseStatement> compare = starter.compare(before, after);
        assertEquals(compare.get(0).toString(),
            "ALTER TABLE dqctest3 CHANGE COLUMN ceshi2 ceshi2 STRING COMMENT 'comment'");

    }

    @Test
    public void beforeNoPartitionAfterPartition() {
        String before = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING PRIMARY KEY,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION,\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION"
            + ")";

        String after = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING PRIMARY KEY,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION, \n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL\n"
            + ") PARTITIONED BY ("
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION WITH ('dict'='dict_code')"
            + ")";
        List<BaseStatement> compare = starter.compare(before, after);
        assertEquals(compare.get(0).toString(),
            "ALTER TABLE dqctest3 DROP COLUMN ceshi5");
        assertEquals(compare.get(1).toString(),
            "ALTER TABLE dqctest3 ADD PARTITION COLUMN ceshi5 ALIAS '测试5' STRING CORRELATION WITH "
                + "('dict'='dict_code')");

    }

    /**
     * 这个地方有个问题，先添加列，再删除分区列，会导致添加失败
     */
    @Test
    public void beforePartitionAfterNoPartition() {
        String before = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING PRIMARY KEY,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION, \n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL\n"
            + ") PARTITIONED BY ("
            + "     test_010102 BIGINT ATTRIBUTE NULL COMMENT '测试2'\n"
            + ")";

        String after = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi4 ALIAS '测试' STRING PRIMARY KEY,\n"
            + "   ceshi2 ALIAS 'ceshi2' STRING CORRELATION,\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   test_010102 BIGINT ATTRIBUTE NULL COMMENT '测试2'"
            + ")";
        List<BaseStatement> compare = starter.compare(before, after);
        assertEquals(compare.get(0).toString(),
            "ALTER TABLE dqctest3 ADD COLUMNS\n"
                + "(\n"
                + "   test_010102 BIGINT COMMENT '测试2'\n"
                + ")");
        assertEquals(compare.get(1).toString(),
            "ALTER TABLE dqctest3 DROP PARTITION COLUMN test_010102");

    }

    @Test
    public void testCompareBeforePrimaryKey1AfterPrimaryKey2() {
        String before = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION, \n"
            + "   PRIMARY KEY(ceshi3)"
            + ")";

        String after = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION,\n"
            + "   PRIMARY KEY(ceshi3, ceshi5)"
            + ")";
        List<BaseStatement> compare = starter.compare(before, after);
        assertEquals("ALTER TABLE dqctest3 CHANGE COLUMN ceshi5 ceshi5 STRING NOT NULL", compare.get(0).toString());
        assertTrue(compare.get(1).toString().contains("ALTER TABLE dqctest3 DROP CONSTRAINT"));
        assertEquals(compare.get(2).toString(), "ALTER TABLE dqctest3 ADD PRIMARY KEY(ceshi3,ceshi5)");
    }

    @Test
    public void testCompareBeforePrimaryKey2AfterPrimaryKey1() {
        String before = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION, \n"
            + "   PRIMARY KEY(ceshi3, ceshi5)"
            + ")";

        String after = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION,\n"
            + "   PRIMARY KEY(ceshi3)"
            + ")";
        List<BaseStatement> compare = starter.compare(before, after);
        assertEquals(compare.get(0).toString(), "ALTER TABLE dqctest3 CHANGE COLUMN ceshi5 ceshi5 STRING NULL");
        assertTrue(compare.get(1).toString().contains("ALTER TABLE dqctest3 DROP CONSTRAINT"));
        assertEquals(compare.get(2).toString(), "ALTER TABLE dqctest3 ADD PRIMARY KEY(ceshi3)");
    }

    @Test
    public void testCompareBeforePrimaryKey2AfterPrimaryKey1WithConstraintName() {
        String before = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION, \n"
            + "   CONSTRAINT c1 PRIMARY KEY(ceshi3, ceshi5)"
            + ")";

        String after = "CREATE OR REPLACE FACT TABLE IF NOT EXISTS dqctest3 ALIAS '测试' \n"
            + "(\n"
            + "   ceshi3 ALIAS '测试' STRING CORRELATION NOT NULL,\n"
            + "   ceshi5 ALIAS '测试5' STRING CORRELATION,\n"
            + "   CONSTRAINT c2 PRIMARY KEY(ceshi3)"
            + ")";
        List<BaseStatement> compare = starter.compare(before, after);
        assertEquals("ALTER TABLE dqctest3 CHANGE COLUMN ceshi5 ceshi5 STRING NULL", compare.get(0).toString());
        assertEquals(compare.get(1).toString(), "ALTER TABLE dqctest3 DROP CONSTRAINT c1");
        assertEquals(compare.get(2).toString(), "ALTER TABLE dqctest3 ADD CONSTRAINT c2 PRIMARY KEY(ceshi3)");
    }

    @Test
    public void testModifyPartitionColumn() {

        String before = "CREATE OR REPLACE DIM TABLE IF NOT EXISTS rengang_ceshi_2 ALIAS '仞刚测试层级维度' \n"
            + "(\n"
            + "   tsss_te ALIAS 'tsss_te' BIGINT CORRELATION WITH ('dict'='tsss_te'),\n"
            + "   tet_tee ALIAS 'tet_tee' BIGINT CORRELATION WITH ('dict'='tet_tee'),\n"
            + "   test_2 ALIAS 'test_2' BIGINT CORRELATION WITH ('dict'='test_2'),\n"
            + "   standard_001 ALIAS 'standard_001' BIGINT CORRELATION WITH ('dict'='standard_001'),\n"
            + "   eileen_test_bianma ALIAS 'eileen_test_bianma' BIGINT CORRELATION WITH ('dict'='eileen_test_bianma')\n"
            + ")\n"
            + " COMMENT '仞刚测试层级维度'\n"
            + " PARTITIONED BY (\n"
            + "   caiyou ALIAS '采油桶1' STRING CORRELATION NOT NULL WITH ('dict'='caiyou')\n"
            + ")\n"
            + " WITH ('life_cycle'='10')";

        String after = "CREATE OR REPLACE DIM TABLE IF NOT EXISTS rengang_ceshi_2 ALIAS '仞刚测试层级维度' \n"
            + "(\n"
            + "   tsss_te ALIAS 'tsss_te' BIGINT CORRELATION WITH ('dict'='tsss_te'),\n"
            + "   tet_tee ALIAS 'tet_tee' BIGINT CORRELATION WITH ('dict'='tet_tee'),\n"
            + "   test_2 ALIAS 'test_2' BIGINT CORRELATION WITH ('dict'='test_2'),\n"
            + "   standard_001 ALIAS 'standard_001' BIGINT CORRELATION WITH ('dict'='standard_001'),\n"
            + "   eileen_test_bianma ALIAS 'eileen_test_bianma' BIGINT CORRELATION WITH ('dict'='eileen_test_bianma')\n"
            + ")\n"
            + " COMMENT '仞刚测试层级维度'\n"
            + " PARTITIONED BY (\n"
            + "   caiyou ALIAS '采油桶2' STRING CORRELATION NOT NULL WITH ('dict'='caiyou')\n"
            + ")\n"
            + " WITH ('life_cycle'='10')";

        List<BaseStatement> compare = starter.compare(before, after);
        assertEquals(compare.get(0).toString(),
            "ALTER TABLE rengang_ceshi_2 CHANGE COLUMN caiyou caiyou ALIAS '采油桶2' STRING");
    }

    @Test
    public void testDimKey() {
        String before
            = "CREATE DIM TABLE r1 (\n b BIGINT COMMENT 'comment', CONSTRAINT c1 DIM KEY (b) REFERENCES dim_shop(b)\n)";
        String after = "CREATE DIM TABLE r1 (\n"
            + " b BIGINT COMMENT 'comment'"
            + ")";
        List<BaseStatement> compare = starter.compare(before, after);
        BaseStatement baseStatement = compare.get(0);
        DropConstraint dropConstraint = (DropConstraint)baseStatement;
        assertEquals(dropConstraint.getConstraintType(), ConstraintType.DIM_KEY);
    }

    @Test
    public void testCompareComment() {
        String before =
            "CREATE DIM TABLE r1 ( b ALIAS 'alias' BIGINT COMMENT 'comment', CONSTRAINT c1 DIM KEY (b) REFERENCES "
                + "dim_shop(b))";
        String after =
            "CREATE DIM TABLE r1 ( b ALIAS 'alias2' BIGINT COMMENT 'comment', CONSTRAINT c1 DIM KEY (b) REFERENCES "
                + "dim_shop(b))";
        List<BaseStatement> compare = starter.compare(before, after);
        ChangeCol changeCol = (ChangeCol)compare.get(0);
        assertNull(changeCol.getCommentValue());
    }
}
