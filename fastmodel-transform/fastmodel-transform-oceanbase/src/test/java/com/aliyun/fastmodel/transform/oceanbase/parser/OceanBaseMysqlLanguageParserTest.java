package com.aliyun.fastmodel.transform.oceanbase.parser;

import java.util.List;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/2/2
 */
public class OceanBaseMysqlLanguageParserTest {

    OceanBaseMysqlLanguageParser oceanBaseMysqlParser = new OceanBaseMysqlLanguageParser();

    @Test
    public void parseNode() {
        String text = "create table non_partrange_hash_pri ("
            + "    col1 int,"
            + "    col2 numeric(10,2),"
            + "    col3 varchar(10),"
            + "    col4 blob,"
            + "    col5 year,"
            + "    constraint pk_non_partrange_hash_pri primary key(col1,col5)"
            + ") partition by range(col2) subpartition by hash(col1) ("
            + "    partition p1 values less than(0)("
            + "        subpartition p1_1,"
            + "        subpartition p1_2"
            + "    ),"
            + "    partition p2 values less than(10000)("
            + "        subpartition p2_1,"
            + "        subpartition p2_2"
            + "    ),"
            + "    partition p3 values less than(100000)("
            + "        subpartition p3_1,"
            + "        subpartition p3_2"
            + "    ),"
            + "    partition p4 values less than (maxvalue)("
            + "        subpartition p4_1"
            + "    )"
            + ");";
        CreateTable createTable = oceanBaseMysqlParser.parseNode(text, ReverseContext.builder().build());
        assertNotNull(createTable);
        assertEquals("non_partrange_hash_pri", createTable.getQualifiedName().toString());
        List<BaseConstraint> constraintStatements = createTable.getConstraintStatements();
        assertEquals(1, constraintStatements.size());
        BaseConstraint constraint = constraintStatements.get(0);
        assertEquals(ConstraintType.PRIMARY_KEY, constraint.getConstraintType());
        PrimaryConstraint primaryConstraint = (PrimaryConstraint)constraint;
        assertEquals(2, primaryConstraint.getColNames().size());
        PartitionedBy partitionedBy = createTable.getPartitionedBy();
        assertNotNull(partitionedBy);
    }

    @Test
    public void testParsePartition() {
        String text = "CREATE TABLE tbl4 (c1 INT PRIMARY KEY, c2 INT) PARTITION BY HASH(c1) PARTITIONS 8;";
        CreateTable createTable = oceanBaseMysqlParser.parseNode(text, ReverseContext.builder().build());
        assertNotNull(createTable);
    }

    @Test
    public void testParseDataType() {
        OceanBaseMysqlLanguageParser mysqlLanguageParser = new OceanBaseMysqlLanguageParser();
        BaseDataType baseDataType = mysqlLanguageParser.parseDataType("varchar(100)", ReverseContext.builder().build());
        assertEquals("VARCHAR", baseDataType.getTypeName().getName());
    }
}