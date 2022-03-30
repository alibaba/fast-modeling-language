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

package com.aliyun.fastmodel.parser.impl;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.exception.SemanticException;
import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnCategory;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.constants.TableType;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.AddPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.CloneTable;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateAdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateFactTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.DropPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.RenameCol;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetColComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableAliasedName;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.ColumnGroupConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.LevelConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.LevelDefine;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.RedundantConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.parser.NodeParser;
import com.aliyun.fastmodel.parser.lexer.ReservedIdentifier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * table 修改内容
 *
 * @author panguanjing
 * @date 2020/9/22
 */
public class TableTest {

    NodeParser fastModelAntlrParser = new NodeParser();

    @Test(expected = ParseException.class)
    public void testParseWithBlank() {
        fastModelAntlrParser.parseStatement(" ");
    }

    @Test
    public void testAlterStatementSuffixProperties() {
        String sql = "alter table t1 set properties('type' = 'dim');";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        BaseStatement parse = fastModelAntlrParser.parse(domainLanguage);
        assertEquals(parse.getClass(), SetTableProperties.class);
        SetTableProperties propertiesStatement = (SetTableProperties)parse;
        assertEquals(propertiesStatement.getIdentifier(), "t1");

    }

    @Test
    public void testWithChinese() {
        String fml = "ALTER TABLE t1 ADD CONSTRAINT 中文名 DIM KEY (uid) REFERENCES dim_user(uid)";
        AddConstraint addConstraint = fastModelAntlrParser.parseStatement(fml);
        assertEquals(addConstraint.getConstraintStatement(), new DimConstraint(
            new Identifier("中文"),
            ImmutableList.of(new Identifier("uid")),
            QualifiedName.of("dim_user"),
            ImmutableList.of(new Identifier("uid")))
        );
    }

    @Test
    public void testSetPropertiesWithoutKey() {
        String sql = "alter table t1 set ('type' ='dim')";
        SetTableProperties properties = (SetTableProperties)fastModelAntlrParser.parse(new DomainLanguage(sql));
        assertEquals(properties.getIdentifier(), "t1");
        assertEquals(properties.getProperties().size(), 1);
    }

    @Test
    public void testSetCommentIgnoreCase() {
        String sql = "alter table T1 set comment 'setComment'";
        BaseStatement parse = fastModelAntlrParser.parse(new DomainLanguage(sql));
        SetTableComment setTableComment = (SetTableComment)parse;
        assertNull(setTableComment.getBusinessUnit());
        assertEquals(setTableComment.getIdentifier(), "t1");
        assertEquals(setTableComment.getComment(), new Comment("setComment"));
    }

    @Test
    public void testAlterStatementSuffixUnSetProperties() {
        String sql = "alter table t1 unset properties('type','type2');";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        BaseStatement st = fastModelAntlrParser.parse(domainLanguage);
        assertEquals(st.getClass(), UnSetTableProperties.class);
        UnSetTableProperties propertiesStatement = (UnSetTableProperties)st;
        assertEquals(propertiesStatement.getPropertyKeys().size(), 2);
    }

    @Test
    public void testCreateTable() {
        DomainLanguage domainLanguage = new DomainLanguage(
            "create dim table t1  (col bigint comment 'abc') comment '表名'");
        BaseStatement parse = fastModelAntlrParser.parse(domainLanguage);
        assertEquals(parse.getClass(), CreateDimTable.class);
        CreateTable createTableStatement = (CreateTable)parse;
        List<ColumnDefinition> columnDefines = createTableStatement.getColumnDefines();
        assertEquals(createTableStatement.getIdentifier(), "t1");
        assertEquals(createTableStatement.getComment(), new Comment("表名"));
        assertEquals(1, columnDefines.size());
        assertEquals(createTableStatement.getCommentValue(), "表名");
        assertTrue(createTableStatement.isPropertyEmpty());
    }

    @Test
    public void testAlterTable() {
        DomainLanguage domainLanguage = new DomainLanguage("alter table t1 rename to t2");
        BaseStatement parse = fastModelAntlrParser.parse(domainLanguage);
        RenameTable alterTableNameStatement = (RenameTable)parse;
        assertEquals(alterTableNameStatement.getIdentifier(), "t1");
        assertEquals(alterTableNameStatement.getNewIdentifier(), "t2");
    }

    @Test
    public void testAlterTableAddCol() {
        DomainLanguage domainLanguage = setDomainLanguage("alter table t1 add columns (a bigint comment 'comment')");
        BaseStatement parse = fastModelAntlrParser.parse(domainLanguage);
        assertEquals(parse.getClass(), AddCols.class);
        AddCols alterAddColsStatement = (AddCols)parse;
        assertEquals(alterAddColsStatement.getIdentifier(), "t1");
        List<ColumnDefinition> columnDefineList = alterAddColsStatement.getColumnDefineList();
        assertEquals(1, columnDefineList.size());
    }

    @Test
    public void testAlterTableAddColMulti() {
        DomainLanguage domainLanguage = setDomainLanguage(
            "alter table t1 add columns (a bigint comment 'comment', b varchar(1) comment 'comment')");
        BaseStatement parse = fastModelAntlrParser.parse(domainLanguage);
        assertEquals(parse.getClass(), AddCols.class);
        AddCols alterAddColsStatement = (AddCols)parse;
        assertEquals(alterAddColsStatement.getIdentifier(), "t1");
        List<ColumnDefinition> columnDefineList = alterAddColsStatement.getColumnDefineList();
        assertEquals(2, columnDefineList.size());
    }

    private DomainLanguage setDomainLanguage(String sql) {
        return new DomainLanguage(sql);
    }

    @Test
    public void testTableRename() {
        DomainLanguage language = setDomainLanguage("alter table t1 rename to t2");
        RenameTable alterTableNameStatement = (RenameTable)fastModelAntlrParser.parse(language);
        assertEquals(alterTableNameStatement.getIdentifier(), "t1");
        assertEquals(alterTableNameStatement.getNewIdentifier(), "t2");
    }

    @Test
    public void testAlterTableAddConstraint() {
        DomainLanguage language = setDomainLanguage("alter table t1 add constraint c1 dim references dim_table");
        AddConstraint alterAddConstraintStatement = (AddConstraint)fastModelAntlrParser
            .parse(language);
        assertEquals(alterAddConstraintStatement.getIdentifier(), "t1");
        BaseConstraint constraintStatement = alterAddConstraintStatement.getConstraintStatement();
        Identifier constraintName = constraintStatement.getName();
        assertEquals(constraintName, new Identifier("c1"));
        ConstraintType constraintType = constraintStatement.getConstraintType();
        assertEquals(constraintType, ConstraintType.DIM_KEY);
    }

    @Test
    public void testAlterTableAddConstraintWithDimKey() {
        DomainLanguage domainLanguage = setDomainLanguage(
            "alter table t1 add constraint c1 dim key (c1) references dim_table (d1)");
        AddConstraint alterAddConstraintStatement = (AddConstraint)fastModelAntlrParser
            .parse(domainLanguage);
        String identifier = alterAddConstraintStatement.getIdentifier();
        assertEquals(identifier, "t1");
        BaseConstraint constraintStatement = alterAddConstraintStatement.getConstraintStatement();
        ConstraintType constraintType = constraintStatement.getConstraintType();
        assertEquals(constraintType, ConstraintType.DIM_KEY);
        DimConstraint dimConstraintStatement = (DimConstraint)constraintStatement;
        assertEquals(dimConstraintStatement.getColNames().size(), 1);
        assertEquals(dimConstraintStatement.getReferenceColNames().size(), 1);
    }

    @Test
    public void testAlterTableAddPrimaryKey() {
        DomainLanguage domainLanguage = setDomainLanguage("alter table t1 add constraint c1 primary key(c1, c2)");
        AddConstraint alterAddConstraintStatement = (AddConstraint)fastModelAntlrParser
            .parse(domainLanguage);
        String identifier = alterAddConstraintStatement.getIdentifier();
        BaseConstraint constraintStatement = alterAddConstraintStatement.getConstraintStatement();
        ConstraintType constraintType = constraintStatement.getConstraintType();
        assertEquals(ConstraintType.PRIMARY_KEY, constraintType);
        assertEquals(identifier, "t1");
        PrimaryConstraint primaryConstraintStatement = (PrimaryConstraint)constraintStatement;
        assertEquals(primaryConstraintStatement.getColNames().size(), 2);

    }

    @Test
    public void testAlterTableDropConstraint() {
        DomainLanguage language = setDomainLanguage("alter table t1 drop constraint c1");
        DropConstraint dropConstraintStatement = (DropConstraint)fastModelAntlrParser.parse(
            language);
        String identifier = dropConstraintStatement.getIdentifier();
        assertEquals("t1", identifier);
        Identifier constraintName = dropConstraintStatement.getConstraintName();
        assertEquals(new Identifier("c1"), constraintName);
    }

    @Test
    public void testDropTable() {
        DomainLanguage domainLanguage = setDomainLanguage("drop table t1");
        DropTable parse = (DropTable)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(parse.getIdentifier(), "t1");
    }

    @Test
    public void testCreateTableWithLevelConstraint() {
        String createTable
            =
            "CREATE LEVEL DIM TABLE a (b bigint, c varchar(1) , constraint c level <b:(c)> comment 'comment') comment"
                + " 'table'";
        DomainLanguage domainLanguage = setDomainLanguage(createTable);
        CreateTable createTableStatement = (CreateTable)fastModelAntlrParser.parse(domainLanguage);
        List<BaseConstraint> constraintStatements = createTableStatement.getConstraintStatements();
        assertEquals(1, constraintStatements.size());
        LevelConstraint constraintStatement = (LevelConstraint)constraintStatements.get(0);
        assertEquals(constraintStatement.getComment().getComment(), "comment");
        assertEquals(constraintStatement.getName().getValue(), "c");
        assertEquals(constraintStatement.getLevelDefines().size(), 1);
        List<LevelDefine> levelDefines = constraintStatement.getLevelDefines();
        assertEquals(levelDefines.size(), 1);
        LevelDefine levelDefine = levelDefines.get(0);
        assertEquals(levelDefine.getLevelColName(), new Identifier("b"));
        assertEquals(levelDefine.getLevelPropColNames().size(), 1);
        Identifier s = levelDefine.getLevelPropColNames().get(0);
        assertEquals(s, new Identifier("c"));
    }

    @Test
    public void testAlterTableCol() {
        String alterCol = "ALTER TABLE t1 CHANGE COLUMN a a1 varchar(1) primary key comment 'hello'";
        DomainLanguage domainLanguage = setDomainLanguage(alterCol);
        ChangeCol renameColStatement = (ChangeCol)fastModelAntlrParser.parse(
            domainLanguage);
        assertEquals(renameColStatement.getOldColName(), new Identifier("a"));
        assertEquals(renameColStatement.getNewColName(), new Identifier("a1"));
        assertEquals(renameColStatement.getDataType().getTypeName(), DataTypeEnums.VARCHAR);
        assertEquals(renameColStatement.getPrimary(), true);
        assertEquals(renameColStatement.getComment(), new Comment("hello"));
    }

    @Test
    public void testAlterTableColEnable() {
        String alterCol = "ALTER TABLE t1 CHANGE COLUMN a a1 varchar(1) primary key enable comment 'hello'";
        DomainLanguage domainLanguage = setDomainLanguage(alterCol);
        ChangeCol renameColStatement = (ChangeCol)fastModelAntlrParser.parse(
            domainLanguage);
        assertEquals(renameColStatement.getOldColName(), new Identifier("a"));
        assertEquals(renameColStatement.getNewColName(), new Identifier("a1"));
        assertEquals(renameColStatement.getDataType().getTypeName(), DataTypeEnums.VARCHAR);
        assertEquals(renameColStatement.getPrimary(), true);
        assertEquals(renameColStatement.getComment(), new Comment("hello"));
    }

    @Test
    public void testAlterTableColDisablePrimaryKey() {
        String alterCol = "ALTER TABLE t1 CHANGE COLUMN a a1 varchar(1) primary key disable comment 'hello'";
        DomainLanguage domainLanguage = setDomainLanguage(alterCol);
        ChangeCol renameColStatement = (ChangeCol)fastModelAntlrParser.parse(
            domainLanguage);
        assertEquals(renameColStatement.getPrimary(), false);
    }

    @Test
    public void testAlterTableColEnablePrimaryKey() {
        String alterCol = "ALTER TABLE t1 CHANGE COLUMN a a1 varchar(1) primary key  comment 'hello'";
        DomainLanguage domainLanguage = setDomainLanguage(alterCol);
        ChangeCol renameColStatement = (ChangeCol)fastModelAntlrParser.parse(
            domainLanguage);
        assertEquals(renameColStatement.getPrimary(), true);
    }

    @Test
    public void testAlterTableColDisable() {
        String alterCol = "ALTER TABLE t1 CHANGE COLUMN a a1 varchar(1) not null disable comment 'hello'";
        DomainLanguage domainLanguage = setDomainLanguage(alterCol);
        ChangeCol renameColStatement = (ChangeCol)fastModelAntlrParser.parse(
            domainLanguage);
        assertEquals(renameColStatement.getOldColName(), new Identifier("a"));
        assertEquals(renameColStatement.getNewColName(), new Identifier("a1"));
        assertEquals(renameColStatement.getDataType().getTypeName(), DataTypeEnums.VARCHAR);
        assertEquals(renameColStatement.getComment(), new Comment("hello"));
        assertEquals(renameColStatement.getNotNull(), false);
    }

    @Test
    public void testAlterTableWithType() {
        String alter = "ALTER FACT TABLE a.b RENAME TO a.c";
        RenameTable renameTable = fastModelAntlrParser.parseStatement(alter);
        assertEquals(renameTable.getNewIdentifier(), "c");
    }

    @Test
    public void testCreateStatementError() {
        String sql =
            "CREATE DIM TABLE dim_emp_info( emp_id bigint not null comment '员工Id', org_id bigint not null comment "
                + "'企业Id',"
                + " primary key (org_id, emp_id), constraint org_constraint dim key (org_id) references dim_org"
                + "(org_id) ) COMMENT '员工信息维度表' WITH TBLPROPERTIES('type' = 'NORMAL_DIM')";
        DomainLanguage domainLanguage = setDomainLanguage(sql);
        CreateTable
            parse = (CreateTable)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(parse.getIdentifier(), "dim_emp_info");
    }

    @Test
    public void testCreateLevel() {
        String sql = "CREATE DIM TABLE demo.dim_emp_area( \n"
            + "    area_code string primary key  comment '地区编号', \n"
            + "    country_code string comment '国家码', \n"
            + "    country_name string comment '国家名', \n"
            + "    province_code string comment '省份码', \n"
            + "    province_name string comment '省份名', \n"
            + "    constraint big level <country_code:(country_code,country_name), province_code:(province_code,"
            + "province_name)> comment '大层级关系', \n"
            + "    constraint little level <area_code, province_code> comment '小层级关系'\n"
            + ") COMMENT '员工地区维度表' WITH TBLPROPERTIES( 'type' = 'LEVEL_DIM')";
        DomainLanguage domainLanguage = setDomainLanguage(sql);
        CreateDimTable dimTableStatement = (CreateDimTable)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(dimTableStatement.getIdentifier(), "dim_emp_area");
        assertEquals(dimTableStatement.getConstraintStatements().size(), 3);
    }

    @Test
    public void testAlterLevelConstraint() {
        String sql = "alter table t1 ADD constraint name level <a> comment 'abc'";
        DomainLanguage domainLanguage = setDomainLanguage(sql);
        AddConstraint alterAddConstraintStatement = (AddConstraint)fastModelAntlrParser
            .parse(domainLanguage);
        assertEquals(alterAddConstraintStatement.getConstraintStatement().getName().getValue(), "name");
        assertEquals(alterAddConstraintStatement.getConstraintStatement().getConstraintType(),
            ConstraintType.LEVEL_KEY);
    }

    @Test
    public void testAlterSetComment() {
        String sql = "alter table t1 set comment 'comment'";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        SetTableComment alterTableCommentStatement = (SetTableComment)fastModelAntlrParser.parse(
            domainLanguage);
        assertEquals(alterTableCommentStatement.getComment(), new Comment("comment"));
        assertEquals(alterTableCommentStatement.getIdentifier(), "t1");
    }

    @Test
    public void testAlterRenameToSupportDot() {
        String sql = "alter table u.t1 rename to b.t2";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        RenameTable alterTableNameStatement = (RenameTable)fastModelAntlrParser.parse(
            domainLanguage);
        assertEquals(alterTableNameStatement.getNewIdentifier(), "t2");
        assertEquals(alterTableNameStatement.getBusinessUnit(), "u");
        assertEquals(alterTableNameStatement.getIdentifier(), "t1");
    }

    @Test
    public void testCreatTableNotNull() {
        String sql = "create DIM table t1 (col1 bigint not null) with tblproperties('type'='normal_dim')";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        CreateTable statement = (CreateTable)fastModelAntlrParser.parse(domainLanguage);
        List<ColumnDefinition> columnDefines = statement.getColumnDefines();
        assertEquals(1, columnDefines.size());
        ColumnDefinition columnDefine = columnDefines.get(0);
        Boolean notNull = columnDefine.getNotNull();
        assertTrue(notNull);
    }

    @Test
    public void testCreateTableWithNewIdentifier() {
        String sql = "create dim table t1 (col1 bigint not null)";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        CreateTable createTableStatement = (CreateTable)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(createTableStatement.getIdentifier(), "t1");
        assertEquals(createTableStatement.getTableType(), TableType.DIM.getCode());
    }

    @Test
    public void testCreateFactTable() {
        String sql = "create fact table f1 (col1 bigint not null)";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        CreateFactTable createTableStatement = (CreateFactTable)fastModelAntlrParser.parse(
            domainLanguage);
        assertEquals(createTableStatement.getIdentifier(), "f1");
        assertEquals(createTableStatement.getTableType(), TableType.FACT.getCode());
    }

    @Test
    public void testCreateDimTableWithType() {
        String sql = "create fact table f1 (col1 bigint not null) with tblproperties('type'='tx')";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        CreateFactTable createTableStatement = (CreateFactTable)fastModelAntlrParser.parse(
            domainLanguage);
        assertEquals(createTableStatement.getIdentifier(), "f1");
        assertEquals(createTableStatement.getTableType(), TableType.FACT.getCode());
        assertEquals(createTableStatement.getTableDetailType(), TableDetailType.TRANSACTION_FACT);
    }

    @Test
    public void testTinyInt() {
        String sql = "create dim table if not exists tt.cat (id TINYINT comment 'primary')";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        CreateDimTable createDimTableStatement = (CreateDimTable)fastModelAntlrParser.parse(
            domainLanguage);
        assertNotNull(createDimTableStatement);
        List<ColumnDefinition> columnDefines = createDimTableStatement.getColumnDefines();
        assertEquals(1, columnDefines.size());
        ColumnDefinition columnDefine = columnDefines.get(0);
        assertEquals(columnDefine.getDataType().getTypeName(), DataTypeEnums.TINYINT);
    }

    @Test
    public void testCreateTableWithVarchar() {
        String sql = String.format("alter table %s.%s change column org_name org_short_name string comment '机构名称';",
            "ding", "contact");
        BaseStatement parse = fastModelAntlrParser.parse(new DomainLanguage(sql));
        ChangeCol renameCol = (ChangeCol)parse;
        assertEquals(renameCol.getNewColName(), new Identifier("org_short_name"));
    }

    @Test
    public void createTableWith() {
        String createTable = "CREATE DIM TABLE a.b (a BIGINT) COMMENT \"comment\"";
        CreateDimTable statement = fastModelAntlrParser.parseStatement(createTable);
        assertEquals(statement.getCommentValue(), "comment");
    }

    @Test
    public void testCreateTableWithPartitionBy() {
        String sql = "CREATE DIM TABLE a.b (a bigint) partitioned by (c varchar(10))";
        CreateDimTable createDimTable = (CreateDimTable)fastModelAntlrParser.parse(new DomainLanguage(sql));
        PartitionedBy partitionedBy = createDimTable.getPartitionedBy();
        assertNotNull(partitionedBy);
    }

    @Test
    public void testCreateTableWithColGroup() {
        String sql = "create dim table a.b (a bigint, constraint c1 column_group(a,b));";
        CreateDimTable createDimTable = (CreateDimTable)fastModelAntlrParser.parse(new DomainLanguage(sql));
        List<BaseConstraint> constraintStatements = createDimTable.getConstraintStatements();
        assertEquals(1, constraintStatements.size());
        ColumnGroupConstraint columnGroupConstraint = (ColumnGroupConstraint)constraintStatements.get(0);
        assertEquals(columnGroupConstraint.getName(), new Identifier("c1"));
        assertEquals(columnGroupConstraint.getColNames().size(), 2);
    }

    @Test
    public void testCreateTableWithNoPropertyKey() {
        String sql = "create dim table a.b(a bigint) with('a' = 'b')";
        CreateDimTable createDimTable = (CreateDimTable)fastModelAntlrParser.parse(new DomainLanguage(sql));
        assertEquals(createDimTable.getProperties().size(), 1);
        assertEquals(sql, createDimTable.getOrigin());
    }

    @Test
    public void testCreateTableWithoutCol() {
        String sql = "create dim table a.b comment 'abc' with ('b'='bc')";
        CreateDimTable createDimTable = (CreateDimTable)fastModelAntlrParser.parse(new DomainLanguage(sql));
        assertEquals(createDimTable.getQualifiedName(), QualifiedName.of("a.b"));
        assertEquals(createDimTable.getTableDetailType(), TableDetailType.NORMAL_DIM);
    }

    @Test
    public void testFactName() {
        String sql = "CREATE FACT TABLE demo.fact_emp_change " +
            "( change_id bigint primary key comment '事务变化Id', " +
            "org_id bigint comment '企业Id', " +
            "emp_id bigint not null comment '员工Id', " +
            "test_01 varchar(15) comment 'test01'," +
            "constraint org_constraint dim key (org_id) references dim_org(org_id), " +
            "constraint emp_constraint dim key (emp_id) references dim_emp_info(emp_id) ) comment '员工档案变化事实表' "
            + "partitioned by(aa bigint "
            + "comment 'test1', bb string comment 'test2')  with properties('type' = 'tx_fact')";

        CreateFactTable createFactTable = fastModelAntlrParser.parseStatement(sql);
        assertEquals(createFactTable.getComment().getComment(), "员工档案变化事实表");
    }

    @Test
    public void testAddPartition() {
        String sql = "alter table a.b add partition column co1 bigint comment 'sgb'";
        AddPartitionCol addPartitionCol = fastModelAntlrParser.parseStatement(sql);
        assertEquals(addPartitionCol.getColumnDefinition().getComment(), new Comment("sgb"));
    }

    @Test
    public void testDropCol() {
        String sql = "ALTER TABLE test_bu.dim_shop DROP COLUMN test_code13";
        DropCol dropCol = fastModelAntlrParser.parseStatement(sql);
        assertEquals(dropCol.getColumnName(), new Identifier("test_code13"));
    }

    @Test
    public void testRenameCol() {
        String sql = "ALTER TABLE a.b CHANGE COLUMN col1 RENAME TO col2";
        RenameCol renameCol = fastModelAntlrParser.parseStatement(sql);
        assertEquals(renameCol.getOldColName(), new Identifier("col1"));
        assertEquals(renameCol.getNewColName(), new Identifier("col2"));
    }

    @Test
    public void testSetColComment() {
        String sql = "ALTER TABLE a.b CHANGE COLUMN col1 COMMENT 'abc'";
        SetColComment setColComment = fastModelAntlrParser.parseStatement(sql);
        assertEquals(setColComment.getChangeColumn(), new Identifier("col1"));
        assertEquals(setColComment.getComment(), new Comment("abc"));
    }

    @Test
    public void testDropPartition() {
        String sql = "ALTER TABLE a.b DROP PARTITION COLUMN c";
        DropPartitionCol dropPartitionCol = fastModelAntlrParser.parseStatement(sql);
        assertEquals(dropPartitionCol.getColumnName(), new Identifier("c"));
    }

    @Test
    public void testColumnDefinition() {
        String fml = "CREATE DIM TABLE a (b bigint MEASUREMENT WITH ('key'='value1'))";
        CreateDimTable dimTable = fastModelAntlrParser.parseStatement(fml);
        List<ColumnDefinition> columnDefines = dimTable.getColumnDefines();
        ColumnDefinition columnDefinition = columnDefines.get(0);
        assertEquals(columnDefinition.getCategory(), ColumnCategory.MEASUREMENT);
    }

    @Test
    public void testParse() {
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(
            QualifiedName.of("1_14255.abc")
        ).build();
        assertEquals(createDimTable.toString(), "CREATE DIM TABLE 1_14255.abc");
    }

    @Test
    public void testPartitionBy() {
        String dsl = "CREATE DIM TABLE 1_23620.dim_qianli_test4 (\n"
            + "   test1 STRING CORRELATION NOT NULL COMMENT 'test1' WITH "
            + "('uuid'='tb_c-f6430e372bf34c30a33bd015c89d95b8'),\n"
            + "   CONSTRAINT PK PRIMARY KEY(test1,test2)\n"
            + ")\n"
            + " COMMENT 'dim_qianli_test4'\n"
            + " PARTITIONED BY (test2 STRING CORRELATION NOT NULL COMMENT 'test2' WITH "
            + "('uuid'='tb_c-c7ffa89e18934b83b8dcd21642f26cdc'))"
            + " WITH ('business_process'='default')\n";
        CreateDimTable createDimTable = fastModelAntlrParser.parseStatement(dsl);
        assertNotNull(createDimTable);
    }

    @Test
    public void testTransfer() {
        String fml = "Create dim table b (`code` bigint) comment 'comment'";
        BaseStatement statement = fastModelAntlrParser.parseStatement(fml);
        assertEquals(statement.toString(), "CREATE DIM TABLE b \n"
            + "(\n"
            + "   `code` BIGINT\n"
            + ")\n"
            + "COMMENT 'comment'");

        CreateDimTable statement1 = (CreateDimTable)statement;
        assertTrue(statement1.isPartitionEmpty());
        assertTrue(statement1.isPropertyEmpty());
        assertTrue(statement1.isConstraintEmpty());
        assertFalse(statement1.isColumnEmpty());
    }

    @Test
    public void testKeyWord() {
        String fml = "create dim table a.b (code bigint) comment 'hello'";
        BaseStatement statement = fastModelAntlrParser.parseStatement(fml);
        assertNotNull(statement);
    }

    @Test
    public void testPrimaryIsNull() {
        String st = "CREATE OR REPLACE DIM TABLE dim_shop \n(\n   shop_code STRING NOT NULL COMMENT '门店code',\n   "
            + "shop_name STRING COMMENT '门店name',\n   shop_type STRING COMMENT '门店类型',\n   merchant_code BIGINT "
            + "COMMENT '商家code',\n   PRIMARY KEY(shop_code)\n)\n COMMENT '门店' WITH ('business_process'='test_bp')";
        CreateDimTable baseStatement = fastModelAntlrParser.parseStatement(st);
        List<ColumnDefinition> columnDefines = baseStatement.getColumnDefines();
        for (ColumnDefinition c : columnDefines) {
            if (c.getColName().equals(new Identifier("shop_name"))) {
                assertNull(c.getPrimary());
                break;
            }
        }
    }

    @Test
    public void testCreateWithAliasedName() {
        String fml = "create dim table dim_shop ALIAS '店铺名' COMMENT '描述信息'";
        CreateDimTable createDimTable = fastModelAntlrParser.parseStatement(fml);
        AliasedName aliasedName = createDimTable.getAliasedName();
        assertNotNull(aliasedName);
        assertEquals(aliasedName.getName(), "店铺名");
    }

    @Test
    public void testCreateTableIssue() {
        List<ColumnDefinition> columnDefines = Lists.newArrayList(
            ColumnDefinition.builder().colName(new Identifier("c1")).dataType(
                new GenericDataType(new Identifier(DataTypeEnums.ARRAY.name()), Lists.newArrayList(
                    new TypeParameter(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                ))).build()
        );
        CreateDimTable build = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(
            columnDefines
        ).build();
        String s = build.toString();
        CreateDimTable createDimTable = fastModelAntlrParser.parseStatement(s);
        assertEquals(createDimTable.getQualifiedName(), build.getQualifiedName());
    }

    @Test
    public void testCreateTableIssueWithMap() {
        List<ColumnDefinition> columnDefines = Lists.newArrayList(
            ColumnDefinition.builder().colName(new Identifier("c1")).dataType(
                new GenericDataType(new Identifier(DataTypeEnums.MAP.name()), Lists.newArrayList(
                    new TypeParameter(DataTypeUtil.simpleType(DataTypeEnums.STRING)),
                    new TypeParameter(DataTypeUtil.simpleType(DataTypeEnums.STRING))
                ))).build()
        );
        CreateDimTable build = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(
            columnDefines
        ).build();
        String s = build.toString();
        CreateDimTable createDimTable = fastModelAntlrParser.parseStatement(s);
        assertEquals(createDimTable.getQualifiedName(), build.getQualifiedName());
    }

    @Test
    public void testCreateTableIssueWithArrayMap() {
        GenericDataType dataType = new GenericDataType(new Identifier(DataTypeEnums.MAP.name()), Lists.newArrayList(
            new TypeParameter(DataTypeUtil.simpleType(DataTypeEnums.STRING)),
            new TypeParameter(DataTypeUtil.simpleType(DataTypeEnums.STRING))
        ));
        List<ColumnDefinition> columnDefines = Lists.newArrayList(
            ColumnDefinition.builder().colName(new Identifier("c1")).dataType(
                new GenericDataType(
                    new Identifier(DataTypeEnums.ARRAY.name()),
                    Lists.newArrayList(new TypeParameter(dataType))
                )).build()
        );
        CreateDimTable build = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(
            columnDefines
        ).build();
        String s = build.toString();
        CreateDimTable createDimTable = fastModelAntlrParser.parseStatement(s);
        assertEquals(createDimTable.getQualifiedName(), build.getQualifiedName());
    }

    @Test
    public void testSetAliasedName() {
        String fml = "ALTER TABLE t.b SET ALIAS 'new_alias'";
        SetTableAliasedName set
            = fastModelAntlrParser.parseStatement(fml);
        assertEquals(set.getAliasedName(), new AliasedName("new_alias"));
    }

    @Test
    public void testRedundant() {
        String fml = "CREATE DIM TABLE dim_test (\n"
            + "     c1 BIGINT COMMENT 'comment',\n"
            + "     user_id STRING COMMENT 'comment',\n"
            + "     user_name STRING COMMENT 'user_name' WITH ('redundant'='cc.user_name'),\n"
            + "     age BIGINT COMMENT 'user_name' WITH ('redundant'='cc.user_age'),\n"
            + "     user_info STRING COMMENT 'comment' WITH ('source' = 'tbcdm.member.user_info'),\n"
            + "     constraint cc REDUNDANT user_id REFERENCES t1.user_id (user_name, user_age)\n"
            + ")";
        CreateDimTable createDimTable = fastModelAntlrParser.parseStatement(fml);
        List<BaseConstraint> constraintStatements = createDimTable.getConstraintStatements();
        assertEquals(1, constraintStatements.size());
        RedundantConstraint redundantConstraint = (RedundantConstraint)constraintStatements.get(0);
        assertEquals(redundantConstraint.getColumn(), new Identifier("user_id"));
        QualifiedName joinColumn = redundantConstraint.getJoinColumn();
        assertEquals(joinColumn.getSuffix(), "user_id");
        List<Identifier> redundantColumns = redundantConstraint.getRedundantColumns();
        assertEquals(2, redundantColumns.size());
    }

    @Test
    public void testCloneTable() {
        String fml = "create dim table clone_table1 like source_table1;";
        CloneTable cloneTable = fastModelAntlrParser.parseStatement(fml);
        TableDetailType tableDetailType = cloneTable.getTableDetailType();
        assertEquals(tableDetailType, TableDetailType.NORMAL_DIM);
        QualifiedName sourceTable = cloneTable.getSourceTable();
        assertEquals(sourceTable.getSuffix(), "source_table1");
    }

    @Test
    public void testUniqueKey() {
        String fml = "Create dim table c1 (a bigint, unique key (a));";
        BaseStatement baseStatement = fastModelAntlrParser.parseStatement(fml);
        assertEquals(baseStatement.toString(), "CREATE DIM TABLE c1 \n"
            + "(\n"
            + "   a BIGINT,\n"
            + "   UNIQUE KEY (a)\n"
            + ")");
    }

    @Test
    public void testIndex() {
        String fml = "CREATE DIM TABLE a1 (A BIGINT, B CUSTOM('abc'), INDEX abc (A));";
        BaseStatement baseStatement = fastModelAntlrParser.parseStatement(fml);
        assertEquals(baseStatement.toString(), "CREATE DIM TABLE a1 \n"
            + "(\n"
            + "   a BIGINT,\n"
            + "   b CUSTOM('abc'),\n"
            + "   INDEX abc (a)\n"
            + ")");
    }

    @Test
    public void testColumn() {
        String fml = "create dim table a1 (dynamic bigint comment 'comment') comment 'comment'";
        BaseStatement baseStatement = fastModelAntlrParser.parseStatement(fml);
        assertEquals(baseStatement.toString(), "CREATE DIM TABLE a1 \n"
            + "(\n"
            + "   dynamic BIGINT COMMENT 'comment'\n"
            + ")\n"
            + "COMMENT 'comment'");
    }

    @Test
    public void testCreateTableColumn() {
        String fml = "create dim table a1 (%s bigint comment 'comment') comment 'coment'";
        Set<String> keywords = ReservedIdentifier.getKeywords();
        List<String> skipWords = Lists.newArrayList("CUBE");
        for (String k : keywords) {
            if (skipWords.contains(k.toUpperCase(Locale.ROOT))) {
                continue;
            }
            String format = String.format(fml, k);
            BaseStatement baseStatement = fastModelAntlrParser.parseStatement(format);
            assertTrue(baseStatement.toString().contains("a1"));
        }
    }

    @Test(expected = SemanticException.class)
    public void testCreateTableError() {
        String f = "CREATE DIM TABLE empty_table ALIAS '测试家娃处理'"
            + "("
            + "   user_id ALIAS 'userId' BIGINT COMMENT 'userId comment',"
            + "   user_name ALIAS 'userName' STRING COMMENT 'userName comment',\n"
            + "   user_name ALIAS 'userName' STRING"
            + ")"
            + "COMMENT 'comment';";
        CreateDimTable baseStatement = fastModelAntlrParser.parseStatement(f);
        assertNotNull(baseStatement);
    }

    @Test(expected = SemanticException.class)
    public void testCreateTableWithNotExistPrimaryKey() {
        String fml = "create dim table a (a bigint, primary key (b))";
        fastModelAntlrParser.parseStatement(fml);
    }

    @Test
    public void testAdsTableCreate() {
        String fml = "create ads table a (a bigint comment 'comment') comment 'comment'";
        CreateAdsTable createAdsTable = fastModelAntlrParser.parseStatement(fml);
        assertEquals(createAdsTable.toString(), "CREATE ADS TABLE a \n"
            + "(\n"
            + "   a BIGINT COMMENT 'comment'\n"
            + ")\n"
            + "COMMENT 'comment'");
    }
}
