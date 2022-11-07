/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetColComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresArrayDataTypeName;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresDataTypeName;
import com.google.common.base.Preconditions;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/6/9
 */
public class HologresParserTest {

    HologresParser hologresParser2 = new HologresParser();

    @Test
    public void parseNode() {
        CompositeStatement compositeStatement = hologresParser2.parseNode("begin; CREATE TABLE public.test (\n"
            + " \"id\" text NOT NULL,\n"
            + " \"ds\" text NOT NULL,\n"
            + "PRIMARY KEY (id,ds)\n"
            + ");commit;");
        assertEquals(3, compositeStatement.getStatements().size());
        CreateTable createTable = (CreateTable)compositeStatement.getStatements().get(1);
        ColumnDefinition columnDefinition = createTable.getColumnDefines().get(0);
        assertEquals(columnDefinition.getDataType().getTypeName(), HologresDataTypeName.TEXT);
        assertEquals(createTable.getConstraintStatements().size(), 1);
    }

    @Test
    public void testNode() {
        CompositeStatement compositeStatement = hologresParser2.parseNode("begin; CREATE TABLE public.test (\n"
            + " \"id\" text[] NOT NULL,\n"
            + " \"ds\" text NOT NULL,\n"
            + "PRIMARY KEY (id,ds)\n"
            + ");\n CALL SET_TABLE_PROPERTY('public.test', 'orientation', 'column');\ncommit;");
        assertEquals(4, compositeStatement.getStatements().size());
        CreateTable createTable = (CreateTable)compositeStatement.getStatements().get(1);
        ColumnDefinition columnDefinition = createTable.getColumnDefines().get(0);
        assertEquals(columnDefinition.getDataType().getTypeName(), new HologresArrayDataTypeName(HologresDataTypeName.TEXT));
        assertEquals(createTable.getConstraintStatements().size(), 1);
        BaseStatement baseStatement = compositeStatement.getStatements().get(2);
        SetTableProperties setTableProperties = (SetTableProperties)baseStatement;
        assertEquals(setTableProperties.getQualifiedName(), QualifiedName.of("public.test"));
    }

    @Test
    public void testComment() {
        CompositeStatement compositeStatement = hologresParser2.parseNode("BEGIN;\n"
            + "CREATE TABLE molin_db.molin_db.aa_not_exist_1 (\n"
            + "   id                         BIGINT NOT NULL,\n"
            + "   name                       TEXT NOT NULL,\n"
            + "   aa_not_exist_1             TEXT,\n"
            + "   _data_integration_deleted_ BOOLEAN NOT NULL\n"
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('molin_db.molin_db.aa_not_exist_1', 'time_to_live_in_seconds', '2592000');\n"
            + "CALL SET_TABLE_PROPERTY('molin_db.molin_db.aa_not_exist_1', 'orientation', 'row');\n"
            + "CALL SET_TABLE_PROPERTY('molin_db.molin_db.aa_not_exist_1', 'binlog.level', 'none');\n"
            + "COMMENT ON COLUMN molin_db.molin_db.aa_not_exist_1.id IS '';\n"
            + "COMMENT ON COLUMN molin_db.molin_db.aa_not_exist_1.name IS '';\n"
            + "COMMENT ON COLUMN molin_db.molin_db.aa_not_exist_1.aa_not_exist_1 IS '';\n"
            + "COMMENT ON COLUMN molin_db.molin_db.aa_not_exist_1._data_integration_deleted_ IS 'Auto generated logical delete column';\n"
            + "COMMIT;");
        assertEquals(10, compositeStatement.getStatements().size());
        BaseStatement baseStatement = compositeStatement.getStatements().get(8);
        SetColComment setColComment = (SetColComment)baseStatement;
        assertEquals(setColComment.getComment(), new Comment("Auto generated logical delete column"));
    }

    @Test
    public void testPrimaryKey() {
        CompositeStatement compositeStatement = hologresParser2.parseNode("BEGIN;\n"
            + "CREATE TABLE molin_db.molin_db.aa_not_exist_1 (\n"
            + "   id                         BIGINT NOT NULL,\n"
            + "   name                       TEXT NOT NULL,\n"
            + "   aa_not_exist_1             TEXT,\n"
            + "   _data_integration_deleted_ BOOLEAN NOT NULL,\n"
            + "   primary key(id)\n"
            + ") PARTITION BY LIST (name);\n"
            + "COMMIT;");
        BaseStatement baseStatement = compositeStatement.getStatements().get(1);
        CreateTable createTable = (CreateTable)baseStatement;
        List<BaseConstraint> constraintStatements = createTable.getConstraintStatements();
        assertEquals(1, constraintStatements.size());
        List<ColumnDefinition> columnDefinitions = createTable.getPartitionedBy().getColumnDefinitions();
        assertEquals(columnDefinitions.size(), 1);
        assertEquals(constraintStatements.size(), 1);
    }

    @Test
    public void testParseDataTypeDouble() {
        BaseDataType baseDataType = hologresParser2.parseDataType(HologresDataTypeName.DOUBLE_PRECISION.getValue(), ReverseContext.builder().build());
        assertNotNull(baseDataType.getTypeName());
    }

    @Test
    public void testParseDataTypeTimestampZ() {
        BaseDataType baseDataType = hologresParser2.parseDataType(HologresDataTypeName.TIMESTAMPTZ.getValue(), ReverseContext.builder().build());
        IDataTypeName typeName = baseDataType.getTypeName();
        assertNotNull(typeName);
        assertEquals(typeName, HologresDataTypeName.TIMESTAMPTZ);
    }

    @Test
    public void testParseDataTypeTimestamp() {
        BaseDataType baseDataType = hologresParser2.parseDataType(HologresDataTypeName.TIMESTAMP.getValue(), ReverseContext.builder().build());
        IDataTypeName typeName = baseDataType.getTypeName();
        assertNotNull(typeName);
        assertEquals(typeName, HologresDataTypeName.TIMESTAMP);
    }

    @Test
    public void testDoublePrecision() {
        CreateTable o = hologresParser2.parseNode("create table a (b double precision, c float8);");
        assertNotNull(o);
        IDataTypeName typeName = o.getColumnDefines().get(0).getDataType().getTypeName();
        assertEquals(typeName, HologresDataTypeName.DOUBLE_PRECISION);
        IDataTypeName typeName1 = o.getColumnDefines().get(1).getDataType().getTypeName();
        assertEquals(typeName1.getValue(), HologresDataTypeName.DOUBLE_PRECISION.getValue());
    }

    @Test
    public void testParse() {
        HologresDataTypeName[] hologresDataTypeNames = HologresDataTypeName.values();
        for (HologresDataTypeName hologresDataTypeName : hologresDataTypeNames) {
            BaseDataType baseDataType = hologresParser2.parseDataType(hologresDataTypeName.getValue(), ReverseContext.builder().build());
            Preconditions.checkNotNull(baseDataType, "dataType can not be null:" + hologresDataTypeName.getValue());
            IDataTypeName dataTypeName = baseDataType.getTypeName();
            assertEquals(dataTypeName.getValue(), hologresDataTypeName.getValue());
        }
    }
}