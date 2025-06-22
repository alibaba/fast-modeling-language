/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser;

import java.nio.charset.Charset;
import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.SetColComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.HologresTransformer;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresArrayDataTypeName;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresDataTypeName;
import com.aliyun.fastmodel.transform.hologres.parser.tree.expr.WithDataTypeNameExpression;
import com.google.common.base.Preconditions;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
    public void parseNode2() {
        String sql = "BEGIN;\n"
            + "\n"
            + "/*\n"
            + "DROP DEFAULT aaaa.all_type.c14;\n"
            + "DROP DEFAULT aaaa.all_type.c13;\n"
            + "\n"
            + "DROP TABLE aaaa.all_type;\n"
            + "*/\n"
            + "\n"
            + "-- Type: TABLE ; Name: all_type; Owner: 1107550004253538\n"
            + "\n"
            + "CREATE TABLE aaaa.all_type (\n"
            + "    c1 text,\n"
            + "    c2 bigint,\n"
            + "    c3 boolean,\n"
            + "    c4 real,\n"
            + "    c5 double precision,\n"
            + "    c7 timestamp with time zone,\n"
            + "    c8 numeric(10,2),\n"
            + "    c9 date,\n"
            + "    c10 timestamp without time zone,\n"
            + "    c11 character(1),\n"
            + "    c12 character varying,\n"
            + "    c13 integer NOT NULL default nextval('aaaa.all_type_c13_seq'::regclass),\n"
            + "    c14 bigint NOT NULL default nextval('aaaa.all_type_c14_seq'::regclass),\n"
            + "    c15 smallint,\n"
            + "    c16 json,\n"
            + "    c17 jsonb,\n"
            + "    c18 bytea,\n"
            + "    c19 roaringbitmap,\n"
            + "    c20 bit(1),\n"
            + "    c21 time with time zone,\n"
            + "    c22 time without time zone,\n"
            + "    c23 inet,\n"
            + "    c24 money,\n"
            + "    c25 interval,\n"
            + "    c26 oid,\n"
            + "    c27 uuid\n"
            + ")with (\n"
            + "orientation = 'column',\n"
            + "storage_format = 'orc',\n"
            + "bitmap_columns = 'c1,c6,c11,c12',\n"
            + "dictionary_encoding_columns = 'c1:auto,c6:auto,c11:auto,c12:auto',\n"
            + "table_group = 'dw01_tg_default',\n"
            + "table_storage_mode = 'any',\n"
            + "time_to_live_in_seconds = '3153600000'\n"
            + ");\n"
            + "\n"
            + "\n"
            + "\n"
            + "COMMENT ON TABLE aaaa.all_type IS 'table comment';\n"
            + "ALTER TABLE aaaa.all_type OWNER TO \"1107550004253538\";\n"
            + "COMMENT ON COLUMN aaaa.all_type.c1 IS 'id';\n"
            + "\n"
            + "\n"
            + "\n"
            + "COMMIT;";
        CompositeStatement compositeStatement = hologresParser2.parseNode(sql);
        assertEquals(5, compositeStatement.getStatements().size());
        CreateTable createTable = (CreateTable)compositeStatement.getStatements().get(1);
        ColumnDefinition columnDefinition = createTable.getColumnDefines().get(0);
        assertEquals(columnDefinition.getDataType().getTypeName(), HologresDataTypeName.TEXT);

        DialectNode dialectNode = new DialectNode(sql);
        HologresTransformer transformer = new HologresTransformer();
        ReverseContext build = ReverseContext.builder().merge(true).build();
        Node reverse = transformer.reverse(dialectNode, build);
        assertNotNull(reverse);

        TransformContext transformContext = HologresTransformContext.builder().build();
        TableConfig config = TableConfig.builder().dialectMeta(DialectMeta.getHologres()).build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(transformer.transformTable(reverse, transformContext))
            .config(config)
            .build();
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertNotNull(dialectNodes);
    }

    @Test
    public void parseNode3() {
        CompositeStatement compositeStatement = hologresParser2.parseNode(
            "BEGIN;"
                + "CREATE TABLE public.user_info ("
                + "    id integer NOT NULL default nextval('user_info_id_seq'::regclass),"
                + "    username character varying(50) NOT NULL default custom_function('xxxx'),"
                + "    password character varying(50) NOT NULL,"
                + "    email character varying(100) NOT NULL,"
                + "    created_at timestamp with time zone NOT NULL default CURRENT_TIMESTAMP    ,"
                + "PRIMARY KEY (id))"
                + "with (orientation = 'column',storage_format = 'orc',bitmap_columns = 'username,password,email',dictionary_encoding_columns = "
                + "'username:auto,password:auto,email:auto',distribution_key = 'id',segment_key = 'created_at',table_group = 'holo_db_tg_default',"
                + "table_storage_mode = 'any',time_to_live_in_seconds = '3153600000');"
                + "COMMENT ON TABLE public.user_info IS NULL;"
                + "ALTER TABLE public.user_info OWNER TO holo_db_developer;"
                + "END;");
        assertEquals(3, compositeStatement.getStatements().size());
        CreateTable createTable = (CreateTable)compositeStatement.getStatements().get(1);
        ColumnDefinition columnDefinition = createTable.getColumnDefines().get(0);
        assertEquals(columnDefinition.getDataType().getTypeName(), HologresDataTypeName.INTEGER);
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

    @Test
    public void testParseDefaultValue() {
        CreateTable o = hologresParser2.parseNode("CREATE TABLE tbl_default (    \n"
            + "  smallint_col smallint DEFAULT 0,    \n"
            + "  int_col int DEFAULT 0,    \n"
            + "  bigint_col bigint DEFAULT 0,    \n"
            + "  boolean_col boolean DEFAULT FALSE,    \n"
            + "  float_col real DEFAULT 0.0,    \n"
            + "  double_col double precision DEFAULT 0.0,    \n"
            + "  decimal_col decimal(2, 1) DEFAULT 0.0,    \n"
            + "  text_col text DEFAULT 'N',    \n"
            + "  char_col char(2) DEFAULT 'N',    \n"
            + "  varchar_col varchar(200) DEFAULT 'N',    \n"
            + "  timestamptz_col timestamptz DEFAULT now(),    \n"
            + "  date_col date DEFAULT now(),    \n"
            + "  timestamp_col timestamp DEFAULT now()\n"
            + ");\n");
        List<ColumnDefinition> columnDefines = o.getColumnDefines();
        ColumnDefinition columnDefinition = columnDefines.get(0);
        BaseExpression defaultValue = columnDefinition.getDefaultValue();
        assertEquals(defaultValue.getOrigin(), "0");
    }

    @Test
    @SneakyThrows
    public void testParseDefaultValueType() {
        String value = IOUtils.resourceToString("/hologres/default_text.txt", Charset.defaultCharset());
        CreateTable createTable = (CreateTable)hologresParser2.parseNode(value, ReverseContext.builder().merge(true).build());
        List<ColumnDefinition> columnDefines = createTable.getColumnDefines();
        BaseExpression defaultValue = columnDefines.get(0).getDefaultValue();
        WithDataTypeNameExpression with = (WithDataTypeNameExpression)defaultValue;
        assertEquals("TEXT", with.getBaseDataType().getTypeName().getValue());
    }

    @Test
    public void testParseExpr() {
        WithDataTypeNameExpression o = hologresParser2.parseExpression("'1'::text");
        assertEquals("TEXT", o.getBaseDataType().getTypeName().getValue());
    }

    @Test
    @SneakyThrows
    public void testParseNpe() {
        String value = IOUtils.resourceToString("/hologres/npe.txt", Charset.defaultCharset());
        CompositeStatement createTable = (CompositeStatement)hologresParser2.parseNode(value, ReverseContext.builder().merge(true).build());
        assertEquals(7, createTable.getChildren().size());
    }

    @Test
    @SneakyThrows
    public void testDefaultValue() {
        String value = IOUtils.resourceToString("/hologres/reverse_default_value.txt", Charset.defaultCharset());
        CompositeStatement node = (CompositeStatement)hologresParser2.parseNode(value, ReverseContext.builder().build());
        assertEquals(2, node.getStatements().size());
        List<BaseStatement> children = node.getChildren();
        CreateTable createTable = (CreateTable)children.get(0);
        List<ColumnDefinition> columnDefines = createTable.getColumnDefines();
        BaseExpression defaultValue = columnDefines.get(0).getDefaultValue();
        assertNotNull(defaultValue);
    }

    @Test
    @SneakyThrows
    public void testDynamicTable() {
        String v = IOUtils.resourceToString("/hologres/dynamic/dynamic_table.txt", Charset.defaultCharset());
        CreateTable c = (CreateTable)hologresParser2.parseNode(v, ReverseContext.builder().build());
        assertEquals("public.tpch_q1_incremental", c.getQualifiedName().toString());
        List<ColumnDefinition> columnDefines = c.getColumnDefines();
        assertTrue(columnDefines.size() > 0);
        PartitionedBy partitionedBy = c.getPartitionedBy();
        assertEquals(1, partitionedBy.getColumnDefinitions().size());
        List<Property> properties = c.getProperties();
        assertEquals(8, properties.size());
    }

    @Test
    @SneakyThrows
    public void testMerge() {
        String txt = IOUtils.resourceToString("/hologres/merge.txt", Charset.defaultCharset());
        CompositeStatement createTable = (CompositeStatement)hologresParser2.parseNode(txt, ReverseContext.builder().merge(true).build());
        List<BaseStatement> statements = createTable.getStatements();
        assertEquals(654, statements.size());
    }

    @Test
    public void testParseExpressionWithNextValue() {
        String code = "nextval('aaaa.all_type_c13_seq'::regclass)";
        BaseExpression expr = hologresParser2.parseExpression(code);
        assertEquals("com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall", expr.getClass().getName());
    }
}