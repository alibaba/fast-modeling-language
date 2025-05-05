package com.aliyun.fastmodel.transform.adbmysql.parser;

import java.nio.charset.Charset;
import java.util.List;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.ClusteredKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.ForeignKeyConstraint;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/10
 */
public class AdbMysqlLanguageParserTest {

    AdbMysqlLanguageParser adbMysqlLanguageParser = new AdbMysqlLanguageParser();

    @Test
    public void parseNode() {
        Node node = adbMysqlLanguageParser.parseNode("create table a (b int not null comment 'abc');");
        assertNotNull(node);
    }

    @Test
    public void testParseDistribute() {
        CreateTable node = adbMysqlLanguageParser.parseNode("CREATE TABLE test (\n"
            + "       id bigint auto_increment,\n"
            + "       name varchar,\n"
            + "       value int,\n"
            + "       ts timestamp\n"
            + ")\n"
            + "DISTRIBUTED BY HASH(id);");
        List<ColumnDefinition> columnDefines = node.getColumnDefines();
        assertEquals(4, columnDefines.size());
    }

    @Test
    public void testParsePartitionBy() {
        CreateTable node = adbMysqlLanguageParser.parseNode("CREATE TABLE customer (\n"
            + "customer_id bigint NOT NULL COMMENT '顾客ID',\n"
            + "customer_name varchar NOT NULL COMMENT '顾客姓名',\n"
            + "phone_num bigint NOT NULL COMMENT '电话',\n"
            + "city_name varchar NOT NULL COMMENT '所属城市',\n"
            + "sex int NOT NULL COMMENT '性别',\n"
            + "id_number varchar NOT NULL COMMENT '身份证号码',\n"
            + "home_address varchar NOT NULL COMMENT '家庭住址',\n"
            + "office_address varchar NOT NULL COMMENT '办公地址',\n"
            + "age int NOT NULL COMMENT '年龄',\n"
            + "login_time timestamp NOT NULL COMMENT '登录时间',\n"
            + "PRIMARY KEY (login_time,customer_id,phone_num)\n"
            + " )\n"
            + "DISTRIBUTED BY HASH(customer_id)\n"
            + "PARTITION BY VALUE(DATE_FORMAT(login_time, '%Y%m%d')) LIFECYCLE 30\n"
            + "COMMENT '客户信息表';  ");
        assertEquals(node.getCommentValue(), "客户信息表");
        List<Property> properties = node.getProperties();
        assertEquals(1, properties.size());
        ColumnDefinition customerId = node.getColumn(new Identifier("customer_id"));
        assertEquals("BIGINT", customerId.getDataType().getTypeName().getValue());
        assertEquals(2, node.getConstraintStatements().size());
    }

    @Test
    @SneakyThrows
    public void testParseIssue() {
        String code = IOUtils.resourceToString("/adbmysql/issue.txt", Charset.defaultCharset());
        CreateTable o = adbMysqlLanguageParser.parseNode(code);
        assertEquals(o.getCommentValue(), "配置表");
    }

    @Test(expected = ClassCastException.class)
    public void testParseWithComment() {
        Node o = adbMysqlLanguageParser.parseNode("SELECT * from abc;\n"
            + " --abc");
        assertNotNull(o);
    }

    @Test
    public void testParseExpression() {
        FunctionCall o = adbMysqlLanguageParser.parseExpression("date_format(create_time, '%Y%M%d')");
        assertEquals("date_format", o.getFuncName().toString());
    }

    @Test
    public void testClusterKey() {
        String sql = "create table t (id bigint not null, name varchar(200), clustered key (name)) comment 'abc';";
        CreateTable o = adbMysqlLanguageParser.parseNode(sql);
        assertEquals(o.getCommentValue(), "abc");
        assertEquals(1, o.getConstraintStatements().size());
        List<BaseConstraint> constraintStatements = o.getConstraintStatements();
        ClusteredKeyConstraint clusterKeyConstraint = (ClusteredKeyConstraint)constraintStatements.get(0);
        assertEquals(1, clusterKeyConstraint.getColumns().size());
    }

    @Test
    public void testForeignKey() {
        String sql = "CREATE TABLE store_returns\n"
            + "(\n"
            + "  sr_sale_id bigint,\n"
            + "  sr_store_sk bigint,\n"
            + "  sr_item_sk bigint NOT NULL,\n"
            + "  FOREIGN KEY (sr_item_sk) REFERENCES item (i_item_sk)\n"
            + ");";
        CreateTable createTable = adbMysqlLanguageParser.parseNode(sql);
        List<BaseConstraint> constraintStatements = createTable.getConstraintStatements();
        assertEquals(1, constraintStatements.size());
        BaseConstraint baseConstraint = constraintStatements.get(0);
        ForeignKeyConstraint foreignKeyConstraint = (ForeignKeyConstraint)baseConstraint;
        List<Identifier> colNames = foreignKeyConstraint.getColNames();
        assertEquals(1, colNames.size());
        List<Identifier> referenceColNames = foreignKeyConstraint.getReferenceColNames();
        assertEquals(1, referenceColNames.size());
    }

    @Test
    public void testAnnKeyConstraint() {
        String sql = "CREATE TABLE vector (\n"
            + "  xid bigint not null,\n"
            + "  cid bigint not null,\n"
            + "  uid varchar not null,\n"
            + "  vid varchar not null,\n"
            + "  wid varchar not null,\n"
            + "  float_feature array<float>(4),\n"
            + "  short_feature array<smallint>(4),\n"
            + "  ANN INDEX idx_short_feature(short_feature),\n"
            + "  ANN INDEX idx_float_feature(float_feature),\n"
            + "  PRIMARY KEY (xid, cid, vid)\n"
            + ") DISTRIBUTED BY HASH(xid);";
        CreateTable createTable = adbMysqlLanguageParser.parseNode(sql);
        List<BaseConstraint> constraintStatements = createTable.getConstraintStatements();
        assertEquals(2, constraintStatements.size());
        List<TableIndex> tableIndexList = createTable.getTableIndexList();
        assertEquals(2, tableIndexList.size());
    }

    @Test
    public void testAnnKey2() {
        String sql = "CREATE TABLE vector (\n"
            + "  xid bigint not null,\n"
            + "  cid bigint not null,\n"
            + "  uid varchar not null,\n"
            + "  vid varchar not null,\n"
            + "  wid varchar not null,\n"
            + "  float_feature array<float>(4),\n"
            + "  short_feature array<smallint>(4),\n"
            + "  ANN INDEX idx_short_feature(short_feature),\n"
            + "  ANN INDEX idx_float_feature(float_feature),\n"
            + "  PRIMARY KEY (xid, cid, vid)\n"
            + ") DISTRIBUTED BY HASH(xid);";
        CreateTable createTable = adbMysqlLanguageParser.parseNode(sql);
        assertEquals(2, createTable.getConstraintStatements().size());
    }

    @Test
    public void testJsonIndex() {
        String sql = "CREATE TABLE json_test(\n"
            + "  id int,\n"
            + "  vj json,\n"
            + "  index idx_vj(vj)\n"
            + ")\n"
            + "DISTRIBUTED BY HASH(id);";
        CreateTable createTable = adbMysqlLanguageParser.parseNode(sql);
        List<BaseConstraint> constraintStatements = createTable.getConstraintStatements();
        assertEquals(1, constraintStatements.size());
    }

    @Test
    public void testJson2Index() {
        String sql = "CREATE TABLE json_test(\n"
            + "  id int,\n"
            + "  vj json, \n"
            + "  index idx_vj_path(vj->'$.name')\n"
            + ")\n"
            + "DISTRIBUTED BY HASH(id);";
        CreateTable createTable = adbMysqlLanguageParser.parseNode(sql);
        List<BaseConstraint> constraintStatements = createTable.getConstraintStatements();
        assertEquals(1, constraintStatements.size());
        List<TableIndex> tableIndexList = createTable.getTableIndexList();
        assertEquals(1, tableIndexList.size());
        TableIndex tableIndex = tableIndexList.get(0);
        Identifier indexName = tableIndex.getIndexName();
        assertEquals("idx_vj_path", indexName.getValue());
    }

    @Test
    public void testExternalTable() {
        String sql = "CREATE EXTERNAL TABLE IF NOT EXISTS adb_external_demo.osstest2\n"
            + "(id int,\n"
            + "name string,\n"
            + "age int,\n"
            + "city string)\n"
            + "STORED AS HUDI\n"
            + "LOCATION  'oss://testBucketName/osstest/test'\n"
            + "TBLPROPERTIES ('type' = 'cow');";
        CreateTable createTable = adbMysqlLanguageParser.parseNode(sql);
        List<Property> properties = createTable.getProperties();
        assertEquals("HUDI", properties.get(0).getValue());
    }

    @Test
    @SneakyThrows
    public void testExternalMaxcomputeTable() {
        String sql = IOUtils.resourceToString("/adbmysql/external/maxcompute.txt", Charset.defaultCharset());
        CreateTable createTable = adbMysqlLanguageParser.parseNode(sql);
        List<Property> properties = createTable.getProperties();
        assertEquals("ODPS", properties.get(0).getValue());

    }

    @Test
    @SneakyThrows
    public void testExternalMongodbTable() {
        String sql = IOUtils.resourceToString("/adbmysql/external/mongodb.txt", Charset.defaultCharset());
        CreateTable createTable = adbMysqlLanguageParser.parseNode(sql);
        List<Property> properties = createTable.getProperties();
        assertEquals("MONGODB", properties.get(0).getValue());
        assertEquals(3, properties.size());
    }

    @Test
    @SneakyThrows
    public void testExternalTextFile() {
        String sql = IOUtils.resourceToString("/adbmysql/external/textfile.txt", Charset.defaultCharset());
        CreateTable createTable = adbMysqlLanguageParser.parseNode(sql);
        List<Property> properties = createTable.getProperties();
        assertEquals(4, properties.size());
    }

    @Test
    @SneakyThrows
    public void testParseIssue2() {
        String sql = IOUtils.resourceToString("/adbmysql/issue2.txt", Charset.defaultCharset());
        CreateTable createTable = adbMysqlLanguageParser.parseNode(sql);
        List<Property> properties = createTable.getProperties();
        assertEquals(3, properties.size());
    }
}