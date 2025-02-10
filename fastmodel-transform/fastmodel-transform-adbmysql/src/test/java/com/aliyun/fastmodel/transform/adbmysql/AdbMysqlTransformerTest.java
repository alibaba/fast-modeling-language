package com.aliyun.fastmodel.transform.adbmysql;

import java.nio.charset.Charset;
import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.adbmysql.context.AdbMysqlTransformContext;
import com.aliyun.fastmodel.transform.adbmysql.format.AdbMysqlPropertyKey;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeNonKeyConstraint;
import com.google.common.collect.Lists;
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
public class AdbMysqlTransformerTest {

    AdbMysqlTransformer adbMysqlTransformer = new AdbMysqlTransformer();

    @Test
    public void reverse() {
        BaseStatement reverse = adbMysqlTransformer.reverse(new DialectNode(" CREATE TABLE CUSTOMER (\n"
            + "    CUSTOMER_ID BIGINT NOT NULL COMMENT '顾客ID',\n"
            + "    CUSTOMER_NAME VARCHAR NOT NULL COMMENT '顾客姓名',\n"
            + "    PHONE_NUM BIGINT NOT NULL COMMENT '电话',\n"
            + "    CITY_NAME VARCHAR NOT NULL COMMENT '所属城市',\n"
            + "    SEX INT NOT NULL COMMENT '性别',\n"
            + "    ID_NUMBER VARCHAR NOT NULL COMMENT '身份证号码',\n"
            + "    HOME_ADDRESS VARCHAR NOT NULL COMMENT '家庭住址',\n"
            + "    OFFICE_ADDRESS VARCHAR NOT NULL COMMENT '办公地址',\n"
            + "    AGE INT NOT NULL COMMENT '年龄',\n"
            + "    LOGIN_TIME TIMESTAMP NOT NULL COMMENT '登录时间',\n"
            + "    PRIMARY KEY (LOGIN_TIME,CUSTOMER_ID,PHONE_NUM)\n"
            + " )\n"
            + "     DISTRIBUTED BY HASH(CUSTOMER_ID)\n"
            + "     PARTITION BY VALUE(DATE_FORMAT(LOGIN_TIME, '%Y%M%D')) LIFECYCLE 30\n"
            + "     COMMENT '客户信息表'; "));
        assertNotNull(reverse);
    }

    @Test
    public void testTransform() {
        List<ColumnDefinition> columns = Lists.newArrayList(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType("bigint", Lists.newArrayList()))
                .build()
        );
        BaseStatement source = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns).build();
        DialectNode transform = adbMysqlTransformer.transform(source);
        assertEquals(transform.getNode(), "CREATE TABLE abc\n"
            + "(\n"
            + "   c1 BIGINT\n"
            + ")");
    }

    @Test
    public void testTransformDistribute() {
        AdbMysqlTransformContext adbMysqlTransformContext = AdbMysqlTransformContext.builder().build();
        List<Property> properties = Lists.newArrayList();
        properties.add(new Property(AdbMysqlPropertyKey.STORAGE_POLICY.getValue(), "HOT"));
        properties.add(new Property(AdbMysqlPropertyKey.LIFE_CYCLE.getValue(), "10"));
        properties.add(new Property(AdbMysqlPropertyKey.BLOCK_SIZE.getValue(), "10"));
        List<ColumnDefinition> columns = Lists.newArrayList();
        ColumnDefinition column = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType("bigint", null))
            .build();
        columns.add(column);
        List<BaseConstraint> constraints = Lists.newArrayList();
        DistributeNonKeyConstraint id = new DistributeNonKeyConstraint(Lists.newArrayList(new Identifier("id")), null);
        constraints.add(id);
        BaseStatement source = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .constraints(constraints)
            .partition(new PartitionedBy(columns))
            .properties(properties)
            .build();
        DialectNode transform = adbMysqlTransformer.transform(source, adbMysqlTransformContext);
        assertEquals(transform.getNode(), "CREATE TABLE abc\n"
            + "(\n"
            + "   c1 BIGINT\n"
            + ")\n"
            + "DISTRIBUTED BY HASH(id)\n"
            + "PARTITION BY VALUE(c1) LIFECYCLE 10\n"
            + "STORAGE_POLICY='HOT'\n"
            + "BLOCK_SIZE=10");
    }

    @Test
    @SneakyThrows
    public void testTransformTable() {
        AdbMysqlTransformer adbMysqlTransformer = new AdbMysqlTransformer();
        String s = IOUtils.resourceToString("/adbmysql/external/hudi.txt", Charset.defaultCharset());
        String result = generator(new DialectNode(s), adbMysqlTransformer);
        assertEquals("CREATE EXTERNAL TABLE IF NOT EXISTS adb_external_demo.osstest2\n"
            + "(\n"
            + "   id   INT,\n"
            + "   name VARCHAR(128),\n"
            + "   age  INT,\n"
            + "   city VARCHAR(128)\n"
            + ")\n"
            + "STORED AS HUDI\n"
            + "LOCATION 'oss://testBucketName/osstest/test'\n"
            + "TBLPROPERTIES('type'='cow')", result);
    }

    @Test
    @SneakyThrows
    public void testTransformTableMaxcompute() {
        AdbMysqlTransformer adbMysqlTransformer = new AdbMysqlTransformer();
        String s = IOUtils.resourceToString("/adbmysql/external/maxcompute.txt", Charset.defaultCharset());
        String result = generator(new DialectNode(s), adbMysqlTransformer);
        assertEquals("CREATE EXTERNAL TABLE IF NOT EXISTS adb_external_demo.mctest\n"
            + "(\n"
            + "   id   INT,\n"
            + "   name VARCHAR(1023),\n"
            + "   age  INT,\n"
            + "   dt   VARCHAR(128)\n"
            + ")\n"
            + "ENGINE='ODPS'\n"
            + "TABLE_PROPERTIES='{\n"
            + "\t\"accessid\":\"LTAILd4****\",\n"
            + "\t\"endpoint\":\"http://service.cn-hangzhou.maxcompute.aliyun.com/api\",\n"
            + "\t\"accesskey\":\"4A5Q7ZVzcYnWMQPysX****\",\n"
            + "\t\"partition_column\":\"dt\",\n"
            + "\t\"project_name\":\"test_adb\",\n"
            + "\t\"table_name\":\"person\"\n"
            + "}'", result);
    }

    @Test
    @SneakyThrows
    public void testTransformTableMongodb() {
        AdbMysqlTransformer adbMysqlTransformer = new AdbMysqlTransformer();
        String s = IOUtils.resourceToString("/adbmysql/external/mongodb.txt", Charset.defaultCharset());
        String result = generator(new DialectNode(s), adbMysqlTransformer);
        assertEquals("CREATE EXTERNAL TABLE adb_external_demo.person\n"
            + "(\n"
            + "   id  INT,\n"
            + "   age INT\n"
            + ")\n"
            + "ENGINE='MONGODB'\n"
            + "TABLE_PROPERTIES='{\n"
            + "\"mapped_name\":\"person\",\n"
            + "\"location\":\"mongodb://testuser:****@dds-bp113d414bca8****.mongodb.rds.aliyuncs.com:3717,dds-bp113d414bca8****.mongodb.rds"
            + ".aliyuncs.com:3717/test_mongodb\",\n"
            + "\"username\":\"testuser\",\n"
            + "\"password\":\"password\",\n"
            + "}'", result);
    }

    @Test
    @SneakyThrows
    public void testTransformTableTextFile() {
        AdbMysqlTransformer adbMysqlTransformer = new AdbMysqlTransformer();
        String s = IOUtils.resourceToString("/adbmysql/external/textfile.txt", Charset.defaultCharset());
        String result = generator(new DialectNode(s), adbMysqlTransformer);
        assertEquals("CREATE EXTERNAL TABLE IF NOT EXISTS adb_external_demo.osstest1\n"
            + "(\n"
            + "   id   INT,\n"
            + "   name VARCHAR(128),\n"
            + "   age  INT,\n"
            + "   city VARCHAR(128)\n"
            + ")\n"
            + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','\n"
            + "STORED AS TEXTFILE\n"
            + "LOCATION 'oss://testBucketName/osstest/p1=hangzhou/p2=2023-06-13/data.csv'", result);
    }

    @Test
    @SneakyThrows
    public void testTransformTableOts() {
        AdbMysqlTransformer adbMysqlTransformer = new AdbMysqlTransformer();
        String s = IOUtils.resourceToString("/adbmysql/external/ots.txt", Charset.defaultCharset());
        String result = generator(new DialectNode(s), adbMysqlTransformer);
        assertEquals("CREATE EXTERNAL TABLE IF NOT EXISTS adb_external_demo.otstest\n"
            + "(\n"
            + "   id   INT,\n"
            + "   name VARCHAR(128),\n"
            + "   age  INT\n"
            + ")\n"
            + "ENGINE='OTS'\n"
            + "TABLE_PROPERTIES='{\n"
            + "\t\"mapped_name\":\"person\",\n"
            + "\t\"location\":\"https://w0****la.cn-hangzhou.vpc.tablestore.aliyuncs.com\"\n"
            + "}'", result);
    }

    @Test
    @SneakyThrows
    public void testTransformTableRds() {
        AdbMysqlTransformer adbMysqlTransformer = new AdbMysqlTransformer();
        String s = IOUtils.resourceToString("/adbmysql/external/rds.txt", Charset.defaultCharset());
        String result = generator(new DialectNode(s), adbMysqlTransformer);
        assertEquals("CREATE EXTERNAL TABLE IF NOT EXISTS adb_external_demo.mysqltest\n"
            + "(\n"
            + "   id   INT,\n"
            + "   name VARCHAR(1023),\n"
            + "   age  INT\n"
            + ")\n"
            + "ENGINE='MYSQL'\n"
            + "TABLE_PROPERTIES='{\n"
            + "   \"url\":\"jdbc:mysql://rm-bp1gx6h1tyd04****.mysql.rds.aliyuncs.com:3306/test_adb\",\n"
            + "   \"tablename\":\"person\",\n"
            + "   \"username\":\"testUserName\",\n"
            + "   \"password\":\"testUserPassword\",\n"
            + "   \"charset\":\"utf8\"\n"
            + "}'", result);
    }

    @Test
    @SneakyThrows
    public void testTransformAnn() {
        AdbMysqlTransformer adbMysqlTransformer = new AdbMysqlTransformer();
        String s = IOUtils.resourceToString("/adbmysql/index/ann.txt", Charset.defaultCharset());
        String result = generator(new DialectNode(s), adbMysqlTransformer);
        assertEquals("CREATE TABLE vector\n"
            + "(\n"
            + "   xid           BIGINT NOT NULL,\n"
            + "   cid           BIGINT NOT NULL,\n"
            + "   uid           VARCHAR NOT NULL,\n"
            + "   vid           VARCHAR NOT NULL,\n"
            + "   wid           VARCHAR NOT NULL,\n"
            + "   float_feature ARRAY<FLOAT>(4),\n"
            + "   short_feature ARRAY<SMALLINT>(4),\n"
            + "   PRIMARY KEY(xid,cid,vid),\n"
            + "   ANN INDEX idx_short_feature(short_feature),\n"
            + "   ANN INDEX idx_float_feature(float_feature)\n"
            + ")\n"
            + "DISTRIBUTED BY HASH(xid)", result);
    }

    @Test
    @SneakyThrows
    public void testJson2() {
        AdbMysqlTransformer adbMysqlTransformer = new AdbMysqlTransformer();
        String s = IOUtils.resourceToString("/adbmysql/index/json2.txt", Charset.defaultCharset());
        String result = generator(new DialectNode(s), adbMysqlTransformer);
        assertEquals("CREATE TABLE json_test\n"
            + "(\n"
            + "   id INT,\n"
            + "   vj JSON,\n"
            + "   INDEX idx_vj(vj)\n"
            + ")\n"
            + "DISTRIBUTED BY HASH(id)", result);
    }

    @Test
    @SneakyThrows
    public void testJson() {
        AdbMysqlTransformer adbMysqlTransformer = new AdbMysqlTransformer();
        String s = IOUtils.resourceToString("/adbmysql/index/json.txt", Charset.defaultCharset());
        String result = generator(new DialectNode(s), adbMysqlTransformer);
        assertEquals("CREATE TABLE json_test\n"
            + "(\n"
            + "   id INT,\n"
            + "   vj JSON,\n"
            + "   INDEX idx_vj_array(vj->'$[*]')\n"
            + ")\n"
            + "DISTRIBUTED BY HASH(id)", result);
    }

    @Test
    @SneakyThrows
    public void testClusterKey() {
        AdbMysqlTransformer adbMysqlTransformer = new AdbMysqlTransformer();
        String s = IOUtils.resourceToString("/adbmysql/index/cluster.txt", Charset.defaultCharset());
        String result = generator(new DialectNode(s), adbMysqlTransformer);
        assertEquals("CREATE TABLE clustered_test\n"
            + "(\n"
            + "   id INT,\n"
            + "   vj JSON,\n"
            + "   CLUSTERED KEY (id)\n"
            + ")\n"
            + "DISTRIBUTED BY HASH(id)", result);
    }

    @Test
    @SneakyThrows
    public void testForeignKey() {
        AdbMysqlTransformer adbMysqlTransformer = new AdbMysqlTransformer();
        String s = IOUtils.resourceToString("/adbmysql/index/foreign.txt", Charset.defaultCharset());
        String result = generator(new DialectNode(s), adbMysqlTransformer);
        assertEquals("CREATE TABLE store_returns\n"
            + "(\n"
            + "   sr_sale_id  BIGINT,\n"
            + "   sr_store_sk BIGINT,\n"
            + "   sr_item_sk  BIGINT NOT NULL,\n"
            + "   FOREIGN KEY(sr_item_sk) REFERENCES item(i_item_sk)\n"
            + ")", result);
    }

    @Test
    @SneakyThrows
    public void testFullText() {
        AdbMysqlTransformer adbMysqlTransformer = new AdbMysqlTransformer();
        String s = IOUtils.resourceToString("/adbmysql/index/fulltext.txt", Charset.defaultCharset());
        String result = generator(new DialectNode(s), adbMysqlTransformer);
        assertEquals("CREATE TABLE tbl_fulltext_demo\n"
            + "(\n"
            + "   id               INT NOT NULL,\n"
            + "   content          VARCHAR,\n"
            + "   content_alinlp   VARCHAR,\n"
            + "   content_ik       VARCHAR,\n"
            + "   content_standard VARCHAR,\n"
            + "   content_ngram    VARCHAR,\n"
            + "   PRIMARY KEY(id),\n"
            + "   FULLTEXT INDEX fidx_c(`content`),\n"
            + "   FULLTEXT INDEX fidx_alinlp(`content_alinlp`) WITH ANALYZER alinlp,\n"
            + "   FULLTEXT INDEX fidx_ik(`content_ik`) WITH ANALYZER ik,\n"
            + "   FULLTEXT INDEX fidx_standard(`content_standard`) WITH ANALYZER standard,\n"
            + "   FULLTEXT INDEX fidx_ngram(`content_ngram`) WITH ANALYZER ngram\n"
            + ")\n"
            + "DISTRIBUTED BY HASH(id)", result);
    }

    @Test
    @SneakyThrows
    public void testGeneratorIssue2() {
        AdbMysqlTransformer adbMysqlTransformer = new AdbMysqlTransformer();
        String s = IOUtils.resourceToString("/adbmysql/issue2.txt", Charset.defaultCharset());
        String result = generator(new DialectNode(s), adbMysqlTransformer);
        assertEquals("CREATE TABLE adb_external_tpch_10gb.external_customer\n"
            + "(\n"
            + "   c_custkey    INT NOT NULL COMMENT '',\n"
            + "   c_name       VARCHAR(1024) NOT NULL COMMENT '',\n"
            + "   c_address    VARCHAR(1024) NOT NULL COMMENT '',\n"
            + "   c_nationkey  INT NOT NULL COMMENT '',\n"
            + "   c_phone      VARCHAR(15) NOT NULL COMMENT '',\n"
            + "   c_acctbal    DECIMAL(15,2) NOT NULL COMMENT '',\n"
            + "   c_mktsegment VARCHAR(10) NOT NULL COMMENT '',\n"
            + "   c_comment    VARCHAR(1024) NOT NULL COMMENT '',\n"
            + "   dummy        VARCHAR(1024)\n"
            + ")\n"
            + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'\n"
            + "STORED AS TEXTFILE\n"
            + "LOCATION 'oss://dataset-cn-shanghai-external/customer/'", result);
    }

    @Test
    @SneakyThrows
    public void testSimpleWithDb() {
        String ss = IOUtils.resourceToString("/adbmysql/simple_db.txt", Charset.defaultCharset());
        String result = generator(new DialectNode(ss), adbMysqlTransformer);
        assertEquals("CREATE TABLE db.customer\n"
            + "(\n"
            + "   customer_id    BIGINT NOT NULL COMMENT '顾客ID',\n"
            + "   customer_name  VARCHAR NOT NULL COMMENT '顾客姓名',\n"
            + "   phone_num      BIGINT NOT NULL COMMENT '电话',\n"
            + "   city_name      VARCHAR NOT NULL COMMENT '所属城市',\n"
            + "   sex            INT NOT NULL COMMENT '性别',\n"
            + "   id_number      VARCHAR NOT NULL COMMENT '身份证号码',\n"
            + "   home_address   VARCHAR NOT NULL COMMENT '家庭住址',\n"
            + "   office_address VARCHAR NOT NULL COMMENT '办公地址',\n"
            + "   age            INT NOT NULL COMMENT '年龄',\n"
            + "   login_time     TIMESTAMP NOT NULL COMMENT '登录时间',\n"
            + "   PRIMARY KEY(login_time,customer_id,phone_num)\n"
            + ")\n"
            + "DISTRIBUTED BY HASH(customer_id)\n"
            + "PARTITION BY VALUE(date_format(login_time, '%Y%m%d')) LIFECYCLE 30\n"
            + "COMMENT '客户信息表'", result);
    }

    private String generator(DialectNode dialectNode, Transformer transformer) {
        ReverseContext build = ReverseContext.builder().merge(true).build();
        Node node = transformer.reverse(dialectNode, build);
        Table table = transformer.transformTable(node, TransformContext.builder().build());
        Node reverseNode = transformer.reverseTable(table);
        return transformer.transform(reverseNode, TransformContext.builder().build()).getNode();
    }

}