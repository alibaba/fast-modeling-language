package com.aliyun.fastmodel.transform.adbmysql.parser;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
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
            + "PRIMARY KEY (login_time,customer_id，phone_num)\n"
            + " )\n"
            + "DISTRIBUTED BY HASH(customer_id)\n"
            + "PARTITION BY VALUE(DATE_FORMAT(login_time, '%Y%m%d')) LIFECYCLE 30\n"
            + "COMMENT '客户信息表';  ");
        assertEquals(node.getCommentValue(), "客户信息表");
        List<Property> properties = node.getProperties();
        assertEquals(2, properties.size());
    }

    @Test(expected = ClassCastException.class)
    public void testParseWithComment() {
        Node o = adbMysqlLanguageParser.parseNode("SELECT * from abc;\n"
            + " --abc");
        assertNotNull(o);
    }
}