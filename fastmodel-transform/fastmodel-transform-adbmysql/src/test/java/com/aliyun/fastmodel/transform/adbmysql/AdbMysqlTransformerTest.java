package com.aliyun.fastmodel.transform.adbmysql;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.adbmysql.format.AdbMysqlPropertyKey;
import com.aliyun.fastmodel.transform.adbmysql.context.AdbMysqlTransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.google.common.collect.Lists;
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
            + "    PRIMARY KEY (LOGIN_TIME,CUSTOMER_ID，PHONE_NUM)\n"
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
        properties.add(new Property(AdbMysqlPropertyKey.DISTRIBUTED_BY.getValue(), "id"));
        properties.add(new Property(AdbMysqlPropertyKey.STORAGE_POLICY.getValue(), "HOT"));
        properties.add(new Property(AdbMysqlPropertyKey.LIFE_CYCLE.getValue(), "10"));
        properties.add(new Property(AdbMysqlPropertyKey.BLOCK_SIZE.getValue(), "10"));
        List<ColumnDefinition> columns = Lists.newArrayList();
        ColumnDefinition column = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType("bigint", null))
            .build();
        columns.add(column);
        BaseStatement source = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
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
}