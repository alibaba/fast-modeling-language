package com.aliyun.fastmodel.transform.adbmysql.builder;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.adbmysql.context.AdbMysqlTransformContext;
import org.junit.Test;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/13
 */
public class DefaultBuilderTest {

    @Test
    public void testBuild() {
        DefaultBuilder defaultBuilder = new DefaultBuilder();
        BaseStatement source = CreateTable.builder().tableName(QualifiedName.of("abc")).build();
        defaultBuilder.build(source, AdbMysqlTransformContext.builder().build());
    }
}