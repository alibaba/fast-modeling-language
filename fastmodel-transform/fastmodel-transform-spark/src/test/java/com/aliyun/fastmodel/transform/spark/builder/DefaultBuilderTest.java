package com.aliyun.fastmodel.transform.spark.builder;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.spark.context.SparkTransformContext;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/13
 */
public class DefaultBuilderTest {

    @Test
    public void build() {
        DefaultBuilder defaultBuilder = new DefaultBuilder();
        BaseStatement source = CreateTable.builder().tableName(QualifiedName.of("abc")).build();
        DialectNode build = defaultBuilder.build(source, SparkTransformContext.builder().build());
        assertNotNull(build);
    }
}