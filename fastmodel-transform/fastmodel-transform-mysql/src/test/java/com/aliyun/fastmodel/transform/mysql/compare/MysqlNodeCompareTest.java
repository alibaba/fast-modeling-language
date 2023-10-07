/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mysql.compare;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mysql.MysqlTransformer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

/**
 * MysqlNodeCompareTest
 *
 * @author panguanjing
 * @date 2021/8/29
 */
public class MysqlNodeCompareTest {

    MysqlNodeCompare mysqlNodeCompare = new MysqlNodeCompare();

    MysqlTransformer mysqlV8Transformer = new MysqlTransformer();

    @Test
    public void testCompareBeforeAfter() throws IOException {
        DialectNode before = new DialectNode(getNode("/sql/adjunct.txt"));
        DialectNode after = new DialectNode(getNode("/sql/adjunct_after.txt"));
        List<BaseStatement> compare = mysqlNodeCompare.compare(before, after);
        System.out.println(compare.stream().map(BaseStatement::toString).collect(Collectors.joining("\n")));
    }

    @Test
    public void testCompareBeforeAfterDdl() throws IOException {
        DialectNode before = new DialectNode(getNode("/sql/compare/before.txt"));
        DialectNode after = new DialectNode(getNode("/sql/compare/after.txt"));
        List<BaseStatement> compare = mysqlNodeCompare.compare(before, after);
        DialectNode transform = mysqlV8Transformer.transform(new CompositeStatement(compare));
        String node = transform.getNode();
        List<String> list = IOUtils.readLines(new StringReader(node));
        List<String> collect = list.stream().filter(x -> {
            return !x.startsWith("--");
        }).collect(Collectors.toList());
        FileUtils.writeLines(new File("diff.sql"), collect);
    }

    private String getNode(String s) throws IOException {
        return IOUtils.resourceToString(s, StandardCharsets.UTF_8);
    }

    private String getByFile(String fileName) throws IOException {
        return getNode(fileName);
    }
}