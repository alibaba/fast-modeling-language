/*
 * Copyright (c)  2020. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.benchmark;

import com.aliyun.fastmodel.benchmarks.Tpch;
import com.aliyun.fastmodel.core.formatter.FastModelFormatter;
import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.parser.NodeParser;
import org.junit.Test;

import static com.google.common.base.Strings.repeat;
import static java.lang.String.format;

/**
 * Tpch
 *
 * @author panguanjing
 * @date 2020/11/23
 */
public class TpchTest {

    private static final NodeParser SQL_PARSER = new NodeParser();

    private static Tpch tpch = new Tpch();

    @Test
    public void testStatementBuilderTpch() {
        printTpchQuery(1, 3);
        printTpchQuery(2, 33, "part type like", "region name");
        printTpchQuery(3, "market segment", "2013-03-05");
        printTpchQuery(4, "2013-03-05");
        printTpchQuery(5, "region name", "2013-03-05");
        printTpchQuery(6, "2013-03-05", 33, 44);
        printTpchQuery(7, "nation name 1", "nation name 2");
        printTpchQuery(8, "nation name", "region name", "part type");
        printTpchQuery(9, "part name like");
        printTpchQuery(10, "2013-03-05");
        printTpchQuery(11, "nation name", 33);
        printTpchQuery(12, "ship mode 1", "ship mode 2", "2013-03-05");
        printTpchQuery(13, "comment like 1", "comment like 2");
        printTpchQuery(14, "2013-03-05");
        // query 15: views not supported
        printTpchQuery(16, "part brand", "part type like", 3, 4, 5, 6, 7, 8, 9, 10);
        printTpchQuery(17, "part brand", "part container");
        printTpchQuery(18, 33);
        printTpchQuery(19, "part brand 1", "part brand 2", "part brand 3", 11, 22, 33);
        printTpchQuery(20, "part name like", "2013-03-05", "nation name");
        printTpchQuery(21, "nation name");
        printTpchQuery(22,
            "phone 1",
            "phone 2",
            "phone 3",
            "phone 4",
            "phone 5",
            "phone 6",
            "phone 7");
    }

    private static void printTpchQuery(int query, Object... values) {
        String sql = tpch.getQuery(query);

        for (int i = values.length - 1; i >= 0; i--) {
            sql = sql.replaceAll(format(":%s", i + 1), String.valueOf(values[i]));
        }

        sql = fixTpchQuery(sql);
        printStatement(sql);
    }

    private static String fixTpchQuery(String s) {
        s = s.replaceFirst("(?m);$", "");
        s = s.replaceAll("(?m)^:[xo]$", "");
        s = s.replaceAll("(?m)^:n -1$", "");
        s = s.replaceAll("(?m)^:n ([0-9]+)$", "LIMIT $1");
        s = s.replace("day (3)", "day"); // for query 1
        return s;
    }

    private static void println(String s) {
        if (Boolean.parseBoolean(System.getProperty("printParse"))) {
            System.out.println(s);
        }
    }

    private static void printStatement(String sql) {
        println(sql.trim());
        println("");

        BaseStatement statement = SQL_PARSER.parse(new DomainLanguage(sql));
        println(statement.toString());
        println("");

        println(FastModelFormatter.formatNode(statement));
        println("");

        println(repeat("=", 60));
        println("");
    }

}
