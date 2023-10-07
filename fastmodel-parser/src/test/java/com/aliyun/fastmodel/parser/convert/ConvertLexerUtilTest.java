/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.parser.convert;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * ConvertUtilTest
 * 因为LSP的服务，默认不支持caseStream的方式，所以这里使用转换的方式
 * 将原有的lexer文件中，涉及到值的部分，采用fragment的方式配置，这样就可以解决大小写问题。
 *
 * @author panguanjing
 * @date 2021/3/29
 */
public class ConvertLexerUtilTest {

    String file = "src/main/antlr4/com/aliyun/fastmodel/parser/generate/FastModelLexer.g4";

    final static String PATTERN = "\\w+ *: *'\\w+\\' *;";

    private Pattern pattern;

    private final static List<String> LETTERS = Lists.newArrayList(
        "A", "B", "C", "D", "E", "F", "G", "H",
        "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T",
        "U", "V", "W", "X", "Y", "Z"
    );

    @Before
    public void before() {
        pattern = Pattern.compile(PATTERN);
    }

    @Test
    public void testConvertToIgnoreCase() throws IOException {
        Path path = Paths.get("", file);
        List<String> lines = IOUtils.readLines(
            new FileInputStream(new File(path.toAbsolutePath().toString())),
            Charset.defaultCharset()
        );
        /**
         * 增加一个缓存list
         * 遍历，看下等号右边的内容。
         * 看是不是存在A到Z的字母范畴, 那么使用进行FRAGMENT进行替换，如果非使用''的进行替换
         * 如果不是范畴的，那么原样进行替换内容
         */
        List<String> newLines = new ArrayList<>();
        for (String l : lines) {
            //过滤一些内容
            if (l.startsWith("fragment")) {
                newLines.add(l);
                continue;
            }
            //如果是换行符，那么直接添加
            if (l.equalsIgnoreCase("\n")) {
                newLines.add(l);
                continue;
            }
            if (l.startsWith("lexer") || l.startsWith("@lexer")) {
                newLines.add(l);
                continue;
            }
            Matcher matcher = pattern.matcher(l);
            if (matcher.matches()) {
                String replace = replace(l);
                newLines.add(replace);
            } else {
                newLines.add(l);
            }
        }
        BaseConvertUtil.writeFile(newLines, "FastModelLexer.g4");

    }

    private String replace(String line) {
        int index = line.indexOf(":");
        StringBuilder sb = new StringBuilder(line.substring(0, index + 1)).append(" ");
        String suffix = line.substring(index + 1).trim();
        for (int i = 0; i < suffix.length(); i++) {
            char c = suffix.charAt(i);
            String o = String.valueOf(c).toUpperCase(Locale.ROOT);
            int pos = LETTERS.indexOf(o);
            if (pos > -1) {
                sb.append(LETTERS.get(pos));
                sb.append(" ");
            } else if (c == '_') {
                sb.append("'").append(c).append("'").append(" ");
            } else if (c == '\'') {
                continue;
            } else {
                sb.append(c);
            }
        }
        return sb.toString().trim();
    }
}
