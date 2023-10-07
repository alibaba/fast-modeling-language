/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.parser.convert;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.io.IOUtils;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/6/8
 */
public class BaseConvertUtil {

    public static final String OUTPUT = "src/test/resources/lsp/";

    public static void writeFile(List<String> newLines, String fileName) throws IOException {
        Path path1 = Paths.get("", OUTPUT);
        IOUtils.writeLines(newLines, "\n", new FileOutputStream(
            new File(path1.toAbsolutePath().toString(), fileName)
        ), Charset.defaultCharset());
    }
}
