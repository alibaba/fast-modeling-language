/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.driver.cli.terminal.printer;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.driver.model.DriverColumnInfo;
import com.aliyun.fastmodel.driver.model.DriverRow;
import org.junit.Before;
import org.junit.Test;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/29
 */
public class AlignedTablePrinterTest {

    AlignedTablePrinter alignedTablePrinter;

    @Before
    public void before() {
        List<DriverColumnInfo> columnInfo = new ArrayList<>();
        alignedTablePrinter = new AlignedTablePrinter(columnInfo, new PrintWriter(System.out));
    }

    @Test
    public void print() throws IOException {
        List<DriverRow> list = new ArrayList<>();
        alignedTablePrinter.printRows(list);

    }

    @Test
    public void testFinish() throws IOException {
        alignedTablePrinter.finish();
    }
}