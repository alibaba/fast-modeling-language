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
import java.util.List;

import com.aliyun.fastmodel.driver.model.DriverRow;

/**
 * 定义输出的printer，目前只有一种实现方式，
 * 未来可能扩展多种输出模式
 *
 * @author panguanjing
 * @date 2020/12/29
 */
public interface OutputPrinter {

    /**
     * 输出行内容到控制台
     *
     * @param rows 行内容
     * @throws IOException io异常
     */
    void printRows(List<DriverRow> rows) throws IOException;

    /**
     * 完结内容
     *
     * @throws IOException io异常
     */
    void finish() throws IOException;
}
