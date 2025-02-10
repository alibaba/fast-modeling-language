/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.driver.cli.terminal.printer;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.aliyun.fastmodel.driver.model.DriverColumnInfo;
import com.aliyun.fastmodel.driver.model.DriverRow;
import org.apache.commons.io.FileUtils;

/**
 * DumpFilePrinter
 *
 * @author panguanjing
 * @date 2024/9/30
 */
public class DumpFilePrinter implements OutputPrinter {

    private final List<String> fieldNames;

    private final String fileName;

    private final Writer writer;

    public DumpFilePrinter(List<DriverColumnInfo> columnInfos, String fileName, Writer writer) {
        String property = System.getProperty("user.dir");
        this.fileName = fileName == null ? property + "/" + UUID.randomUUID() + ".json" : fileName;
        this.writer = writer;
        fieldNames = columnInfos.stream().map(DriverColumnInfo::getColumnName).collect(Collectors.toList());
    }

    @Override
    public void printRows(List<DriverRow> rows) throws IOException {
        JSONArray jsonArray = new JSONArray();
        for (DriverRow row : rows) {
            JSONObject jsonObject = new JSONObject();
            for (int i = 0; i < fieldNames.size(); i++) {
                jsonObject.put(fieldNames.get(i), row.getValue(i));
            }
            jsonArray.add(jsonObject);
        }
        FileUtils.writeStringToFile(new File(fileName), jsonArray.toJSONString());
    }

    @Override
    public void finish() throws IOException {
        writer.append(String.format("dump file (%s)%n", fileName));
        writer.flush();
    }
}
