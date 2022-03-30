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
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.driver.model.DriverColumnInfo;
import com.aliyun.fastmodel.driver.model.DriverRow;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.jline.utils.AttributedString;
import org.jline.utils.WCWidth;

import static java.lang.Math.max;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/29
 */
public class AlignedTablePrinter implements OutputPrinter {

    private static final Splitter LINE_SPLITTER = Splitter.on('\n');

    private final List<String> fieldNames;

    private final Writer writer;

    private int rowCount;

    private boolean headerOutput;

    private final AtomicBoolean closed = new AtomicBoolean();

    public AlignedTablePrinter(List<DriverColumnInfo> columnInfos, Writer writer) {
        fieldNames =
            columnInfos.stream().map(DriverColumnInfo::getColumnName).collect(Collectors.toList());
        this.writer = writer;
    }

    @Override
    public void printRows(List<DriverRow> rows) throws IOException {
        rowCount += rows.size();
        int columns = fieldNames.size();
        //calc max width
        int[] maxWidth = new int[columns];
        for (int i = 0; i < columns; i++) {
            maxWidth[i] = max(1, consoleWidth(fieldNames.get(i)));
        }
        //calc row width
        for (DriverRow row : rows) {
            List<?> data = row.getData();
            for (int i = 0; i < data.size(); i++) {
                String s = formatValue(data.get(i));
                maxWidth[i] = max(maxWidth[i], maxLineLength(s));
            }
        }

        //print header
        if (!headerOutput) {
            headerOutput = true;
            for (int i = 0; i < columns; i++) {
                if (i > 0) {
                    writer.append('|');
                }
                String name = fieldNames.get(i);
                writer.append(center(name, maxWidth[i], 1));
            }

            writer.append('\n');

            for (int i = 0; i < columns; i++) {
                if (i > 0) {
                    writer.append('+');
                }
                writer.append(Strings.repeat("-", maxWidth[i] + 2));
            }

            writer.append('\n');
        }

        //print rows
        for (DriverRow row : rows) {
            List<List<String>> columnLines = new ArrayList<>(columns);
            int maxLines = 1;
            for (int i = 0; i < columns; i++) {
                String s = formatValue(row.getValue(i));
                List<String> lines = ImmutableList.copyOf(LINE_SPLITTER.split(s));
                columnLines.add(lines);
                maxLines = max(maxLines, lines.size());
            }

            for (int line = 0; line < maxLines; line++) {
                for (int column = 0; column < columns; column++) {
                    if (column > 0) {
                        writer.append("|");
                    }
                    List<String> lines = columnLines.get(column);
                    String s = (line < lines.size()) ? lines.get(line) : "";
                    String out = align(s, maxWidth[column], 1);
                    writer.append(out);
                }
                writer.append('\n');
            }
        }
        writer.flush();
    }

    private static String align(String s, int maxWidth, int padding) {
        int width = consoleWidth(s);
        String large = Strings.repeat(" ", (maxWidth - width) + padding);
        String small = Strings.repeat(" ", padding);
        return small + s + large;
    }

    private static String center(String s, int maxWidth, int padding) {
        int width = consoleWidth(s);
        int left = (maxWidth - width) / 2;
        int right = (maxWidth - (left + width));
        return Strings.repeat(" ", left + padding) + s + Strings.repeat(" ", right + padding);
    }

    /**
     * 取最大的line的大小
     *
     * @param s 输入的字符串
     * @return 最大的line的大小的宽度
     */
    private int maxLineLength(String s) {
        int n = 0;
        for (String line : LINE_SPLITTER.split(s)) {
            n = max(n, consoleWidth(line));
        }
        return n;
    }

    private String formatValue(Object o) {
        if (o == null) {
            return "NULL";
        }
        return o.toString();
    }

    private static int consoleWidth(String s) {
        CharSequence plain = AttributedString.stripAnsi(s);
        int n = 0;
        for (int i = 0; i < plain.length(); i++) {
            n += max(WCWidth.wcwidth(plain.charAt(i)), 0);
        }
        return n;
    }

    @Override
    public void finish() throws IOException {
        if (!closed.getAndSet(true)) {
            printRows(ImmutableList.of());
            writer.append(String.format("(%s row%s)%n", rowCount, (rowCount != 1) ? "s" : ""));
            writer.flush();
        }
    }
}
