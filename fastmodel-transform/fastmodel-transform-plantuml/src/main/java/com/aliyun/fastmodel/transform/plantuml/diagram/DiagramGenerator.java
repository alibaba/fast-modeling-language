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

package com.aliyun.fastmodel.transform.plantuml.diagram;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import com.aliyun.fastmodel.transform.plantuml.exception.VisualParseException;
import net.sourceforge.plantuml.BlockUml;
import net.sourceforge.plantuml.FileFormat;
import net.sourceforge.plantuml.FileFormatOption;
import net.sourceforge.plantuml.SourceStringReader;
import net.sourceforge.plantuml.core.Diagram;

/**
 * Plantuml生成器
 *
 * @author panguanjing
 * @date 2021/10/3
 */
public class DiagramGenerator {

    /**
     * export png image
     *
     * @param uml          UML text
     * @param outputStream 流内容
     * @throws VisualParseException
     */
    public static void exportPNG(String uml, OutputStream outputStream) throws VisualParseException {
        List<BlockUml> blocks = new SourceStringReader(uml).getBlocks();
        if (blocks.isEmpty()) {
            throw new VisualParseException("format is invalid");
        }
        BlockUml blockUml = blocks.get(0);
        Diagram diagram = blockUml.getDiagram();
        FileFormatOption fileFormatOption = new FileFormatOption(FileFormat.PNG);
        try {
            diagram.exportDiagram(outputStream, 0, fileFormatOption);
        } catch (IOException e) {
            throw new VisualParseException("export diagram error", e);
        }
    }
}
