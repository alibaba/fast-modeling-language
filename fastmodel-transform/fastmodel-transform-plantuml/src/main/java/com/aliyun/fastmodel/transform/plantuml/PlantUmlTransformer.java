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

package com.aliyun.fastmodel.transform.plantuml;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.plantuml.exception.VisualParseException;
import com.aliyun.fastmodel.transform.plantuml.exception.VisualTemplateNotFoundException;
import com.aliyun.fastmodel.transform.plantuml.parser.Fragment;
import com.aliyun.fastmodel.transform.plantuml.parser.FragmentFactory;
import com.aliyun.fastmodel.transform.plantuml.parser.FragmentParser;
import com.google.auto.service.AutoService;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * plantUmlEngine
 *
 * @author panguanjing
 * @date 2020/9/18
 */
@AutoService(Transformer.class)
@Dialect(DialectName.Constants.PLANTUML)
public class PlantUmlTransformer implements Transformer<CompositeStatement> {

    public static final String START_TAG = "@startuml";
    public static final String END_TAG = "@enduml";
    public static final String NEW_LINE = "\n";
    public static final String LIBRARY_IUML = "/Library.iuml";
    private String libraryText;

    public PlantUmlTransformer() {
        //load the library iuml
        try {
            libraryText = IOUtils.resourceToString(LIBRARY_IUML, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new VisualTemplateNotFoundException("can't find the library uml:" + LIBRARY_IUML, e);
        }
    }

    /**
     * generate plantuml
     * for each statement,
     * get the fragment parser from statement
     * combine the fragment
     * output the stream
     *
     * @param statements       statements
     * @param fileOutputStream outputStream
     */
    public void generate(List<BaseStatement> statements, OutputStream fileOutputStream) throws VisualParseException {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(START_TAG);
        stringBuilder.append(NEW_LINE);
        if (StringUtils.isNotBlank(libraryText)) {
            stringBuilder.append(libraryText);
            stringBuilder.append(NEW_LINE);
        }
        for (BaseStatement statement : statements) {
            FragmentParser fragmentParser = FragmentFactory.getInstance().get(statement);
            if (fragmentParser == null) {
                continue;
            }
            Fragment fragment = fragmentParser.parse(statement);
            stringBuilder.append(fragment.content());
            stringBuilder.append(NEW_LINE);
        }
        if (!isEndNewLine(stringBuilder.toString())) {
            stringBuilder.append(NEW_LINE);
        }
        stringBuilder.append(END_TAG);
        try {
            fileOutputStream.write(stringBuilder.toString().getBytes(Charset.defaultCharset()));
        } catch (IOException e) {
            throw new VisualParseException("visual error", e);
        }
    }

    private boolean isEndNewLine(String toString) {
        return toString.endsWith(NEW_LINE);
    }

    @Override
    public DialectNode transform(CompositeStatement source, TransformContext context) {
        ByteArrayOutputStream fileOutputStream = new ByteArrayOutputStream();
        generate(source.getChildren(), fileOutputStream);
        try {
            return new DialectNode(fileOutputStream.toString(Charset.defaultCharset().name()));
        } catch (UnsupportedEncodingException e) {
            throw new VisualParseException("transform exception", e);
        }
    }

}
