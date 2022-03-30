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

package com.aliyun.fastmodel.transform.example;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.compare.CompareNodeExecute;
import com.aliyun.fastmodel.compare.CompareStrategy;
import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.TransformerFactory;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.hive.context.HiveTransformContext;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;

/**
 * MaxComputer Transform Example
 *
 * @author panguanjing
 * @date 2021/2/7
 */
public class TransformStarter {

    /**
     * Transform Hive
     *
     * @param statement
     * @return
     */
    public String transformHive(BaseStatement statement) {
        return transform(DialectMeta.getHive(), statement, HiveTransformContext.builder().build());
    }

    /**
     * Transform DialectMeta
     *
     * @param dialectMeta
     * @param statement
     * @param context
     * @return
     */
    public String transform(DialectMeta dialectMeta, BaseStatement statement,
                            TransformContext context) {
        Transformer<BaseStatement> statementTransformer = TransformerFactory.getInstance().get(dialectMeta);
        return statementTransformer.transform(statement, context).getNode();
    }

    /**
     * Transform Hive
     *
     * @param list
     * @param separator
     * @return
     */
    public String transformHive(List<BaseStatement> list, String separator) {
        return list.stream().map(this::transformHive).collect(Collectors.joining(separator));
    }

    public String transformHologres(BaseStatement statement) {
        return transform(DialectMeta.getHologres(), statement, HologresTransformContext.builder().parseValid(true)
            .appendSemicolon(true).build());
    }

    /**
     * compare and Transform
     *
     * @param before
     * @param after
     * @param dialectMeta
     * @return
     */
    public String compareAndTransform(BaseStatement before, BaseStatement after, DialectMeta dialectMeta,
                                      TransformContext context) {
        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare(before, after,
            CompareStrategy.INCREMENTAL);
        return compare.stream().map(x -> transform(dialectMeta, x, context)).collect(Collectors.joining(";"));
    }

    public String compareAndTransformFml(String before, String after, DialectMeta hive, TransformContext build) {
        FastModelParser fastModelParser = FastModelParserFactory.getInstance().get();
        return compareAndTransform(fastModelParser.parseStatement(before), fastModelParser.parseStatement(after), hive,
            build);
    }

    public List<BaseStatement> compare(String before, String after) {
        BaseStatement baseStatement = FastModelParserFactory.getInstance().get().parseStatement(before);
        BaseStatement afterStatement = FastModelParserFactory.getInstance().get().parseStatement(after);
        return CompareNodeExecute.getInstance().compare(baseStatement, afterStatement, CompareStrategy.INCREMENTAL);
    }

}
