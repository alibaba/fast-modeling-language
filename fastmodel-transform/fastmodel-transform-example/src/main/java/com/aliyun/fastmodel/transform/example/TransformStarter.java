/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
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
        return transform(DialectMeta.getHologres(), statement, HologresTransformContext.builder()
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
