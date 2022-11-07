/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import com.aliyun.fastmodel.compare.merge.Pipeline;
import com.aliyun.fastmodel.compare.merge.PipelineComponent;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * merge util
 *
 * @author panguanjing
 * @date 2022/10/9
 */
public class MergeNodeExecute {

    private static Map<String, Pipeline> map = Maps.newHashMap();
    private static MergeNodeExecute INSTANCE = new MergeNodeExecute();

    private MergeNodeExecute() {
        ServiceLoader<Pipeline> serviceLoader = ServiceLoader.load(Pipeline.class);
        for (Pipeline pipeline : serviceLoader) {
            PipelineComponent annotation = pipeline.getClass().getAnnotation(PipelineComponent.class);
            Class<?> value = annotation.value();
            map.put(value.getName(), pipeline);
        }
    }

    public static MergeNodeExecute getInstance() {
        return INSTANCE;
    }

    /**
     * merge the alter statement
     *
     * @param source
     * @param alterStatement
     * @return
     */
    public <T extends BaseStatement> T merge(T source, List<BaseStatement> alterStatement) {
        Preconditions.checkNotNull(source);
        if (alterStatement == null) {
            return source;
        }
        T input = source;
        for (BaseStatement baseStatement : alterStatement) {
            Pipeline<BaseStatement, T> pipeline = getPipeLine(baseStatement);
            input = pipeline.process(input, baseStatement);
        }
        return input;
    }

    private Pipeline getPipeLine(BaseStatement baseStatement) {
        return map.get(baseStatement.getClass().getName());
    }
}
