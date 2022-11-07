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

package com.aliyun.fastmodel.converter.spi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import com.aliyun.fastmodel.converter.util.GenericTypeUtil;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.google.common.collect.Lists;

/**
 * 转换器工厂
 *
 * @author panguanjing
 * @date 2020/10/16
 */
public abstract class BaseConverterFactory {

    private static final BaseConverterFactory INSTANCE = new ConverterFactoryImpl();

    /**
     * 构造函数，只在内容进行调用
     */
    protected BaseConverterFactory() {
    }

    public static BaseConverterFactory getInstance() {
        return INSTANCE;
    }

    /**
     * 提供方便的操作方法
     *
     * @param source
     * @return
     */
    public List<BaseStatement> convert(List<BaseStatement> source) {
        return convert(source, null);
    }

    /**
     * 单个转换
     *
     * @param source
     * @param convertContext
     * @return
     */
    public List<BaseStatement> convert(BaseStatement source) {
        return convert(Lists.newArrayList(source), null);
    }

    /**
     * 单个转换
     *
     * @param source
     * @param convertContext
     * @return
     */
    public List<BaseStatement> convert(BaseStatement source, ConvertContext convertContext) {
        return convert(Lists.newArrayList(source), convertContext);
    }

    public List<BaseStatement> convert(List<BaseStatement> source, ConvertContext convertContext) {
        List<BaseStatement> list = new ArrayList<>();
        for (BaseStatement baseStatement : source) {
            StatementConverter statementConverter = create(baseStatement);
            if (statementConverter == null) {
                continue;
            }
            BaseStatement convert = statementConverter.convert(baseStatement, convertContext);
            if (convert == null) {
                continue;
            }
            list.add(convert);
        }
        return list;
    }

    /**
     * get the converter from base statement
     *
     * @param baseStatement statement
     * @return {@link Transformer}
     */
    public abstract StatementConverter create(BaseStatement baseStatement);

    /**
     * 内部的工厂类的实现，使用autoService的方式
     */
    private static class ConverterFactoryImpl extends BaseConverterFactory {

        protected Map<String, StatementConverter> maps = new ConcurrentHashMap<>();

        public ConverterFactoryImpl() {
            init();
        }

        public void init() {
            ServiceLoader<StatementConverter> load = ServiceLoader.load(StatementConverter.class);
            for (StatementConverter transformer : load) {
                Class superClassGenericType = GenericTypeUtil.getSuperClassGenericType(transformer.getClass(), 0);
                maps.put(superClassGenericType.getName(), transformer);
            }
        }

        @Override
        public StatementConverter create(BaseStatement baseStatement) {
            Class<? extends BaseStatement> aClass = baseStatement.getClass();
            StatementConverter statementConverter = maps.get(aClass.getName());
            if (statementConverter == null) {
                return maps.get(aClass.getSuperclass().getName());
            } else {
                return statementConverter;
            }
        }
    }
}
