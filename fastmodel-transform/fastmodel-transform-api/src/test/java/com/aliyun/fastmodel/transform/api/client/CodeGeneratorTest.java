/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.client;

import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * CodeGeneratorTest
 *
 * @author panguanjing
 * @date 2022/6/7
 */
public class CodeGeneratorTest {

    CodeGenerator ddlGenerator = new DefaultCodeGenerator();

    @Test(expected = NullPointerException.class)
    public void ddlGenerator() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        Table tableClientDTO = Table.builder().build();
        request.setAfter(tableClientDTO);
        request.setConfig(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_HOLO).build());
        DdlGeneratorResult generatorResult = ddlGenerator.generate(request);
        assertNotNull(generatorResult);
    }
}