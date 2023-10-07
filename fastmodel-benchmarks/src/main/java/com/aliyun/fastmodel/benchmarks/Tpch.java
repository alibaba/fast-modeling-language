/*
 * Copyright (c)  2020. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.benchmarks;

import java.io.IOException;
import java.io.UncheckedIOException;

import com.google.common.io.Resources;

import static com.google.common.base.Charsets.UTF_8;

/**
 * TPCH
 *
 * @author panguanjing
 * @date 2020/11/23
 */
public class Tpch {

    public String getQuery(int i){
        return getTpchQuery(i);
    }

    private String getTpchQuery(int q) {
        return readResource("tpch/" + q + ".sql");
    }

    private String readResource(String name) {
        try {
            return Resources.toString(
                Resources.getResource(name), UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
