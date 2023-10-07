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

package com.aliyun.fastmodel.ide.open.start.controller;

import com.aliyun.fastmodel.ide.open.start.constants.ApiConstant;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/10/7
 */
@Controller
@RequestMapping(value = ApiConstant.INDEX)
public class IndexMenuController {

    public static final String IDE = "ide";
    public static final String COMPARE = "compare";
    private static final String X6 = "x6";
    private static final String FML = "fml";

    @RequestMapping(IDE)
    public String ide() {
        return IDE;
    }

    @RequestMapping(COMPARE)
    public String compare() {
        return COMPARE;
    }

    @RequestMapping(X6)
    public String x6() {
        return X6;
    }

    @RequestMapping(FML)
    public String fml() {
        return FML;
    }
}
