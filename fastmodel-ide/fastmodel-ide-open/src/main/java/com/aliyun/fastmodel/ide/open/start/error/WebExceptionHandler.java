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

package com.aliyun.fastmodel.ide.open.start.error;

import javax.servlet.http.HttpServletRequest;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.ide.spi.exception.BaseIdeException;
import com.aliyun.fastmodel.ide.spi.exception.error.IdeErrorCode;
import com.aliyun.fastmodel.ide.spi.invoker.InvokeResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestController;

/**
 * WebExceptionHandler
 *
 * @author panguanjing
 * @date 2022/1/5
 */
@Slf4j
@RestController
@ControllerAdvice(value = {"com.aliyun.fastmodel.ide.open.start.controller"})
public class WebExceptionHandler {
    @ExceptionHandler(ParseException.class)
    public InvokeResult parseException(HttpServletRequest request, Exception ex) {
        return InvokeResult.error(IdeErrorCode.FML_IS_NOT_VALID.name(), ex.getMessage());
    }

    @ExceptionHandler(BaseIdeException.class)
    public InvokeResult handleBaseIdeException(HttpServletRequest request, Exception ex) {
        BaseIdeException baseIdeException = (BaseIdeException)ex;
        return InvokeResult.error(baseIdeException.getIdeErrorCode().code(), ex.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public InvokeResult argumentException(HttpServletRequest request, Exception ex) {
        return InvokeResult.error(IdeErrorCode.ARGUMENT_NOT_VALIE.name(), ex.getMessage());
    }

}
