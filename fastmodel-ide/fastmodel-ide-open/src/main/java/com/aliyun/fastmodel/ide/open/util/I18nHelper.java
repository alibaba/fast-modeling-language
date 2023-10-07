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

package com.aliyun.fastmodel.ide.open.util;

import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.MessageSource;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.stereotype.Component;

/**
 * i18n helper
 *
 * @author panguanjing
 * @date 2022/1/15
 */
@Component
public class I18nHelper {

    private final MessageSource messageSource;

    public I18nHelper(MessageSource messageSource) {this.messageSource = messageSource;}

    public String getMessage(String code, Object[] args, String defaultMessage) {
        return messageSource.getMessage(code.toUpperCase(Locale.ROOT), args,
            defaultMessage,
            I18nHelper.adaptLocale(LocaleContextHolder.getLocale()));
    }

    /**
     * 对locale做兼容处理, 只管语言, 不管国家
     */
    public static Locale adaptLocale(Locale locale) {
        if (locale == null || StringUtils.isBlank(locale.getLanguage())) {
            locale = Locale.CHINA;
        } else if (locale.getLanguage().equals(Locale.ENGLISH.getLanguage())) {
            locale = Locale.US;
        } else if (locale.getLanguage().equals(Locale.CHINESE.getLanguage())) {
            locale = Locale.CHINA;
        }
        return locale;
    }
}
