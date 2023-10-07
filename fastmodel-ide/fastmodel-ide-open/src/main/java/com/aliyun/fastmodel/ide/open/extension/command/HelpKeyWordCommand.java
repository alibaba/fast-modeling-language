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

package com.aliyun.fastmodel.ide.open.extension.command;

import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.command.HelpCommand;
import com.aliyun.fastmodel.ide.open.util.I18nHelper;
import com.aliyun.fastmodel.ide.spi.command.BaseIdeCommand;
import com.aliyun.fastmodel.ide.spi.invoker.InvokeResult;
import com.aliyun.fastmodel.parser.lexer.ReservedIdentifier;
import org.springframework.context.MessageSource;
import org.springframework.context.NoSuchMessageException;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.stereotype.Component;

/**
 * Help KeyWord Command
 * ```
 * help keyword -t 'comment'
 * ```
 *
 * @author panguanjing
 * @date 2022/1/15
 */
@Component
public class HelpKeyWordCommand extends BaseIdeCommand<String, HelpCommand> {

    public static final String KEYWORD = "keyword";
    public static final String NOT_FOUND_KEY = "NOT_FOUND";
    public static final String DELIMITER = ",";
    private final MessageSource messageSource;

    public HelpKeyWordCommand(MessageSource messageSource) {this.messageSource = messageSource;}

    @Override
    public InvokeResult<String> execute(HelpCommand statement) {
        Optional<String> text = statement.getText();
        if (text.isPresent()) {
            String keyword = text.get();
            String message = null;
            try {
                message = messageSource.getMessage(keyword.toUpperCase(Locale.ROOT), new Object[] {keyword},
                    I18nHelper.adaptLocale(LocaleContextHolder.getLocale()));
            } catch (NoSuchMessageException e) {
                message = messageSource.getMessage(NOT_FOUND_KEY, new Object[] {keyword},
                    I18nHelper.adaptLocale(LocaleContextHolder.getLocale()));
            }
            return InvokeResult.success(message);
        } else {
            return InvokeResult.success(
                ReservedIdentifier.getKeywords().stream().collect(Collectors.joining(DELIMITER)));
        }
    }

    @Override
    public boolean isMatch(HelpCommand baseStatement) {
        return baseStatement.getCommand().equals(new Identifier(KEYWORD));
    }
}
