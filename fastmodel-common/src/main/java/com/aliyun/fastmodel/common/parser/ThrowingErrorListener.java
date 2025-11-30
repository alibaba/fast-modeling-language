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

package com.aliyun.fastmodel.common.parser;

import com.aliyun.fastmodel.core.exception.ParseException;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

/**
 * 用于抛出异常的Listener
 *
 * @author panguanjing
 * @date 2020/9/3
 */
public class ThrowingErrorListener extends BaseErrorListener {

    private final String originalInput;

    public ThrowingErrorListener() {
        this.originalInput = null;
    }

    public ThrowingErrorListener(String originalInput) {
        this.originalInput = originalInput;
    }

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
        String msg, RecognitionException e) throws ParseException {
        String errorMessage = buildErrorMessage(line, charPositionInLine, msg, offendingSymbol);
        throw new ParseException(errorMessage);
    }

    private String buildErrorMessage(int line, int charPositionInLine, String msg, Object offendingSymbol) {
        StringBuilder sb = new StringBuilder();
        sb.append("line ").append(line).append(":").append(charPositionInLine).append(" ").append(msg);
        // 如果有原始输入文本，则添加错误附近的上下文
        if (originalInput != null) {
            sb.append("\n");
            sb.append("Error context: '...").append(getErrorContext(originalInput, line, charPositionInLine)).append("...'\n");
            sb.append("Full input: '\n").append(originalInput).append("\n'");
        }

        return sb.toString();
    }

    /**
     * 获取错误位置附近的上下文
     */
    private String getErrorContext(String input, int line, int charPositionInLine) {
        try {
            String[] lines = input.split("\n");
            if (line > 0 && line <= lines.length) {
                String errorLine = lines[line - 1];
                int start = Math.max(0, charPositionInLine - 10);
                int end = Math.min(errorLine.length(), charPositionInLine + 10);
                return errorLine.substring(start, end);
            }
        } catch (Exception e) {
            // 静默处理错误，不干扰原始异常抛出
        }
        return "unknown";
    }
}
