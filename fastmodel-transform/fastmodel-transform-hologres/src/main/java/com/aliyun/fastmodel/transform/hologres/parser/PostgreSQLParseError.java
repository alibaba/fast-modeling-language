package com.aliyun.fastmodel.transform.hologres.parser;

import lombok.Getter;

/**
 * PostgreSQLLexerBase
 *
 * @author panguanjing
 */
@Getter
public class PostgreSQLParseError {

    private final int number;
    private final int offset;
    private final int line;
    private final int column;
    private final String Message;

    public PostgreSQLParseError(int number, int offset, int line, int column, String message) {
        this.number = number;
        this.offset = offset;
        Message = message;
        this.line = line;
        this.column = column;
    }

}
