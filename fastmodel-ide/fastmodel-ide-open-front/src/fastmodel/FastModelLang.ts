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
import * as monaco from "monaco-editor-core";
import IRichLanguageConfiguration = monaco.languages.LanguageConfiguration;
import ILanguage = monaco.languages.IMonarchLanguage;

export const richLanguageConfiguration: IRichLanguageConfiguration = {};


export const monarchLanguage = <ILanguage>{
    defaultToken: '',
    tokenPostfix: '.sql',
    ignoreCase: true,
    brackets: [
        {open: '[', close: ']', token: 'delimiter.square'},
        {open: '(', close: ')', token: 'delimiter.parenthesis'}
    ],
    keywords: ["ADS", "REF", "FACT", "ONLY", "UNBOUNDED", "PRECEDING", "GROUPING", "ALL", "ADD", "TIME_PERIOD", "OR", "THEN", "REPLACE", "EXCLUDE", "BOOLEAN", "DYNAMIC", "EXISTS", "JOIN", "CHECKERS", "TIMESTAMP", "TIME_PERIODS", "FALSE", "RESPECT", "DECREASE", "RIGHT", "HOUR", "TABLE", "FIRST", "RULES", "DIV", "AGGREGATE", "ELSE", "MAP", "INSERT", "INTERSECT", "URL", "ALTER", "INCLUDE", "SOURCE", "DOUBLE", "CASE", "INT", "ROWS", "PRIMARY", "BERNOULLI", "DATETIME", "PASSWORD", "CONSTRAINT", "CLUSTER", "DIALECT", "RENAME", "FLOAT", "USE", "PIPE", "PERIODIC_SNAPSHOT", "DIM", "GROUP", "IGNORE", "SECOND", "DOMAIN", "WEEK", "CURRENT", "REFERENCES", "LATERAL", "TRANSACTION", "DECIMAL", "TABLESAMPLE", "RANGE", "BATCH", "ADJUNCTS", "TRUE", "TINYINT", "AS", "FLOOR", "CURRENT_DATE", "MATERIALIZED", "TIES", "FOR", "DEFAULT", "TRUNCATE", "COLUMNS", "STRONG", "BUSINESS_PROCESSES", "ASYNC", "CUBE", "TASK", "LEFT", "UNSET", "SHOW", "DATE_FIELD", "VALUES", "COMPOSITE", "IS", "OVER", "INDICATOR", "COPY_MODE", "OFFSET", "WITH", "PERCENT", "CAST", "SETS", "USING", "QUARTER", "ROW", "TIMESTAMPLOCALTZ", "DATE", "ARRAY", "NORMAL", "BATCHES", "CODES", "VIEW", "SYSTEM", "TARGET", "EXTRACT", "INDICATORS", "CHANGE", "DESCRIBE", "NULL", "ADJUNCT", "ATTRIBUTE", "RULE", "NULLS", "MEASURE_UNIT", "END", "DISTRIBUTE", "DICT", "COLUMN_GROUP", "ANY", "SORT", "TABLES", "MINUTE", "ENABLE", "WEAK", "ENUM", "LIMIT", "YEAR", "WHERE", "FACTLESS", "ON", "BINARY", "DICTS", "DESC", "SET", "SEMI", "PROPERTIES", "FETCH", "MONTH", "NORELY", "OF", "BY", "CALL", "DAY", "STRING", "WINDOW", "FOLLOWING", "NEXT", "SMALLINT", "DWS", "UNNEST", "LAST", "TARGET_TYPE", "TYPE", "COLUMN", "INCREASE", "INTO", "MEASURE_UNITS", "BIGINT", "VARCHAR", "FILTER", "ORDINALITY", "CHAR", "DROP", "OUTER", "ENGINE", "CROSS", "STRUCT", "RLIKE", "LIKE", "NATURAL", "BUSINESS_PROCESS", "ORDER", "DELETE", "OVERWRITE", "ROLLUP", "LAYER", "DOMAINS", "PRECISION", "DISTINCT", "TRANSFORM", "EXCEPT", "SQL", "HAVING", "CODE", "OUTPUT", "IN", "KEY", "SUBSTRING", "CURRENT_TIMESTAMP", "DAYOFWEEK", "FROM", "RECURSIVE", "SYNC", "NOVALIDATE", "SELECT", "BIZ_DATE", "CREATE", "LAYERS", "REGEXP", "DERIVATIVE", "INTERVAL", "GROUPS", "LEVEL", "DISABLE", "IF", "COPY", "PARTITION", "BUSINESS_UNIT", "TO", "UNIONTYPE", "RELY", "ATOMIC", "PATH", "VALIDATE", "USER", "PRESERVE", "ASC", "CHECKER", "PARTITIONED", "MEASUREMENT", "BUSINESS_UNITS", "CORRELATION", "INNER", "ENFORCED", "CONSOLIDATED", "FULL", "UNION", "ACCUMULATING_SNAPSHOT", "AND", "ALIAS", "COMMENT", "WHEN", "BETWEEN", "REDUCE"],
    operators: [
        "AND", "BETWEEN", "IN", "LIKE", "NOT", "OR", "IS", "NULL", "INTERSECT", "UNION", "INNER", "JOIN", "LEFT", "OUTER", "RIGHT"
    ],
    builtinFunctions: [],
    builtinVariables: [
        // NOT SUPPORTED
    ],
    tokenizer: {
        root: [
            {include: '@comments'},
            {include: '@whitespace'},
            {include: '@numbers'},
            {include: '@strings'},
            {include: '@complexIdentifiers'},
            {include: '@scopes'},
            [/[;,.]/, 'delimiter'],
            [/[()]/, '@brackets'],
            [/[\w@]+/, {
                cases: {
                    '@keywords': 'keyword',
                    '@operators': 'operator',
                    '@builtinVariables': 'predefined',
                    '@builtinFunctions': 'predefined',
                    '@default': 'identifier'
                }
            }],
            [/[<>=!%&+\-*/|~^]/, 'operator'],
        ],
        whitespace: [
            [/\s+/, 'white']
        ],
        comments: [
            [/--+.*/, 'comment'],
            [/#+.*/, 'comment'],
            [/\/\*/, {token: 'comment.quote', next: '@comment'}]
        ],
        comment: [
            [/[^*/]+/, 'comment'],
            [/\*\//, {token: 'comment.quote', next: '@pop'}],
            [/./, 'comment']
        ],
        numbers: [
            [/0[xX][0-9a-fA-F]*/, 'number'],
            [/[$][+-]*\d*(\.\d*)?/, 'number'],
            [/((\d+(\.\d*)?)|(\.\d+))([eE][\-+]?\d+)?/, 'number']
        ],
        strings: [
            [/'/, {token: 'string', next: '@string'}],
            [/"/, {token: 'string.double', next: '@stringDouble'}]
        ],
        string: [
            [/[^']+/, 'string'],
            [/''/, 'string'],
            [/'/, {token: 'string', next: '@pop'}],
        ],
        stringDouble: [
            [/[^"]+/, 'string.double'],
            [/""/, 'string.double'],
            [/"/, {token: 'string.double', next: '@pop'}]
        ],
        complexIdentifiers: [
            [/`/, {token: 'identifier.quote', next: '@quotedIdentifier'}]
        ],
        quotedIdentifier: [
            [/[^`]+/, 'identifier'],
            [/``/, 'identifier'],
            [/`/, {token: 'identifier.quote', next: '@pop'}]
        ],
        scopes: [
            // NOT SUPPORTED
        ]
    }
}
