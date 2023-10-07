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
declare var SERVER_ENV: string | undefined;

const FORM_CONTENT_TYPE = 'application/x-www-form-urlencoded; charset=UTF-8';

const HOST: string = SERVER_ENV === 'production' ? "/" : "http://localhost:7001/"

export const Server = {
    RENDER_URL: HOST + "command/x6",
    IMPORT_URL: HOST + "command/import_sql",
    EXPORT_URL: HOST + "command/export_sql",
    UPLOAD_FILE: HOST + "command/upload_sql",
    EXECUTE_FML_URL : HOST + "command/execute"
}

export interface KeyValue {
    key: string,
    value: string
}


export const httpRequest = async (url: string, value: KeyValue[], parse = false) => {
    let formBody = [];
    for (const valElement of value) {
        formBody.push(`${valElement.key}=` + encodeURIComponent(valElement.value));
    }
    const response = await window.fetch(url, {
        headers: {
            'Content-Type': FORM_CONTENT_TYPE,
        },
        method: "POST",
        body: formBody.join("&")
    });
    const {data, success, message, errors} = await response.json();
    if (response.ok) {
        if (success) {
            const ret = parse ? JSON.parse(data): data;
            return ret;
        } else {
            return Promise.reject(new Error(message))
        }
    } else {
        // handle the graphql errors
        const error = new Error(errors?.map(e => e.message).join('\n') ?? 'unknown')
        return Promise.reject(error)
    }
}

export const getServerRender = async (value: string) => {
    return httpRequest(Server.RENDER_URL, [{
        key: 'fml',
        value: value
    }], true);
}

export const importSql = (dialectName: string, value: string) => {
    let input = Server.IMPORT_URL;
    return httpRequest(input, [
        {
            key: 'dialectName',
            value: dialectName
        },
        {
            key: 'text',
            value: value
        }
    ]);

}

export const exportSql = (dialectName: string, value: string) => {
    let input = Server.EXPORT_URL;
    return httpRequest(input, [
        {
            key: 'dialectName',
            value: dialectName
        },
        {
            key: 'fml',
            value: value
        }
    ]);
}

export const executeFml = (value : string) => {
    let url = Server.EXECUTE_FML_URL;
    return httpRequest(url, [
        {
            key : 'fml',
            value : value
        }
    ])
}