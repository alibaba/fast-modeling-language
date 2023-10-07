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

const path = require('path');
const htmlWebpackPlugin = require('html-webpack-plugin');
const webpack = require("webpack");

module.exports = (env, options) => {
    return {
        mode: options.mode ? options.mode : 'development',
        entry: {
            app: './src/index.tsx',
            "editor.worker": 'monaco-editor-core/esm/vs/editor/editor.worker.js',
            "fastmodelWorker": './src/fastmodel/fastmodel.worker.ts'
        },
        output: {
            globalObject: 'self',
            filename: (chunkData) => {
                switch (chunkData.chunk.name) {
                    case 'editor.worker':
                        return 'editor.worker.js';
                    case 'fastmodelWorker':
                        return 'fastmodelWorker.js';
                    default:
                        return options.mode === 'production' ? 'bundle.js' : 'bundle.[hash].js';
                }
            },
            path: path.resolve(__dirname, 'dist')
        },
        resolve: {
            extensions: ['.ts', '.tsx', '.js', '.jsx', '.css']
        },
        module: {
            rules: [
                {
                    test: /\.tsx?/,
                    loader: 'ts-loader'
                },
                {
                    test: /\.css/,
                    use: ['style-loader', 'css-loader']
                }
            ]
        },
        plugins: [
            new htmlWebpackPlugin({
                template: './src/index.html'
            }),
            new webpack.DefinePlugin({
                SERVER_ENV: JSON.stringify(options.mode),
            }),
        ]
    }

}