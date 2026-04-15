/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Button, Col, Row, Space, Typography, Tag } from 'antd';
import Table from 'Src/components/table';
import { getLogFiles, viewLogFile } from 'Src/api/api';
import { Result } from '@src/interfaces/http.interface';
import { getBasePath } from 'Src/utils/utils';
import { useHistory } from 'react-router-dom';

const { Title, Text } = Typography;

function getPathFromSearch(): string {
    const sp = new URLSearchParams(location.search);
    const p = sp.get('path') || '/';
    if (p === '/') {
        return '/';
    }
    return p.endsWith('/') ? p : p + '/';
}

function getParentPath(p: string): string {
    if (!p || p === '/') {
        return '/';
    }
    const s = p.endsWith('/') ? p.slice(0, -1) : p;
    const idx = s.lastIndexOf('/');
    if (idx <= 0) {
        return '/';
    }
    return s.slice(0, idx) + '/';
}

function openDownload(url: string) {
    const tagA = document.createElement('a');
    tagA.style.display = 'none';
    tagA.href = url;
    document.body.appendChild(tagA);
    tagA.click();
    document.body.removeChild(tagA);
}

function submitArchive(paths: string[]) {
    const basePath = getBasePath();
    const form = document.createElement('form');
    form.method = 'POST';
    form.action = `${basePath}/rest/v1/log_file/archive`;
    form.style.display = 'none';
    paths.forEach((p) => {
        const input = document.createElement('input');
        input.name = 'path';
        input.value = p;
        form.appendChild(input);
    });
    const downloadName = document.createElement('input');
    downloadName.name = 'download_name';
    downloadName.value = 'fe_logs.tar.gz';
    form.appendChild(downloadName);
    document.body.appendChild(form);
    form.submit();
    document.body.removeChild(form);
}

export default function LogFiles() {
    const container = useRef<HTMLDivElement>(null);
    const history = useHistory();
    const [allTableData, setAllTableData] = useState({
        column_names: [],
        rows: [],
    });
    const [packPaths, setPackPaths] = useState<string[]>([]);

    const currentPath = useMemo(() => getPathFromSearch(), [location.search]);

    const refresh = function (ac?: AbortController) {
        getLogFiles({
            path: currentPath,
            signal: ac?.signal,
        })
            .then((res: Result<any>) => {
                if (res && res.msg === 'success') {
                    const data = res.data || {};
                    const colNames = (data.column_names || []).slice();
                    if (!colNames.includes('Action')) {
                        colNames.push('Action');
                    }
                    const rows = (data.rows || []).map((row) => {
                        if (row.Type === 'FILE') {
                            const filePath = row.__path;
                            const isAdded = packPaths.includes(filePath);
                            row.Action = (
                                <Space>
                                    <Button
                                        size="small"
                                        onClick={() => {
                                            viewLogFile({
                                                path: filePath,
                                            }).then((r: Result<any>) => {
                                                if (
                                                    r &&
                                                    r.msg === 'success' &&
                                                    typeof r.data === 'string'
                                                ) {
                                                    if (
                                                        container.current !==
                                                        null
                                                    ) {
                                                        container.current.innerText =
                                                            r.data;
                                                    }
                                                }
                                            });
                                        }}
                                    >
                                        View
                                    </Button>
                                    <Button
                                        size="small"
                                        onClick={() => {
                                            const basePath = getBasePath();
                                            openDownload(
                                                `${basePath}/rest/v1/log_file/download?path=${encodeURIComponent(
                                                    filePath
                                                )}`
                                            );
                                        }}
                                    >
                                        Download
                                    </Button>
                                    <Button
                                        size="small"
                                        onClick={() => {
                                            setPackPaths((prev) => {
                                                if (prev.includes(filePath)) {
                                                    return prev.filter(
                                                        (x) => x !== filePath
                                                    );
                                                }
                                                return prev.concat(filePath);
                                            });
                                        }}
                                    >
                                        {isAdded ? 'Remove' : 'Add'}
                                    </Button>
                                </Space>
                            );
                        } else if (row.Type === 'DIR') {
                            const dirPath = row.__path;
                            row.Action = (
                                <Button
                                    size="small"
                                    onClick={() => {
                                        const p = dirPath.endsWith('/')
                                            ? dirPath
                                            : dirPath + '/';
                                        history.push(
                                            `/LogFiles?path=${encodeURIComponent(
                                                p
                                            )}`
                                        );
                                    }}
                                >
                                    Open
                                </Button>
                            );
                        } else {
                            row.Action = '';
                        }
                        return row;
                    });
                    setAllTableData({
                        column_names: colNames,
                        rows,
                    });
                } else {
                    setAllTableData({
                        column_names: [],
                        rows: [],
                    });
                }
            })
            .catch(() => {});
    };

    useEffect(() => {
        const ac = new AbortController();
        refresh(ac);
        return () => ac.abort();
    }, [location.search, packPaths.length]);

    return (
        <Typography style={{ padding: '30px' }}>
            <Title>Log Files</Title>
            <Row style={{ paddingBottom: '15px' }}>
                <Col span={12}>
                    <Space>
                        <Text strong={true}>Current path:</Text>
                        <Tag>{currentPath}</Tag>
                    </Space>
                </Col>
                <Col span={12} style={{ textAlign: 'right' }}>
                    <Space>
                        <Button
                            onClick={() => {
                                const parent = getParentPath(currentPath);
                                history.push(
                                    `/LogFiles?path=${encodeURIComponent(
                                        parent
                                    )}`
                                );
                            }}
                            disabled={currentPath === '/'}
                        >
                            Back
                        </Button>
                        <Button
                            onClick={() => {
                                refresh();
                            }}
                        >
                            Refresh
                        </Button>
                        <Button
                            type="primary"
                            onClick={() => submitArchive(packPaths)}
                            disabled={packPaths.length === 0}
                        >
                            Download tar.gz
                        </Button>
                        <Button
                            onClick={() => {
                                setPackPaths([]);
                            }}
                            disabled={packPaths.length === 0}
                        >
                            Clear
                        </Button>
                    </Space>
                </Col>
            </Row>
            <Row gutter={16}>
                <Col span={14}>
                    <Table
                        rowKey={(record) => record.__path || record.Name}
                        isSort={true}
                        isFilter={true}
                        allTableData={allTableData}
                    />
                </Col>
                <Col span={10}>
                    <Title level={5}>Preview</Title>
                    <pre
                        ref={container}
                        style={{
                            background: '#f9f9f9',
                            padding: '20px',
                            whiteSpace: 'pre-wrap',
                            fontFamily:
                                "Menlo, Monaco, 'Courier New', monospace",
                            height: '800px',
                            overflowY: 'scroll',
                        }}
                    ></pre>
                </Col>
            </Row>
        </Typography>
    );
}
