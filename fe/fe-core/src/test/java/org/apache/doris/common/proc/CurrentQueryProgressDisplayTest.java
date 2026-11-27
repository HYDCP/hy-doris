// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.common.proc;

import org.apache.doris.analysis.ShowProcStmt;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ShowExecutor;
import org.apache.doris.qe.ShowResultSet;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

class CurrentQueryProgressDisplayTest {
    @Test
    void onlyFinishedQueriesRoundToOneHundred() {
        Assertions.assertEquals("99.9%", CurrentQueryStatementsProcNode.formatProgress(2047, 2048));
        Assertions.assertEquals("100.0%", CurrentQueryStatementsProcNode.formatProgress(2048, 2048));
        Assertions.assertEquals("66.7%", CurrentQueryStatementsProcNode.formatProgress(2, 3));
        Assertions.assertEquals("0.0%", CurrentQueryStatementsProcNode.formatProgress(0, 3));
    }

    @Test
    void showProcUsesOneResultForColumnsAndRows() throws Exception {
        for (int firstWidth : new int[] {8, 22}) {
            BaseProcResult first = result(firstWidth);
            ProcNodeInterface node = Mockito.mock(ProcNodeInterface.class);
            Mockito.when(node.fetchResult()).thenReturn(first, result(30 - firstWidth));
            ShowProcStmt stmt = new ShowProcStmt("/current_query_stmts");
            Deencapsulation.setField(stmt, "node", node);

            ShowResultSet result = new ShowExecutor(null, stmt).execute();
            Assertions.assertEquals(firstWidth, result.getMetaData().getColumnCount());
            Assertions.assertEquals(firstWidth, result.getResultRows().get(0).size());
            Mockito.verify(node).fetchResult();
        }
    }

    private BaseProcResult result(int width) {
        BaseProcResult result = new BaseProcResult();
        result.setNames(Collections.nCopies(width, "column"));
        result.addRow(Collections.nCopies(width, "value"));
        return result;
    }
}
