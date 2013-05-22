/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * This expression evaluates to true if the given input columns is null.
 * The boolean output is stored in the specified output column.
 */
public class IsNull extends VectorExpression {
  int colNum;
  int outputColumn;

  public IsNull(int colNum, int outputColumn) {
    this.colNum = colNum;
    this.outputColumn = outputColumn;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    ColumnVector inputColVector = batch.cols[colNum];
    int[] sel = batch.selected;
    boolean[] nullPos = inputColVector.isNull;
    int n = batch.size;
    long[] outputVector = ((LongColumnVector) batch.cols[outputColumn]).vector;
    if (n <= 0) {
      // Nothing to do, this is EOF
      return;
    }

    // output never has nulls for this operator
    batch.cols[outputColumn].noNulls = true;
    if (inputColVector.noNulls) {
      outputVector[0] = 0;
      batch.cols[outputColumn].isRepeating = true;
    } else if (inputColVector.isRepeating) {
      outputVector[0] = nullPos[0] ? 1 : 0;
      batch.cols[outputColumn].isRepeating = true;
    } else {
      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = nullPos[i] ? 1 : 0;
        }
      } else {
        for (int i = 0; i != n; i++) {
          outputVector[i] = nullPos[i] ? 1 : 0;
        }
      }
      batch.cols[outputColumn].isRepeating = false;
    }
  }

  @Override
  public int getOutputColumn() {
    return -1;
  }

  @Override
  public String getOutputType() {
    return "boolean";
  }
}
