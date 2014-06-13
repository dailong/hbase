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
package org.apache.hadoop.hbase.expression;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RowExpression implements Expression {
	public EvaluationResult evaluate(EvaluationContext context)
			throws EvaluationException {
		return new EvaluationResult(context.getRow(),
				EvaluationResult.ResultType.BYTESREFERENCE);
	}

	public boolean equals(Object expr) {
		return (expr != null) && ((expr instanceof RowExpression));
	}

	public int hashCode() {
		return 1;
	}

	public String toString() {
		return "row()";
	}

	public void readFields(DataInput in) throws IOException {
	}

	public void write(DataOutput out) throws IOException {
	}

	public EvaluationResult.ResultType getReturnType() {
		return EvaluationResult.ResultType.BYTESREFERENCE;
	}
}