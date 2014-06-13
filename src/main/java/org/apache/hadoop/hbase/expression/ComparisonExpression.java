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
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ComparisonExpression implements Expression {
	private ComparisonOperator operator;
	private Expression left;
	private Expression right;

	public ComparisonExpression() {
		this(ComparisonOperator.NO_OP, null, null);
	}

	public ComparisonExpression(ComparisonOperator operator, Expression left,
			Expression right) {
		this.operator = operator;
		this.left = left;
		this.right = right;
	}

	public ComparisonOperator getOperator() {
		return this.operator;
	}

	public Expression getLeft() {
		return this.left;
	}

	public Expression getRight() {
		return this.right;
	}

	public EvaluationResult evaluate(EvaluationContext context)
			throws EvaluationException {
		if ((this.operator == null) || (this.left == null)
				|| (this.right == null)) {
			throw new EvaluationException("Missing required arguments");
		}
		EvaluationResult l = this.left.evaluate(context);
		EvaluationResult r = this.right.evaluate(context);
		if ((l.isNullResult()) || (r.isNullResult())) {
			if (this.operator == ComparisonOperator.EQUAL)
				return new EvaluationResult(Boolean.valueOf((l.isNullResult())
						&& (r.isNullResult())),
						EvaluationResult.ResultType.BOOLEAN);
			if (this.operator == ComparisonOperator.NOT_EQUAL) {
				return new EvaluationResult(
						Boolean.valueOf(((l.isNullResult()) && (!r
								.isNullResult()))
								|| ((!l.isNullResult()) && (r.isNullResult()))),
						EvaluationResult.ResultType.BOOLEAN);
			}

			return new EvaluationResult();
		}

		int comp = EvaluationResult.compare(l, r);

		Boolean res = null;
		switch (this.operator.ordinal()) {
		case 1:
			res = Boolean.valueOf(comp < 0);
			break;
		case 2:
			res = Boolean.valueOf(comp <= 0);
			break;
		case 3:
			res = Boolean.valueOf(comp == 0);
			break;
		case 4:
			res = Boolean.valueOf(comp != 0);
			break;
		case 5:
			res = Boolean.valueOf(comp >= 0);
			break;
		case 6:
			res = Boolean.valueOf(comp > 0);
			break;
		default:
			throw new EvaluationException("Unsupported operator: "
					+ this.operator);
		}

		return new EvaluationResult(res, EvaluationResult.ResultType.BOOLEAN);
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof ComparisonExpression)))
			return false;
		ComparisonExpression other = (ComparisonExpression) expr;
		boolean b = this.operator == other.operator;
		if (b) {
			if (this.left == null)
				b = other.left == null;
			else {
				b = this.left.equals(other.left);
			}
		}
		if (b) {
			if (this.right == null)
				b = other.right == null;
			else {
				b = this.right.equals(other.right);
			}
		}

		return b;
	}

	public int hashCode() {
		int result = this.operator == null ? 1 : this.operator.hashCode();
		result = result * 31 + (this.left == null ? 1 : this.left.hashCode());
		result = result * 31 + (this.right == null ? 1 : this.right.hashCode());
		return result;
	}

	public String toString() {
		return this.operator.toString().toLowerCase() + "("
				+ this.left.toString() + ", " + this.right.toString() + ")";
	}

	public void readFields(DataInput in) throws IOException {
		this.operator = ComparisonOperator.valueOf(Text.readString(in));
		this.left = ((Expression) HbaseObjectWritable.readObject(in, null));
		this.right = ((Expression) HbaseObjectWritable.readObject(in, null));
	}

	public void write(DataOutput out) throws IOException {
		Text.writeString(out, this.operator.toString());
		HbaseObjectWritable.writeObject(out, this.left, this.left.getClass(),
                null);
		HbaseObjectWritable.writeObject(out, this.right, this.right.getClass(),
                null);
	}

	public EvaluationResult.ResultType getReturnType() {
		return EvaluationResult.ResultType.BOOLEAN;
	}

	public static enum ComparisonOperator {
		LESS, LESS_OR_EQUAL, EQUAL, NOT_EQUAL, GREATER_OR_EQUAL, GREATER, NO_OP;
	}
}