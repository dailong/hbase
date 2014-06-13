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
import org.apache.hadoop.hbase.expression.evaluation.TypeConversionException;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ToByteExpression implements Expression {
	private Expression subExpression;

	public ToByteExpression() {
	}

	public ToByteExpression(Expression subExpression) {
		this.subExpression = subExpression;
	}

	public Expression getSubExpression() {
		return this.subExpression;
	}

	public EvaluationResult evaluate(EvaluationContext context)
			throws EvaluationException {
		if (this.subExpression == null) {
			throw new EvaluationException("Missing required arguments");
		}
		EvaluationResult eval = this.subExpression.evaluate(context);
		Byte res = null;
		try {
			if (eval.isNullResult()) {
				res = null;
			} else if (eval.isNumber()) {
				res = eval.asByte();
			} else if ((eval.getResultType() == EvaluationResult.ResultType.BYTEARRAY)
					|| (eval.getResultType() == EvaluationResult.ResultType.BYTESREFERENCE)) {
				byte[] b = eval.asBytes();
				if ((b.length == 0)
						&& (context.getCompatibilityMode() == EvaluationContext.CompatibilityMode.HIVE))
					res = null;
				else
					res = Byte.valueOf(b[0]);
			} else if (eval.getResultType() == EvaluationResult.ResultType.STRING) {
				String str = eval.asString();
				if ((str.isEmpty())
						&& (context.getCompatibilityMode() == EvaluationContext.CompatibilityMode.HIVE))
					res = null;
				else
					res = Byte.valueOf(str);
			} else {
				throw new TypeConversionException(eval.getResultType(),
						Byte.class);
			}
		} catch (Throwable t) {
			if ((t instanceof EvaluationException))
				throw ((EvaluationException) t);
			throw ((EvaluationException) new TypeConversionException(
					eval.getResultType(), Byte.class).initCause(t));
		}

		return new EvaluationResult(res, EvaluationResult.ResultType.BYTE);
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof ToByteExpression)))
			return false;
		ToByteExpression other = (ToByteExpression) expr;
		return ((this.subExpression == null) && (other.subExpression == null))
				|| ((this.subExpression != null) && (this.subExpression
						.equals(other.subExpression)));
	}

	public int hashCode() {
		return this.subExpression == null ? 1 : this.subExpression.hashCode();
	}

	public String toString() {
		return "toByte(" + this.subExpression.toString() + ")";
	}

	public void readFields(DataInput in) throws IOException {
		this.subExpression = ((Expression) HbaseObjectWritable.readObject(in,
                null));
	}

	public void write(DataOutput out) throws IOException {
		HbaseObjectWritable.writeObject(out, this.subExpression,
                this.subExpression.getClass(), null);
	}

	public EvaluationResult.ResultType getReturnType() {
		return EvaluationResult.ResultType.BYTE;
	}
}