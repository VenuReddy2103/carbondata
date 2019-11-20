/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.scan.expression.geo;

import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;
import org.apache.carbondata.core.scan.expression.logical.RangeExpression;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.util.CustomIndex;

/**
 * InPolygon expression processor. It inputs the InPolygon string to the GeoHash implementation's
 * query method, gets the list of ranges of GeoHash IDs to filter as an output. And then, multiple
 * range expressions are build from those list of ranges.
 */
@InterfaceAudience.Internal
public class PolygonExpression extends Expression {
  private String polygon;
  private String columnName;
  private CustomIndex<List<Long[]>> handler;
  private List<Long[]> ranges;

  public PolygonExpression(String polygon, String columnName, CustomIndex handler) {
    this.polygon = polygon;
    this.handler = handler;
    this.columnName = columnName;
  }

  /**
   * This method builds the GeoHash range expressions from the list of ranges of GeoHash IDs.
   */
  public void buildRangeExpression() {
    try {
      ranges = handler.query(polygon);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // Convert these ranges into range expressions
    Expression expression = null;
    Expression prevExpression = null;
    for (Long[] range : ranges) {
      if (range.length != 2) {
        throw new RuntimeException("Handler query must return list of ranges with each range " +
                "containing minimum and maximum values");
      }
      if (range[0].equals(range[1])) {
        expression = new EqualToExpression(
                new ColumnExpression(columnName, DataTypes.LONG),
                new LiteralExpression(range[0], DataTypes.LONG));
      } else {
        expression = new RangeExpression(
                new GreaterThanEqualToExpression(
                        new ColumnExpression(columnName, DataTypes.LONG),
                        new LiteralExpression(range[0], DataTypes.LONG)),
                new LessThanEqualToExpression(
                        new ColumnExpression(columnName, DataTypes.LONG),
                        new LiteralExpression(range[1], DataTypes.LONG)));
      }
      if (prevExpression != null) {
        expression = new OrExpression(prevExpression, expression);
      }
      prevExpression = expression;
    }
    if (expression != null) {
      this.children.add(expression);
    }
    return;
  }

  @Override
  public ExpressionResult evaluate(RowIntf value)
          throws FilterUnsupportedException, FilterIllegalMemberException {
    throw new UnsupportedOperationException("Operation not supported for Polygon expression");
  }

  @Override
  public ExpressionType getFilterExpressionType() {
    return ExpressionType.POLYGON;
  }

  @Override
  public void findAndSetChild(Expression oldExpr, Expression newExpr) {
  }

  @Override
  public String getString() {
    return polygon;
  }

  @Override
  public String getStatement() {
    return "INPOLYGON('" + polygon + "')";
  }
}
