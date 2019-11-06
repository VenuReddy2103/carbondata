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
package org.apache.carbondata.processing.loading.parser.impl;

import java.util.ArrayList;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.loading.parser.CarbonParserFactory;
import org.apache.carbondata.processing.loading.parser.GenericParser;
import org.apache.carbondata.processing.loading.parser.RowParser;

public class RowParserImpl implements RowParser {

  private GenericParser[] genericParsers;

  private int[] outputMapping;

  private int[] inputMapping;

  private Map<String, String> tableProperties;
  private DataField[] input;

  private int numberOfColumns;

  public RowParserImpl(DataField[] output, CarbonDataLoadConfiguration configuration) {
    String[] tempComplexDelimiters =
        (String[]) configuration.getDataLoadProperty(DataLoadProcessorConstants.COMPLEX_DELIMITERS);
    ArrayList<String> complexDelimiters = new ArrayList<>();
    for (int i = 0; i < tempComplexDelimiters.length; i++) {
      complexDelimiters.add(tempComplexDelimiters[i]);
    }
    String nullFormat =
        configuration.getDataLoadProperty(DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT)
            .toString();
    tableProperties = configuration.getTableSpec().getCarbonTable().getTableInfo()
            .getFactTable().getTableProperties();
    input = getInput(configuration);
    genericParsers = new GenericParser[input.length];
    for (int i = 0; i < genericParsers.length; i++) {
      genericParsers[i] =
          CarbonParserFactory.createParser(input[i].getColumn(), complexDelimiters, nullFormat);
    }
    outputMapping = new int[output.length];
    for (int i = 0; i < input.length; i++) {
      for (int j = 0; j < output.length; j++) {
        if (input[i].getColumn().equals(output[j].getColumn())) {
          outputMapping[i] = j;
          break;
        }
      }
    }
  }

  public DataField[] getInput(CarbonDataLoadConfiguration configuration) {
    DataField[] fields = configuration.getDataFields();
    String[] header = configuration.getHeader();
    numberOfColumns = header.length;
    DataField[] input = new DataField[fields.length];
    inputMapping = new int[input.length];
    int k = 0;
    for (int i = 0; i < fields.length; i++) {
      for (int j = 0; j < numberOfColumns; j++) {
        if (header[j].equalsIgnoreCase(fields[i].getColumn().getColName())) {
          input[k] = fields[i];
          inputMapping[k] = j;
          k++;
          break;
        }
      }

      if (fields[i].getColumn().isInvisible()) {
        String handler = tableProperties.get(CarbonCommonConstants.INDEX_HANDLER + "." +
                fields[i].getColumn().getColName() + ".class");
        if (handler != null) {
          input[k] = fields[i];
          // TODO Need to check if it is ok to schema ordinal for invisible columns be next to that
          // of user configured columns
          inputMapping[k] = fields[i].getColumn().getSchemaOrdinal();
          k++;
        }
      }
    }
    return input;
  }

  @Override
  public Object[] parseRow(Object[] row) {
    if (row == null) {
      return new String[numberOfColumns];
    }
    // If number of columns are less in a row then create new array with same size of header.
    if (row.length < numberOfColumns) {
      String[] temp = new String[numberOfColumns];
      System.arraycopy(row, 0, temp, 0, row.length);
      row = temp;
    }

    Object[] out = new Object[genericParsers.length];
    for (int i = 0; i < genericParsers.length; i++) {
      Object obj = null;
      if (input[i].getColumn().isInvisible()) {
        String handler = tableProperties.get(CarbonCommonConstants.INDEX_HANDLER + "." +
                input[i].getColumn().getColName() + ".class");
        if (handler != null) {
          try {
            // TODO Need to call generate index handler class here and assign it to obj
            //obj = ((CustomIndex)Class.forName(handler).newInstance()).generate();
            obj = "0";
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      } else {
        obj = row[inputMapping[i]];
      }
      out[outputMapping[i]] = genericParsers[i].parse(obj);
    }
    return out;
  }

}
