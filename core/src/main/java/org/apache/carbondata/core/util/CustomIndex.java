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

package org.apache.carbondata.core.util;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface CustomIndex<SourceElementType, Query, Result> extends Serializable {
  /**
   * Initialize the custom index handler instance.
   * @param handlerName
   * @param properties
   * @throws Exception
   */
  void init(String handlerName, Map<String, String> properties) throws Exception;

  /**
   * Generates the custom index column value from the given source columns.
   * @param columns
   * @return Returns generated column value
   * @throws Exception
   */
  String generate(List<SourceElementType> columns) throws Exception;

  /**
   * Query processor for custom index handler.
   * @param query
   * @return Returns list of ranges to be fetched
   * @throws Exception
   */
  Result query(Query query) throws Exception;
}
