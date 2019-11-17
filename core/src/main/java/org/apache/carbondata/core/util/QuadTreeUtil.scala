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

package org.apache.carbondata.core.util

import java.util


class QuadTreeUtil{

  /**
    * 二分查找法,传入的是列表  List<long[]> arrays, 待查询的是 des
    *
    * @param arrays 数据区域,数据是有序的
    * @param des    待查询数据
    * @return >=0 返回找到的元素在列表中的位置  < 0 未找到
    */
  def binarySearch(arrays: util.List[Array[Long]], des: Long): (Int,(Int, Int) ) = {
    val mid = arrays.size / 2
    if (compareRange(arrays.get(mid), des) == 0){
      return (mid,(0, arrays.size))
    }
    var low = 0
    var high = arrays.size - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if (compareRange(arrays.get(mid), des) == 0){
        return (middle, (low, high))
      }
      else if (compareRange(arrays.get(mid), des) > 0){
        high = middle - 1
      } else{
        low = middle + 1
      }
    }
    (-1,(low, high))
  }

  /**
    * 判断一个连续区域与一个数值是否有交集
    *
    * @param range 一个连续的区域
    * @param des   待比较对象
    * @return == 0 表示有交集, > 0 表示 des >range[0], < 0 表示 desc <range[0]
    */
  private def compareRange(range: Array[Long], des: Long) = {
    var result = -1
    val res = (des >= range(0)) && (des <= range(1))
    if (!res) result = if (des > range(0)) 1 else -1
    else result = 0
    result
  }
}