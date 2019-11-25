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

package org.apache.carbondata.core.geo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CustomIndex;

import org.apache.commons.lang.StringUtils;

/**
 * GeoHash default implementation
 */
public class GeoHashImpl extends CustomIndex<List<Long[]>> {
  // 角度转弧度的转换因子
  private static final double CONVERT_FACTOR = 180.0;
  // 地球半径
  private static final double EARTH_RADIUS = 6371004.0;

  private static final String GEOHASH = "geohash";
  // 赤道经度1度或者纬度1度对应的地理空间距离
  private static double transValue = Math.PI / CONVERT_FACTOR * EARTH_RADIUS;
  // 坐标原点的经度
  // private double oriLongitude = 0;
  // 坐标原点的纬度
  private double oriLatitude = 0;
  // 用户定义地图最大的经度
  private double userDefineMaxLongitude = 0;
  // 用户定义地图最大的纬度
  private double userDefineMaxLatitude = 0;
  // 用户定义地图最小的经度
  private double userDefineMinLongitude = 0;
  // 用户定义地图最小的纬度
  private double userDefineMinLatitude = 0;
  // 计算后得出的补齐地图最大的经度
  private double CalculateMaxLongitude = 0;
  // 计算后得出的补齐地图最大的纬度
  private double CalculateMaxLatitude = 0;
  //栅格长度单位是米
  private int gridSize = 0;
  // 坐标原点纬度的余玄数值
  private double mCos;
  // 每一个gridSize长度对应Y轴的度数
  private double deltaY = 0;
  // 每一个gridSize长度应X轴的度数
  private double deltaX = 0;
  // 每一个gridSize长度对应Y轴的度数 * 系数
  private double deltaYByRatio = 0;
  // 每一个gridSize长度应X轴的度数 * 系数
  private double deltaXByRatio = 0;
  // 对整个区域切的刀数（一横一竖为1刀），就是四叉树的深度
  private int cutLevel = 0;
  //    private int totalRowNumber = 0;    // 整个区域的行数，从左上开始到右下
  //    private int totalCloumnNumber = 0;   // 整个区域的列数，从左上开始到右下
  //    private int udfRowStartNumber = 0;   // 用户定义区域的开始行数
  //    private int udfRowEndNumber = 0;   // 用户定义区域的结束的行数
  //    private int udfCloumnStartNumber = 0;   // 用户定义区域的开始列数
  //    private int udfCloumnEndNumber = 0;   // 用户定义区域的开始结束列数
  //    private double lon0 = 0;              // 栅格最小数值的经度坐标,最小栅格坐标是扩展区域最左上角的经纬度坐标
  //    private double lat0 = 0;              // 栅格最小数值的纬度坐标,最小栅格坐标是扩展区域最左上角的经纬度坐标
  // *系数的常量
  private double lon0ByRation = 0;
  // *系数的常量
  private double lat0ByRation = 0;
  // 系数，用于将double类型的经纬度，转换成int类型后计算
  private int conversionRatio = 1;

  /**
   * Initialize the geohash index handler instance.
   * the properties is like that:
   * TBLPROPERTIES ('INDEX_HANDLER'='mygeohash',
   * 'INDEX_HANDLER.mygeohash.type'='geohash',
   * 'INDEX_HANDLER.mygeohash.sourcecolumns'='longitude, latitude',
   * 'INDEX_HANDLER.mygeohash.gridSize'='50'
   * 'INDEX_HANDLER.mygeohash.minLongitude'='0'
   * 'INDEX_HANDLER.mygeohash.maxLongitude'='0'
   * 'INDEX_HANDLER.mygeohash.minLatitude'='100'
   * 'INDEX_HANDLER.mygeohash.maxLatitude'='100'
   * 'INDEX_HANDLER.mygeohash.orilatitude''8')
   * @param handlerName the class name of generating algorithm
   * @param properties input properties,please check the describe
   * @throws Exception
   */
  @Override
  public void init(String handlerName, Map<String, String> properties) throws Exception {
    String options = properties.get(CarbonCommonConstants.INDEX_HANDLER);
    if (StringUtils.isEmpty(options)) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid.", CarbonCommonConstants.INDEX_HANDLER));
    }
    options = options.toLowerCase();
    if (!options.contains(handlerName.toLowerCase())) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. %s is not present.",
                      CarbonCommonConstants.INDEX_HANDLER, handlerName));
    }
    String commonKey = CarbonCommonConstants.INDEX_HANDLER + CarbonCommonConstants.POINT +
            handlerName + CarbonCommonConstants.POINT;
    String TYPE = commonKey + "type";
    String type = properties.get(TYPE);
    if (!CarbonCommonConstants.GEOHASH.equalsIgnoreCase(type)) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. %s property must be %s for this class.",
                      CarbonCommonConstants.INDEX_HANDLER, TYPE, CarbonCommonConstants.GEOHASH));
    }
    String SOURCE_COLUMNS = commonKey + "sourcecolumns";
    String sourceColumnsOption = properties.get(SOURCE_COLUMNS);
    if (sourceColumnsOption == null) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. %s property is not specified.",
                      CarbonCommonConstants.INDEX_HANDLER, SOURCE_COLUMNS));
    }
    if (sourceColumnsOption.split(",").length != 2) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. %s property must have 2 columns.",
                      CarbonCommonConstants.INDEX_HANDLER, SOURCE_COLUMNS));
    }
    String SOURCE_COLUMN_TYPES = commonKey + "sourcecolumntypes";
    String sourceDataTypes = properties.get(SOURCE_COLUMN_TYPES);
    String[] srcTypes = sourceDataTypes.split(",");
    for (String srcdataType : srcTypes) {
      if (!"bigint".equalsIgnoreCase(srcdataType)) {
        throw new MalformedCarbonCommandException(
                String.format("%s property is invalid. %s datatypes must be long.",
                        CarbonCommonConstants.INDEX_HANDLER, SOURCE_COLUMNS));
      }
    }
    String TARGET_DATA_TYPE = commonKey + "datatype";
    String dataType = properties.get(TARGET_DATA_TYPE);
    if (dataType != null && !"long".equalsIgnoreCase(dataType)) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. %s property must be long for this class.",
                      CarbonCommonConstants.INDEX_HANDLER, TARGET_DATA_TYPE));
    }
    // Set the generated column data type as long
    properties.put(TARGET_DATA_TYPE, "long");
    String ORIGIN_LATITUDE = commonKey + "originlatitude";
    String originLatitude = properties.get(ORIGIN_LATITUDE);
    if (originLatitude == null) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. Must specify %s property.",
                      CarbonCommonConstants.INDEX_HANDLER, ORIGIN_LATITUDE));
    }
    String MIN_LONGITUDE = commonKey + "minlongitude";
    String MAX_LONGITUDE = commonKey + "maxlongitude";
    String MIN_LATITUDE = commonKey + "minlatitude";
    String MAX_LATITUDE = commonKey + "maxlatitude";
    String minLongitude = properties.get(MIN_LONGITUDE);
    String maxLongitude = properties.get(MAX_LONGITUDE);
    String minLatitude = properties.get(MIN_LATITUDE);
    String maxLatitude = properties.get(MAX_LATITUDE);
    if (minLongitude == null || maxLongitude == null
            || minLatitude == null || maxLatitude == null) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. Must specify %s, %s, %s and %s properties.",
                      CarbonCommonConstants.INDEX_HANDLER, MIN_LONGITUDE, MAX_LONGITUDE,
                      MIN_LATITUDE, MAX_LATITUDE));
    }
    String GRID_SIZE = commonKey + "gridsize";
    String gridSize = properties.get(GRID_SIZE);
    if (gridSize == null) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. %s property must be specified.",
                      CarbonCommonConstants.INDEX_HANDLER, GRID_SIZE));
    }
    String CONVERSION_RATIO = commonKey + "conversionratio";
    String conversionRatio = properties.get(CONVERSION_RATIO);
    if (conversionRatio == null) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. %s property must be specified.",
                      CarbonCommonConstants.INDEX_HANDLER, CONVERSION_RATIO));
    }

    // Fill the values to the instance fields
    this.oriLatitude = Double.valueOf(originLatitude);
    this.userDefineMaxLongitude = Double.valueOf(maxLongitude);
    this.userDefineMaxLatitude = Double.valueOf(maxLatitude);
    this.userDefineMinLongitude = Double.valueOf(minLongitude);
    this.userDefineMinLatitude = Double.valueOf(minLatitude);
    this.gridSize = Integer.parseInt(gridSize);
    this.conversionRatio = Integer.parseInt(conversionRatio);
    // calculate the data
    calculateData();
  }

  /**
   * Generates the GeoHash ID column value from the given source columns.
   * @param sources Longitude and Latitude
   * @return Returns the generated hash id
   * @throws Exception
   */
  @Override
  public String generate(List<?> sources) throws Exception {
    if (sources.size() != 2) {
      throw new RuntimeException("Source columns list must be of size 2.");
    }
    if (!(sources.get(0) instanceof Long) || !(sources.get(1) instanceof Long)) {
      throw new RuntimeException("Source columns must be of Long type.");
    }
    Long longitude = (Long) sources.get(0);
    Long latitude  = (Long) sources.get(1);
    // generate the hash code
    int[] gridPoint = calculateID(longitude, latitude);
    Long hashId = createHashID(gridPoint[0], gridPoint[1]);
    return String.valueOf(hashId);
  }

  /**
   * Query processor for GeoHash.
   * example: (`LONGITUDE`, `LATITUDE`, '116270714,40112476;116217155,40028403;
   * 116337318,39927378;116459541,39966859;116447868,40076233;116385384,40129279;')
   * @param polygon a group of pints, close out to form an area
   * @return Returns list of ranges of GeoHash IDs
   * @throws Exception
   */
  @Override
  public List<Long[]> query(String polygon) throws Exception {
    String[] pointList = polygon.split(";");
    if (3  >= pointList.length) {
      throw new RuntimeException("polygon need at least 3 points.");
    } else {
      List<double[]> queryList = new ArrayList<>();
      for (String str: pointList) {
        String[] points = str.split(",");
        if (2 != points.length) {
          throw new RuntimeException("longitude and latitude is a pair need 2 data");
        } else {
          try {
            queryList.add(new double[] {Double.valueOf(points[0]), Double.valueOf(points[1])});
          } catch (ClassCastException e) {
            throw new RuntimeException("can not covert the string data to double");
          }
        }
      }
      if (3 > queryList.size()) {
        throw new RuntimeException("query polygon point list size less than three");
      } else {
        List<Long[]> rangeList = getPolygonRangeList(queryList);
        return rangeList;
      }
    }
  }

  /**
   * use query polygon condition to get the hash id list, the list is merged and sorted.
   * @param queryList polygon points close out to form an area
   * @return hash id list
   * @throws Exception
   */
  private  List<Long[]> getPolygonRangeList(List<double[]> queryList) throws Exception {
    QuadTreeCls qTreee = new QuadTreeCls(userDefineMinLongitude, userDefineMinLatitude,
        CalculateMaxLongitude, CalculateMaxLatitude, cutLevel);
    qTreee.insert(queryList);
    return qTreee.getNodesData();
  }

  /**
   *  After necessary attributes, perform necessary calculation
   * @throws Exception
   */
  private void calculateData() throws Exception {
    // 角度转弧度 radians = (Math.PI / 180) * degrees 求得原点纬度角度的余弦数值
    this.mCos = Math.cos(this.oriLatitude * Math.PI / CONVERT_FACTOR);
    // 求得 δx=L∗360/(2πR∗cos(lat))
    this.deltaX = (this.gridSize * 360) / (2 * Math.PI * EARTH_RADIUS * this.mCos);
    this.deltaXByRatio = this.deltaX * this.conversionRatio;
    // 求得 δy=L∗360/2πR
    this.deltaY = (this.gridSize * 360) / (2 * Math.PI * EARTH_RADIUS);
    this.deltaYByRatio = this.deltaY * this.conversionRatio;
    // 计算补齐区域并计算栅格i,j表示栅格编号
    // Xmax = x0+(2^n∗δx) Ymax = y0+(2^n∗δx) 其中N是切的刀数
    // 其中x0,y0是给定区域的最小x，y坐标， Xmax >= maxLongitude Ymax >= maxLatitude
    // 计算过程先把maxLongitude, maxLatitude 代入计算出N，如果N不是整数，则取N的下一个整数，代入后求得Xmax，Ymax。
    this.calculateArea();
  }

  /**
   * 计算补齐区域，包括计算补齐区域的范围，以及切的刀数及四叉树的深度
   */
  private void calculateArea() {
    // step 1 使用maxLongitude, maxLatitude, minLongitude, minLatitude 代入公式计算出Xn,Yn
    // 这里用户给定区域大多数是长方形,这里需要扩展到正方形处理,求Xn,Yn的最大数值
    // n=log_2 （Xmax−X0)/δx， log_2 （Ymax−Y0)/δy
    double Xn = Math.log((userDefineMaxLongitude - userDefineMinLongitude) / deltaX) / Math.log(2);
    double Yn = Math.log((userDefineMaxLatitude - userDefineMinLatitude) / deltaY) / Math.log(2);
    double doubleMax = Math.max(Xn, Yn);
    this.cutLevel = doubleMax % 1 == 0 ? (int) doubleMax : (int) (doubleMax + 1);
    // setep 2 根据得到的切分的次数，重新计算区域
    this.CalculateMaxLongitude = userDefineMinLongitude + Math.pow(2, this.cutLevel) * deltaX;
    this.CalculateMaxLatitude = userDefineMinLatitude + Math.pow(2, this.cutLevel) * deltaY;
  }

  /**
   * 通过栅格索引坐标和计算hashID，栅格经纬度坐标可以通过经纬度转化
   * @param longitude 经度, 实际传入的经纬度是经过了*系数处理的,将浮点计算转为整数计算
   * @param latitude 纬度, 实际传入的经纬度是经过了*系数处理的,将浮点计算转为整数计算
   * @return 栅格ID数值[row,column] 行列从1开始
   */
  private int[] calculateID(long longitude, long latitude) throws Exception {
    try {
      int row = (int) ((longitude - this.lon0ByRation) / this.deltaXByRatio);
      int column = (int) ((latitude - this.lat0ByRation) / this.deltaYByRatio);
      return new int[]{row, column};
    } catch (ArithmeticException  e) {
      throw new RuntimeException("can not divide by zero.");
    }
  }

  /**
   * 由栅格的坐标计算出对应的hashID数值
   * @param row 栅格化后的行index
   * @param column 栅格化后的列index
   * @return hash id
   */
  private long createHashID(long row, long column) {
    long index = 0L;
    for (int i = 0; i < cutLevel + 1; i++) {
      long x = (row >> i) & 1;    //取第i位
      long y = (column >> i) & 1;
      index = index | (x << (2 * i + 1)) | (y << 2 * i);
    }
    return index;
  }
}
