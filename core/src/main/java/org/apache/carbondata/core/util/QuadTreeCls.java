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

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.locationtech.jts.geom.*;

/**
 * 空间区域函数处理相关类
 */
class GeometryOperation {
  private static final GeometryFactory geoFactory = new GeometryFactory();

  /**
   * 将点对象转换成geo 中的polygon对象
   *
   * @param polygon 以列表形式存储的区域坐标
   * @return JTS 中的多边形对象
   */
  public static Polygon getPolygonByPoinitList(List<Point2D.Double> polygon) {
    int size = polygon.size();
    if (size < 3) {
      return null;
    } else {
      Coordinate[] rect = new Coordinate[size + 1];
      for (int i = 0; i < size; i++) {
        rect[i] = new Coordinate(polygon.get(i).x, polygon.get(i).y);
      }
      rect[size] = new Coordinate(polygon.get(0).x, polygon.get(0).y);
      return geoFactory.createPolygon(rect);
    }
  }

  /**
   * 将点对象转换成geo 中的polygon对象
   * @param polygon 以列表形式存储的区域坐标
   * @return JTS 中的多边形对象
   */
  public static Polygon getPolygonByDoubleList(List<double[]> polygon) {
    int size = polygon.size();
    if (size < 3) {
      return null;
    } else {
      Coordinate[] rect = new Coordinate[size + 1];
      for (int i = 0; i < size; i++) {
        rect[i] = new Coordinate(polygon.get(i)[0], polygon.get(i)[1]);
      }
      double x = polygon.get(0)[0];
      double y = polygon.get(0)[1];
      rect[size] = new Coordinate(x, y);
      return geoFactory.createPolygon(rect);
    }
  }

  /**
   * 将点对象转换成geo 中的Point对象
   * @param pointB Point2D 点对象
   * @return JTS 中的点对象
   */
  public static Point getPointByPoint2D(Point2D.Double pointB) {
    Coordinate point = new Coordinate(pointB.x,pointB.y);
    return geoFactory.createPoint(point);
  }


  /**
   * 相离 A 与 B 不相交, A,B 都是多边形
   * @param polygonA 多边形
   * @param polygonB 多边形
   * @return true 多边形相离，false 多边形不相离
   */
  public static boolean disjoint(Geometry polygonA, List<Point2D.Double> polygonB) {
    Polygon polyB = getPolygonByPoinitList(polygonB);
    boolean result  = polygonA.disjoint(polyB);
    return result;
  }

  /**
   * 相离 A 与 B 不相交, A 是多边形,B是一个点
   * @param polygonA 多边形
   * @param pointB 一个点
   * @return true 点与多边形相离，false 点与多边形不相离
   */
  public static boolean disjoint(Geometry polygonA, Point2D.Double pointB) {
    Point pointGeo = getPointByPoint2D(pointB);
    boolean result = polygonA.disjoint(pointGeo);
    return result;
  }

  /**
   * 包含 - A contains B 比较多边形A是否包含多边形B
   * @param polygonA  多边形
   * @param polygonB  多边形
   * @return 0 多边形A包含多边形B(A=B或者A>B), -1 多边形A不包含多边形B
   */
  public static boolean contains(Geometry polygonA, List<Point2D.Double> polygonB) {
    Polygon polyB = getPolygonByPoinitList(polygonB);
    return polygonA.contains(polyB);
  }


  /**
   * 包含 - A contains B 比较多边形A是否包含B
   * @param polygonA  多边形
   * @param pointB   点
   * @return true 多边形A包含点B(B在A), false 多边形A不包含点B
   */
  public static boolean contains(Geometry polygonA, Point2D.Double pointB) {
    Point pointGeo = getPointByPoint2D(pointB);
    boolean result = polygonA.contains(pointGeo);
    return result;
  }

  // 相交
  /**
   * 相交 - A intersects B 表示多边形A与多边形B相交
   * @param polygonA 多边形
   * @param polygonB 多边形
   * @return true 多边形A与多边形B相交,false 多边形A与多边形B不相交
   */
  public static boolean intersects(Geometry polygonA, List<Point2D.Double> polygonB) {
    Polygon polyB = getPolygonByPoinitList(polygonB);
    boolean result = polygonA.intersects(polyB);
    return result;
  }

  /**
   * 相交 - A intersects B 表示多边形A与点B相交
   * @param polygonA 多边形
   * @param pointB 点
   * @return true 多边形A与点B相交,false 多边形A与点B不相交
   */
  public static boolean intersects(Geometry polygonA, Point2D.Double pointB) {
    Point pointGeo = getPointByPoint2D(pointB);
    boolean result = polygonA.intersects(pointGeo);
    return result;
  }
}


/**
 * 多边形区域对象
 */
class QuadRect {
  public Double left = 0.0;
  public Double top = 0.0;
  public Double right = 0.0;
  public Double bottom = 0.0;
  /**
   * 构造区域
   * @param topleft 左上点
   * @param bottomRight 右下点
   */
  public QuadRect(Point2D.Double topleft, Point2D.Double bottomRight) {
    this.left = topleft.x;
    this.top = topleft.y;
    this.right = bottomRight.x;
    this.bottom = bottomRight.y;
  }

  /**
   * 构造区域
   * @param x 最左的坐标
   * @param y 最下的坐标
   * @param x2 最右的坐标
   * @param y2 最上的坐标
   */
  public QuadRect(double x, double y, double x2, double y2) {
    this.left = x;
    this.bottom = y;
    this.right = x2;
    this.top = y2;
  }

  /**
   * 给定区域在当前节点区域以外
   * @param polygonRect 给定区域的外接矩形,如果比跟节点坐标大,则表示在整个区域外.
   * @return true 给定区域在该点以外,false给定区域在该点区域之内
   */
  public boolean outsideBox(QuadRect polygonRect) {
    return polygonRect.left < this.left || polygonRect.right > this.right ||
      polygonRect.top > this.top || polygonRect.bottom < this.bottom;

  }

  /**
   * 获取这个区域的Polygon列表,点是栅格区域中心点的内接中心点矩形,如果是最小栅格,则是外围点的坐标
   * @return 矩形区域的顶点列表
   */
  public List<Point2D.Double> getPolygonPointList() {
    Point2D.Double topLeft = new Point2D.Double(this.left, this.top);
    Point2D.Double topRight = new Point2D.Double(this.right, this.top);
    Point2D.Double bottomRight = new Point2D.Double(this.right, this.bottom);
    Point2D.Double bottomLeft = new Point2D.Double(this.left, this.bottom);
    List<Point2D.Double> polygon = new ArrayList<>();
    polygon.add(topLeft);
    polygon.add(topRight);
    polygon.add(bottomRight);
    polygon.add(bottomLeft);
    return polygon;
  }

  /**
   * 获取区域的中心点坐标
   * @return 中心点坐标
   */
  public Double[] getMidelePoint() {
    double x = left + (right - left) / 2;
    double y = bottom + (top - bottom) / 2;
    return new Double [] {x, y};
  }

  /**
   * 获取栅格的中心点
   * @return 该栅格表示区域的中心点
   */
  public Point2D.Double getMiddlePoint() {
    Double [] mPoint = getMidelePoint();
    return new Point2D.Double(mPoint[0], mPoint[1]);
  }

  /**
   * 从一个区域拆分成四个区域,减少对象的创建,直接返回区域两个坐标
   * @return 返回切成四个区域的9个点的坐标，返回的点的顺序是，左上，中上，右上；左中，中，右中，左下，中下，右下
   *
   */
  public List<Point2D.Double> getSplitRect() {
    Double [] mPoint = getMidelePoint();
    // 再获取剩余的四个点，加上边界的四个点，就构成四个区域了。
    //一个区域由9个点切分成4个子区域
    double middleTopX = mPoint[0];
    double middleTopY = this.top;
    Point2D.Double middleTop = new Point2D.Double(middleTopX, middleTopY);

    double middleBottomX = mPoint[0];
    double middleBottomY = this.bottom;
    Point2D.Double middleBottom = new Point2D.Double(middleBottomX, middleBottomY);

    double leftMiddleX = this.left;
    double leftMiddleY = mPoint[1];
    Point2D.Double leftMiddle = new Point2D.Double(leftMiddleX, leftMiddleY);

    double rightMiddleX = this.right;
    double rightMiddleY = mPoint[1];
    Point2D.Double rightMiddle = new Point2D.Double(rightMiddleX, rightMiddleY);

    Point2D.Double middle = new Point2D.Double(mPoint[0], mPoint[1]);
    Point2D.Double topLeft = new Point2D.Double(this.left, this.top);
    Point2D.Double topRight = new Point2D.Double(this.right, this.top);
    Point2D.Double bottomLeft = new Point2D.Double(this.left, this.bottom);
    Point2D.Double bottomRight = new Point2D.Double(this.right, this.bottom);

    List<Point2D.Double> rectList = new ArrayList<>();
    rectList.add(topLeft);
    rectList.add(middleTop);
    rectList.add(topRight);

    rectList.add(leftMiddle);
    rectList.add(middle);
    rectList.add(rightMiddle);

    rectList.add(bottomLeft);
    rectList.add(middleBottom);
    rectList.add(bottomRight);
    return rectList;
  }
}


/**
 * 存储栅格数据
 */
class GridData {
  public static final int STATUS_CONTAIN = 0;  // contains sub nodes in the polygon.
  public static final int STATUS_ALL = 1;  // all children are in the polygon.
  public static final int STATUS_DISJOIN = 2;  // contains sub nodes in the polygon.

  public int startRow = 0;  // 开始的行数
  public int endRow   = 0;  // 结束的行数
  public int startColumn = 0; // 开始的列数
  public int endColumn = 0;   // 结束的列数
  private long startHash = 0L ; // 开始的hash
  private long endHash = 0L ;   // 结束的hash
  private int maxDepth = 0;
  private int status = STATUS_DISJOIN;  // 表示相离

  /**
   * 构造栅格区域数据,data域
   * @param rs startRow
   * @param re endRow
   * @param cs startColumn
   * @param ce endColumn
   * @param maxDepth 最大递归深度
   */
  public GridData(int rs, int re, int cs, int ce, int maxDepth) {
    this.startRow = rs;
    this.endRow = re;
    this.startColumn = cs;
    this.endColumn = ce;
    this.maxDepth = maxDepth;
    computeHashidRange();
  }

  public void setGridData(GridData grid) {
    this.startRow = grid.startRow;
    this.endRow = grid.endRow;
    this.startColumn = grid.startColumn;
    this.endColumn = grid.endColumn;
    this.maxDepth = grid.maxDepth;
    this.startHash = grid.startHash;
    this.endHash = grid.endHash;
    this.status = grid.status;
  }

  /**
   * 构造hashID的范围,构造一个开始一个结束范围
   */
  private void computeHashidRange() {
    startHash = createHashID(startRow, startColumn);
    endHash = createHashID(endRow - 1, endColumn - 1);
  }

  /**
   * 由栅格的坐标计算出对应的hashID数值
   * @param row 栅格化后的行index
   * @param column 栅格化后的列index
   * @return 具体的栅格的hash数值, 实际时,i,j与经纬度有关系,最终要带入经纬度数据的hash数值.
   */
  public long createHashID(int row, int column) {
    long index = 0L;
    for (int i = 0; i < maxDepth + 1 ; i++) {
      long x = (row >> i) & 1;    //取第i位
      long y = (column >> i) & 1;
      index = index | (x << (2 * i + 1)) | (y << 2 * i);
    }
    return index;
  }

  /**
   * 设置网格的状态
   * @param status  允许输入的范围是 STATUS_CONTAIN STATUS_ALL
   */
  public void setStatus(int status) {
    this.status = status;
  }

  /**
   * 获取栅格状态
   * @return 栅格状态
   */
  public int getStatus() {
    return this.status;
  }

  /**
   * 获取栅格对应的ID范围
   * @return 开始ID，结束ID
   */
  public Long[] getHashIDRange() {
    return new Long[] {startHash, endHash};
  }
}

/**
 * 四叉树的节点
 */
class QuadNode {
  private QuadRect rect;   // 四叉树表示的区域 hashID的范围z order是一个连续的范围
  private GridData grid; // 网格数据,实际代表hashID
  private int currentDepth; // 当前数的深度 深度默认从1开始 每一层 4^n 个节点
  private int maxDepth; // 最大深度
  // private long[] rangeHashID; // hashID范围，0最小，1最大
  private QuadNode northWest = null;
  private QuadNode northEast = null;
  private QuadNode southWest = null;
  private QuadNode southEast = null;


  /* 一个矩形区域的象限划分：:

     TL(1)   |    TR(0)
   ----------|-----------
     BL(2)   |    BR(3)
  */

  public enum ChildEnum {
    TOPLEFT, TOPRIGHT, BOTTOMLEFT, BOTTOMRIGHT
  }

  /**
   * 构造函数
   *
   * @param rect         区域
   * @param grid         栅格数据
   * @param currentDepth 当前深度
   * @param maxDepth     最大深度
   */
  public QuadNode(QuadRect rect, GridData grid, int currentDepth, int maxDepth) {
    this.rect = rect;
    this.grid = grid;
    this.currentDepth = currentDepth;
    this.maxDepth = maxDepth;
    // this.rangeHashID = this.grid.getHashIDRange();
  }


  /**
   * 插入一个给定的多边形到4叉树中.
   *
   * @param queryPolygon 给定的多边形区域
   */
  public boolean insert(List<double[]> queryPolygon) {
    Polygon polygonGeo = GeometryOperation.getPolygonByDoubleList(queryPolygon);
    if (polygonGeo != null) {
      // 将多边形插入区域
      // 插入前 先使用外接矩形框来判断是否相离,如果相离则跟矩形就相离,否则再判断矩形
      // 进入insert的函数都是不相离的，在外层先判断
      Geometry rect = polygonGeo.getEnvelope();
      List<Point2D.Double> polygon = this.rect.getPolygonPointList();
      if (GeometryOperation.disjoint(rect, polygon)) {
        System.out.println("polygon disJoint with query polygon envelope  return");
        return false;
      } else {
        if (!GeometryOperation.disjoint(polygonGeo, polygon)) {
          System.out.printf("start to insert query to tree");
          insert(polygonGeo);
          System.out.println("end to insert query to tree");
          return true;
        } else {
          System.out.println("polygon disJoint with query polygon return");
          return false;
        }
      }
    } else {
      System.out.println("query polygon is null return");
      return false;
    }
  }

  /**
   * 向四叉树的点中插入一个区域,能够插入的都不是disjoin的区域，能插入一定不相离
   *
   * @param queryPolygon 待插入的区域
   */
  public void insert(Polygon queryPolygon) {
    List<Point2D.Double> polygon = this.rect.getPolygonPointList();
    Geometry queryRect = queryPolygon.getEnvelope();
    if (isMaxDepth()) {
      // 如果是最终的栅格划分则求出该栅格区域的中心点，判断中心点是否在多边形内
      // 不相离,不包含,则一定相交和状态，相交表示部分选中，当为最后一个节点时也可能不选中
      Point2D.Double middlePoint = this.rect.getMiddlePoint();
      if (!GeometryOperation.disjoint(queryPolygon, middlePoint)) {
        // 选中该区域,填写数据范围
        this.grid.setStatus(GridData.STATUS_ALL);
      } else {
        // 如果没有选中则什么都不做
        this.grid.setStatus(GridData.STATUS_DISJOIN);
      }
    } else {
      if (GeometryOperation.contains(queryPolygon, polygon)) {
        // 如果该点区域被待查询区域包含则该区域整体被选中,整体选中以后需要填写数据范围
        this.grid.setStatus(GridData.STATUS_ALL);
      } else {
        // 设置状态为部分包含
        this.grid.setStatus(GridData.STATUS_CONTAIN);
        // 不到最大深度则向下切分,向下切分直接找到其对应的四个孩子节点
        List<Point2D.Double> rectList = this.rect.getSplitRect();
        // 判断一下区域,有交集才创建子孩子,否则略过.分别判断四个象限
        List<Point2D.Double> topLeft = Arrays.asList(rectList.get(0), rectList.get(1),
            rectList.get(4), rectList.get(3));
        List<Point2D.Double> topRight = Arrays.asList(rectList.get(1), rectList.get(2),
            rectList.get(5), rectList.get(4));
        List<Point2D.Double> bottomLeft = Arrays.asList(rectList.get(3), rectList.get(4),
            rectList.get(7), rectList.get(6));
        List<Point2D.Double> bottomRight = Arrays.asList(rectList.get(4), rectList.get(5),
            rectList.get(8), rectList.get(7));
        int gridRowMiddle = this.grid.startRow + (this.grid.endRow - this.grid.startRow) / 2;
        int gridColumnMiddle = this.grid.startColumn + (this.grid.endColumn -
                                                            this.grid.startColumn) / 2;
        if (!GeometryOperation.disjoint(queryRect, topLeft) && !GeometryOperation
                                        .disjoint(queryPolygon, topLeft)) {
          // 如果不相离,网格选择左上半区
          GridData grid = new GridData(this.grid.startRow, gridRowMiddle, gridColumnMiddle,
              this.grid.endColumn, this.maxDepth);
          insertIntoChildren(ChildEnum.TOPLEFT, grid, topLeft, queryPolygon);
        }
        if (!GeometryOperation.disjoint(queryRect, topRight) && !GeometryOperation
                                             .disjoint(queryPolygon, topRight)) {
          // 如果不相离,网格选择右上半区
          GridData grid = new GridData(gridRowMiddle, this.grid.endRow, gridColumnMiddle,
              this.grid.endColumn, this.maxDepth);
          insertIntoChildren(ChildEnum.TOPRIGHT, grid, topRight, queryPolygon);
        }
        if (!GeometryOperation.disjoint(queryRect, bottomLeft) && !GeometryOperation
                                              .disjoint(queryPolygon, bottomLeft)) {
          // 如果不相离,网格选择左下半区
          GridData grid = new GridData(this.grid.startRow, gridRowMiddle, this.grid.startColumn,
              gridColumnMiddle, this.maxDepth);
          insertIntoChildren(ChildEnum.BOTTOMLEFT, grid, bottomLeft, queryPolygon);
        }
        if (!GeometryOperation.disjoint(queryRect, bottomRight) && !GeometryOperation
                                            .disjoint(queryPolygon, bottomRight)) {
          // 如果不相离,网格选择右下半区
          GridData grid = new GridData(gridRowMiddle, this.grid.endRow, this.grid.startColumn,
              gridColumnMiddle, this.maxDepth);
          insertIntoChildren(ChildEnum.BOTTOMRIGHT, grid, bottomRight, queryPolygon);
        }
        // 当处理完四个孩子的时候需要判断四个孩子是否都是选中,选中的话合并
        combineChild();
        // 当有非空节点,并且该节点状态是disjoin,除了该节点外其他节点都是null，
        // 则需要刷新该节点状态为disjoin并设置其所有children 为空
        checkAndSetDisJoin();
      }
    }
  }

  /**
   * 将栅格插入树种孩子节点中
   *
   * @param childType 孩子节点
   * @param rectangle 该孩子节点表示的区域
   */
  private void insertIntoChildren(ChildEnum childType, GridData grid,
                                  List<Point2D.Double> rectangle, Polygon queryPolygon) {
    QuadRect rect = new QuadRect(rectangle.get(0), rectangle.get(2));
    switch (childType) {
      case TOPLEFT:
        this.northWest = new QuadNode(rect, grid, currentDepth + 1, maxDepth);
        this.northWest.insert(queryPolygon);
        break;
      case TOPRIGHT:
        this.northEast = new QuadNode(rect, grid, currentDepth + 1, maxDepth);
        this.northEast.insert(queryPolygon);
        break;
      case BOTTOMLEFT:
        this.southWest = new QuadNode(rect, grid, currentDepth + 1, maxDepth);
        this.southWest.insert(queryPolygon);
        break;
      case BOTTOMRIGHT:
        this.southEast = new QuadNode(rect, grid, currentDepth + 1, maxDepth);
        this.southEast.insert(queryPolygon);
        break;
      default:
        System.out.println("child type not match");
    }
  }

  /**
   * 合并孩子节点
   */
  private void combineChild() {
    if (checkChildCanCombine(this.northWest) && checkChildCanCombine(this.northEast) &&
          checkChildCanCombine(this.southWest) && checkChildCanCombine(this.southEast)) {
      // 可以合并
      this.getGrid().setStatus(GridData.STATUS_ALL);
      this.northWest.clean();
      this.northWest = null;
      this.northEast.clean();
      this.northEast = null;
      this.southWest.clean();
      this.southWest = null;
      this.southEast.clean();
      this.southEast = null;
    }
  }

  /**
   * 判断孩子节点是否可以合并,当孩子节点不为空,并且孩子节点的网格数据状态是全包含时可以合并
   *
   * @param child 树中的孩子节点
   * @return true 可以合并,false 不可以合并
   */
  private boolean checkChildCanCombine(QuadNode child) {
    return child != null && child.getGrid().getStatus() == GridData.STATUS_ALL;
  }

  /**
   * 判断是否能够设置该区域为disjoin状态
   */
  private void checkAndSetDisJoin() {
    // 是否允许修改标志，默认不许修改
    boolean canChange = false;
    // 如果一个孩子节点的状态是STATUS_DISJOIN，那么这个孩子节点可以置空
    if (this.northEast != null && this.northEast.getGrid().getStatus() == GridData.STATUS_DISJOIN) {
      this.northEast.clean();
      this.northEast = null;
      canChange = true;
    }
    if (this.northWest != null && this.northWest.getGrid().getStatus() == GridData.STATUS_DISJOIN) {
      this.northWest.clean();
      this.northWest = null;
      canChange = true;
    }
    if (this.southWest != null && this.southWest.getGrid().getStatus() == GridData.STATUS_DISJOIN) {
      this.southWest.clean();
      this.southWest = null;
      canChange = true;
    }
    if (this.southEast != null && this.southEast.getGrid().getStatus() == GridData.STATUS_DISJOIN) {
      this.southEast.clean();
      this.southEast = null;
      canChange = true;
    }
    if (canChange) {
      if (childrenIsNull()) {
        this.getGrid().setStatus(GridData.STATUS_DISJOIN);
      }
    }
  }

  /**
   * 获取节点的区域
   *
   * @return rect
   */
  public QuadRect getRect() {
    return rect;
  }

  public GridData getGrid() {
    return grid;
  }

  /**
   * 判断是否已经达到叶子节点
   *
   * @return true 已经达到,false 还没有达到
   */
  protected boolean isMaxDepth() {
    return currentDepth > maxDepth;
  }

  /**
   * 判断孩子节点是否为空
   *
   * @return true 孩子节点为空, false 孩子节点不为空
   */
  public boolean childrenIsNull() {
    return this.northWest == null && this.northEast == null && this.southWest == null
            && this.southEast == null;
  }

  /**
   * 清理数的节点
   */
  public void clean() {
    rect = null;
    grid = null;
    if (northWest != null) northWest.clean();
    if (northEast != null) northEast.clean();
    if (southWest != null) southWest.clean();
    if (southEast != null) southEast.clean();
  }

  /**
   * 获取一个树节点的孩子节点
   *
   * @param childType 孩子节点的枚举数值
   * @return 树对象
   */
  public QuadNode getChildren(ChildEnum childType) {
    switch (childType) {
      case TOPLEFT:
        return this.northWest;
      case TOPRIGHT:
        return this.northEast;
      case BOTTOMRIGHT:
        return this.southEast;
      case BOTTOMLEFT:
        return this.southWest;
      default:
        return null;
    }
  }

  /**
   * 获取当前节点的状态
   *
   * @return STATUS_CONTAIN 0 or STATUS_ALL 1
   */
  public int getNodeStatus() {
    return grid.getStatus();
  }

}

/**
 * 四叉树对象
 */
public class QuadTreeCls {
  private QuadNode root;
  /**
   * 创建root节点,root节点包含了整个栅格区域
   * @param depth 树的深度
   * @param left 坐标左下点
   * @param down 坐标左下点
   * @param width 区域的宽度
   * @param height 区域的高度
   */
  public QuadTreeCls(double left, double down, double width, double height, int depth) {
    QuadRect rect = new QuadRect(left, down, width, height);
    // 根据深度计算出的最大的列长度
    int maxColumn = (int) Math.pow(2, depth);
    // 这里面是写入的row,column这样的数据
    GridData grid = new GridData(0, maxColumn,0, maxColumn, depth);
    root = new QuadNode(rect, grid, 1, depth);
  }

  /**
   * 给定切割深度，四叉树的区域范围，以及查询区域，构建一个四叉树
   * @param vertexes 查询时给定的多边形列表
   */
  public boolean insert(List<double[]> vertexes) {
    // 如果初始区域比根节点表示区域大，则直接退出
    QuadRect outerRectangle = getOuterRectangle(vertexes);
    if (this.root.getRect().outsideBox(outerRectangle)) {
      System.out.println("the top rect is bigger than the root node return");
      return false;
    }
    return root.insert(vertexes);
  }

  /**
   * 获取树种所有节点的范围
   * @return 范围列表
   */
  public List<Long[]> getNodesData() {
    List<Long[]> result = new ArrayList<>();
    getNodeGridRange(root, result);
    sortRange(result);
    combineRange(result);
    return result;
  }

  /**
   * 获取一个节点的数据范围
   * @param node 节点
   * @param result 返回值
   */
  private void getNodeGridRange(QuadNode node, List<Long[]> result) {
    if (node.getNodeStatus() == GridData.STATUS_ALL) {
      Long[] range = node.getGrid().getHashIDRange();
      result.add(range);
    } else {
      // 加的顺序是，左下，左上，右上，右下
      getSubCildGridRange(node, QuadNode.ChildEnum.BOTTOMLEFT, result);
      getSubCildGridRange(node, QuadNode.ChildEnum.TOPLEFT, result);
      getSubCildGridRange(node, QuadNode.ChildEnum.TOPRIGHT, result);
      getSubCildGridRange(node, QuadNode.ChildEnum.BOTTOMRIGHT, result);
    }
  }

  /**
   * 获取一个节点孩子节点的范围
   * @param node 节点
   * @param childType 孩子节点类型
   * @param result  返回值
   */
  private void getSubCildGridRange(QuadNode node, QuadNode.ChildEnum childType,
                                   List<Long[]> result) {
    QuadNode child = node.getChildren(childType);
    if (child != null) {
      getNodeGridRange(child, result);
    }
  }

  /**
   * 对区域结果进行排序和合并
   * @param rangeList 区域列表
   */
  public void sortRange(List<Long[]> rangeList) {
    // 排序的时候只需要对 long[] 的首节点进行排序即可,因为可以保证区间没有重复.
    rangeList.sort(new Comparator<Long[]>() {
      @Override
      public int compare(Long[] x, Long[] y) {
        return Long.compare(x[0], y[0]);
      }
    });
  }

  /**
   * 将排好序的数据作合并，合并后的数据段数可能减少。
   * @param rangeList 区域列表，已经排好序
   */
  public void combineRange(List<Long[]> rangeList) {
    if (rangeList.size() > 1) {
      for (int i = 0, j = i + 1; i < rangeList.size() - 1; i++, j++) {
        long previousEnd = rangeList.get(i)[1];
        long nextStart = rangeList.get(j)[0];
        if (previousEnd + 1 == nextStart) {
          rangeList.get(j)[0] = rangeList.get(i)[0];
          rangeList.remove(i);
        }
      }
    }
  }

  /**
   * 获取一个多边形的外接矩形
   * @param polygon 多边形
   * @return 矩形区域
   */
  private QuadRect getOuterRectangle(List<double[]> polygon) {
    double left = Double.MAX_VALUE;
    double top = Double.MIN_VALUE;
    double right = Double.MIN_VALUE;
    double bottom = Double.MAX_VALUE;
    for (double[] point : polygon) {
      if (point[0] < left) {
        left = point[0];
      }
      if (point[0] > right) {
        right = point[0];
      }
      if (point[1] < bottom) {
        bottom = point[1];
      }
      if (point[1] > top) {
        top = point[1];
      }
    }
    Point2D.Double topLeft = new Point2D.Double(left, top);
    Point2D.Double bottomRight = new Point2D.Double(right, bottom);
    return new QuadRect(topLeft, bottomRight);
  }

  public QuadNode getRoot() {
    return root;
  }

  public void clean() {
    if (root != null) {
      root.clean();
      root = null;
    }
  }
}







