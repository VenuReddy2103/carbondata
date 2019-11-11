package org.apache.carbondata.core.util;

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GeoHashDefault implements CustomIndex<Long, String, List<Long[]>> {
    // 角度转弧度的转换因子
    private final static double CONVERT_FACTOR = 180.0;
    // 地球半径
    private final static double EARTH_RADIUS = 6371004.0;

    private static double transValue = Math.PI / CONVERT_FACTOR * EARTH_RADIUS; // 赤道经度1度或者纬度1度对应的地理空间距离

    private double oriLongitude = 0;  // 坐标原点的经度

    private double oriLatitude = 0;   // 坐标原点的纬度

    private double userDefineMaxLongitude = 0;  // 用户定义地图最大的经度

    private double userDefineMaxLatitude = 0;   // 用户定义地图最大的纬度

    private double userDefineMinLongitude = 0;  // 用户定义地图最小的经度

    private double userDefineMinLatitude = 0;   // 用户定义地图最小的纬度

    private double CalculateMaxLongitude = 0;  // 计算后得出的补齐地图最大的经度

    private double CalculateMaxLatitude = 0;  // 计算后得出的补齐地图最大的纬度

    private int gridSize = 0;          //栅格长度单位是米

    private double mCos;               // 坐标原点纬度的余玄数值

    private  double deltaY = 0;        // 每一个gridSize长度对应Y轴的度数

    private  double deltaX = 0;        // 每一个gridSize长度应X轴的度数

    private  double deltaYByRatio = 0; // 每一个gridSize长度对应Y轴的度数 * 系数

    private  double deltaXByRatio = 0; // 每一个gridSize长度应X轴的度数 * 系数

    private int cutLevel = 0;          // 对整个区域切的刀数（一横一竖为1刀），就是四叉树的深度

    private int totalRowNumber = 0;    // 整个区域的行数，从左上开始到右下

    private int totalCloumnNumber = 0;   // 整个区域的列数，从左上开始到右下

    private int udfRowStartNumber = 0;   // 用户定义区域的开始行数

    private int udfRowEndNumber = 0;   // 用户定义区域的结束的行数

    private int udfCloumnStartNumber = 0;   // 用户定义区域的开始列数

    private int udfCloumnEndNumber = 0;   // 用户定义区域的开始结束列数

    private double lon0 = 0;              // 栅格最小数值的经度坐标,最小栅格坐标是扩展区域最左上角的经纬度坐标

    private double lat0 = 0;              // 栅格最小数值的纬度坐标,最小栅格坐标是扩展区域最左上角的经纬度坐标

    private double lon0ByRation = 0;      // *系数的常量
    private double lat0ByRation = 0;      // *系数的常量

    private int conversionRatio = 1;      // 系数，用于将double类型的经纬度，转换成int类型后计算


    @Override
    public void validateOption(Map<String, String> properties) throws Exception {
        String option = properties.get(CarbonCommonConstants.INDEX_HANDLER);
        if (option == null || option.isEmpty()) {
            throw new MalformedCarbonCommandException(
                    String.format("%s property is invalid.", CarbonCommonConstants.INDEX_HANDLER));
        }

        String commonKey = "." + option + ".";
        String sourceColumnsOption = properties.get(CarbonCommonConstants.INDEX_HANDLER + commonKey + "sourcecolumns");
        if (sourceColumnsOption == null) {
            throw new MalformedCarbonCommandException(
                    String.format("%s property is invalid. %s property is not specified.",
                            CarbonCommonConstants.INDEX_HANDLER,
                            CarbonCommonConstants.INDEX_HANDLER + commonKey + "sourcecolumns"));
        }

        if (sourceColumnsOption.split(",").length != 2) {
            throw new MalformedCarbonCommandException(
                    String.format("%s property is invalid. %s property must have 2 columns.",
                            CarbonCommonConstants.INDEX_HANDLER,
                            CarbonCommonConstants.INDEX_HANDLER + commonKey + "sourcecolumns"));
        }

        String type = properties.get(CarbonCommonConstants.INDEX_HANDLER + commonKey + "type");
        if (type != null && !"geohash".equalsIgnoreCase(type)) {
            throw new MalformedCarbonCommandException(
                    String.format("%s property is invalid. %s property must be geohash for this class",
                            CarbonCommonConstants.INDEX_HANDLER,
                            CarbonCommonConstants.INDEX_HANDLER + commonKey + "type"));
        }

        properties.put(CarbonCommonConstants.INDEX_HANDLER + commonKey + "type", "geohash");

        String sourceDataTypes = properties.get(CarbonCommonConstants.INDEX_HANDLER + commonKey + "sourcecolumntypes");
        String[] srcTypes = sourceDataTypes.split(",");
        for (String srcdataType : srcTypes) {
            if (!"int".equalsIgnoreCase(srcdataType)) {
                throw new MalformedCarbonCommandException(
                        String.format("%s property is invalid. source columns datatype must be int",
                                CarbonCommonConstants.INDEX_HANDLER));
            }
        }

        String dataType = properties.get(CarbonCommonConstants.INDEX_HANDLER + commonKey + "datatype");
        if (dataType != null && !"long".equalsIgnoreCase(dataType)) {
            throw new MalformedCarbonCommandException(
                    String.format("%s property is invalid. %s property must be long for this class",
                            CarbonCommonConstants.INDEX_HANDLER,
                            CarbonCommonConstants.INDEX_HANDLER + commonKey + "datatype"));
        }

        /* set the generated column data type as long */
        properties.put(CarbonCommonConstants.INDEX_HANDLER + commonKey + "datatype", "long");
    }

    @Override
    public void init(Map<String, String> properties) throws Exception {
        String option = properties.get(CarbonCommonConstants.INDEX_HANDLER);
        String commonKey = "." + option + ".";
        String oriLongitude = properties.get(CarbonCommonConstants.INDEX_HANDLER + commonKey + "oriLongitude");
        String oriLatitude = properties.get(CarbonCommonConstants.INDEX_HANDLER + commonKey + "oriLatitude");
        String userDefineMaxLongitude = properties.get(CarbonCommonConstants.INDEX_HANDLER + commonKey + "userDefineMaxLongitude");
        String userDefineMaxLatitude = properties.get(CarbonCommonConstants.INDEX_HANDLER + commonKey + "userDefineMaxLatitude");
        String userDefineMinLongitude = properties.get(CarbonCommonConstants.INDEX_HANDLER + commonKey + "userDefineMinLongitude");
        String userDefineMinLatitude = properties.get(CarbonCommonConstants.INDEX_HANDLER + commonKey + "userDefineMinLatitude");
        String gridSize = properties.get(CarbonCommonConstants.INDEX_HANDLER + commonKey + "gridSize");
        String conversionRatio = properties.get(CarbonCommonConstants.INDEX_HANDLER + commonKey + "conversionRatio");

        this.oriLongitude = Double.valueOf(oriLongitude);
        this.oriLatitude = Double.valueOf(oriLatitude);
        this.userDefineMaxLongitude = Double.valueOf(userDefineMaxLongitude);
        this.userDefineMaxLatitude = Double.valueOf(userDefineMaxLatitude);
        this.userDefineMinLongitude = Double.valueOf(userDefineMinLongitude);
        this.userDefineMinLatitude = Double.valueOf(userDefineMinLatitude);
        this.gridSize = Integer.valueOf(gridSize);
        this.conversionRatio = Integer.valueOf(conversionRatio);

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
        // 计算区域被划分成多少行多少列
        this.gridExpansionArea();
    }

    /**
     * 计算补齐区域，包括计算补齐区域的范围，以及切的刀数及四叉树的深度
     */
    private void calculateArea() {
        // step 1 使用maxLongitude, maxLatitude, minLongitude, minLatitude 代入公式计算出Xn,Yn
        // 这里用户给定区域大多数是长方形,这里需要扩展到正方形处理,求Xn,Yn的最大数值
        // n=log_2 （Xmax−X0)/δx， log_2 （Ymax−Y0)/δy
        double Xn = Math.log((userDefineMaxLongitude - userDefineMinLongitude)/deltaX) / Math.log(2);
        double Yn = Math.log((userDefineMaxLatitude - userDefineMinLatitude)/deltaY) / Math.log(2);
        double doubleMax = Math.max(Xn, Yn);
        this.cutLevel = doubleMax % 1 == 0 ? (int)doubleMax:(int)(doubleMax+1);
        //setep 2 根据得到的切分的次数，重新计算区域
        this.CalculateMaxLongitude = userDefineMinLongitude + Math.pow(2, this.cutLevel) * deltaX;
        this.CalculateMaxLatitude = userDefineMinLatitude + Math.pow(2, this.cutLevel) * deltaY;
    }


    /**
     * 将整个区域包含用户给定区域以及扩充区域进行栅格化.栅格化切的刀数与cutLevel相同,这里要计算出用户给定区域的i,j范围
     * i,j从左上角开始,向右下角逐渐递增,将区域划分为2^n*2^n个栅格,栅格从(1,1)开始。
     */
    private void gridExpansionArea() {
        // 先计算整个区域
        this.totalRowNumber = (int) Math.pow(2, this.cutLevel);   // 经度上栅格ID范围
        this.totalCloumnNumber = (int) Math.pow(2, this.cutLevel);   // 纬度上栅格ID范围
        // 再计算用户选择区域 行 = (Ymax - Yuser)/δy 列 = （Xmax - Xuser）/δx
        //最终行的范围 [行，N],最终列的范围[1, 列]
        this.udfRowStartNumber = (int)((this.CalculateMaxLongitude - this.userDefineMaxLongitude) / this.deltaY);
        this.udfRowEndNumber = this.totalRowNumber;
        this.udfCloumnEndNumber = (int)((this.CalculateMaxLatitude - this.userDefineMaxLatitude)/ this.deltaX);
        this.udfCloumnStartNumber = 1;
        //计算栅格最小数值的坐标,最小栅格坐标是扩展区域最左上角的经纬度坐标
        this.lon0 = this.userDefineMinLongitude;
        this.lat0 = this.CalculateMaxLatitude;
    }


    /**
     * 通过栅格索引坐标和计算hashID，栅格经纬度坐标可以通过经纬度转化
     * @param longitude 经度, 实际传入的经纬度是经过了*系数处理的,将浮点计算转为整数计算
     * @param latitude 纬度, 实际传入的经纬度是经过了*系数处理的,将浮点计算转为整数计算
     * @return 栅格ID数值[row,column] 行列从1开始
     */
    private int[] calculateID(long longitude, long latitude) {
        int row = (int)((longitude - this.lon0ByRation) / this.deltaXByRatio);
        int column = (int)((latitude - this.lat0ByRation) / this.deltaYByRatio);
        return new int[]{row, column};
    }

    /**
     * 由栅格的坐标计算出对应的hashID数值
     * @param row 栅格化后的行index
     * @param column 栅格化后的列index
     * @return
     */
    private long createHashID(long row, long column) {
        long index = 0L;
        for (int i = 0; i < cutLevel +1 ; i++) {
            long x = (row >> i) & 1;    //取第i位
            long y = (column >> i) & 1;
            index = index | (x << (2 * i + 1)) | (y << 2 * i);
        }
        return index;
    }


    /**
     * hash ID start at "0", so if the value < 0 then the id should be wrong.
     * @param source Longitude and Latitude
     * @return the hash id number
     * @throws Exception
     */
    @Override
    public Long generate(List<Long> source) throws Exception {
        if (source.size() != 2) {
            throw new RuntimeException("Source list must be of size 2.");
        }
        //TODO generate geohashId
        int[] gridPoint = calculateID(source.get(0), source.get(1));
        Long hashId = createHashID(gridPoint[0], gridPoint[1]);
        return hashId;
    }

    @Override
    public List<Long[]> query(String polygon) throws Exception {
        List<Long[]> rangeList = new ArrayList<Long[]>();
        //TODO call polygon query and get the ranges
        rangeList.add(new Long[2]);
        return rangeList;
    }
}
