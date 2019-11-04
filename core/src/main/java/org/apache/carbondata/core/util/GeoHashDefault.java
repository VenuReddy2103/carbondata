package org.apache.carbondata.core.util;

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GeoHashDefault implements CustomIndex<Long, String, List<Long[]>> {
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
    public String generate(List<Long> source) throws Exception {
        if (source.size() != 2) {
            throw new RuntimeException("Source list must be of size 2.");
        }
        //TODO generate geohashId
        return "0";
    }

    @Override
    public List<Long[]> query(String polygon) throws Exception {
        List<Long[]> rangeList = new ArrayList<Long[]>();
        //TODO call polygon query and get the ranges
        rangeList.add(new Long[2]);
        return rangeList;
    }
}
