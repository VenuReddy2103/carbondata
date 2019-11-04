package org.apache.carbondata.core.util;

import java.util.List;
import java.util.Map;

public interface CustomIndex<SourceElementType, Query, Result> {
    void validateOption(Map<String, String> properties) throws Exception;

    String generate(List<SourceElementType> columns) throws Exception;

    Result query(Query query) throws Exception;
}
