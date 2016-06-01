package com.codethink.common;

import java.util.HashMap;
import java.util.Map;

/**
 * @author codethink
 * @date 5/26/16 9:51 AM
 */
public class Config {
    public static Map<String, String> getEsConfigMap() {
        Map<String, String> esConfigMap=new HashMap<>();
        esConfigMap.put("es.clusterName","dashboard");
        esConfigMap.put("es.nodes","172.26.32.18");
        esConfigMap.put("es.port","9200");
        esConfigMap.put("es.write.operation","upsert");
        esConfigMap.put("es.index.auto.create","false");
        return esConfigMap;
    }
}
