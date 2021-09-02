package tech.mlsql.plugins.langserver;

import net.sf.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * 2/9/2021 WilliamZhu(allwefantasy@gmail.com)
 */
public class LSContext {
    final public static Map<String, String> initParams = new HashMap<>();

    public static void parse(String jsonStr) {
        JSONObject obj = JSONObject.fromObject(jsonStr);
        for (Object key : obj.keySet()) {
            initParams.put(key.toString(), obj.getString(key.toString()));
        }
    }
}
