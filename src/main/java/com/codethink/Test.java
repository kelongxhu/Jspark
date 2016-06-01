package com.codethink;

import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * @author codethink
 * @date 5/24/16 4:33 PM
 */
public class Test {
    private final static Logger logger = LoggerFactory.getLogger(Test.class);
    public static void main(String[] args) {
        String s = "imei_1 app_1";
        Iterable<String> iter = Splitter.on(" ").split(s);
        for (String attr : iter) {
            logger.info("attr:{}",attr);
            logger.error("attr:{}",attr);
        }
        logger.info("============================");
        logger.error("error log...................");
        List<String>
            jsonData = Arrays.asList("{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}",
            "{\"name\":\"LongKe\",\"address\":{\"city\":\"Hy\",\"state\":\"China\"}}");
        for (String a:jsonData){
            logger.info(a);
        }
    }
}
