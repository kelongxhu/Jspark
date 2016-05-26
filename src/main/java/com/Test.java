package com;

import com.google.common.base.Splitter;

/**
 * @author codethink
 * @date 5/24/16 4:33 PM
 */
public class Test {
    public static void main(String[] args) {
        String s = "imei_1 app_1";
        Iterable<String> iter = Splitter.on(" ").split(s);
        for (String attr : iter) {
            System.out.println("========"+attr);
        }
    }
}
