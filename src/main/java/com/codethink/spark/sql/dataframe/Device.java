package com.codethink.spark.sql.dataframe;

/**
 * @author codethink
 * @date 5/24/16 8:13 PM
 */
public class Device {
    private String imei;
    private String app;

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }
}
