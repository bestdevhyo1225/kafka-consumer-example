package com.example.kafka;

public abstract class ConsumerConfigValue {
    public static final String BOOTSTRAP_SERVERS = "localhost:19093,localhost:29093,localhost:39093";
    public static final String ACK_0 = "0";
    public static final String ACK_1 = "1";
    public static final String ACK_ALL = "all"; // -1 값과 동일하다.
    public static final String COMPRESSION_TYPE_GZIP = "gzip";
    public static final String ENABLE_AUTO_COMMIT_TRUE = "true";
    public static final String ENABLE_AUTO_COMMIT_FALSE = "false";
    public static final String AUTO_OFFSET_RESET_EARLIEST = "earliest";
    public static final String AUTO_OFFSET_RESET_LATEST = "latest";
    public static final String AUTO_OFFSET_RESET_NONE = "none";
}