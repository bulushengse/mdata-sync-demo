package com.zhoubc.mdata.sync;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/2/5 17:53
 */
@SpringBootApplication(scanBasePackages = { "com.zhoubc.mdata.sync" })
@ImportResource(locations = { "classpath:applicationContext.xml" })
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}
