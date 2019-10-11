package com.inspur.mtn.checkDataTool;

import com.inspur.mtn.checkDataTool.oracle.DataConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import lombok.Data;

@Data
@Configuration
@ComponentScan(basePackages = {"com.inspur.mtn"})
//@Import(DataConfig.class)
@PropertySource("classpath:jdbc.properties")
public class AppConfig {

    @Value("${hiveSID}")
    private   String hiveSID;
}
