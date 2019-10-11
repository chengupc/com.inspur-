package com.inspur.mtn.checkDataTool;

import com.inspur.mtn.checkDataTool.oracle.HisCheckTaskMapper;
import com.inspur.mtn.checkDataTool.oracle.HisCheckTaskReultMapper;
import com.inspur.mtn.checkDataTool.service.IndicatorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
@Component
@Slf4j

public class checkTestMain {

    @Resource
    private HisCheckTaskMapper hisCheckTaskMapper;

    @Resource
    private HisCheckTaskReultMapper hisCheckTaskReultMapper;

    @Resource
    private IndicatorService indicatorService;

    @Resource(name = "oracleJdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    @Resource(name = "sqlSessionFactory")
    private SqlSessionFactory sqlSessionFactory;

    public static void main(String[] args) {

        log.info("dataCheck begin");

        LogicMotypeConstant.init();

        log.info("dataSet init");
        ApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
        SyncCheckTask syncCheckTask = context.getBean(SyncCheckTask.class);


        SparkSession sparkSession = SparkSession.builder().appName("histDataCheck").enableHiveSupport().getOrCreate();
        SparkSession newSession = sparkSession.newSession();
        SparkContext sc = sparkSession.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);
    }
}
