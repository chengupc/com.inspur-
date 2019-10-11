package com.inspur.mtn.checkDataTool;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.inspur.mtn.checkDataTool.oracle.HisCheckTaskMapper;
import com.inspur.mtn.checkDataTool.oracle.HisCheckTaskReultMapper;
import com.inspur.mtn.checkDataTool.pojo.CheckTask;
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
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Component
@Slf4j
public class SyncCheckTaskTest {

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
        SyncCheckTaskTest testMain = context.getBean(SyncCheckTaskTest.class);

        Properties properties = new Properties();
        try {
            properties.load(SyncCheckTaskTest.class.getClassLoader().getResourceAsStream("dataBase.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        String hiveSID = properties.getProperty("hiveSID");
        log.info("hvieSID {}", hiveSID);

        SparkSession sparkSession = SparkSession.builder().appName("histDataCheck").enableHiveSupport().getOrCreate();
        SparkSession newSession = sparkSession.newSession();
        SparkContext sc = sparkSession.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);

        log.info("Spark ok {}");

        int threadNum = 2;
        ExecutorService exec = Executors.newFixedThreadPool(threadNum,
                new ThreadFactoryBuilder().setNameFormat("work-threadt-%d").build());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shut down thread pool.");
            exec.shutdown();
            try {
                while (!exec.awaitTermination(5, TimeUnit.MINUTES)) {

                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }));
        //格式化时间
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

        log.info("format {}", format);
        if (true) {

            List<CheckTask> checkTaskList = testMain.hisCheckTaskMapper.getCheckTaskList();
            log.info("checkTaskSize {}", checkTaskList.size());

            if (checkTaskList.isEmpty() || checkTaskList == null) {
                log.info("There are no taskCheck");
                //log.info("Spark path is {}", sparkSession.sparkContext().getConf().get("spark.hive.root.path"));

                try {
                    log.info("waiting 7s .........");
                    TimeUnit.SECONDS.sleep(7);
                } catch (InterruptedException e) {
                    log.info("Wait error -> {}", e.getMessage());
                }
            }

            log.info("Get {} taskChecks", checkTaskList.size());

            List<Future<String>> futures = new ArrayList<>(checkTaskList.size());
            log.info("future size {}", futures.size());

            //更新任务单状态为3，表示正在执行。
            checkTaskList.stream().forEach(checkTask -> testMain.hisCheckTaskMapper.updateStatusAndExecTimes(3, checkTask.getTaskId(), Long.valueOf(format.format(LocalDateTime.now()))));
            log.info("There are {} taskChecks to deal with", checkTaskList.size());

            Map<String, List<CheckTask>> groupByKey = checkTaskList.stream().collect(Collectors.groupingBy(t -> t.getMotype() + t.getPeriod() + t.getStartDay() + t.getStartTime()));
            for (String groupKey : groupByKey.keySet()) {
                List<CheckTask> checkTasks = groupByKey.get(groupKey);
                List<Long> checkTaskIds = checkTasks.stream().map(checkTask -> checkTask.getTaskId()).collect(Collectors.toList());
                CheckTask checkTask = checkTasks.get(0);

                Future<String> future = exec
                        .submit(new CheckWorkerTest(checkTaskIds, testMain.jdbcTemplate, testMain.indicatorService,
                                checkTask, jsc, newSession, sparkSession, testMain.hisCheckTaskMapper, testMain.hisCheckTaskReultMapper, hiveSID));
                futures.add(future);
                break;
            }
            for (Future<String> f : futures) {
                try {
                    log.info("CheckTask finish result is {}", f.get());
                } catch (InterruptedException | ExecutionException e) {
                    log.error(e.getMessage(), e);
                }
            }
            log.info("task deal with end");

            }
        }
    }

