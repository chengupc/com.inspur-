package com.inspur.mtn.checkDataTool;

import com.inspur.mtn.checkDataTool.dao.IndicatorMapper;
import com.inspur.mtn.checkDataTool.oracle.HisCheckTaskMapper;
import com.inspur.mtn.checkDataTool.pojo.Generate;
import com.inspur.mtn.checkDataTool.service.IndicatorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

@Component
@Slf4j
public class GeneratingTaskSheets {

    //？codis配置文件，zk地址
    @Resource
    private CodisService codisService;

    @Resource
    private HisCheckTaskMapper hisCheckTaskMapper;

    @Resource
    private IndicatorMapper indicatorMapper;

    @Resource
    private IndicatorService indicatorService;

    @Resource(name = "oracleJdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    @Resource(name = "sqlSessionFactory")
    private SqlSessionFactory sqlSessionFactory;

    public static void main(String[] args) {

        ApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
        GeneratingTaskSheets generatingTaskSheets = context.getBean(GeneratingTaskSheets.class);

        List<Generate> GenerateTasksList = generatingTaskSheets.hisCheckTaskMapper.getGenerateTaskList();
        for(Generate generate : GenerateTasksList){
            generatingTaskSheets.hisCheckTaskMapper.insertGenerateTask(generate.getModelID(),"h",20190926,0,20190926,20190926,20190926);

        }
    }
}

