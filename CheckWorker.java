package com.inspur.mtn.checkDataTool;

import com.inspur.mtn.checkDataTool.dao.OracleDao;
import com.inspur.mtn.checkDataTool.oracle.HisCheckTaskMapper;
import com.inspur.mtn.checkDataTool.oracle.HisCheckTaskReultMapper;
import com.inspur.mtn.checkDataTool.service.IndicatorService;
import com.inspur.mtn.checkDataTool.pojo.CheckTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
import java.util.HashMap;

import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

@Slf4j
public class CheckWorker implements java.io.Serializable, Callable<String> {

    private transient JavaSparkContext jsc;

    private transient SparkSession sparkSession;
    private transient SparkSession newSession;
    private transient CheckTask checkTask = null;
    private transient IndicatorService indicatorService;

    private transient HisCheckTaskMapper hisCheckTaskMapper;
    private transient HisCheckTaskReultMapper hisCheckTaskReultMapper;
    private transient JdbcTemplate jdbcTemplate;
    private transient List<Long> hisCheckTaskIds;
    private transient DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private transient String hiveSID;

    public CheckWorker(List<Long> checkTaskIds, JdbcTemplate jdbcTemplate,IndicatorService indicatorService,
                       CheckTask checkTask, JavaSparkContext jsc, SparkSession newSession, SparkSession sparkSession,
                       HisCheckTaskMapper hisCheckTaskMapper, HisCheckTaskReultMapper hisCheckTaskReultMapper, String hiveSID) {
        this.indicatorService = indicatorService;
        this.jdbcTemplate = jdbcTemplate;
        this.newSession = newSession;
        this.jsc = jsc;
        this.sparkSession = sparkSession;
        this.hisCheckTaskMapper = hisCheckTaskMapper;
        this.checkTask = checkTask;
        this.hisCheckTaskIds = checkTaskIds;
        this.hisCheckTaskReultMapper = hisCheckTaskReultMapper;
        this.hiveSID = hiveSID;
    }

    @Override
    public String call() throws Exception, AnalysisException {

        //等待写入checkTask表中的几个字段
        int allCheckFieldsNum =0 ;
        int errorCheckFieldsNum = 0;
        int rightCheckFieldsNum = 0;

        double errorThreshold = 0.99;
        double checkAccuracyRate = 0.000;
        Double diffCheck = 0.01;

        boolean isCatchException = false;
        HashMap<String,List<String>> mosAndFieldsResult = new HashMap<>();
        String resultStatuted = "is sucess ";

        //开始时间
        long start = System.currentTimeMillis();
        try{
            log.info("checkWorker begin primary motype is -> {}_{}_{}", checkTask.getMotype(), checkTask.getStartDay(),checkTask.getStartTime());

            //初始化hive数据库连接
            String getHiveConnect = "use "+ hiveSID;
            newSession.sql(getHiveConnect);

            log.info("hive dataset get connnection");

            //获取hiveFilelds的字段（i+数字结尾的字段）
            List<String> hiveFields = getHiveFileds(newSession,checkTask,hiveSID);
            //log.info("hiveFields {}", hiveFields.toString());

            //从checkTask中获取配置实体
            List<String> checkKPIs = new ArrayList<>();

            //从checkTask中获取配置指标
            String indicators = checkTask.getIndicators();
            if(indicators != null){
                String[] indicatorsArray = indicators.split(",");
                List<String> indicatorsList = Arrays.asList(indicatorsArray);
                checkKPIs = indicatorsList;
                log.info("indicatorsList size = {}" ,indicatorsList.size());
            }
            else{
                log.info("indicators is null");
                checkKPIs = hiveFields;
            }


            StringBuilder firstCommonSql = new StringBuilder();
            StringBuilder secondCommonSql = new StringBuilder();

            StringBuilder hiveFirstSql = new StringBuilder();

            firstCommonSql.append("select mo");
            for(String checkKPI:checkKPIs){
                if(checkKPI == "mo"){
                    continue;
                }
                else{
                    firstCommonSql.append(",");
                    firstCommonSql.append(checkKPI);
                }
            }
            hiveFirstSql.append(" from ").append("t_").append(checkTask.getMotype()).append("_").append(checkTask.getPeriod().toLowerCase());
            secondCommonSql.append(" where sd=").append(checkTask.getStartDay()).append(" and").append(" st=").append(checkTask.getStartTime()).append(" order by mo desc");

            String hiveGetAllSql = firstCommonSql.toString() + hiveFirstSql.toString() + secondCommonSql.toString();

            String hiveTmpTableName = "hiveTmpTable";
            Dataset<Row> hiveData = getHiveData(hiveGetAllSql);
            hiveData.createTempView(hiveTmpTableName);

            //hive实体
            Dataset<Row> mosByHiveData = newSession.sql("select mo from "+ hiveTmpTableName);
            log.info("moData is ok");
            List<Row> mosByHiveRow = mosByHiveData.collectAsList();
            log.info("moRow is ok");
            List<String> moByHiveList = new ArrayList<>();
            mosByHiveRow.forEach(t->moByHiveList.add(String.valueOf(t.get(0))));
            //log.info("moList is ok");
            log.info("moList size is {}", moByHiveList.size());
            //log.info("mosByHiveList are {}", moByHiveList);

            //获取oracle数据，
            //List<String> allFields = new ArrayList<>(256);
            JavaRDD<Row> oracleDataJavaRDD = OracleDao.getOldData(indicatorService,checkTask,jsc,jdbcTemplate,hiveFields);
            Dataset<Row> oracleMidData = changeDataType(oracleDataJavaRDD, hiveFields);
            log.info("oracleMidData size {}", oracleMidData.collectAsList().size());
            String oracleMidTmpTableName ="oracleMidTmpTable";
            oracleMidData.createOrReplaceTempView(oracleMidTmpTableName);
            log.info("oracleMidTmpTable is ok");

            String oracleSql = firstCommonSql.toString()+" from "+oracleMidTmpTableName+" order by mo desc";
            Dataset<Row> oracleData = newSession.sql(oracleSql);
            log.info("oracleResult data is ok");
            //log.info("oracleData schema {}", oracleData.schema());

            String oracleTmpTableName = "oracleTmpTable";
            oracleData.createTempView(oracleTmpTableName);
            log.info("oracleTmpTable is ok");

            //oracleMo
            Dataset<Row> mosByOracleData = newSession.sql("select mo from "+oracleTmpTableName);
            //log.info("mosByOracleData is ok");

            List<Row> mosByOracleRow = mosByOracleData.collectAsList();
            //log.info("moByOracleRow is ok");

            List<String> mosByOracleList = new ArrayList<>();
            mosByOracleRow.forEach(t-> mosByOracleList.add(String.valueOf(t.get(0))));

            List<String> resultMosList = new ArrayList<>();

            for(String mo:moByHiveList){
                if(mosByOracleList.contains(mo)){
                    resultMosList.add(mo);
                }
            }

            List<String> checkMos = new ArrayList<>();
            String moentities = checkTask.getMoentities();

            if(moentities != null){
                String[] moentitiesArray = indicators.split(",");
                List<String> moentitiesList = Arrays.asList(moentitiesArray);
                resultMosList = mosByOracleList;
                log.info("moentitiesList size = {}" ,moentitiesList.size());
            }
            else{
                log.info("Moentities is null");
            }
            allCheckFieldsNum = resultMosList.size() * checkKPIs.size();
            log.info("allCheckFieldsNum =: {}",allCheckFieldsNum);

            log.info("resultMosList size {}", resultMosList.size());

            long start1 = System.currentTimeMillis();

            if(resultMosList.size() <= 1000){

                StringBuilder sbOneBatchSql = new StringBuilder("( ");

                for(String mo:resultMosList){
                    sbOneBatchSql.append(mo);
                    sbOneBatchSql.append(",");
                }
                sbOneBatchSql.deleteCharAt(sbOneBatchSql.length()-1);
                sbOneBatchSql.append(")");
                log.info("sbOneBatchSql low num {}",sbOneBatchSql.toString());

                Dataset<Row> hiveOneBatchData = newSession.sql("select * from "+hiveTmpTableName +" where mo in "+sbOneBatchSql.toString());
                Dataset<Row> oracleOneBatchData = newSession.sql("select * from "+oracleTmpTableName +" where mo in "+sbOneBatchSql.toString());

                List<Row> hiveRowDataList = hiveOneBatchData.collectAsList();
                List<Row> oracleRowDataList = oracleOneBatchData.collectAsList();

                for(int j =0; j< resultMosList.size(); j++){

                    List<String> diffFiled = new ArrayList<>();
                    for(int i =0 ; i< checkKPIs.size(); i++){
                        Object hiveField = hiveRowDataList.get(j).get(i);
                        Object oracleField = oracleRowDataList.get(j).get(i);
                        if(hiveField != null && oracleField != null){
                            Double didiff = Double.parseDouble(hiveField.toString())  - Double.parseDouble(oracleField.toString());
                            if (Math.abs(didiff) <= diffCheck){
                                continue;
                            }
                            else {
                                diffFiled.add(checkKPIs.get(i));
                                errorCheckFieldsNum++;
                            }
                        }
                        else {
                            continue;
                        }
                    }
                    if(diffFiled != null && diffFiled.size() !=0 ){
                        mosAndFieldsResult.put(hiveRowDataList.get(j).get(0).toString(), diffFiled);
                    }
                }
            }
            else {
                int batchNum = resultMosList.size() / 1000;
                int leftNum = resultMosList.size() % 1000;
                log.info("leftNum {}",leftNum);
                int k = 0;
                for (; k < batchNum; k++) {
                    StringBuilder sbOneBatchSql = new StringBuilder("( ");

                    for (String mo : resultMosList.subList(k * 1000, (k + 1) * 1000)) {
                        sbOneBatchSql.append(mo);
                        sbOneBatchSql.append(",");
                    }
                    sbOneBatchSql.deleteCharAt(sbOneBatchSql.length() - 1);
                    sbOneBatchSql.append(")");
                    log.info("sbOneBatchSql {}",sbOneBatchSql.toString());

                    Dataset<Row> hiveOneBatchData = newSession.sql("select * from " + hiveTmpTableName + " where mo in " + sbOneBatchSql.toString());
                    Dataset<Row> oracleOneBatchData = newSession.sql("select * from " + oracleTmpTableName + " where mo in " + sbOneBatchSql.toString());

                    List<Row> hiveRowDataList = hiveOneBatchData.collectAsList();
                    List<Row> oracleRowDataList = oracleOneBatchData.collectAsList();

                    for (int j = 0; j < 1000; j++) {

                        List<String> diffFiled = new ArrayList<>();
                        for (int i = 0; i < checkKPIs.size(); i++) {
                            Object hiveField = hiveRowDataList.get(j).get(i);
                            Object oracleField = oracleRowDataList.get(j).get(i);
                            if (hiveField != null && oracleField != null) {
                                Double didiff = Double.parseDouble(hiveField.toString()) - Double.parseDouble(oracleField.toString());
                                if (Math.abs(didiff) <= diffCheck) {
                                    continue;
                                } else {
                                    //fieldsResult.add(hiveField.toString());
                                    diffFiled.add(checkKPIs.get(i));
                                    errorCheckFieldsNum++;
                                }
                            } else {
                                continue;
                            }

                        }
                        if(diffFiled != null && diffFiled.size() !=0 ){
                            mosAndFieldsResult.put(hiveRowDataList.get(j).get(0).toString(), diffFiled);
                        }
                    }

                }
                if (leftNum > 0) {
                    StringBuilder sbOneBatchSql = new StringBuilder("( ");

                    for (String mo : resultMosList.subList(k * 1000, k * 1000 + leftNum )) {
                        sbOneBatchSql.append(mo);
                        sbOneBatchSql.append(",");
                    }
                    sbOneBatchSql.deleteCharAt(sbOneBatchSql.length() - 1);
                    sbOneBatchSql.append(")");
                    log.info("sbOneBatchSql leftnum {}",sbOneBatchSql.toString());

                    Dataset<Row> hiveOneBatchData = newSession.sql("select * from " + hiveTmpTableName + " where mo in " + sbOneBatchSql.toString());
                    Dataset<Row> oracleOneBatchData = newSession.sql("select * from " + oracleTmpTableName + " where mo in " + sbOneBatchSql.toString());

                    List<Row> hiveRowDataList = hiveOneBatchData.collectAsList();
                    List<Row> oracleRowDataList = oracleOneBatchData.collectAsList();

                    for (int j = 0; j < leftNum; j++) {

                        List<String> diffFiled = new ArrayList<>();
                        for (int i = 0; i < checkKPIs.size(); i++) {
                            Object hiveField = hiveRowDataList.get(j).get(i);
                            Object oracleField = oracleRowDataList.get(j).get(i);
                            if (hiveField != null && oracleField != null) {
                                Double didiff = Double.parseDouble(hiveField.toString()) - Double.parseDouble(oracleField.toString());
                                if (Math.abs(didiff) <= diffCheck) {
                                    continue;
                                } else {
                                    //fieldsResult.add(hiveField.toString());
                                    diffFiled.add(checkKPIs.get(i));
                                    errorCheckFieldsNum++;
                                }
                            } else {
                                continue;
                            }
                        }
                        if(diffFiled != null && diffFiled.size() !=0 ){
                            mosAndFieldsResult.put(hiveRowDataList.get(j).get(0).toString(), diffFiled);
                        }
                    }
                }
            }
            long end = System.currentTimeMillis();
            log.info("check cost time {}", end - start1);
//
            rightCheckFieldsNum = allCheckFieldsNum - errorCheckFieldsNum;
            log.info("allCheckFieldsNum = {}, rightCheckFieldsNum = {}, errorCheckFieldsNum = {}",
                    allCheckFieldsNum,rightCheckFieldsNum,errorCheckFieldsNum);
            checkAccuracyRate = (double) rightCheckFieldsNum / (double) allCheckFieldsNum;
            log.info("checkAccuracyRate = {}",checkAccuracyRate);
        }catch (Exception e) {
            isCatchException = true;
            log.info("isCatchException = {}",isCatchException);
            e.printStackTrace();
        }
        log.info("finish task spend time {}",System.currentTimeMillis()-start);
        if(isCatchException){
            //更新任务单失败，任务状态为2
            this.hisCheckTaskIds.stream().forEach(t -> hisCheckTaskMapper.updateCheckTaskStatus(2, t));

            resultStatuted = " is failed";
        }
        else {

            //更新任务单成功，且数据相同，任务状态为1
            if(checkAccuracyRate > errorThreshold){
                int finalAllCheckFields = allCheckFieldsNum;
                int finalErrorCheckFields = errorCheckFieldsNum;
                int finalRightCheckFields = rightCheckFieldsNum;
                this.hisCheckTaskIds.stream().forEach(t->hisCheckTaskMapper.updateStatusAndResultAndFinishTime(1,
                        finalAllCheckFields, finalRightCheckFields, finalErrorCheckFields,Long.valueOf(format.format(LocalDateTime.now())), t));
            }

            //更新任务单成功，且数据相同，任务状态为-1
            else {
                int finalAllCheckFields = allCheckFieldsNum;
                int finalErrorCheckFields = errorCheckFieldsNum;
                int finalRightCheckFields = rightCheckFieldsNum;
                this.hisCheckTaskIds.stream().forEach(t -> hisCheckTaskMapper.updateStatusAndResultAndFinishTime(-1,
                        finalAllCheckFields, finalRightCheckFields, finalErrorCheckFields, Long.valueOf(format.format(LocalDateTime.now())), t));
                try{
                    for (Map.Entry<String, List<String>> entry : mosAndFieldsResult.entrySet()) {

                        hisCheckTaskReultMapper.InsertCheckErroInfo(checkTask.getMotype(), checkTask.getPeriod(), checkTask.getStartDay(), checkTask.getStartTime(), entry.getKey(), entry.getValue().toString());

                    }
                }catch (Exception e ){
                    log.error("checkResult insert errror");
                    e.printStackTrace();
                    resultStatuted = " is failed";
                }

            }
        }
        return "worker-" + checkTask.getPeriod() + "_" + checkTask.getMotype() + "_" + checkTask.getStartDay() + "_"
                + checkTask.getStartTime() +" "+ resultStatuted;

    }
    private Dataset<Row> getHiveData(String hiveGetAllSql) {
        return newSession.sql(hiveGetAllSql);
    }

    private Dataset<Row> changeDataType(JavaRDD<Row> oldDataJavaRDDAfterFilter, List<String> allFields) {
        ArrayList<StructField> fields = new ArrayList<>();
        StructField field = null;
        for (String allField :allFields) {
            if (allField.equalsIgnoreCase("MO")){
                field = DataTypes.createStructField(allField, DataTypes.IntegerType, true);
                fields.add(field);
            }else if (allField.startsWith("i")){
                field = DataTypes.createStructField(allField, DataTypes.DoubleType, true);
                fields.add(field);
            }
        }
        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> dfs = newSession.createDataFrame(oldDataJavaRDDAfterFilter, schema);
        return dfs;
    }
//        private HashMap<Integer, HashMap<String, List<String>>> checkFunction(Dataset<Row> newData, Dataset<Row> oldData, List<String> allFields, String hivetmpdata, String oralcetmpdata , int allFieldsize) {
//
//            log.info("check data start...");
//            HashMap<Integer,HashMap<String, List<String>>> errorFieldsNumAndMoAndFields = new HashMap<>();
//
//            //有待优化，可以从两个临时表中获取
//            Dataset<Row> checkResultTmp = newData.except(oldData);
//            List<Row> checkResultTmpList = checkResultTmp.collectAsList();
//
//            List<String> errorMoList = new ArrayList<>();
//
//            checkResultTmpList.stream().forEach(t-> errorMoList.add(t.get(0).toString()));
//
//            HashMap<String, List<String>> errorMoAndFields = new HashMap<>();
//
//            int errorFieldsNum = 0;
//
//            for(String errorMo: errorMoList){
//
//                String hiveCheckSql = "select * from "+hivetmpdata+" t where t.mo="+"'"+errorMo+"'";
////            log.info("hiveCheckSql =: {}",hiveCheckSql);
//                String oracleCheckSql = "select * from "+oralcetmpdata+" t where t.mo="+"'"+errorMo+"'";
////            log.info("oracleCheckSql =: {}",oracleCheckSql);
//                Dataset<Row> hiveRowData = newSession.sql(hiveCheckSql);
//                Dataset<Row> oracleRowData = newSession.sql(oracleCheckSql);
//
//                List<Row> hiveRowDataList = hiveRowData.collectAsList();
////            log.info("hiveRowDataList size = {}",hiveRowDataList.size());
//                List<Row> oracleRowDataList = oracleRowData.collectAsList();
////            log.info("oracleRowDataList size = {}",oracleRowDataList.size());
//                List<String> errorFields = new ArrayList<>();
//
//                for(int i=0 ; i< allFieldsize; i++){
//                    if(hiveRowDataList.get(i) != oracleRowDataList.get(i)){
//                        errorFields.add(allFields.get(i));
//                        errorFieldsNum++;
//                    }
//                }
//                errorMoAndFields.put(errorMo, errorFields);
//            }
//
//            errorFieldsNumAndMoAndFields.put(errorFieldsNum,errorMoAndFields);
//
//
//            return errorFieldsNumAndMoAndFields;
//        }

    private static List<String> getHiveFileds(SparkSession sparkSession, CheckTask checkTask, String hiveSID) {

        List<String> allFieldsByHive = new ArrayList<>();

        List<org.apache.spark.sql.catalog.Column> columns = null;
        try{
            columns = sparkSession.catalog().listColumns(hiveSID,"t_"+checkTask.getMotype()+"_"+checkTask.getPeriod().toLowerCase()).collectAsList();

        }catch (Exception e){
            log.info("hive get columns error");
            e.printStackTrace();

        }

        columns.stream().forEach(c -> {
            String field_ = c.name();
            if(field_.startsWith("i")&& Pattern.matches("^i[0-9]+$", field_)){
                allFieldsByHive.add(field_);
            }else if(field_.equalsIgnoreCase("mo")) {
                allFieldsByHive.add(field_);
            }
        });
        return allFieldsByHive;
    }

}





