package com.inspur.mtn.checkDataTool;

import com.inspur.mtn.checkDataTool.codis.CodisPool;
import com.inspur.mtn.checkDataTool.service.Compress;
import io.codis.jodis.JedisResourcePool;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class CodisWriterRdd implements VoidFunction<Iterator<Row>> {
    private List<String> headers;
    private String motypeid;
    private String period;
    private String startDay;
    private String startTime;

    public CodisWriterRdd(List<String> headers, String motypeid, String period, String startDay, String startTime) {
        this.headers = headers;
        this.motypeid = motypeid;
        this.period = period;
        this.startDay = startDay;
        this.startTime = startTime;

    }

    public void call(Iterator<Row> rowIterator)  {

        if (!rowIterator.hasNext()) {
            return;
        }
        JedisResourcePool pool = null;
        Jedis jedis = null;
        try {
            pool = CodisPool.getInstance().getPool();
            StringBuffer headerString = new StringBuffer("");
            for (String headerStr : headers) {
                if(headerStr.startsWith("a")){
                    continue;
                }
                headerString.append(headerStr).append("|");
            }
            String headerResult = headerString.toString().substring(0, headerString.length()-1);
            String key2 = motypeid + period + startDay + startTime;
            Pipeline p;
            jedis = pool.getResource();
            p = jedis.pipelined();
            String result;
            Row row;
            String header;
            StringBuffer entityResult = new StringBuffer("");
            entityResult.append(headerResult);
            entityResult.append("\r\n");
            while (rowIterator.hasNext()) {
                StringBuffer indicatorValue = new StringBuffer("");
                row = rowIterator.next();
                for (int i = 0; i < headers.size(); i++) {
                    header = headers.get(i);
                    boolean need = true;
                    if (header.equalsIgnoreCase("mo")) {
                        if(null!=row.get(i)){
                            indicatorValue.append(row.getString(i));
                        }
                    }
                    else if(header.equalsIgnoreCase("name"))
                    {
                        if(null!=row.get(i)){
                            indicatorValue.append(row.getString(i));
                        }
                    }
                    else if (header.startsWith("r")) {
                        if(null!=row.get(i)){
                            indicatorValue.append(row.getInt(i));
                        }
                    } else if (header.startsWith("n") && !header.equalsIgnoreCase("name")) {//n745
                        if(null!=row.get(i))
                        {
                            indicatorValue.append(row.getString(i));
                        }
                    } else if (header.startsWith("a")) {
                        log.info("oracle2hive CodisWriterRdd ignore attribute....");
                        need = false;
                    } else if (header.equalsIgnoreCase("sd")) {
                        if(null!=row.get(i))
                        {
                            indicatorValue.append(row.getInt(i));
                        }
                    } else if (header.equalsIgnoreCase("st")) {
                        if(null!=row.get(i))
                        {
                            indicatorValue.append(row.getInt(i));
                        }
                    } else if (header.equalsIgnoreCase("sdt")) {
                        if(null!=row.get(i))
                        {
                            indicatorValue.append(row.getLong(i));
                        }
                    }
                    else if (header.equalsIgnoreCase("isonsite")) {
                        if(null!=row.get(i))
                        {
                            indicatorValue.append(row.getString(i));
                        }
                    }
                    else if (header.startsWith("i") || header.startsWith("k")) {
                        if(null!=row.get(i))
                        {
                            indicatorValue.append(row.getDouble(i));
                        }
                    }else if (header.contains("bh") || header.contains("wh")||header.contains("mh")) {
                        if(null!=row.get(i))
                        {
                            indicatorValue.append(row.getDouble(i));
                        }

                    }

                    if(need){
                        indicatorValue.append("|");
                    }
                }
                //key1: motypeid+period+startday+endday  value1: motypeid+moentityid+period+startday+starttime
                //key2: motypeid+period+startday+starttime+moentityid value2:hive one row data after zip
                result = indicatorValue.toString().substring(0, indicatorValue.length()-1);
                entityResult.append(result);
                entityResult.append("\r\n");

            }

            byte[] byteArray = key2.getBytes();
            p.set(byteArray, new Compress().zip(entityResult.toString().getBytes()));
            p.sync();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("CodisWriterRdd error."+e.getMessage());
        }finally {
            if(jedis !=null){
                jedis.close();
            }
            if(pool !=null){
                try {
                    pool.close();
                } catch (IOException e) {
                    log.error("close redis pool error."+e.getMessage());
                }
            }
        }

    }

}