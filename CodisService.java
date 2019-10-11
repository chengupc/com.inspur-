package com.inspur.mtn.checkDataTool;

import com.google.common.collect.Lists;
import io.codis.jodis.RoundRobinJedisPool;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import javax.annotation.Resource;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

@Slf4j
@Component
@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
public class CodisService implements Serializable {
	private static final long serialVersionUID = -102101L;
	@Resource(name="codisPool")
	private RoundRobinJedisPool pool;

	public Map<String,String> getMappingMotype() {
		Map<String, String> motypes = null;
		try (Jedis jedis = pool.getResource()) {
			try (Pipeline p = jedis.pipelined()) {
				Response<Map<String, String>> res = p.hgetAll("ALLMODELMAPPING");
				p.sync();
				motypes = res.get();
			} catch (Exception e) {
				log.error("getMappingMotype error1 -> {}", e.getMessage());
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return motypes;
	}
	
	
	/**
	 * 获取模型数据
	 * 
	 * @param moType
	 * @return
	 */
	public Map<String, Map<String, String>> getMoData(String moType) {
		log.info("getMoData from codis begin motype -> {}", moType);

		Map<String, Map<String, String>> moData = new HashMap<String, Map<String, String>>();
		Map<String,String>[] motype2sequence = new Map[1];
		try(Jedis jedis = pool.getResource()){

			try(Pipeline p = jedis.pipelined()){
				Response<Map<String,String>> res = p.hgetAll("MOTYPE:MOTYPEID:SEQUENCE");
				p.sync();
				motype2sequence[0] = res.get();
			}catch(Exception e) {
				log.error(e.getMessage(),e);
			}
			String keys = null;
			try(Pipeline p = jedis.pipelined()){
				Response<String> res = p.get("MOTYPE:OBJ:LIST:" + moType);
				p.sync();
				keys = res.get();
			}catch(Exception e) {
				log.error("getMoData() error1 -> {}",e.getMessage());
			}
			if (keys == null) {
				log.warn("obj is null,motype -> {}", moType);
				return null;
			}
			List<String> objs = null;
			List<String> keyList = Arrays.asList(keys.split(","));
			try(Pipeline p = jedis.pipelined()){
				List<Response<String>> responses = keyList.stream().map(col -> p.get(col)).collect(Collectors.toList());
				p.sync();
				objs = responses.stream().flatMap(k -> Arrays.stream(k.get().split(","))).collect(Collectors.toList());
			}catch(Exception e) {
				log.error("getMoData() error2 -> {}",e.getMessage());
			}
			List<List<String>> objList = Lists.partition(objs, 5000);
			for (int i=0;i<objList.size();i++) {
				log.info("begin query codis {}/{},motype -> {}",i+1,objList.size(),moType);
				List<String> obj = objList.get(i);
				try(Pipeline p = jedis.pipelined()){
					List<Response<Map<String, String>>> responses = obj.stream()
							.map(col -> p.hgetAll("OBJ:"+motype2sequence[0].get(moType)+":" + col)).collect(Collectors.toList());
					p.sync();
					List<Map<String, String>> objMap = responses.stream().map(k -> k.get()).collect(Collectors.toList());
					for (Map<String, String> map : objMap) {
						if(map.get("MO")==null||"null".equalsIgnoreCase(map.get("MO"))) {
							continue;
						}
						moData.put(map.get("MO"), map);
					}
				}catch(Exception e) {
					log.error("getMoData() error3 -> {}",e.getMessage());
				}
			}
		}catch(Exception e) {
			log.error("getMoData() error -> {}",e.getMessage());
		}
		log.info("getMoData from codis end obj size -> {},motype->{}", moData.size(), moType);
		return moData;
	}

	/**
	 * 获取Hive元数据
	 * 
	 * @param motypeid
	 * @return
	 */
	public String[] getHiveMeta(String motypeid) {
		log.info("getHiveMeta from codis begin motype -> {}", motypeid);
		String str = null;
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			str = jedis.get("MOTYPE:CONFIG:" + motypeid);
		} catch (Exception e) {
			log.error("getHiveMeta {} get data error -> {}", motypeid, e.getMessage());
			e.printStackTrace();
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
		if (str == null) {
			log.warn("getHiveMeta error :data is null motype -> {}", motypeid);
			return null;
		}
		log.info("getHiveMeta from codis end motype -> {}", motypeid);
		return str.split(",");
	}

	/**
	 * 获取指标id和seq的对应关系
	 * 
	 * @param
	 * @return
	 */
	public Map<String, String> getInidcatorIdByModel() {
		log.info("getInidcatorIdByModel begin motype");
		Map<String, String> result = null;
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			result = jedis.hgetAll("ALLINDICATORID2SEQ:CONF");
		} catch (Exception e) {
			log.error("getMoData get redis error -> {}", e.getMessage());
			e.printStackTrace();
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
		if (result == null) {
			log.warn("getInidcatorCodeByModel warn:indicator is null -> {}");
			return null;
		}
		for (Entry<String, String> res : result.entrySet()) {
			result.put(res.getKey(), "I" + res.getValue());
		}
		log.info("getInidcatorIdByModel end size -> {}", result.size());
		return result;
	}
	/**
	 * 
	 */
	public void Close() {
		if (pool != null) {
			pool.close();
		}
	}

}
