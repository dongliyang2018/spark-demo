package com.ultrapower.bms.spark;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.ultrapower.bms.spark.entity.Log;

import scala.Tuple2;

/**
 * Spark示例
 * @version 1.0 2018/08/06
 * @author dongliyang
 */
public class App {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("App")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
	
		firstRule(sc);
		secondRule(sc);
		thirdRule(sc);
		
		sc.close();
	}
	
	public static void firstRule(JavaSparkContext sc) {
		
		/*
		 * 规则1: 统计单1文件、或者几个文件、或者连续上报的几个文件中符合某个条件的记录数或者某个字段的求和
		 * 应用场景：统计符合规则的记录数
		 */
		
		List<Log> logList = new ArrayList<>();
		logList.add(new Log("Zhang", "A", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 10, 30).getTime()));
		logList.add(new Log("Wang", "B", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 10, 35).getTime()));
		logList.add(new Log("Li", "A", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 10, 40).getTime()));
		logList.add(new Log("Zhao", "B", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 10, 50).getTime()));
		logList.add(new Log("Zhou", "C", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 11, 00).getTime()));
		logList.add(new Log("Wu", "D", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 11, 10).getTime()));
		logList.add(new Log("Zheng", "C", 0, new GregorianCalendar(2018, Calendar.AUGUST, 11, 30, 50).getTime()));
		logList.add(new Log("Sun", "D", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 12, 50).getTime()));
		
		JavaRDD<Log> logRDD = sc.parallelize(logList).cache();
		
		//符合某个条件的记录数: 比如：resType为A的记录数
		long totalTypeCount = logRDD.filter(log -> "A".equals(log.getResType())).count();
		System.out.println("资源类型为A的记录数:" + totalTypeCount);
		
		//某个字段的求和:  比如：loginResult的值，1代表成功，0代表失败， 对loginResult求和，计算总的成功登录次数
		int successLoginTimes = logRDD.map(log -> log.getLoginResult()).reduce((x,y) -> x + y);
		System.out.println("总的成功登录次数:" + successLoginTimes);
		
	}
	
	public static void secondRule(JavaSparkContext sc) {
		
		/*
		 * 规则2：统计几个文件中，相同条件记录的，时间差
		 * 应用场景：第一个日志为登录日志，第二个日志为操作日志，一个用户操作日志的时间应该在登录日志时间之后，否则，就涉嫌没有登录就执行操作
		 */
		
		//第一个日志文件
		List<Log> firstLogList = new ArrayList<>();
		firstLogList.add(new Log("Zhang", "A", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 10, 30).getTime()));
		firstLogList.add(new Log("Wang", "B", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 10, 35).getTime()));
		firstLogList.add(new Log("Li", "A", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 10, 40).getTime()));
		firstLogList.add(new Log("Zhao", "B", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 10, 50).getTime()));
		firstLogList.add(new Log("Zhou", "C", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 11, 00).getTime()));
		firstLogList.add(new Log("Wu", "D", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 11, 10).getTime()));
		firstLogList.add(new Log("Zheng", "C", 0, new GregorianCalendar(2018, Calendar.AUGUST, 6, 11, 50).getTime()));
		firstLogList.add(new Log("Sun", "D", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 12, 50).getTime()));
		
		//第二个日志文件
		List<Log> secondLogList = new ArrayList<>();
		secondLogList.add(new Log("Zhang", "A", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 10, 40).getTime()));
		secondLogList.add(new Log("Wang", "B", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 10, 50).getTime()));
		secondLogList.add(new Log("Li", "A", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 10, 46).getTime()));
		secondLogList.add(new Log("Zhao", "B", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 11, 20).getTime()));
		secondLogList.add(new Log("Zhou", "C", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 11, 30).getTime()));
		secondLogList.add(new Log("Wu", "D", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 11, 17).getTime()));
		secondLogList.add(new Log("Zheng", "C", 0, new GregorianCalendar(2018, Calendar.AUGUST, 6, 11, 52).getTime()));
		secondLogList.add(new Log("Sun", "D", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 12, 58).getTime()));
		
		secondLogList.add(new Log("Hacker", "A", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 11, 58).getTime()));
		
		
		JavaRDD<Log> firstLogRDD = sc.parallelize(firstLogList);
		JavaRDD<Log> secondLogRDD = sc.parallelize(secondLogList);
		
		JavaPairRDD<String, Date> firstLogPairRDD = firstLogRDD.mapToPair(log -> new Tuple2<String,Date>(log.getLoginName(), log.getLoginTime()));
		JavaPairRDD<String, Date> secondPairRDD = secondLogRDD.mapToPair(log -> new Tuple2<String,Date>(log.getLoginName(), log.getLoginTime()));
		//注意join、leftOuterJoin、rightOuterJoin的区别
		JavaRDD<Tuple2<String, Long>>  timeRangeRDD = firstLogPairRDD.rightOuterJoin(secondPairRDD)
					   .map(tp -> new Tuple2<String,Long>(tp._1, Duration.between(tp._2._1.isPresent() ? tp._2._1.get().toInstant() : new GregorianCalendar(9999, Calendar.DECEMBER, 31, 23, 59).getTime().toInstant(),tp._2._2.toInstant()).toMinutes())).cache();
					   
		
		//打印
		timeRangeRDD.foreach(tp -> System.out.println("用户: " + tp._1 + "，记录时间差:" + tp._2 + "分钟"));
				   
		//更近一步，有了时间差，筛选出没有登录的用户
		timeRangeRDD.filter(tp -> tp._2 < 0).foreach(tp -> System.out.println("用户: " + tp._1 + "非法操作"));
	}
	
	public static void thirdRule(JavaSparkContext sc) {
		
		/*
		 * 统计几个文件中，相同条件记录的，另外字段是否相等
		 */
		
		//第一个日志文件
		List<Log> firstLogList = new ArrayList<>();
		firstLogList.add(new Log("Zhang", "A", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 10, 30).getTime()));
		firstLogList.add(new Log("Wang", "B", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 10, 35).getTime()));
		firstLogList.add(new Log("Li", "A", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 10, 40).getTime()));
		firstLogList.add(new Log("Zhao", "B", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 10, 50).getTime()));
		firstLogList.add(new Log("Zhou", "C", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 11, 00).getTime()));
		firstLogList.add(new Log("Wu", "D", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 11, 10).getTime()));
		firstLogList.add(new Log("Zheng", "C", 0, new GregorianCalendar(2018, Calendar.AUGUST, 6, 11, 50).getTime()));
		firstLogList.add(new Log("Sun", "D", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 12, 50).getTime()));
		
		//第二个日志文件
		List<Log> secondLogList = new ArrayList<>();
		secondLogList.add(new Log("Zhang", "C", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 10, 40).getTime()));
		secondLogList.add(new Log("Wang", "B", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 10, 50).getTime()));
		secondLogList.add(new Log("Li", "D", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 10, 46).getTime()));
		secondLogList.add(new Log("Zhao", "B", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 11, 20).getTime()));
		secondLogList.add(new Log("Zhou", "C", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 11, 30).getTime()));
		secondLogList.add(new Log("Wu", "A", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 11, 17).getTime()));
		secondLogList.add(new Log("Zheng", "A", 0, new GregorianCalendar(2018, Calendar.AUGUST, 6, 11, 52).getTime()));
		secondLogList.add(new Log("Sun", "D", 1, new GregorianCalendar(2018, Calendar.AUGUST, 6, 12, 58).getTime()));
		
		
		JavaRDD<Log> firstLogRDD = sc.parallelize(firstLogList);
		JavaRDD<Log> secondLogRDD = sc.parallelize(secondLogList);
		
		JavaPairRDD<String, String> firstLogPairRDD = firstLogRDD.mapToPair(log -> new Tuple2<String,String>(log.getLoginName(), log.getResType()));
		JavaPairRDD<String, String> secondPairRDD = secondLogRDD.mapToPair(log -> new Tuple2<String,String>(log.getLoginName(), log.getResType()));
		
		firstLogPairRDD.join(secondPairRDD).filter(tp -> !tp._2._1.equals(tp._2._2))
					   .foreach(tp -> System.out.println("用户:" + tp._1 + "，在两个日志文件中，resType值不同"));
		
	}
}
