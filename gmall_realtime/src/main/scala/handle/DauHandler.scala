package handle

import com.atguigu.bean.StartUpLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.{lang, util}

object DauHandler {
  /**
   * 批次间去重
   * @param startUpLogDStream
   */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog]) = {
    /*startUpLogDStream.filter(log=>{
      //1.获取jedis连接
      println("创建Redis连接")
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      //2.获取redis中的数据
      val redisKey: String = "Dau" + log.logDate
  
      val mids: util.Set[String] = jedis.smembers(redisKey)
      /*3.判断当前mid是否包含在redis中
      val bool: Boolean = mids.contains(log.mid)*/
      //3.直接用redis中set类型的方法来判断是否存在
      val bool: lang.Boolean = jedis.sismember(redisKey, log.mid)
      jedis.close()
      !bool
    })*/
    //方案二：在每个分区下获取连接
    
    val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
      println("创建Redis连接")
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val logs: Iterator[StartUpLog] = partition.filter(log => {
        //1.获取jedis连接
        val jedis: Jedis = new Jedis("hadoop102", 6379)
        //2.获取redis中的数据
        val redisKey: String = "Dau" + log.logDate
  
        val mids: util.Set[String] = jedis.smembers(redisKey)
        /*3.判断当前mid是否包含在redis中
        val bool: Boolean = mids.contains(log.mid)*/
        //3.直接用redis中set类型的方法来判断是否存在
        val bool: lang.Boolean = jedis.sismember(redisKey, log.mid)
        jedis.close()
        !bool
      })
      jedis.close()
      logs
    })
    value
  }
  
  /**
   * 将mid保存至Redis
   *
   * @param startUpLogDStream
   */
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val jedis: Jedis = new Jedis("hadoop102", 6379)
        partition.foreach(starUpLog => {
          //将数据写入redis
          val redisKey: String = "Dau" + starUpLog.logDate
          jedis.sadd(redisKey,starUpLog.mid)
        })
        jedis.close()
      })
    })
  }
  
}
