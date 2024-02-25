package com.zhoubc.mdata.sync.kafka;

import com.alibaba.dts.formats.avro.Operation;
import com.zhoubc.mdata.sync.dts.recordprocessor.ChangeLog;
import com.zhoubc.mdata.sync.queue.ErrorStatusException;
import com.zhoubc.mdata.sync.queue.RoutableWorker;
import com.zhoubc.mdata.sync.tbuilder.IndexBuilder;
import com.zhoubc.mdata.sync.tbuilder.Environment;
import com.zhoubc.mdata.sync.utils.Pair;
import com.zhoubc.mdata.sync.utils.ThreadPoolUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/2/5 19:26
 */
public class KafkaListenner {
    private static final Logger logger = LoggerFactory.getLogger(KafkaListenner.class);
    private static int SIZE = 2048;

    public static void start(KafkaConsumerProperties props){
        String threadName = "thread-kafkaConsumer-"+props.getGroup();
        new Thread(() -> {

            KafkaConsumer<byte[],byte[]> consumer = KafkaHelper.buildConsumer(props, SIZE);

            consumer.subscribe(Stream.of(props.getTopic()).collect(Collectors.toList()));
            logger.info("===>KafkaConsumer创建成功：props={}", props.toString());

            ExecutorService consumeExcutor = ThreadPoolUtils.buildConsumeThreadPool(props.getGroup());

            mainLoop(consumer,consumeExcutor,props);

        },threadName).start();
    }


    private static void mainLoop(KafkaConsumer<byte[],byte[]> kafkaConsumer, ExecutorService threadPool, KafkaConsumerProperties props){
        String caller = "thread-task-group-"+props.getGroup();//生成一个任务线程名称

        Map<Pair<String,Object>,ChangeLog> cmap = new HashMap<>();

        while (true) {//这里可以是个配置项，暂时停止不消费mq,

            //kafkaConsumer.poll拉取消息的长轮询，拉到数据(最大拉取条数可配置)立即返回，拉不到数据最长等待时间1500ms
            ConsumerRecords<byte[],byte[]> consumerRecords = kafkaConsumer.poll(1500);
            if(consumerRecords == null){
                continue;
            }

            //创建队列，用于同步数据
            RoutableWorker routableWorker = new RoutableWorker(caller,2048, 100, IndexBuilder::batchHandler);
            try {
                routableWorker.acquire();  //队列状态改为工作态
            } catch (Exception e) {
                if (e instanceof ErrorStatusException) {
                    logger.warn("队列acquire失败：{}", e.getMessage());
                    routableWorker.release(6000);
                }

                routableWorker.acquire();
            }

            //*******一批消息里有同一条记录的多条消息，则合并掉     Map<Pair<String,Object>,ChangeLog> cmap的用处
            cmap.clear(); // 复用对象，mainLoop方法是单线程处理，问题不大

            int tmpPkv = 0; // 没有找到pk时，用这个当做cmap的key
            for (ConsumerRecord<byte[],byte[]> record : consumerRecords) {
                logger.info(String.format("key:%s,value:%s",record.key(),record.value()));

                ChangeLog cl = IndexBuilder.toChangeLog(record); //得到change log
                if (cl == null){
                    continue;
                }

                tmpPkv++;
                String dt = cl.getObjectName();//db.tablename
                String pk = Environment.getTablePrimaryKey(dt);//table.pk
                if(pk != null){
                    //get binlog data
                    Object pkv = cl.getOperation() == Operation.DELETE ? cl.getBeforeFieldMap().get(pk):cl.getAfterFieldMap().get(pk);

                    if (pkv == null) {
                        cmap.put(Pair.of(null, tmpPkv), cl);
                        continue;
                    }

                    Pair<String,Object> ck = Pair.of(dt,pkv);//<tablename,pk的value>
                    cmap.put(ck,cl);
                }else{
                    //没有找到表的pk...
                    cmap.put(Pair.of(null, tmpPkv), cl);
                }
            }
            //*******  重复消息合并处理结束

            //cmap构建多线程处理
            for (Map.Entry<Pair<String,Object>,ChangeLog> m : cmap.entrySet()) {
                Pair<String,Object> pair = m.getKey();
                ChangeLog cl = m.getValue();
                //多线程处理
                Future<?> future = threadPool.submit(()->{
                    //判断要不要放入队列同步数据    enviroment.executeSchema()
                    IndexBuilder.buildSyncData(cl, (endpoint, updateDataMap) -> {
                        //回调函数：放入执行队列，真正的数据同步处理逻辑队列，请看IndexBuilder.batchWorker
                        routableWorker.queue(endpoint, updateDataMap);
                    });
                });

                cl.pk = pair.getRight();
                cl.future = future;
            }

            //等待线程池执行完毕
            for (Map.Entry<Pair<String,Object>,ChangeLog> m : cmap.entrySet()) {
                ChangeLog cl = m.getValue();
                Future<?> future = cl.future;
                if (future == null) {
                    return;
                }
                try {
                    future.get(30, TimeUnit.SECONDS);
                } catch (Exception e) {
                    logger.info("futureGetError|{}|{}|{}", cl.getObjectName(), cl.pk, e.getClass().getName(), e);
                }

            }

            //全部结束，有序释放 ，队列状态改为准备状态，等待下一次同步轮询
            boolean b = routableWorker.release(1000*12);

            //记录耗时统计


            kafkaConsumer.commitAsync();//kafka手动提交
        }
    }



}
