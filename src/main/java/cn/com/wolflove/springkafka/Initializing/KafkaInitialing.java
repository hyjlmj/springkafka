package cn.com.wolflove.springkafka.Initializing;

import cn.com.wolflove.springkafka.Controller.KafkaController;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

@Order(value=1)
@Component
public class KafkaInitialing implements CommandLineRunner {

    @Value("${zookeeper.servers}")
    private String zookeeperServers;

    @Value("${kafka.testTopic}")
    private String testTopic;

    private static Logger logger = LoggerFactory.getLogger(KafkaInitialing.class);

    @Override
    public void run(String... args) throws Exception {
        logger.info("初始化加载[kafka配置]。。。。。。。。。。。。。。。");
        ZkUtils zkUtils = ZkUtils.apply(zookeeperServers, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        // 获取topic 'test'的topic属性属性
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), testTopic);
        // 查询topic-level属性
        if (props.size() == 0 ){
            //AdminUtils.createTopic(zkUtils, testTopic, 1, 3, new Properties(), RackAwareMode.Enforced$.MODULE$);
        }else{
            Iterator it = props.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry entry=(Map.Entry)it.next();
                Object key = entry.getKey();
                Object value = entry.getValue();
                System.out.println(key + " = " + value);
            }
        }
        zkUtils.close();
    }

    public String getZookeeperServers() {
        return zookeeperServers;
    }

    public void setZookeeperServers(String zookeeperServers) {
        this.zookeeperServers = zookeeperServers;
    }

    public String getTestTopic() {
        return testTopic;
    }

    public void setTestTopic(String testTopic) {
        this.testTopic = testTopic;
    }
}
