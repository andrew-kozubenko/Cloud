package ru.nsu.cloud.master;

import org.junit.jupiter.api.Test;
import ru.nsu.cloud.client.CloudContext;
import ru.nsu.cloud.client.CloudDataset;
import ru.nsu.cloud.client.CloudSession;

import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MasterTest {

    @Test
    public void testRemoteComputation() {
        CloudSession cloud = CloudSession.builder()
                .master("192.168.1.100", 9090)
                .build();

        // 1. Создаём контекст для работы с облаком
        CloudContext cloudContext = cloud.cloudContext();

        // 📌 Вариант 1: Передача лямбды
        CloudDataset<Integer> dataset = cloudContext.parallelize(List.of(1, 2, 3, 4, 5));
        CloudDataset<Integer> squared = dataset.map(x -> x * x);
        List<Integer> result = squared.collect();
        System.out.println(result);  // [1, 4, 9, 16, 25]

        // 📌 Вариант 2: Передача JAR-файла
        cloudContext.submitJar("my-computation.jar", "com.example.Main", "run");
    }
}

//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//
//public class SparkRDDExample {
//    public static void main(String[] args) {
//        SparkSession spark = SparkSession.builder()
//                .appName("RDD Example")
//                .master("spark://<SPARK_MASTER_URL>:7077")
//                .getOrCreate();
//
//        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
//
//        // Распределённый список чисел
//        JavaRDD<Integer> numbers = sc.parallelize(List.of(1, 2, 3, 4, 5));
//
//        // Применение map (преобразование каждого элемента)
//        JavaRDD<Integer> squared = numbers.map(x -> x * x);
//
//        // Собираем результат обратно в один список
//        List<Integer> result = squared.collect();
//        System.out.println(result);  // Выведет: [1, 4, 9, 16, 25]
//    }
//}
