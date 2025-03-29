package ru.nsu.cloud.master;

import org.junit.jupiter.api.Test;
import ru.nsu.cloud.client.CloudContext;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MasterTest {

    @Test
    public void testRemoteComputation() {

        // 1. Создаём контекст для работы с облаком
        CloudContext cloudContext = new CloudContext("192.168.1.100", 9090);

        // 2. Загружаем список данных в CloudDataset
        List<Integer> data = List.of(1, 2, 3, 4, 5);
        var dataset = cloudContext.parallelize(data);

        // 3. Применяем удалённое вычисление (умножаем на 2)
        var transformedDataset = dataset.map(x -> x * 2);

        // 4. Собираем результат
        List<Integer> result = transformedDataset.collect();

        // 5. Проверяем, что все элементы умножились на 2
        assertEquals(List.of(2, 4, 6, 8, 10), result);
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
