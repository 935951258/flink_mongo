package com.event.ingestion;



import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;

import java.util.Arrays;

/**
 * 启动类
 */
public class Driver implements CommandLineRunner {

    //程序入口
    public static void main(String[] args) {
        SpringApplication.run(Driver.class,args);
    }

    @Override
    public void run(String... args) throws Exception {

        if (args == null || args.length < 1){
            throw new Exception("请指定需要执行的类");
        }

        //动态加载 args[0] 这个类，并调用 newInstance 创造实例以便调用
        Object o = Class.forName(args[0]).newInstance();

        //判断o 是否为 ingestionExecutor 的实例
        if (o instanceof ingestionExecutor){

            //强转为ingestionExecutor 调用execute方法 ，并将run方法中1至args.length的参数传入
            ((ingestionExecutor)o).execute(Arrays.copyOfRange(args,1,args.length));

        }else {
            throw new Exception("指定的执行程序不是ingestionExecutor的实例");
        }
    }
}
