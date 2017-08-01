package com.infobarbosa;

import com.infobarbosa.KafkaService;

public class App {
    public static void main( String[] args ){

    	KafkaService kafka = KafkaService.getInstance();
    	kafka.pool();
    }
}
