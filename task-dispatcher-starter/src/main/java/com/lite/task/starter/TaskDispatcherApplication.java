package com.lite.task.starter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Lite Task Dispatcher Application
 *
 * A lightweight distributed task scheduling and processing platform
 *
 * @author lite-task-dispatcher
 */
@SpringBootApplication(scanBasePackages = "com.lite.task")
@EntityScan(basePackages = "com.lite.task.domain")
@EnableJpaRepositories(basePackages = "com.lite.task.infrastructure.persistence.repository")
@EnableAsync
@EnableScheduling
public class TaskDispatcherApplication {

    public static void main(String[] args) {
        SpringApplication.run(TaskDispatcherApplication.class, args);
    }
}
