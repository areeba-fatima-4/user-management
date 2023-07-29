package com.story.usermanagement.configuration

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient


@Configuration
class DynamoConfig {

    @Bean
    fun dynamoDbClient(
        @Value("\${cloud.aws.region.static}") region: String
    ): DynamoDbClient =
        DynamoDbClient
            .builder()
            .region(Region.of(region))
            .build()

    @Bean
    fun dynamoDbEnhancedClient(
        dynamoDbClient: DynamoDbClient
    ) : DynamoDbEnhancedClient =
        DynamoDbEnhancedClient
            .builder()
            .dynamoDbClient(dynamoDbClient)
            .build()


}