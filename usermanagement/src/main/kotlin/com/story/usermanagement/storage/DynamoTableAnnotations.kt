package com.story.usermanagement.storage

@Target(AnnotationTarget.CLASS)
annotation class DynamoTable(
    val baseTableName: String
)

@Target(AnnotationTarget.CLASS)
annotation class Provisions(
    val readCapacityUnits: Long,
    val writeCapacityUnits: Long
)