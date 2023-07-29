package com.story.usermanagement.storage

import org.springframework.stereotype.Component
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
import software.amazon.awssdk.enhanced.dynamodb.TableSchema
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest

interface UserRepository {
    fun findByEmail(emailAddress: String): UserDynamoDto?
    fun save(dto: UserDynamoDto)
    fun delete(id: Long)
}

@Component
class UserRepositoryImpl(
    private val dynamoDbClient: DynamoDbClient,
    private val dynamoDbEnhancedClient: DynamoDbEnhancedClient,
    dynamoTableManager: DynamoTableManager
) : UserRepository {

    private val qualifiedUserTableName = dynamoTableManager.getTableName(UserDynamoDto::class)

    init {
        dynamoTableManager.getTable(UserDynamoDto::class)
            ?:dynamoTableManager.createTable(UserDynamoDto::class)

    }

    override fun findByEmail(emailAddress: String): UserDynamoDto? {
        val index = dynamoDbEnhancedClient
            .table(qualifiedUserTableName, TableSchema.fromBean(UserDynamoDto::class.java))
            .index(emailAddressIndex)

        val response = index
            .query { r -> r.queryConditional(QueryConditional.keyEqualTo { k -> k.partitionValue(emailAddress.lowercase()) }) }
            .let { PageIterable.create(it) }
            .flatMap { it.items() }
            .toList()

        return if(response.count() == 1) response.single()
        else null
    }

    override fun save(dto: UserDynamoDto) =
        dynamoDbEnhancedClient
            .table(qualifiedUserTableName, TableSchema.fromBean(UserDynamoDto::class.java))
            .putItem(dto)

    override fun delete(id: Long) {
        DeleteItemRequest
            .builder()
            .tableName(qualifiedUserTableName)
            .key(buildKey(id))
            .build()
            .let { dynamoDbClient.deleteItem(it) }
    }

    private fun buildKey(key: Long) =
        mapOf("id" to AttributeValue.builder().n(key.toString()).build())
}