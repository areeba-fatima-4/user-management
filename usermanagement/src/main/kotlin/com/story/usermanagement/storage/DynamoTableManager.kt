package com.story.usermanagement.storage

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
import software.amazon.awssdk.enhanced.dynamodb.TableSchema
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondarySortKey
import software.amazon.awssdk.enhanced.dynamodb.model.EnhancedGlobalSecondaryIndex
import software.amazon.awssdk.enhanced.dynamodb.model.EnhancedLocalSecondaryIndex
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.ProjectionType
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter
import kotlin.reflect.KClass
import kotlin.reflect.full.declaredMemberProperties

@Component
class DynamoTableManager(
    private val dynamoDbClient: DynamoDbClient,
    private val dynamoDbEnhancedClient: DynamoDbEnhancedClient,
    @Value("\${default.read.units}") private val readUnitsDefault: Long,
    @Value("\${default.write.units}") private val writeUnitesDefault: Long,

    ) {
    private val log = LoggerFactory.getLogger(this::class.java)

    fun<T: Any> getTable(dtoType: KClass<T>) =
        getTableIfExists(dtoType)

    fun<T: Any> createTable(dtoType: KClass<T>) {
        val tableName = getTableName(dtoType)

        dynamoDbEnhancedClient
                .table(tableName, TableSchema.fromBean(dtoType.java))
            .apply {
                createTable {
                    it.provisionedThroughput(getProvisionedThroughput(dtoType))
                    it.localSecondaryIndices(getLocalSecondaryIndices(dtoType))
                    it.globalSecondaryIndices(getGlobalSecondaryIndices(dtoType))
                }
            }
            .also { waitForTableCreation(tableName) }

    }

    private fun waitForTableCreation(tableName: String) {
        log.info("Waiting for table creation : $tableName")

        DynamoDbWaiter
            .builder()
            .client(dynamoDbClient)
            .build()
            .use {waiter ->
                waiter
                    .waitUntilTableExists { it.tableName(tableName).build() }
                    .matched()
            }

        log.info("Table was created: $tableName")

    }


    private fun getProvisionedThroughput(dtoType: KClass<*>) =
         getProvisions(
            getReadUnits(dtoType),
            getWriteUnites(dtoType)
        )

    private fun getProvisions(
        readUnits: Long?,
        writeUnites: Long?
    ) =
       ProvisionedThroughput
            .builder()
            .readCapacityUnits(readUnits ?: readUnitsDefault)
            .writeCapacityUnits(writeUnites ?: writeUnitesDefault)
            .build()

    private fun getReadUnits(dtoType: KClass<*>) =
        getProvisionAnnotation(dtoType)
            ?.readCapacityUnits

    private fun getWriteUnites(dtoType: KClass<*>) =
        getProvisionAnnotation(dtoType)
            ?.writeCapacityUnits

    private fun getProvisionAnnotation(dtoType: KClass<*>) =
        dtoType
            .annotations
            .filterIsInstance<Provisions>()
            .singleOrNull()

    private fun getLocalSecondaryIndices(
        dtoType: KClass<*>
    ) =
        getLocalSecondaryIndexNames(dtoType)
            .map(::getLocalIndexRequestFromName)

    private fun getGlobalSecondaryIndexNames(dtoType: KClass<*>) =
        dtoType.
        declaredMemberProperties
            .flatMap { it.getter.annotations }
            .filterIsInstance<DynamoDbSecondaryPartitionKey>()
            .flatMap { it.indexNames.toList() }

    private fun getLocalSecondaryIndexNames(dtoType: KClass<*>) =
         dtoType.declaredMemberProperties
            .flatMap { it.getter.annotations }
            .filterIsInstance<DynamoDbSecondarySortKey>()
            .flatMap { it.indexNames.toList() }
            .toSet()
            .filter {!getGlobalSecondaryIndexNames(dtoType).contains(it)}
            

    private fun getGlobalSecondaryIndices(
        dtoType: KClass<*>
    ) =
        getGlobalSecondaryIndexNames(dtoType)
            .map(::getGlobalIndexRequestFromName)

    private fun getGlobalIndexRequestFromName(indexName: String) =
        EnhancedGlobalSecondaryIndex
            .builder()
            .indexName(indexName)
            .projection { b -> b.projectionType(ProjectionType.ALL).build() }
            .build()

    private fun getLocalIndexRequestFromName(indexName: String) =
        EnhancedLocalSecondaryIndex
            .builder()
            .indexName(indexName)
            .projection { b -> b.projectionType(ProjectionType.ALL).build() }
            .build()

    private fun <T: Any> getTableIfExists(dtoType: KClass<T>) =
        try {
            dynamoDbEnhancedClient
                .table(getTableName(dtoType), TableSchema.fromBean(dtoType.java))
                .apply { describeTable() }
        }
        catch (e: Exception) {
            null
        }

    fun <T: Any> getTableName(dtoType: KClass<T>) =
        dtoType
            .annotations
            .filterIsInstance<DynamoTable>()
            .singleOrNull()
            ?.baseTableName
            ?: throw IllegalStateException("Type ${dtoType.qualifiedName} does not have ${DynamoTable::class.simpleName} annotation.")


}

