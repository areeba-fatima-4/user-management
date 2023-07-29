package com.story.usermanagement.storage

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey

const val emailAddressIndex = "email-address-index"

@DynamoDbBean
@DynamoTable(baseTableName = "users")
@Provisions(readCapacityUnits = 10L, writeCapacityUnits = 10L)
class UserDynamoDto {

    @get:DynamoDbPartitionKey var id: Long = 0

    @get:DynamoDbSecondaryPartitionKey(indexNames = [emailAddressIndex]) var emailAddress: String = ""
    var userJson: String = "null"


    companion object {
        fun create(
            id: Long,
            emailAddress: String,
            userJson: String
        ) = UserDynamoDto()
            .apply {
                this.id = id
                this.emailAddress = emailAddress
                this.userJson = userJson
            }
    }
}
