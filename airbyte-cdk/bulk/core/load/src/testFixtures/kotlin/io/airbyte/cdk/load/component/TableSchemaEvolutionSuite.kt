/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.component

import io.airbyte.cdk.load.component.TableOperationsFixtures as Fixtures
import io.airbyte.cdk.load.component.TableOperationsFixtures.ID_FIELD
import io.airbyte.cdk.load.component.TableOperationsFixtures.TEST_FIELD
import io.airbyte.cdk.load.component.TableOperationsFixtures.reverseColumnNameMapping
import io.airbyte.cdk.load.data.FieldType
import io.airbyte.cdk.load.data.IntegerType
import io.airbyte.cdk.load.data.IntegerValue
import io.airbyte.cdk.load.data.ObjectType
import io.airbyte.cdk.load.data.StringType
import io.airbyte.cdk.load.data.StringValue
import io.airbyte.cdk.load.message.Meta
import io.airbyte.cdk.load.table.ColumnNameMapping
import io.airbyte.cdk.load.table.TableName
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import kotlin.test.assertEquals
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.assertAll

@MicronautTest(environments = ["component"])
interface TableSchemaEvolutionSuite {
    val client: TableSchemaEvolutionClient
    val airbyteMetaColumnMapping: Map<String, String>
        get() = Meta.COLUMN_NAMES.associateWith { it }

    val opsClient: TableOperationsClient
    val testClient: TestTableOperationsClient

    private val harness: TableOperationsTestHarness
        get() = TableOperationsTestHarness(opsClient, testClient, airbyteMetaColumnMapping)

    fun discover() {
        discover(Fixtures.ID_AND_TEST_SCHEMA, Fixtures.ID_AND_TEST_MAPPING)
    }

    fun discover(
        schema: ObjectType,
        columnNameMapping: ColumnNameMapping,
    ) = runTest {
        val testNamespace = Fixtures.generateTestNamespace("namespace-test")
        val testTable = Fixtures.generateTestTableName("table-test-table", testNamespace)
        val stream =
            Fixtures.createAppendStream(
                namespace = testTable.namespace,
                name = testTable.name,
                schema = schema,
            )

        opsClient.createNamespace(testNamespace)
        opsClient.createTable(
            tableName = testTable,
            columnNameMapping = columnNameMapping,
            stream = stream,
            replace = false,
        )

        assertSchemaContainsColumns(
            columnNameMapping,
            listOf(ID_FIELD, TEST_FIELD),
            client.discoverSchema(testTable),
        )
    }

    fun computeSchema() {
        computeSchema(Fixtures.ID_AND_TEST_SCHEMA, Fixtures.ID_AND_TEST_MAPPING)
    }

    fun computeSchema(
        schema: ObjectType,
        columnNameMapping: ColumnNameMapping,
    ) {
        val testNamespace = Fixtures.generateTestNamespace("namespace-test")
        val testTable = Fixtures.generateTestTableName("table-test-table", testNamespace)
        val stream =
            Fixtures.createAppendStream(
                namespace = testTable.namespace,
                name = testTable.name,
                schema = schema,
            )
        assertSchemaContainsColumns(
            columnNameMapping,
            listOf(ID_FIELD, TEST_FIELD),
            client.computeSchema(stream, columnNameMapping),
        )
    }

    fun `noop diff`() = runTest {
        val testNamespace = Fixtures.generateTestNamespace("namespace-test")
        val testTable = Fixtures.generateTestTableName("table-test-table", testNamespace)
        val (_, _, columnChangeset) =
            computeSchemaEvolution(
                testTable,
                Fixtures.TEST_INTEGER_SCHEMA,
                Fixtures.TEST_MAPPING,
                Fixtures.TEST_INTEGER_SCHEMA,
                Fixtures.TEST_MAPPING,
            )

        assertTrue(columnChangeset.isNoop())
    }

    fun `changeset is correct when adding a column`() = runTest {
        val testNamespace = Fixtures.generateTestNamespace("namespace-test")
        val testTable = Fixtures.generateTestTableName("table-test-table", testNamespace)
        val (_, _, columnChangeset) =
            computeSchemaEvolution(
                testTable,
                Fixtures.TEST_INTEGER_SCHEMA,
                Fixtures.TEST_MAPPING,
                Fixtures.ID_AND_TEST_SCHEMA,
                Fixtures.ID_AND_TEST_MAPPING,
            )

        // The changeset should indicate that we're trying to add a column
        assertAll(
            {
                assertEquals(
                    columnChangeset.columnsToAdd.keys,
                    setOf(Fixtures.ID_AND_TEST_MAPPING[ID_FIELD]),
                    "Expected to add exactly one column. Got ${columnChangeset.columnsToAdd}"
                )
            },
            {
                assertEquals(
                    0,
                    columnChangeset.columnsToDrop.size,
                    "Expected to not drop any columns. Got ${columnChangeset.columnsToDrop}"
                )
            },
            {
                assertEquals(
                    0,
                    columnChangeset.columnsToChange.size,
                    "Expected to not change any columns. Got ${columnChangeset.columnsToChange}"
                )
            },
            {
                assertEquals(
                    setOf(Fixtures.ID_AND_TEST_MAPPING[TEST_FIELD]),
                    columnChangeset.columnsToRetain.keys,
                    "Expected to retain the original column. Got ${columnChangeset.columnsToRetain}"
                )
            },
        )
    }

    fun `changeset is correct when dropping a column`() = runTest {
        val testNamespace = Fixtures.generateTestNamespace("namespace-test")
        val testTable = Fixtures.generateTestTableName("table-test-table", testNamespace)
        val (_, _, columnChangeset) =
            computeSchemaEvolution(
                testTable,
                Fixtures.ID_AND_TEST_SCHEMA,
                Fixtures.ID_AND_TEST_MAPPING,
                Fixtures.TEST_INTEGER_SCHEMA,
                Fixtures.TEST_MAPPING,
            )

        // The changeset should indicate that we're trying to drop a column
        assertAll(
            {
                assertEquals(
                    0,
                    columnChangeset.columnsToAdd.size,
                    "Expected to not add any columns. Got ${columnChangeset.columnsToAdd}"
                )
            },
            {
                assertEquals(
                    setOf(Fixtures.ID_AND_TEST_MAPPING[ID_FIELD]),
                    columnChangeset.columnsToDrop.keys,
                    "Expected to drop exactly one column. Got ${columnChangeset.columnsToDrop}"
                )
            },
            {
                assertEquals(
                    0,
                    columnChangeset.columnsToChange.size,
                    "Expected to not change any columns. Got ${columnChangeset.columnsToChange}"
                )
            },
            {
                assertEquals(
                    setOf(Fixtures.ID_AND_TEST_MAPPING[TEST_FIELD]),
                    columnChangeset.columnsToRetain.keys,
                    "Expected to retain the original column. Got ${columnChangeset.columnsToRetain}"
                )
            },
        )
    }

    fun `changeset is correct when changing a column's type`() = runTest {
        val testNamespace = Fixtures.generateTestNamespace("namespace-test")
        val testTable = Fixtures.generateTestTableName("table-test-table", testNamespace)
        val (actualSchema, expectedSchema, columnChangeset) =
            computeSchemaEvolution(
                testTable,
                Fixtures.TEST_INTEGER_SCHEMA,
                Fixtures.TEST_MAPPING,
                Fixtures.TEST_STRING_SCHEMA,
                Fixtures.TEST_MAPPING,
            )

        val actualType = actualSchema.columns[Fixtures.TEST_MAPPING[TEST_FIELD]]!!
        val expectedType = expectedSchema.columns[Fixtures.TEST_MAPPING[TEST_FIELD]]!!

        // The changeset should indicate that we're trying to drop a column
        assertAll(
            {
                assertEquals(
                    0,
                    columnChangeset.columnsToAdd.size,
                    "Expected to not add any columns. Got ${columnChangeset.columnsToAdd}"
                )
            },
            {
                assertEquals(
                    0,
                    columnChangeset.columnsToDrop.size,
                    "Expected to not drop any columns. Got ${columnChangeset.columnsToDrop}"
                )
            },
            {
                assertEquals(
                    1,
                    columnChangeset.columnsToChange.size,
                    "Expected to change exactly one column. Got ${columnChangeset.columnsToChange}"
                )
            },
            {
                assertEquals(
                    ColumnTypeChange(actualType, expectedType),
                    columnChangeset.columnsToChange[Fixtures.TEST_MAPPING[TEST_FIELD]],
                    "Expected column to change from $actualType to $expectedType. Got ${columnChangeset.columnsToChange}"
                )
            },
            {
                assertEquals(
                    0,
                    columnChangeset.columnsToRetain.size,
                    "Expected to retain the original column. Got ${columnChangeset.columnsToRetain}"
                )
            },
        )
    }

    /**
     * Execute a basic set of schema changes. We're not changing the sync mode, the types are just
     * string/int (i.e. no JSON), and there's no funky characters anywhere.
     */
    fun `basic apply changeset`() = runTest {
        val testNamespace = Fixtures.generateTestNamespace("namespace-test")
        val testTable = Fixtures.generateTestTableName("table-test-table", testNamespace)
        val initialSchema =
            ObjectType(
                linkedMapOf(
                    "to_retain" to FieldType(StringType, true),
                    "to_change" to FieldType(IntegerType, true),
                    "to_drop" to FieldType(StringType, true),
                ),
            )
        val initialColumnNameMapping =
            ColumnNameMapping(
                mapOf(
                    "to_retain" to "to_retain",
                    "to_change" to "to_change",
                    "to_drop" to "to_drop",
                )
            )
        val modifiedSchema =
            ObjectType(
                linkedMapOf(
                    "to_retain" to FieldType(StringType, true),
                    "to_change" to FieldType(StringType, true),
                    "to_add" to FieldType(StringType, true),
                ),
            )
        val modifiedColumnNameMapping =
            ColumnNameMapping(
                mapOf(
                    "to_retain" to "to_retain",
                    "to_change" to "to_change",
                    "to_add" to "to_add",
                )
            )
        val modifiedStream =
            Fixtures.createAppendStream(
                namespace = testTable.namespace,
                name = testTable.name,
                schema = modifiedSchema,
            )

        // Create the table and compute the schema changeset
        val (_, expectedSchema, changeset) =
            computeSchemaEvolution(
                testTable,
                initialSchema,
                initialColumnNameMapping,
                modifiedSchema,
                modifiedColumnNameMapping,
            )
        // Insert a record before applying the changeset
        testClient.insertRecords(
            testTable,
            mapOf(
                "to_retain" to StringValue("to_retain original value"),
                "to_change" to IntegerValue(42),
                "to_drop" to StringValue("to_drop original value"),
            ),
        )

        client.applyChangeset(
            modifiedStream,
            modifiedColumnNameMapping,
            testTable,
            expectedSchema.columns,
            changeset,
        )

        val postAlterationRecords = harness.readTableWithoutMetaColumns(testTable)
        Assertions.assertEquals(
            listOf(
                mapOf(
                    "to_retain" to "to_retain original value",
                    // changed from int to string
                    "to_change" to "42",
                    // new column should be initialized to null
                    "to_add" to null,
                )
            ),
            postAlterationRecords.reverseColumnNameMapping(
                modifiedColumnNameMapping,
                airbyteMetaColumnMapping
            ),
        ) {
            "Expected records were not in the overwritten table."
        }
    }

    /**
     * Assert that a [TableSchema] contains the correct column names.
     *
     * TODO should we require implementers to supply the ColumnType objects so that we can assert
     * against the TableSchema directly?
     */
    private fun assertSchemaContainsColumns(
        columnNameMapping: ColumnNameMapping,
        expectedColumnNames: List<String>,
        schema: TableSchema,
    ) {
        // TODO currently implementers are free to include / not include the airbyte columns in
        //   `discover` / `computeSchema`.
        //   Eventually we should enforce that implementations include them.
        val nonAirbyteColumns =
            schema.columns.filterKeys { !airbyteMetaColumnMapping.containsValue((it)) }
        val unmappedNonAirbyteColumnNames =
            nonAirbyteColumns.keys.map { columnNameMapping[it]!! }.sorted()
        assertEquals(expectedColumnNames, unmappedNonAirbyteColumnNames)
    }

    /**
     * Utility method for a typical schema evolution test. Creates a table with [initialSchema]
     * using [initialColumnNameMapping], then computes the column changeset using [modifiedSchema]
     * and [modifiedColumnNameMapping]. This method does _not_ actually apply the changeset.
     *
     * Returns a tuple of `(discoveredInitialSchema, computedModifiedSchema, changeset)`.
     */
    private suspend fun computeSchemaEvolution(
        testTable: TableName,
        initialSchema: ObjectType,
        initialColumnNameMapping: ColumnNameMapping,
        modifiedSchema: ObjectType,
        modifiedColumnNameMapping: ColumnNameMapping,
    ): Triple<TableSchema, TableSchema, ColumnChangeset> {
        val initialStream =
            Fixtures.createAppendStream(
                namespace = testTable.namespace,
                name = testTable.name,
                schema = initialSchema,
            )
        val modifiedStream =
            Fixtures.createAppendStream(
                namespace = testTable.namespace,
                name = testTable.name,
                schema = modifiedSchema,
            )

        opsClient.createNamespace(testTable.namespace)
        opsClient.createTable(
            tableName = testTable,
            columnNameMapping = initialColumnNameMapping,
            stream = initialStream,
            replace = false,
        )

        val actualSchema = client.discoverSchema(testTable)
        val expectedSchema = client.computeSchema(modifiedStream, modifiedColumnNameMapping)
        val columnChangeset = client.computeChangeset(actualSchema.columns, expectedSchema.columns)
        return Triple(
            actualSchema,
            expectedSchema,
            columnChangeset,
        )
    }
}
