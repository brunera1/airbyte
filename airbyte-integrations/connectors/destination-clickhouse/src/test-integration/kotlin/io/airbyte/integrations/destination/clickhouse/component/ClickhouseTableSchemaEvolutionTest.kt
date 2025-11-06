/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.clickhouse.component

import io.airbyte.cdk.load.component.TableOperationsClient
import io.airbyte.cdk.load.component.TableSchemaEvolutionClient
import io.airbyte.cdk.load.component.TableSchemaEvolutionSuite
import io.airbyte.cdk.load.component.TestTableOperationsClient
import org.junit.jupiter.api.Test

class ClickhouseTableSchemaEvolutionTest(
    override val client: TableSchemaEvolutionClient,
    override val opsClient: TableOperationsClient,
    override val testClient: TestTableOperationsClient
) : TableSchemaEvolutionSuite {
    @Test
    override fun discover() {
        super.discover()
    }

    @Test
    override fun computeSchema() {
        super.computeSchema()
    }

    @Test
    override fun `noop diff`() {
        super.`noop diff`()
    }

    @Test
    override fun `changeset is correct when adding a column`() {
        super.`changeset is correct when adding a column`()
    }

    @Test
    override fun `changeset is correct when dropping a column`() {
        super.`changeset is correct when dropping a column`()
    }

    @Test
    override fun `changeset is correct when changing a column's type`() {
        super.`changeset is correct when changing a column's type`()
    }

    @Test
    override fun `basic apply changeset`() {
        super.`basic apply changeset`()
    }
}
