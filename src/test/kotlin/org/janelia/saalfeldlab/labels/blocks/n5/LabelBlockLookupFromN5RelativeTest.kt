package org.janelia.saalfeldlab.labels.blocks.n5

import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import net.imglib2.Interval
import net.imglib2.util.Intervals
import org.hamcrest.Description
import org.hamcrest.TypeSafeMatcher
import org.janelia.saalfeldlab.labels.blocks.LabelBlockLookup
import org.janelia.saalfeldlab.labels.blocks.LabelBlockLookupAdapter
import org.janelia.saalfeldlab.labels.blocks.LabelBlockLookupKey
import org.janelia.saalfeldlab.n5.DataType
import org.janelia.saalfeldlab.n5.DatasetAttributes
import org.janelia.saalfeldlab.n5.GzipCompression
import org.janelia.saalfeldlab.n5.N5FSWriter
import org.junit.AfterClass
import org.junit.Assert
import org.junit.Test
import org.slf4j.LoggerFactory
import java.io.File
import java.lang.invoke.MethodHandles

class LabelBlockLookupFromN5RelativeTest {

    @Test(expected = UninitializedPropertyAccessException::class)
    fun `test uninitialized for LabelBlockLookupFromN5Relative`() {
        LabelBlockLookupFromN5Relative("s%d").read(LabelBlockLookupKey(0, 0))
    }

    @Test
    fun `test serialization for LabelBlockLookupFromN5Relative`() {
        Assert.assertEquals("n5-filesystem-relative", LabelBlockLookupFromN5Relative.LOOKUP_TYPE)

        val pattern1 = "1/s%d"
        val pattern2 = "2/s%d"
        val lookup1 = LabelBlockLookupFromN5Relative(pattern1)
        val lookup2 = LabelBlockLookupFromN5Relative(pattern2)

        Assert.assertEquals(lookup1, LabelBlockLookupFromN5Relative(pattern1))
        Assert.assertEquals(lookup2, LabelBlockLookupFromN5Relative(pattern2))
        Assert.assertNotEquals(lookup1, lookup2)

        val gson = GsonBuilder()
                .registerTypeHierarchyAdapter(LabelBlockLookup::class.java, LabelBlockLookupAdapter.getJsonAdapter())
                .create()

        val serialized1 = gson.toJsonTree(lookup1)
        val serialized2 = gson.toJsonTree(lookup2)

        Assert.assertEquals(
                JsonObject()
                        .also { it.addProperty("type", LabelBlockLookupFromN5Relative.LOOKUP_TYPE) }
                        .also { it.addProperty("scaleDatasetPattern", pattern1) },
                serialized1)
        Assert.assertEquals(
                JsonObject()
                        .also { it.addProperty("type", LabelBlockLookupFromN5Relative.LOOKUP_TYPE) }
                        .also { it.addProperty("scaleDatasetPattern", pattern2) },
                serialized2)

        val deserialized1 = gson.fromJson(serialized1, LabelBlockLookup::class.java)
        val deserialized2 = gson.fromJson(serialized2, LabelBlockLookup::class.java)

        Assert.assertEquals(lookup1, deserialized1)
        Assert.assertEquals(lookup2, deserialized2)
    }

    @Test
    fun `test LabelBlockLookupFromN5Relative`() {
        val containerPath = testDirectory.resolve("container.n5").absolutePath
        LOG.debug("container={}", containerPath)

        for (group in arrayOf("group", null)) {

            val level = 1
            val pattern = "label-to-block-mapping/s%d"
            val writer = N5FSWriter(containerPath)
            val lookup = LabelBlockLookupFromN5Relative(pattern)
            val dataset = String.format(group?.let { "$it/$pattern" } ?: pattern, level)
            lookup.setRelativeTo(containerPath, group)

            writer.createDataset(dataset, DatasetAttributes(longArrayOf(100), intArrayOf(3), DataType.INT8, GzipCompression()))

            val map = mutableMapOf<Long, Array<Interval>>()
            val intervalsForId1 = arrayOf<Interval>(Intervals.createMinMax(1, 2, 3, 3, 4, 5))
            map[1L] = intervalsForId1

            val intervalsForId0 = arrayOf<Interval>(Intervals.createMinMax(1, 1, 1, 2, 2, 2))
            val intervalsForId10 = arrayOf<Interval>(
                    Intervals.createMinMax(4, 5, 6, 7, 8, 9),
                    Intervals.createMinMax(10, 11, 12, 123, 123, 123))


            lookup.set(level, map)
            lookup.write(LabelBlockLookupKey(level, 10L), *intervalsForId10)
            lookup.write(LabelBlockLookupKey(level, 0L), *intervalsForId0)

            val groundTruthMap = mapOf(
                    Pair(0L, intervalsForId0),
                    Pair(1L, intervalsForId1),
                    Pair(10L, intervalsForId10))

            for (i in 0L..11L) {
                val intervals = lookup.read(LabelBlockLookupKey(level, i))
                val groundTruth = groundTruthMap[i] ?: arrayOf()
                LOG.debug("Block {}: Got intervals {}", i, intervals)
                Assert.assertEquals("Size Mismatch for index $i", groundTruth.size, intervals.size)
                (groundTruth zip intervals).forEachIndexed { idx, p ->
                    Assert.assertThat(
                            "Mismatch for interval $idx of entry $i",
                            p.second,
                            IntervalMatcher(p.first))
                }
            }
        }
    }

    companion object {

        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        private lateinit var _testDirectory: File

        private val testDirectory: File
            get() {
                if (!::_testDirectory.isInitialized)
                    _testDirectory = tempDirectory()
                return _testDirectory
            }

        private val isDeleteOnExit: Boolean
            get() = !LOG.isDebugEnabled

        @AfterClass
        @JvmStatic
        fun deleteTestDirectory() {
            if (isDeleteOnExit && ::_testDirectory.isInitialized)
                testDirectory.deleteRecursively()
        }

        private fun tempDirectory(
                prefix: String = "label-utilities-n5-",
                suffix: String? = ".test") = createTempDir(prefix, suffix).also { LOG.debug("Created tmp directory {}", it) }

    }

    private class IntervalMatcher(private val interval: Interval) : TypeSafeMatcher<Interval>() {

        override fun describeTo(description: Description?) = description?.appendValue(interval).let {}

        override fun matchesSafely(item: Interval?) = item?.let { Intervals.equals(interval, it) } ?: false

    }

}
