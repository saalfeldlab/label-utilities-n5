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
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock
import org.janelia.saalfeldlab.n5.DataType
import org.janelia.saalfeldlab.n5.DatasetAttributes
import org.janelia.saalfeldlab.n5.GzipCompression
import org.janelia.saalfeldlab.n5.N5FSWriter
import org.junit.AfterClass
import org.junit.Assert
import org.junit.Test
import org.slf4j.LoggerFactory
import java.io.File
import kotlin.io.path.createTempDirectory
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
                        .also { it.addProperty("scaleDatasetPattern", pattern1) }
                        .also { it.addProperty("numDimensions", 3) },
                serialized1)
        Assert.assertEquals(
                JsonObject()
                        .also { it.addProperty("type", LabelBlockLookupFromN5Relative.LOOKUP_TYPE) }
                        .also { it.addProperty("scaleDatasetPattern", pattern2) }
                        .also { it.addProperty("numDimensions", 3) },
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
            lookup.setRelativeTo(writer, group)

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

    @Test
    fun `test nD intervals for LabelBlockLookupFromN5Relative`() {
        val containerPath = testDirectory.resolve("nd-container.n5").absolutePath
        val level = 0
        val pattern = "label-to-block-mapping/s%d"
        val writer = N5FSWriter(containerPath)
        val lookup = LabelBlockLookupFromN5Relative(pattern, 5)
        lookup.setRelativeTo(writer, null)
        writer.createDataset(String.format(pattern, level), DatasetAttributes(longArrayOf(100), intArrayOf(3), DataType.INT8, GzipCompression()))

        /* two 5D blocks of the same label at different timepoints; the non-spatial extent is what a 3D lookup lost */
        val intervals = arrayOf<Interval>(
                Intervals.createMinMax(0, 0, 0, 1, 2, 31, 31, 31, 1, 2),
                Intervals.createMinMax(0, 0, 0, 1, 7, 31, 31, 31, 1, 7))
        lookup.write(LabelBlockLookupKey(level, 1L), *intervals)

        val read = lookup.read(LabelBlockLookupKey(level, 1L))
        Assert.assertEquals(2, read.size)
        (intervals zip read).forEachIndexed { idx, p ->
            Assert.assertEquals("Dimensionality mismatch for interval $idx", 5, p.second.numDimensions())
            Assert.assertThat("Mismatch for interval $idx", p.second, IntervalMatcher(p.first))
        }
    }

    @Test
    fun `an interval of the wrong dimensionality is rejected rather than truncated`() {
        val containerPath = testDirectory.resolve("mismatch-container.n5").absolutePath
        val level = 0
        val pattern = "label-to-block-mapping/s%d"
        val writer = N5FSWriter(containerPath)
        val lookup = LabelBlockLookupFromN5Relative(pattern, 5)
        lookup.setRelativeTo(writer, null)
        writer.createDataset(String.format(pattern, level), DatasetAttributes(longArrayOf(100), intArrayOf(3), DataType.INT8, GzipCompression()))

        Assert.assertThrows(IllegalArgumentException::class.java) {
            lookup.write(LabelBlockLookupKey(level, 1L), Intervals.createMinMax(0, 0, 0, 1, 1, 1))
        }
    }

    @Test
    fun `a lookup serialized without numDimensions still reads 3D intervals`() {
        val containerPath = testDirectory.resolve("legacy-json.n5").absolutePath
        val level = 0
        val pattern = "label-to-block-mapping/s%d"
        val writer = N5FSWriter(containerPath)
        writer.createDataset(String.format(pattern, level), DatasetAttributes(longArrayOf(100), intArrayOf(3), DataType.INT8, GzipCompression()))

        val gson = GsonBuilder()
                .registerTypeHierarchyAdapter(LabelBlockLookup::class.java, LabelBlockLookupAdapter.getJsonAdapter())
                .create()

        /* exactly what versions before nD support wrote: no numDimensions */
        val legacyJson = JsonObject()
                .also { it.addProperty("type", LabelBlockLookupFromN5Relative.LOOKUP_TYPE) }
                .also { it.addProperty("scaleDatasetPattern", pattern) }

        val lookup = gson.fromJson(legacyJson, LabelBlockLookup::class.java)
        Assert.assertNotNull("a lookup written before numDimensions existed must still deserialize", lookup)
        (lookup as IsRelativeToContainer).setRelativeTo(writer, null)

        val interval = Intervals.createMinMax(1, 2, 3, 4, 5, 6)
        lookup.write(LabelBlockLookupKey(level, 1L), interval)
        val read = lookup.read(LabelBlockLookupKey(level, 1L))

        Assert.assertEquals(1, read.size)
        Assert.assertEquals("an undeclared dimensionality must be read as 3D", 3, read[0].numDimensions())
        Assert.assertThat(read[0], IntervalMatcher(interval))
    }

    @Test
    fun `a 3D lookup is written in the legacy byte layout`() {
        val containerPath = testDirectory.resolve("legacy-layout.n5").absolutePath
        val level = 0
        val pattern = "label-to-block-mapping/s%d"
        val dataset = String.format(pattern, level)
        val writer = N5FSWriter(containerPath)
        val lookup = LabelBlockLookupFromN5Relative(pattern)
        lookup.setRelativeTo(writer, null)
        val attributes = DatasetAttributes(longArrayOf(100), intArrayOf(3), DataType.INT8, GzipCompression())
        writer.createDataset(dataset, attributes)

        lookup.write(LabelBlockLookupKey(level, 1L), Intervals.createMinMax(1, 2, 3, 4, 5, 6))

        /* what a reader that predates nD support does: id, count, then three mins and three maxes */
        val bytes = (writer.readBlock<ByteArray>(dataset, attributes, 0) as ByteArrayDataBlock).data
        Assert.assertEquals("a 3D entry must still occupy id + count + 6 longs", Long.SIZE_BYTES + Int.SIZE_BYTES + 6 * Long.SIZE_BYTES, bytes.size)
        val bb = java.nio.ByteBuffer.wrap(bytes)
        Assert.assertEquals(1L, bb.long)
        Assert.assertEquals(1, bb.int)
        Assert.assertEquals(listOf(1L, 2L, 3L, 4L, 5L, 6L), (0 until 6).map { bb.long })
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
                suffix: String = ".test") = createTempDirectory(prefix + suffix).toFile().also { LOG.debug("Created tmp directory {}", it) }

    }

    private class IntervalMatcher(private val interval: Interval) : TypeSafeMatcher<Interval>() {

        override fun describeTo(description: Description?) = description?.appendValue(interval).let {}

        override fun matchesSafely(item: Interval?) = item?.let { Intervals.equals(interval, it) } ?: false

    }

}
