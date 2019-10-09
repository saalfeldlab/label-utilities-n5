package org.janelia.saalfeldlab.labels.blocks.n5

import net.imglib2.FinalInterval
import net.imglib2.Interval
import net.imglib2.cache.ref.SoftRefLoaderCache
import org.janelia.saalfeldlab.labels.blocks.CachedLabelBlockLookup
import org.janelia.saalfeldlab.labels.blocks.LabelBlockLookup
import org.janelia.saalfeldlab.labels.blocks.LabelBlockLookupKey
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock
import org.janelia.saalfeldlab.n5.DatasetAttributes
import org.janelia.saalfeldlab.n5.N5FSWriter
import org.slf4j.LoggerFactory
import java.io.IOException
import java.lang.invoke.MethodHandles
import java.nio.ByteBuffer
import java.util.function.Predicate

private const val LOOKUP_TYPE_IDENTIFIER = "n5-filesystem-relative"

@LabelBlockLookup.LookupType(LOOKUP_TYPE_IDENTIFIER)
class LabelBlockLookupFromN5Relative(
        @LabelBlockLookup.Parameter private val scaleDatasetPattern: String) : CachedLabelBlockLookup, IsRelativeToContainer {

    private constructor() : this("")

    private lateinit var n5: N5FSWriter

    private lateinit var _container: String

    var container: String
        get() = _container
        private set(container) {
            _container = container
            n5 = N5FSWriter(_container)
        }

    private var group: String? = null

    private val actualScaleDatasetPattern: String
        get() = group?.let { "$it/$scaleDatasetPattern" } ?: scaleDatasetPattern

    private val attributes = mutableMapOf<Int, DatasetAttributes>()

    private data class N5LabelBlockLookupKey(val level: Int, val blockId: Long)

    // Cache all lookups in a block when it's requested for the first time
    private val cache = SoftRefLoaderCache<N5LabelBlockLookupKey, Map<Long, Array<Interval>>>()

    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        const val LOOKUP_TYPE = LOOKUP_TYPE_IDENTIFIER

        private const val SINGLE_ENTRY_BYTE_SIZE = 3 * 2 * java.lang.Long.BYTES

        private fun fromBytes(array: ByteArray): MutableMap<Long, Array<Interval>> {
            val map = mutableMapOf<Long, Array<Interval>>()
            val bb = ByteBuffer.wrap(array)
            while (bb.hasRemaining()) {
                val id = bb.long
                val numIntervals = bb.int
                map[id] = (0 until numIntervals).map<Int, Interval> { bb.readFinalInterval3D() }.toTypedArray()
            }
            return map
        }

        private fun toBytes(map: Map<Long, Array<Interval>>): ByteArray {
            val sizeInBytes = map.values.stream().mapToInt { java.lang.Long.BYTES + Integer.BYTES + SINGLE_ENTRY_BYTE_SIZE * it.size }.sum()
            val bytes = ByteArray(sizeInBytes)
            val bb = ByteBuffer.wrap(bytes)
            for (entry in map) {
                bb.putLong(entry.key)
                bb.putInt(entry.value.size)
                entry.value.forEach { bb.writeInterval3D(it) }
            }
            return bytes
        }

        private fun ByteBuffer.readLongArray3D() = longArrayOf(long, long, long)
        private fun ByteBuffer.readFinalInterval3D() = FinalInterval(readLongArray3D(), readLongArray3D())
        private inline fun ByteBuffer.writeThreeLongValues(generator: (Int) -> Long) {
            putLong(generator(0))
            putLong(generator(1))
            putLong(generator(2))
        }

        private fun ByteBuffer.writeInterval3D(interval: Interval) {
            writeThreeLongValues { interval.min(it) }
            writeThreeLongValues { interval.max(it) }
        }
    }

    @Synchronized
    override fun read(key: LabelBlockLookupKey): Array<Interval> {
        val map = cache.get(getBlockKey(key), this::readBlock)
        return map[key.id] ?: arrayOf()
    }

    @Synchronized
    override fun write(key: LabelBlockLookupKey, vararg intervals: Interval) {
        val blockKey = getBlockKey(key)
        cache.invalidate(blockKey)
        val map = readBlock(blockKey)
        map[key.id] = arrayOf(*intervals)
        writeBlock(getBlockKey(key), map)
    }

    @Synchronized
    override fun invalidate(key: LabelBlockLookupKey) = cache.invalidate(getBlockKey(key))

    @Synchronized
    override fun invalidateAll(parallelismThreshold: Long) = cache.invalidateAll(parallelismThreshold)

    @Synchronized
    override fun invalidateIf(parallelismThreshold: Long, condition: Predicate<LabelBlockLookupKey>) {
        cache.invalidateIf(parallelismThreshold) { key ->
            cache.getIfPresent(key)?.keys?.any { id -> condition.test(LabelBlockLookupKey(key.level, id)) } ?: false
        }
    }

    @Synchronized
    @Throws(IOException::class)
    fun set(level: Int, map: Map<Long, Array<Interval>>) {
        val mapByBlockKey = mutableMapOf<N5LabelBlockLookupKey, MutableMap<Long, Array<Interval>>>()
        for (entry in map)
            mapByBlockKey.computeIfAbsent(getBlockKey(level, entry.key)) { mutableMapOf() }[entry.key] = entry.value
        cache.invalidateIf { it.level == level }
        mapByBlockKey.forEach(this::writeBlock)
    }

    @Throws(IOException::class)
    private fun readBlock(blockKey: N5LabelBlockLookupKey): MutableMap<Long, Array<Interval>> {
        LOG.debug("Reading block id {} at scale level={}", blockKey.blockId, blockKey.level)
        val dataset = String.format(actualScaleDatasetPattern, blockKey.level)
        val attributes = this.attributes.getOrPut(blockKey.level, { n5.getDatasetAttributes(dataset) })

        val block = n5.readBlock(dataset, attributes, longArrayOf(blockKey.blockId)) as? ByteArrayDataBlock
        return if (block != null) fromBytes(block.data) else mutableMapOf()
    }

    @Throws(IOException::class)
    private fun writeBlock(blockKey: N5LabelBlockLookupKey, map: Map<Long, Array<Interval>>) {
        LOG.debug("Writing block id {} at scale level={}", blockKey.blockId, blockKey.level)
        val dataset = String.format(actualScaleDatasetPattern, blockKey.level)
        val attributes = this.attributes.getOrPut(blockKey.level, { n5.getDatasetAttributes(dataset) })

        val size = intArrayOf(attributes.blockSize[0])
        val block = ByteArrayDataBlock(size, longArrayOf(blockKey.blockId), toBytes(map))
        n5.writeBlock(dataset, attributes, block)
    }

    private fun getBlockKey(key: LabelBlockLookupKey) = getBlockKey(key.level, key.id)

    private fun getBlockKey(level: Int, id: Long): N5LabelBlockLookupKey {
        val dataset = String.format(actualScaleDatasetPattern, level)
        val attributes = this.attributes.getOrPut(level, { n5.getDatasetAttributes(dataset) })
        val blockSize = attributes.blockSize[0]
        val blockId = id / blockSize
        return N5LabelBlockLookupKey(level, blockId)
    }

    override fun setRelativeTo(container: String, group: String?) {
        this.container = container
        this.group = group
    }

    override fun equals(other: Any?) = other is LabelBlockLookupFromN5Relative
            && other.scaleDatasetPattern == scaleDatasetPattern

    override fun hashCode() = scaleDatasetPattern.hashCode()

}
