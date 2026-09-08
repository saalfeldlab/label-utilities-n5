package org.janelia.saalfeldlab.labels.blocks.n5

import net.imglib2.Interval
import net.imglib2.cache.ref.SoftRefLoaderCache
import org.janelia.saalfeldlab.labels.blocks.CachedLabelBlockLookup
import org.janelia.saalfeldlab.labels.blocks.LabelBlockLookup
import org.janelia.saalfeldlab.labels.blocks.LabelBlockLookupKey
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock
import org.janelia.saalfeldlab.n5.DatasetAttributes
import org.janelia.saalfeldlab.n5.N5Writer
import org.slf4j.LoggerFactory
import java.io.IOException
import java.lang.invoke.MethodHandles
import java.util.function.Predicate

private const val LOOKUP_TYPE_IDENTIFIER = "n5-filesystem-relative"

@LabelBlockLookup.LookupType(LOOKUP_TYPE_IDENTIFIER)
class LabelBlockLookupFromN5Relative @JvmOverloads constructor(
        @LabelBlockLookup.Parameter private val scaleDatasetPattern: String,
        @LabelBlockLookup.Parameter private val numDimensions: Int? = LabelBlockLookupCodec.LEGACY_NUM_DIMENSIONS
) : CachedLabelBlockLookup, IsRelativeToContainer {

    private constructor() : this("")

    private lateinit var container: N5Writer

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
    }

    @Synchronized
    override fun read(key: LabelBlockLookupKey): Array<Interval> {
        return getBlockKey(key)?.let { blockKey ->
            val map = cache.get(blockKey, this::readBlock)
            map[key.id] ?: arrayOf()
        } ?: arrayOf()
    }

    @Synchronized
    override fun write(key: LabelBlockLookupKey, vararg intervals: Interval) {
        val blockKey = getBlockKey(key)!!
        cache.invalidate(blockKey)
        val map = readBlock(blockKey)
        map[key.id] = arrayOf(*intervals)
        writeBlock(blockKey, map)
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
            mapByBlockKey.computeIfAbsent(getBlockKey(level, entry.key)!!) { mutableMapOf() }[entry.key] = entry.value
        cache.invalidateIf { it.level == level }
        mapByBlockKey.forEach(this::writeBlock)
    }

    @Throws(IOException::class)
    private fun readBlock(blockKey: N5LabelBlockLookupKey): MutableMap<Long, Array<Interval>> {
        LOG.debug("Reading block id {} at scale level={}", blockKey.blockId, blockKey.level)
        val dataset = String.format(actualScaleDatasetPattern, blockKey.level)
        val attributes = this.attributes.getOrPut(blockKey.level, { container.getDatasetAttributes(dataset) })

        val block = container.readBlock<ByteArray>(dataset, attributes, *longArrayOf(blockKey.blockId)) as? ByteArrayDataBlock
        return if (block != null) LabelBlockLookupCodec.fromBytes(block.data, numDimensions) else mutableMapOf()
    }

    @Throws(IOException::class)
    private fun writeBlock(blockKey: N5LabelBlockLookupKey, map: Map<Long, Array<Interval>>) {
        LOG.debug("Writing block id {} at scale level={}", blockKey.blockId, blockKey.level)
        val dataset = String.format(actualScaleDatasetPattern, blockKey.level)
        val attributes = this.attributes.getOrPut(blockKey.level, { container.getDatasetAttributes(dataset) })

        val size = intArrayOf(attributes.blockSize[0])
        val block = ByteArrayDataBlock(size, longArrayOf(blockKey.blockId), LabelBlockLookupCodec.toBytes(map, numDimensions))
        container.writeBlock(dataset, attributes, block)
    }

    private fun getBlockKey(key: LabelBlockLookupKey) = getBlockKey(key.level, key.id)

    private fun getBlockKey(level: Int, id: Long): N5LabelBlockLookupKey? {
        val dataset = String.format(actualScaleDatasetPattern, level)
        if (!container.datasetExists(dataset)) return null
        val attributes = this.attributes.getOrPut(level, { container.getDatasetAttributes(dataset) })
        val blockSize = attributes.blockSize[0]
        val blockId = id / blockSize
        return N5LabelBlockLookupKey(level, blockId)
    }

    override fun setRelativeTo(container: N5Writer, group: String?) {
        this.container = container
        this.group = group
    }

    override fun equals(other: Any?) = other is LabelBlockLookupFromN5Relative
            && other.scaleDatasetPattern == scaleDatasetPattern
            && other.numDimensions == numDimensions

    override fun hashCode() = java.util.Objects.hash(scaleDatasetPattern, numDimensions)

}
