package org.janelia.saalfeldlab.labels.blocks.n5

import net.imglib2.FinalInterval
import net.imglib2.Interval
import net.imglib2.cache.Invalidate
import net.imglib2.cache.ref.SoftRefLoaderCache
import net.imglib2.util.Intervals
import org.janelia.saalfeldlab.labels.blocks.CachedLabelBlockLookup
import org.janelia.saalfeldlab.labels.blocks.LabelBlockLookup
import org.janelia.saalfeldlab.labels.blocks.LabelBlockLookupKey
import org.janelia.saalfeldlab.n5.*
import org.slf4j.LoggerFactory
import java.io.IOException
import java.lang.invoke.MethodHandles
import java.nio.ByteBuffer
import java.util.*
import java.util.function.Predicate
import java.util.stream.Collectors
import java.util.stream.Stream

@LabelBlockLookup.LookupType("n5-filesystem")
class LabelBlockLookupFromN5(
		@LabelBlockLookup.Parameter private val root: String,
		@LabelBlockLookup.Parameter private val scaleDatasetPattern: String) : CachedLabelBlockLookup {

	private constructor(): this("", "")

	private var n5: N5FSWriter? = null

	private val attributes = mutableMapOf<Int, DatasetAttributes>()

	private data class N5LabelBlockLookupKey(val level: Int, val blockId: Long)

	// Cache all lookups in a block when it's requested for the first time
	private val cache = SoftRefLoaderCache<N5LabelBlockLookupKey, Map<Long, Array<Interval>>>()

	companion object {
		private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

		private const val SINGLE_ENTRY_BYTE_SIZE = 3 * 2 * java.lang.Long.BYTES

		private fun fromBytes(array: ByteArray): Map<Long, Array<Interval>> {
			val map = mutableMapOf<Long, Array<Interval>>()
			val bb = ByteBuffer.wrap(array)
			while (bb.hasRemaining()) {
				val id = bb.long
				val numIntervals = bb.int
				val intervals = Stream
						.generate(
								({
									FinalInterval(
											longArrayOf(bb.long, bb.long, bb.long),
											longArrayOf(bb.long, bb.long, bb.long)
									) as Interval
								}))
						.limit(numIntervals.toLong())
						.collect(Collectors.toList())
						.toTypedArray()
				map[id] = intervals
			}
			return map.toMap()
		}

		private fun toBytes(map: Map<Long, Array<Interval>>): ByteArray {
			val sizeInBytes = map.values.stream().mapToInt { java.lang.Long.BYTES + Integer.BYTES + SINGLE_ENTRY_BYTE_SIZE * it.size }.sum()
			val bytes = ByteArray(sizeInBytes)
			val bb = ByteBuffer.wrap(bytes)
			for (entry in map) {
				bb.putLong(entry.key)
				bb.putInt(entry.value.size)
				for (interval in entry.value) {
					bb.putLong(interval.min(0))
					bb.putLong(interval.min(1))
					bb.putLong(interval.min(2))
					bb.putLong(interval.max(0))
					bb.putLong(interval.max(1))
					bb.putLong(interval.max(2))
				}
			}
			return bytes
		}
	}

	override fun read(key: LabelBlockLookupKey): Array<Interval> {
		val map = cache.get(getBlockKey(key), this::readBlock)
		return map[key.id] ?: arrayOf()
	}

	override fun write(key: LabelBlockLookupKey, vararg intervals: Interval) {
		val blockKey = getBlockKey(key)
		cache.invalidate(blockKey)
		val map = readBlock(blockKey).toMutableMap()
		map[key.id] = arrayOf(*intervals)
		writeBlock(getBlockKey(key), map)
	}

	override fun invalidate(key: LabelBlockLookupKey) {
		val blockKey = getBlockKey(key)
		cache.invalidate(blockKey)
	}

	override fun invalidateAll(parallelismThreshold: Long) {
		cache.invalidateAll(parallelismThreshold)
	}

	override fun invalidateIf(parallelismThreshold: Long, condition: Predicate<LabelBlockLookupKey>) {
		val blockPredicate = Predicate<N5LabelBlockLookupKey> {
			var ret = false
			val map = cache.getIfPresent(it)
			if (map != null) {
				for (id in map.keys) {
					if (condition.test(LabelBlockLookupKey(it.level, id))) {
						ret = true
						break
					}
				}
			}
			ret
		}
		cache.invalidateIf(parallelismThreshold, blockPredicate)
	}

	@Throws(IOException::class)
	fun set(level: Int, map: Map<Long, Array<Interval>>) {
		val mapByBlockKey = mutableMapOf<N5LabelBlockLookupKey, MutableMap<Long, Array<Interval>>>()
		for (entry in map)
			mapByBlockKey.computeIfAbsent(getBlockKey(level, entry.key)) { mutableMapOf() } [entry.key] = entry.value
		cache.invalidateIf { it.level == level }
		mapByBlockKey.forEach(this::writeBlock)
	}

	@Throws(IOException::class)
	private fun readBlock(blockKey: N5LabelBlockLookupKey): Map<Long, Array<Interval>> {
		LOG.debug("Reading block id {} at scale level={}", blockKey.blockId, blockKey.level)
		println("Reading block id " + blockKey.blockId + " at scale level " + blockKey.level)

		val dataset = "${String.format(scaleDatasetPattern, blockKey.level)}"
		val attributes = this.attributes.getOrPut(blockKey.level, { n5().getDatasetAttributes(dataset) })

		val block = n5().readBlock(dataset, attributes, longArrayOf(blockKey.blockId)) as? ByteArrayDataBlock
		return if (block != null) fromBytes(block.data) else mapOf()
	}

	@Throws(IOException::class)
	private fun writeBlock(blockKey: N5LabelBlockLookupKey, map: Map<Long, Array<Interval>>) {
		LOG.debug("Writing block id {} at scale level={}", blockKey.blockId, blockKey.level)
		val dataset = "${String.format(scaleDatasetPattern, blockKey.level)}"
		val attributes = this.attributes.getOrPut(blockKey.level, { n5().getDatasetAttributes(dataset) })

		val size = intArrayOf(attributes.blockSize[0])
		val block = ByteArrayDataBlock(size, longArrayOf(blockKey.blockId), toBytes(map))
		n5().writeBlock(dataset, attributes, block)
	}

	@Throws(IOException::class)
	private fun n5(): N5FSWriter {
		if (n5 == null)
			n5 = N5FSWriter(root)
		return n5!!
	}

	private fun getBlockKey(key: LabelBlockLookupKey): N5LabelBlockLookupKey {
		return getBlockKey(key.level, key.id)
	}

	private fun getBlockKey(level: Int, id: Long): N5LabelBlockLookupKey {
		val dataset = "${String.format(scaleDatasetPattern, level)}"
		val attributes = this.attributes.getOrPut(level, { n5().getDatasetAttributes(dataset) })
		val blockSize = attributes.blockSize[0]
		val blockId = id / blockSize
		return N5LabelBlockLookupKey(level, blockId)
	}
}

fun main(args: Array<String>) {
	val level = 1
	val basePath = "bla-test"
	val pattern = "label-to-block-mapping/s%d"
	val writer = N5FSWriter(basePath)
	val lookup = LabelBlockLookupFromN5(basePath, pattern)

	writer.createDataset(String.format(pattern, level), DatasetAttributes(longArrayOf(100), intArrayOf(3), DataType.INT8, GzipCompression()))

	val map = mutableMapOf<Long, Array<Interval>>()
	map[1L] = arrayOf(FinalInterval(longArrayOf(1, 2, 3), longArrayOf(3, 4, 5)) as Interval)

	lookup.set(level, map)
	lookup.write(LabelBlockLookupKey(level, 10L), FinalInterval(longArrayOf(4, 5, 6), longArrayOf(7, 8, 9)) as Interval, FinalInterval(longArrayOf(10, 11, 12), longArrayOf(123, 123, 123)))
	lookup.write(LabelBlockLookupKey(level, 0L), FinalInterval(longArrayOf(1, 1, 1), longArrayOf(2, 2, 2)))

	for (i in 0L..11L)
		println(lookup.read(LabelBlockLookupKey(level, i)).asList().stream().map { "(${Arrays.toString(Intervals.minAsLongArray(it))}-${Arrays.toString(Intervals.maxAsLongArray(it))})" }.collect(Collectors.toList()) as List<String>)
}
