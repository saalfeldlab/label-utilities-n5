package org.janelia.saalfeldlab.labels.blocks.n5

import gnu.trove.map.hash.TIntObjectHashMap
import gnu.trove.map.hash.TLongObjectHashMap
import net.imglib2.FinalInterval
import net.imglib2.Interval
import net.imglib2.util.Intervals
import org.janelia.saalfeldlab.labels.blocks.LabelBlockLookup
import org.janelia.saalfeldlab.n5.*
import org.slf4j.LoggerFactory
import java.io.IOException
import java.lang.invoke.MethodHandles
import java.nio.ByteBuffer
import java.util.*
import java.util.stream.Collectors
import java.util.stream.Stream

@LabelBlockLookup.LookupType("n5-filesystem")
class LabelBlockLookupFromN5(
		@LabelBlockLookup.Parameter private val root: String,
		@LabelBlockLookup.Parameter private val scaleDatasetPattern: String) : LabelBlockLookup {

	private constructor(): this("", "")

	private var n5: N5FSWriter? = null

	companion object {
		private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

		private const val SINGLE_ENTRY_BYTE_SIZE = 3 * 2 * java.lang.Long.BYTES

		private fun fromBytes(array: ByteArray): TLongObjectHashMap<Array<Interval>> {

			val map = TLongObjectHashMap<Array<Interval>>()

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
				map.put(id, intervals)
			}

			return map
		}

		private fun toBytes(map: TLongObjectHashMap<Array<Interval>>): ByteArray {
			val sizeInBytes = map.valueCollection().stream().mapToInt { java.lang.Long.BYTES + Integer.BYTES + SINGLE_ENTRY_BYTE_SIZE * it.size }.sum()
			val bytes = ByteArray(sizeInBytes)
			val bb = ByteBuffer.wrap(bytes)
			map.forEachEntry { key, value ->
				bb.putLong(key)
				bb.putInt(value.size)
				for (interval in value) {
					bb.putLong(interval.min(0))
					bb.putLong(interval.min(1))
					bb.putLong(interval.min(2))
					bb.putLong(interval.max(0))
					bb.putLong(interval.max(1))
					bb.putLong(interval.max(2))
				}
				true
			}
			return bytes
		}
	}

	private val attributes = mutableMapOf<Int, DatasetAttributes>()

	private val cache = TIntObjectHashMap<TLongObjectHashMap<TLongObjectHashMap<Array<Interval>>>>() // scale -> block index -> id -> intervals

	@Throws(IOException::class)
	@Synchronized
	fun set(level: Int, ids: TLongObjectHashMap<Array<Interval>>) {
		val mapByBlock = mutableMapOf<Long, TLongObjectHashMap<Array<Interval>>>()
		ids.forEachEntry { key, value ->
			mapByBlock.computeIfAbsent(getBlockId(level, key)) { TLongObjectHashMap() }.put(key, value)
			true
		}
		for (m in mapByBlock)
			writeAndCacheMap(level, m.key, m.value)
	}

	@Throws(IOException::class)
	@Synchronized
	override fun read(level: Int, id: Long): Array<Interval> {
		LOG.debug("Reading id {} for level={}", id, level)
		val map = getMap(level, getBlockId(level, id))
		return map.get(id)
	}

	@Throws(IOException::class)
	@Synchronized
	override fun write(level: Int, id: Long, vararg intervals: Interval) {
		val blockId = getBlockId(level, id)
		val map = readAndCacheMap(level, blockId)
		map.put(id, arrayOf(*intervals))
		writeAndCacheMap(level, blockId, map)
	}

	@Synchronized
	fun invalidateCache() {
		cache.clear()
	}

	@Throws(IOException::class)
	private fun getMap(level: Int, blockId: Long): TLongObjectHashMap<Array<Interval>> {
		// check if map is already in the cache
		if (cache.contains(level) && cache.get(level).contains(blockId))
			return cache.get(level).get(blockId)

		// not in the cache yet
		return readAndCacheMap(level, blockId)
	}

	private fun cacheMap(level: Int, blockId: Long, map: TLongObjectHashMap<Array<Interval>>) {
		if (!cache.contains(level))
			cache.put(level, TLongObjectHashMap())
		val cacheAtLevel = cache.get(level)

		if (!cacheAtLevel.contains(blockId))
			cacheAtLevel.put(blockId, TLongObjectHashMap())
		val cacheAtLevelForBlock = cacheAtLevel.get(blockId)

		cacheAtLevelForBlock.clear()
		cacheAtLevelForBlock.putAll(map)
	}

	@Throws(IOException::class)
	private fun readAndCacheMap(level: Int, blockId: Long): TLongObjectHashMap<Array<Interval>> {
		val dataset = "${String.format(scaleDatasetPattern, level)}"
		val attributes = this.attributes.getOrPut(level, { n5().getDatasetAttributes(dataset) })

		val block = n5().readBlock(dataset, attributes, longArrayOf(blockId)) as? ByteArrayDataBlock
		val map = if (block != null) fromBytes(block.data) else TLongObjectHashMap()

		cacheMap(level, blockId, map)
		return map
	}

	@Throws(IOException::class)
	private fun writeAndCacheMap(level: Int, blockId: Long, map: TLongObjectHashMap<Array<Interval>>) {
		val dataset = "${String.format(scaleDatasetPattern, level)}"
		val attributes = this.attributes.getOrPut(level, { n5().getDatasetAttributes(dataset) })

		val size = intArrayOf(attributes.blockSize[0])
		val block = ByteArrayDataBlock(size, longArrayOf(blockId), toBytes(map))

		n5().writeBlock(dataset, attributes, block)
		cacheMap(level, blockId, map)
	}

	@Throws(IOException::class)
	private fun n5(): N5FSWriter {
		if (n5 == null)
			n5 = N5FSWriter(root)
		return n5!!
	}

	private fun getBlockId(level: Int, id: Long): Long {
		val dataset = "${String.format(scaleDatasetPattern, level)}"
		val attributes = this.attributes.getOrPut(level, { n5().getDatasetAttributes(dataset) })
		val blockSize = attributes.blockSize[0]
		return id / blockSize
	}
}

fun main(args: Array<String>) {
	val level = 1
	val basePath = "bla-test"
	val pattern = "label-to-block-mapping/s%d"
	val writer = N5FSWriter(basePath)
	val lookup = LabelBlockLookupFromN5(basePath, pattern)

	writer.createDataset(String.format(pattern, level), DatasetAttributes(longArrayOf(100), intArrayOf(3), DataType.INT8, GzipCompression()))

	val map = TLongObjectHashMap<Array<Interval>>()
	map.put(1L, arrayOf(FinalInterval(longArrayOf(1, 2, 3), longArrayOf(3, 4, 5)) as Interval))

	lookup.set(level, map)
	lookup.write(level, 10L, FinalInterval(longArrayOf(4, 5, 6), longArrayOf(7, 8, 9)) as Interval, FinalInterval(longArrayOf(10, 11, 12), longArrayOf(123, 123, 123)))
	lookup.write(level, 0L, FinalInterval(longArrayOf(1, 1, 1), longArrayOf(2, 2, 2)))

	for (i in 0L..11L)
		println(lookup.read(level, i).asList().stream().map { "(${Arrays.toString(Intervals.minAsLongArray(it))}-${Arrays.toString(Intervals.maxAsLongArray(it))})" }.collect(Collectors.toList()) as List<String>)
}
