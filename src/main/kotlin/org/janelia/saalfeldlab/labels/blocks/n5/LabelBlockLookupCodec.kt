package org.janelia.saalfeldlab.labels.blocks.n5

import net.imglib2.FinalInterval
import net.imglib2.Interval
import java.nio.ByteBuffer

/**
 * The byte layout of one label-to-block-mapping block.
 * Each entry is a label id, followed by a list of intervals that contain the label id.
 */
internal object LabelBlockLookupCodec {

	/** Older versions assumed 3D. */
	const val LEGACY_NUM_DIMENSIONS = 3

	fun fromBytes(array: ByteArray, numDimensions: Int?) = fromBytes(array, numDimensions ?: LEGACY_NUM_DIMENSIONS)

	fun fromBytes(array: ByteArray, numDimensions: Int): MutableMap<Long, Array<Interval>> {
		val map = mutableMapOf<Long, Array<Interval>>()
		val bb = ByteBuffer.wrap(array)
		while (bb.hasRemaining()) {
			val id = bb.long
			val numIntervals = bb.int
			map[id] = (0 until numIntervals).map { bb.readInterval(numDimensions) }.toTypedArray()
		}
		return map
	}

	fun toBytes(map: Map<Long, Array<Interval>>, numDimensions: Int?) = toBytes(map, numDimensions ?: LEGACY_NUM_DIMENSIONS)

	fun toBytes(map: Map<Long, Array<Interval>>, numDimensions: Int): ByteArray {
		val entryByteSize = numDimensions * 2 * Long.SIZE_BYTES
		val sizeInBytes = map.values.stream().mapToInt { Long.SIZE_BYTES + Int.SIZE_BYTES + entryByteSize * it.size }.sum()
		val bytes = ByteArray(sizeInBytes)
		val bb = ByteBuffer.wrap(bytes)
		for ((key, value) in map) {
			bb.putLong(key)
			bb.putInt(value.size)
			value.forEach { bb.writeInterval(it, numDimensions) }
		}
		return bytes
	}

	private fun ByteBuffer.readInterval(numDimensions: Int): Interval {
		val min = LongArray(numDimensions)
		val max = LongArray(numDimensions)
		for (d in 0 until numDimensions)
			min[d] = long
		for (d in 0 until numDimensions)
			max[d] = long
		return FinalInterval(min, max)
	}

	private fun ByteBuffer.writeInterval(interval: Interval, numDimensions: Int) {
		/* a shorter interval would read the next record's bytes; a longer one would be silently truncated */
		require(interval.numDimensions() == numDimensions) {
			"expected $numDimensions dimensions, got ${interval.numDimensions()}"
		}
		for (d in 0 until numDimensions)
			putLong(interval.min(d))
		for (d in 0 until numDimensions)
			putLong(interval.max(d))
	}
}
