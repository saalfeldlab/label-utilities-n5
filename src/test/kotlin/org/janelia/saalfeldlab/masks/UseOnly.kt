package org.janelia.saalfeldlab.masks

import net.imglib2.RandomAccessibleInterval
import net.imglib2.algorithm.util.Grids
import net.imglib2.img.array.ArrayImgFactory
import net.imglib2.loops.LoopBuilder
import net.imglib2.type.NativeType
import net.imglib2.type.numeric.IntegerType
import net.imglib2.type.numeric.integer.UnsignedLongType
import net.imglib2.util.Intervals
import net.imglib2.util.Util
import net.imglib2.view.Views
import org.janelia.saalfeldlab.n5.DataType
import org.janelia.saalfeldlab.n5.N5FSWriter
import org.janelia.saalfeldlab.n5.N5Reader
import org.janelia.saalfeldlab.n5.N5Writer
import org.janelia.saalfeldlab.n5.imglib2.N5Utils
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.function.BiConsumer

enum class Axis(val index: Int) {X(0), Y(1), Z(2)}

class UseOnly {


	companion object {

		private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

		fun cutoffMaskNoGenerics(
				inputContainer: N5Reader,
				outputContainer: N5Writer,
				inputDataset: String,
				outputDataset: String,
				cutoff: Double,
				axis: Axis,
				es: ExecutorService
		) {
			val dataType = inputContainer.getDatasetAttributes(inputDataset).dataType
			when (dataType) {
				DataType.UINT64 -> cutoffMask<UnsignedLongType>(inputContainer, outputContainer, inputDataset, outputDataset, cutoff, axis, es)
				else -> throw RuntimeException("Cannot process data type $dataType")
			}
		}

		fun <T>cutoffMask(
				inputContainer: N5Reader,
				outputContainer: N5Writer,
				inputDataset: String,
				outputDataset: String,
				cutoff: Double,
				axis: Axis,
				es: ExecutorService
		) where T: NativeType<T>, T: IntegerType<T> {
			require(inputDataset != outputDataset)
			val mask = N5Utils.open<T>(inputContainer, inputDataset)
			val index = getCuttoffIndex(mask, cutoff, es, axis)
			val cutoffMask = ArrayImgFactory(Util.getTypeFromInterval(mask)).create(mask)
			val dims = Intervals.dimensionsAsLongArray(cutoffMask)
			dims[axis.index] = index
			val blocks = Grids.collectAllContainedIntervals(dims, IntArray(dims.size, {64}))
			val futures = es.invokeAll(blocks.map { Callable { LoopBuilder.setImages(Views.interval(mask, it), Views.interval(cutoffMask, it)).forEachPixel(BiConsumer { t, u -> u.set(t) }); null } })
			futures.forEach { it.get() }

			val attrs = inputContainer.getDatasetAttributes(inputDataset)
			outputContainer.createDataset(outputDataset, attrs)
			inputContainer.listAttributes(inputDataset).forEach { s, clazz -> outputContainer.setAttribute(outputDataset, s, inputContainer.getAttribute(inputDataset, s, clazz)) }

			LOG.info("Index {} for cutoff {} and mask of size {} at dataset {}", index, cutoff, Intervals.dimensionsAsLongArray(mask), inputDataset)

			N5Utils.save(cutoffMask, outputContainer, outputDataset, attrs.blockSize, attrs.compression, es)
		}

		fun getCuttoffIndex(
				mask: RandomAccessibleInterval<out IntegerType<*>>,
				cutoff: Double,
				es: ExecutorService,
				axis: Axis = Axis.Y): Long {

			val counts = accumulatedMaskCount(mask, es, axis)
			return getLastRelevantIndex(counts, cutoff).toLong()
		}

		fun getLastRelevantIndex(counts: List<Long>, cutoff: Double): Int {
			require(cutoff > 0)
			require(cutoff < 1)
			val totalCount = counts.sum()
			val threshold = cutoff * totalCount
			var sum = 0L
			var index = 0
			for (i in counts.indices) {
				if (sum >= threshold)
					break
				index = i
				sum += counts[i]
			}

			return index

		}

		fun accumulatedMaskCount(
				mask: RandomAccessibleInterval<out IntegerType<*>>,
				es: ExecutorService,
				axis: Axis = Axis.Y
		): List<Long> {
			LOG.debug("Getting accumulated mask counts")
			val tasks = (0 until mask.dimension(axis.index)).map {
				Callable<Long> {
					var count = 0L
					Views.hyperSlice(mask, axis.index, it).forEach { count += it.getIntegerLong() }
					LOG.debug("Mask count for slice {} is {}", it, count)
					count
				}
			}
			val futureCounts = es.invokeAll(tasks)
			return futureCounts.map { it.get() }
		}
	}

}

private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

fun main(args: Array<String>) {

	val samples = arrayOf("0", "1", "2", "A", "B", "C")
	val tags = arrayOf("-downsampled", "")
	val es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())
	val axis = Axis.Y

	for (sample in samples) {
		LOG.info("Processing sample {}", sample)
		val containerPath = "/groups/saalfeld/home/hanslovskyp/data/from-arlo/interpolated-combined/sample_$sample.n5"
		val container = N5FSWriter(containerPath)
		for (tag in tags) {
			val inputDataset = "volumes/labels/mask$tag"
			val outputDataset = "$inputDataset-75-y%"

			UseOnly.cutoffMaskNoGenerics(container, container, inputDataset, outputDataset, 0.75, axis, es)
		}
	}

	es.shutdown()



}
