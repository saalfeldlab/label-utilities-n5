package org.janelia.saalfeldlab.labels.blocks.n5

interface IsRelativeToContainer {

    fun setRelativeTo(container: String, group: String? = null)

}
