package org.janelia.saalfeldlab.labels.blocks.n5

import org.janelia.saalfeldlab.n5.N5Writer

interface IsRelativeToContainer {

    fun setRelativeTo(container: N5Writer, group: String? = null)

}
