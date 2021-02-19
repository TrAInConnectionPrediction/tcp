function rdylgr_colormap (x, min = 0, max = 1, max_brightness = 255) {
  x = (x - min) / (max - min)
  x = Math.max(0, Math.min(x, 1))
  if (x > 0.5) {
    // yellow -> green
    const r = Math.round(
      Math.sqrt(max_brightness * max_brightness * 2 * (1 - x))
    )
    return `rgb(${r}, ${max_brightness}, 0)`
  } else {
    // yellow -> red
    const g = Math.round(Math.sqrt(max_brightness * max_brightness * 2 * x))
    return `rgb(${max_brightness}, ${g}, 0)`
  }
}

export { rdylgr_colormap }
