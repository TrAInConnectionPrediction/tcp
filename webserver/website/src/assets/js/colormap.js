function rdylgrColormap (x, min = 0, max = 1, maxBrightness = 255) {
  x = (x - min) / (max - min)
  x = Math.max(0, Math.min(x, 1))
  if (x > 0.5) {
    // yellow -> green
    const r = Math.round(
      Math.sqrt(maxBrightness * maxBrightness * 2 * (1 - x))
    )
    return `rgb(${r}, ${maxBrightness}, 0)`
  } else {
    // yellow -> red
    const g = Math.round(Math.sqrt(maxBrightness * maxBrightness * 2 * x))
    return `rgb(${maxBrightness}, ${g}, 0)`
  }
}

export { rdylgrColormap }
