<template>
  <div id="stats" class="obstacles">
    <h1 class="text-center"> Zug-Behinderungen in Deutschland </h1>
    <div class="stats-picker">
      <vue-slider
        v-model="value"
        :data="data"
        :tooltipPlacement="['top', 'bottom']"
        :maxRange="168"
        :lazy="true"
        :tooltip="'always'"
      ></vue-slider>
      <input
        class="pretty_button"
        type="button"
        value="Plot generieren"
        v-on:click="updatePlot"
      />
    </div>
    <img
      class="stats-image"
      id="stats_image"
      v-if="plotURL"
      :src="plotURL"
      @error="$parent.display_img_load_error"
      @load="loaded_img()"
    />
  </div>
</template>

<script>
import VueSlider from 'vue-slider-component'
import 'vue-slider-component/theme/default.css'

const date1 = new Date(2021, 3, 1)
const date2 = new Date().setHours(0, 0, 0, 0)
const diffDays = Math.ceil(Math.abs(date2 - date1) / (1000 * 60 * 60))
const dates = []
let lastDate = date1
for (let i = 0; i < diffDays; i++) {
  lastDate = new Date(lastDate.getTime() + 60 * 60 * 1000)
  dates.push(
    lastDate.toLocaleString('de', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',

      hour: 'numeric',
      minute: 'numeric'
    })
  )
}
export default {
  components: {
    VueSlider
  },
  data: function () {
    return {
      value: [
        dates[Math.floor(Math.random() * dates.length)],
        dates[Math.floor(Math.random() * dates.length)]
      ],
      data: dates,
      plotURL:
        window.location.protocol +
        '//' +
        window.location.host +
        '/api/obstacleplot/default.png'
    }
  },
  methods: {
    updatePlot () {
      const new_url =
        window.location.protocol +
        '//' +
        window.location.host +
        '/api/obstacleplot/' +
        this.value[0].replace(/,/g, '') +
        '-' +
        this.value[1].replace(/,/g, '') +
        '.png'
      if (new_url !== this.plotURL) {
        this.$parent.start_progress()
        document
          .getElementById('prg_bar_anchor')
          .scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'nearest' })
        // this.progress.animate(60, { duration: 30000, easing: 'linear' })
        this.plotURL = new_url
      }
    },
    loaded_img () {
      this.$parent.stop_progress()
      document
        .getElementById('stats_image')
        .scrollIntoView({ behavior: 'smooth' })
    },
    replaceByDefault () {
      this.plotURL =
        window.location.protocol +
        '//' +
        window.location.host +
        '/api/obstacleplot/default.png'
    }
  }
}
</script>
