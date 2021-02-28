<template>
  <div id="stats" class="stats">
    <div class="stats-picker">
      <vue-slider
        v-model="value"
        :data="data"
        :tooltipPlacement="['top', 'bottom']"
        :maxRange="168"
        :lazy="true"
        :tooltip="'always'"
        style="margin: 20px; width: 100%"
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
      v-if="plotURL"
      :src="plotURL"
      @error="replaceByDefault"
    />
  </div>
</template>

<script>
import VueSlider from 'vue-slider-component'
import 'vue-slider-component/theme/default.css'

const date1 = new Date(2021, 0, 1)
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
        new Date('2021-02-13T22:00:00.000Z').toLocaleString('de', {
          day: '2-digit',
          month: '2-digit',
          year: 'numeric',
          hour: 'numeric',
          minute: 'numeric'
        }),
        new Date('2021-02-16T23:00:00.000Z').toLocaleString('de', {
          day: '2-digit',
          month: '2-digit',
          year: 'numeric',
          hour: 'numeric',
          minute: 'numeric'
        })
      ],
      data: dates,
      plotURL:
        window.location.protocol +
        '//' +
        window.location.host +
        '/api/stationplot/default.jpg'
    }
  },
  methods: {
    updatePlot () {
      this.plotURL =
        window.location.protocol +
        '//' +
        window.location.host +
        '/api/stationplot/' +
        this.value[0].replace(/,/g, '') +
        '-' +
        this.value[1].replace(/,/g, '') +
        '.jpg'
    },
    replaceByDefault () {
      this.plotURL =
        window.location.protocol +
        '//' +
        window.location.host +
        '/api/stationplot/default.jpg'
    }
  }
}
</script>

<style>
.stats {
  height: 100%;
  margin: 20px;
  display: flex;
  flex-direction: row;
  flex-wrap: wrap;
}

.stats-picker {
  display: flex;
  margin: auto;
  width: 100%;
  align-items: center;
}

.stats-image {
  margin: auto;
  max-height: 80vh;
}

.vue-slider-process {
  background-color: #3f51b5;
}

.vue-slider-dot-tooltip-inner {
  border-color: #3f51b5;
  background-color: #3f51b5;
}
</style>
