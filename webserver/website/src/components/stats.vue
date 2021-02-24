<template>
  <div>
    <div>value: {{ value }}</div>
    <vue-slider
      v-model="value"
      :data="data"
      :tooltip-placement="['top', 'bottom']"
      :max-range="168"
      :lazy="true"
      style="margin: 50px"
    ></vue-slider>
    <button v-on:click="updatePlot">Plot generieren</button>
    <img :src="plotURL" />
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
      plotURL: 'http://tcp.sfz-eningen.de/assets/img/IC.svg'
    }
  },
  methods: {
    async updatePlot () {
      this.plotURL =
        'http://localhost:5000/api/stationplot/' +
        this.value[0].replace(/,/g, '') +
        '-' +
        this.value[1].replace(/,/g, '') +
        '.jpg'
    }
  }
}
</script>
