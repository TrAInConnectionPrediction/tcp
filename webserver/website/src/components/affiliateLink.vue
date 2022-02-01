<template>
  <div>
    <div style="margin: auto;">
      <div v-if="price === -1"></div>
      <a v-else v-bind:href="link" target="_blank" rel="noopener" v-on:click.stop class='btn btn-primary text-nowrap' style="margin: 10px; display: block;">{{ (price / 100).toFixed(2) }}€</a>
    </div>
  </div>
</template>

<script>
export default {
  name: 'affiliateLink',
  props: ['date', 'time', 'start', 'destination', 'price'],
  computed: {
    link: function () {
      return `https://reiseauskunft.bahn.de//bin/query.exe/dn?dbkanal_003=L01_S01_D001_KAF0001_Linkgenerator_individuelle_Verbindungen-!!!affid!!!_LZ01&S=${encodeURIComponent(this.start)}&Z=${encodeURIComponent(this.destination)}&date=${this.date.format('DD.MM.YYYY')}&time=${this.time.format('HH:mm')}&timesel=depart&start=1&l=d`
    }
  }
}
</script>
