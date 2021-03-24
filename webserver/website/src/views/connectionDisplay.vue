<template>
  <div>
    <connection
      v-for="(connection, index) in connections"
      :key="index"
      :summary="connection.summary"
      :segments="connection.segments"
    ></connection>
  </div>
</template>

<script>
import { mapState } from 'vuex'
import connection from '../components/connection.vue'

export default {
  name: 'connectionDisplay',
  computed: {
    ...mapState(['connections'])
  },
  components: {
    connection
  }
}
</script>

<style>
.card_header_item {
  display: inline-grid;
  height: max-content;
  min-width: 100px;
  flex-grow: 1;
}

.card_header_item_header {
  padding: 10px 10px 0 10px;
  border-bottom: 1.5px solid #4b4b4b;
  font-weight: bold;
}

.card_header_item_item {
  padding: 0 10px 0 10px;
}

.card_contents {
  border-top: 1px solid rgba(0, 0, 0, 0.1);
  overflow: auto;
}

.details_grid {
  margin: 20px;
  display: inline-grid;
  grid-template-columns: repeat(4, max-content);
  grid-gap: 0px;
  width: max-content;
  background-color: rgba(255, 255, 255, 0.05);
}

.open-enter-active,
.open-leave-active {
  transition: all 0.5s;
  max-height: 99em;
  overflow: hidden;
}

.open-enter,
.open-leave-to {
  display: block;
  max-height: 0px;
  overflow: hidden;
}

.station {
  margin: 10px 0;
  width: 100%;
  padding: 5px;
  background-color: #212529;
}

.time {
  margin: 15px;
}

.platform {
  margin: 15px;
}

.train {
  margin: 10px;
}

.score {
  border: solid 10px transparent;
  grid-column-start: span 2;
}

.transfer {
  border: solid 10px transparent;
}

.walk {
  border: solid 10px transparent;
  grid-column-start: span 3;
}

@media (max-width: 700px) {
  .m-5 {
    margin: 3rem 0.5rem 3rem 0.5rem !important;
  }
}

.station_delay_line {
  grid-row-start: span 3;
  width: 10px;
  margin: 10px 5px;
}
</style>
