<template>
  <div>
    <h1 v-if="connections.length !== 0">
      {{ connections[0].summary['dp_station_display_name'] }} nach
      {{ connections[0].summary['ar_station_display_name'] }}
    </h1>
    <div
      v-if="connections.length !== 0"
      class="custom_card"
      style="border-left: 10px solid black; background-color: #212529"
    >
      <div class="connections_header">
        <div class="col1">Zeit</div>
        <div class="col2">Dauer</div>
        <div class="col3">Umstiege</div>
        <div class="col4">Produkte</div>
        <div class="col5">Score</div>
        <div class="col6">Ticket</div>
      </div>
    </div>
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
