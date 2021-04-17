<template>
  <div>
    <h1 v-if="connections.length !== 0">
      {{ connections[0].summary['dp_station_display_name'] }} nach
      {{ connections[0].summary['ar_station_display_name'] }}
    </h1>
    <div v-if="connections.length !== 0" class="custom_card">
      <div class="connections_header">
        <div class="col1 sort_col" @click="sort_time()">Zeit
          <span v-if="last_sort === 'dp_ct' || last_sort === 'ar_ct'">
            <span v-if="last_sort === 'dp_ct'">Ab </span>
            <span v-if="last_sort === 'ar_ct'">An </span>
            <i v-if="asc_sort[last_sort]" class="arrow up"></i>
            <i v-else-if="!asc_sort[last_sort]" class="arrow down"></i>
          </span>
        </div>
        <div class="col2 sort_col" @click="sort_by_key('duration')">
          Dauer
          <span v-if="last_sort === 'duration'">
            <i v-if="asc_sort[last_sort]" class="arrow up"></i>
            <i v-else-if="!asc_sort[last_sort]" class="arrow down"></i>
          </span>
        </div>
        <div class="col3 sort_col" @click="sort_by_key('transfers')">
          Umstiege
          <span v-if="last_sort === 'transfers'">
            <i v-if="asc_sort[last_sort]" class="arrow up"></i>
            <i v-else-if="!asc_sort[last_sort]" class="arrow down"></i>
          </span>
        </div>
        <div class="col4">
          Produkte
        </div>
        <div class="col5 sort_col" @click="sort_by_key('score')">
          Score
          <span v-if="last_sort === 'score'">
            <i v-if="asc_sort[last_sort]" class="arrow up"></i>
            <i v-else-if="!asc_sort[last_sort]" class="arrow down"></i>
          </span>
          </div>
        <div class="col6 sort_col" @click="sort_by_key('price')">
          Ticket
          <span v-if="last_sort === 'price'">
            <i v-if="asc_sort[last_sort]" class="arrow up"></i>
            <i v-else-if="!asc_sort[last_sort]" class="arrow down"></i>
          </span>
        </div>
      </div>
    </div>
    <transition-group name="connections" tag="div">
      <connection
        v-for="connection in connections"
        :key="connection.id"
        :summary="connection.summary"
        :segments="connection.segments"
      ></connection>
    </transition-group>
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
  },
  data: function () {
    return {
      last_time_key: 'dp_ct',
      last_sort: 'dp_ct',
      asc_sort: {
        dp_ct: true,
        ar_ct: false,
        duration: false,
        trasfers: true,
        score: true,
        price: false
      }
    }
  },
  methods: {
    sort_time: function () {
      if (this.last_time_key === 'dp_ct' && !this.asc_sort[this.last_time_key]) {
        this.last_time_key = 'ar_ct'
        this.sort_by_key(this.last_time_key)
        this.asc_sort[this.last_time_key] = true
      } else if (this.last_time_key === 'ar_ct' && !this.asc_sort[this.last_time_key]) {
        this.last_time_key = 'dp_ct'
        this.sort_by_key(this.last_time_key)
        this.asc_sort[this.last_time_key] = true
      } else {
        this.sort_by_key(this.last_time_key)
      }
    },
    sort_by_key: function (key) {
      this.last_sort = key
      // switch sort oder
      this.asc_sort[key] = !this.asc_sort[key]
      if (this.asc_sort[key]) {
        // sort ascending
        this.connections.sort(function (a, b) {
          const x = a.summary[key]
          const y = b.summary[key]
          return x < y ? -1 : x > y ? 1 : 0
        })
      } else if (!this.asc_sort[key]) {
        // sort descending
        this.connections.sort(function (a, b) {
          const x = a.summary[key]
          const y = b.summary[key]
          return x < y ? 1 : x > y ? -1 : 0
        })
      }
      this.$store.commit('set_connections', this.connections)
    }
  }
}
</script>
