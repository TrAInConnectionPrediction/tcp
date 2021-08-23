<template>
  <form v-on:submit="get_connections" class="pretty_form">
    <!-- Heading -->
    <h3 style="text-align: center;">
      <strong>Verbindung finden</strong>
    </h3>
    <!-- Start Bhf Form-->
    <div style="display: flex; align-items: end;">
      <i class="tcp-train" style="font-size: 2.7rem;"></i>
      <div style="width: 100%;">
      <label for="start">Von</label><br />
      <autosuggest
        name="start"
        placeholder="Bahnhof"
        @input="update_start"
        v-bind:class="{invalid: start_invalid}"
      >
      </autosuggest>
      </div>
    </div>
    <!-- End Bhf Form -->
    <div style="display: flex; align-items: end;">
      <i class="tcp-train" style="font-size: 2.7rem;"></i>
      <div style="width: 100%;">
      <label for="destination">Nach</label><br />
      <autosuggest
        name="destination"
        placeholder="Bahnhof"
        @input="update_destination"
        v-bind:class="{invalid: destination_invalid}"
      >
      </autosuggest>
      </div>
    </div>
    <!-- Date Form -->
    <div style="display: flex; align-items: end;">
      <i class="tcp-calendar" style="font-size: 2.7rem;"></i>
      <div style="width: 100%;">
      <label for="datetime">Datum</label><br />
      <flat-pickr
        v-model="date"
        :config="config"
        class="pretty_textbox"
        placeholder="Select date"
        name="date"
      >
      </flat-pickr>
      </div>
      <toggleSwitch style="padding: 6px 12px;" v-model="search_for_arrival"></toggleSwitch>
    </div>
    <!-- Submit Button -->
    <div class="text-center">
      <input
        class="pretty_button"
        id=""
        name="submit"
        type="submit"
        value="SUCHEN"
      />
    </div>
  </form>
</template>

<script>
import { mapState } from 'vuex'
import flatpickr from 'flatpickr'
import flatPickr from 'vue-flatpickr-component'
import 'flatpickr/dist/flatpickr.css'

import autosuggest from './autosuggest.vue'
import toggleSwitch from './toggle_switch.vue'

require('flatpickr/dist/themes/dark.css')

export default {
  name: 'searchform',
  data: function () {
    return {
      start: '',
      start_invalid: false,
      destination: '',
      destination_invalid: false,
      date: flatpickr.formatDate(new Date(), 'd.m.Y H:i'),
      // Get more from https://flatpickr.js.org/options/
      config: {
        enableTime: true,
        time_24hr: true,
        dateFormat: 'd.m.Y H:i',
        altFormat: 'd.m.Y H:i'
      },
      search_for_arrival: false
    }
  },
  created () {
    fetch(window.location.protocol + '//' + window.location.host + '/api/connect', {
      type: 'GET',
      data: null,
      dataType: 'json'
    })
      .then(response => this.$parent.display_fetch_error(response))
      .then(response => response.json())
      .then(data => {
        this.$store.commit('set_stations', data.stations)
        // this.stations = data.stations
      })
  },
  methods: {
    get_connections: function (event) {
      event.preventDefault() // prevent page reload

      if (
        this.stations.includes(this.start) &&
        this.stations.includes(this.destination)
      ) {
        this.start_invalid = false
        this.destination_invalid = false
        this.$parent.get_connections({
          start: this.start,
          destination: this.destination,
          date: this.date, // flatpickr.formatDate(new Date(this.date), "d.m.Y H:i")
          search_for_departure: !this.search_for_arrival
        })
      } else {
        if (!this.stations.includes(this.start)) {
          this.start_invalid = true
        }
        if (!this.stations.includes(this.destination)) {
          this.destination_invalid = true
        }
      }
    },
    update_start (station) {
      this.start = station
    },
    update_destination (station) {
      this.destination = station
    }
  },
  computed: {
    ...mapState(['stations'])
  },
  components: {
    flatPickr,
    autosuggest,
    toggleSwitch
  }
}
</script>
