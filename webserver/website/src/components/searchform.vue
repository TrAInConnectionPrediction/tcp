<template>
  <form v-on:submit="get_connections" class="pretty_form">
    <!-- Heading -->
    <h3 style="text-align: center;">
      <strong>Verbindung finden:</strong>
      <hr />
    </h3>
    <!-- Start Bhf Form-->
    <div style="display: flex;">
      <!-- <i class="fas fa-train prefix grey-text"></i> -->
      <i class="tcp-train" style="position: relative; font-size: 2.5rem; top: 20px;width: 40px;"></i>
      <div style="margin-left: 10px;width: 100%;">
      <label for="start">Von</label><br />
      <autosuggest
        name="start"
        placeholder="Bahnhof"
        :suggestions="stations"
        @input="update_start"
      >
      </autosuggest>
      </div>
    </div>
    <!-- End Bhf Form -->
    <div style="display: flex;">
      <!-- <i class="fas fa-train prefix grey-text"></i> -->
      <i class="tcp-train" style="position: relative; font-size: 2.5rem; top: 20px;width: 40px;"></i>
      <div style="margin-left: 10px;width: 100%;">
      <label for="destination">Nach</label><br />
      <autosuggest
        name="destination"
        placeholder="Bahnhof"
        :suggestions="stations"
        @input="update_destination"
      >
      </autosuggest>
      </div>
    </div>
    <!-- Date Form -->
    <div style="display: flex;">
      <!-- <i class="fas fa-calendar prefix grey-text"></i> -->
      <i class="tcp-calendar" style="position: relative; font-size: 2.5rem; top: 20px;width: 40px;"></i>
      <div style="margin-left: 10px;width: 100%;">
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
import flatpickr from 'flatpickr'
import flatPickr from 'vue-flatpickr-component'
import 'flatpickr/dist/flatpickr.css'

import autosuggest from './autosuggest.vue'

require('flatpickr/dist/themes/dark.css')

export default {
  name: 'searchform',
  data: function () {
    return {
      start: '',
      destination: '',
      date: flatpickr.formatDate(new Date(), 'd.m.Y H:i'),
      stations: [],
      // Get more from https://flatpickr.js.org/options/
      config: {
        enableTime: true,
        time_24hr: true,
        dateFormat: 'd.m.Y H:i',
        altFormat: 'd.m.Y H:i'
      }
    }
  },
  created () {
    fetch('/api/connect', {
      type: 'GET',
      data: null,
      dataType: 'json'
    })
      .then(response => this.$parent.display_fetch_error(response))
      .then(response => response.json())
      .then(data => {
        this.stations = data.stations
      })
  },
  methods: {
    get_connections: function (event) {
      event.preventDefault() // prevent page reload

      // First show and hide stuff
      // document.querySelector("#datetime")._flatpickr.close();
      // showSection("pgr_bar");
      // window.location.hash = ""; //delete any # in the url

      if (
        this.stations.includes(this.start) &&
        this.stations.includes(this.destination)
      ) {
        this.$parent.get_connections({
          start: this.start,
          destination: this.destination,
          date: this.date // flatpickr.formatDate(new Date(this.date), "d.m.Y H:i")
        })
      }
    },
    update_start (station) {
      this.start = station
    },
    update_destination (station) {
      this.destination = station
    }
  },
  components: {
    flatPickr,
    autosuggest
  }
}
</script>
<style>
.pretty_form {
  width: 95%;
  /* max-width: 400px; */
  margin: auto;
  display: grid;
  row-gap: 20px;
  color: white;
}

.pretty_textbox {
  color: #e0e0e0 !important;
  border-radius: 0px;
  background-color: #292d31;
  /* box-shadow: inset 0 1px 1px rgba(0, 0, 0, 0.075); */
  padding: 6px 12px;
  width: 100%;
  border-width: 0;
  /* border-bottom-width: 2px; */
  /* border-color: #000000; */
  line-height: 1.6 !important;
  transition: border-color ease-in-out 0.15s, box-shadow ease-in-out 0.15s,
    -webkit-box-shadow ease-in-out 0.15s;
}

.pretty_button {
  background-color: #3f51b5;
  color: #e0e0e0 !important;
  padding: 12px 34px;
  border: none;
  border-radius: 0px;
  line-height: 1.6 !important;
}

.pretty_button:hover {
  background-color: #344295;
}

.pretty_button:active {
    position: relative;
    top: 2px;
}

.dropdown-item.active {
  background-color: #3f51b5 !important;
}

.flatpickr-calendar,
.flatpickr-calendar.arrowTop,
.flatpickr-months .flatpickr-month,
.flatpickr-current-month,
span.flatpickr-weekday,
.flatpickr-current-month .flatpickr-monthDropdown-months {
  background: #2f2f2f;
}

.flatpickr-day.selected,
.flatpickr-day.startRange,
.flatpickr-day.endRange,
.flatpickr-day.selected.inRange,
.flatpickr-day.startRange.inRange,
.flatpickr-day.endRange.inRange,
.flatpickr-day.selected:focus,
.flatpickr-day.startRange:focus,
.flatpickr-day.endRange:focus,
.flatpickr-day.selected:hover,
.flatpickr-day.startRange:hover,
.flatpickr-day.endRange:hover,
.flatpickr-day.selected.prevMonthDay,
.flatpickr-day.startRange.prevMonthDay,
.flatpickr-day.endRange.prevMonthDay,
.flatpickr-day.selected.nextMonthDay,
.flatpickr-day.startRange.nextMonthDay,
.flatpickr-day.endRange.nextMonthDay {
  background: #3f51b5;
  border-color: #3f51b5;
}
</style>
