<template>
  <form v-on:submit="get_connections">
    <!-- Heading -->
    <h3 class="white-text text-center">
      <strong>Verbindung finden:</strong>
    </h3>
    <hr />
    <!-- Start Bhf Form-->
    <div class="md-form">
      <i class="fas fa-train prefix grey-text"></i>
      <label class="form-control-label" for="startbhf">Start Bahnhof</label>
      <input
        class="form-control white-text typeahead"
        id="startbhf"
        name="startbhf"
        type="text"
        v-model="start"
      />
    </div>
    <!-- End Bhf Form -->
    <div class="md-form">
      <i class="fas fa-train prefix grey-text"></i>
      <label class="form-control-label" for="zielbhf">Ziel Bahnhof</label>
      <input
        class="form-control white-text typeahead"
        id="zielbhf"
        name="zielbhf"
        type="text"
        v-model="destination"
      />
    </div>
    <!-- Date Form -->
    <div class="md-form">
      <i class="fas fa-calendar prefix grey-text"></i>
      <label class="form-control-label" for="datetime">Datum</label>
      <flat-pickr
        v-model="date"
        :config="config"
        class="form-control white-text"
        placeholder="Select date"
        name="date"
      >
      </flat-pickr>
      <!-- <input
        class="form-control white-text"
        type="text"
        id="datetime"
        name="datetime"
        v-model="time"
      /> -->
    </div>
    <!-- Submit Button -->
    <div class="text-center">
      <input
        class="btn btn-indigo backshadow"
        id=""
        name="submit"
        type="submit"
        value="SUCHEN"
      />
    </div>
  </form>
</template>

<script>
import flatPickr from "vue-flatpickr-component";
import "flatpickr/dist/flatpickr.css";
import flatpickr from "flatpickr";

export default {
  name: "searchform",
  data: function() {
    return {
      start: "",
      destination: "",
      date: new Date().getTime(),
      stations: [],
      // Get more form https://flatpickr.js.org/options/
      config: {
        enableTime: true,
        time_24hr: true,
        dateFormat: "d.m.Y H:i",
        altFormat: "d.m.Y H:i"
      }
    };
  },
  created() {
    fetch("/api/connect", {
      type: "GET",
      data: null,
      dataType: "json"
    })
      .then(response => response.json())
      .then(data => {
        this.stations = data.stations;
      });
  },
  methods: {
    get_connections: function(event) {
      event.preventDefault(); // it prevent from page reload

      //First show and hide stuff
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
          date: flatpickr.formatDate(new Date(this.date), "d.m.Y H:i")
        });
      }
    }
  },
  components: {
    flatPickr
  }
};
</script>
<style>
.flatpickr-calendar,
.flatpickr-calendar.arrowTop {
  background: #202020;
}

.flatpickr-months .flatpickr-month {
  background: #202020;
}

.flatpickr-current-month
  .flatpickr-monthDropdown-months
  .flatpickr-monthDropdown-month {
  background: #202020;
}

span.flatpickr-weekday {
  background: #202020;
}

.flatpickr-current-month .flatpickr-monthDropdown-months {
  background: #202020;
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
