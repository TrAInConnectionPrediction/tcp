<template>
  <div class="my-5">
    <h1 v-if="connections.length !== 0" class="text-center">
      {{ connections[0].summary['dp_station_display_name'] }} nach
      {{ connections[0].summary['ar_station_display_name'] }}
    </h1>
    <div v-else>Bitte benutze die <router-link to="#search">Suchfunktion</router-link> um Zugverbindungen zu bewerten</div>
    <div v-if="connections.length !== 0" class="custom_card">
      <div class="connections_header">
        <div class="col1 sort_col" @click="sort_time()">
          Zeit
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
        <div class="col4">Produkte</div>
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

<style lang="scss">
.connections-move {
  transition: transform 1s;
}

.custom_card {
  margin-bottom: 5px;
  color: $text_color;
}

.card_header {
  height: max-content;
  display: grid;
  grid-template-columns: 1fr 1fr 1fr 1fr 1fr minmax(190px, 1fr);

  .col1,
  .col3,
  .col5,
  .col6 {
    background-color: $page_gray;
  }

  .col2,
  .col4 {
    background-color: $page_lighter_gray;
  }
}

@media (max-width: 1000px) {
  .card_header {
    grid-template-columns: 1fr 1fr minmax(190px, 1fr);

    .col1,
    .col3,
    .col4,
    .col6 {
      background-color: $page_gray;
    }

    .col2,
    .col5 {
      background-color: $page_lighter_gray;
    }
  }
}

@media (max-width: 450px) {
  .card_header {
    grid-template-columns: 1fr minmax(190px, 1fr);

    .col1,
    .col3,
    .col5 {
      background-color: $page_lighter_gray;
    }

    .col2,
    .col4,
    .col6 {
      background-color: $page_gray;
    }
  }

  .pretty_form {
    padding: 20px 8px;
  }
}

@media (max-width: 350px) {
  .card_header {
    grid-template-columns: minmax(190px, 1fr);
  }
}

.connections_header {
  @extend .card_header;
  border-left: 10px solid transparent;

  div {
    padding: 5px 20px;
  }
}

.card_header_item {
  display: inline-grid;
  flex-grow: 1;
  flex-basis: 1px;
  width: min-content;
  grid-auto-rows: min-content;
}

.outdated {
  color: $page_outdated_text;
}

.affiliate_link_container {
  display: grid;
  flex-grow: 1;
  flex-basis: 0;

  .pretty_button {
    padding: 12px 12px;
  }
}

.card_contents {
  border-top: 1px solid $page_background;
  overflow: auto;
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

.arrow {
  border: solid $text_color;
  border-width: 0 3px 3px 0;
  display: inline-block;
  padding: 3px;
}

.right {
  transform: rotate(-45deg);
  -webkit-transform: rotate(-45deg);
}

.left {
  transform: rotate(135deg);
  -webkit-transform: rotate(135deg);
}

.up {
  transform: rotate(-135deg);
  -webkit-transform: rotate(-135deg);
}

.down {
  transform: rotate(45deg);
  -webkit-transform: rotate(45deg);
}

.sort_col {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  cursor: pointer;
}

.sort_col:hover {
  outline: solid 1px $page_accent;
  z-index: 1;
}

.sort_col:active {
  position: relative;
  top: 2px;
}
</style>
