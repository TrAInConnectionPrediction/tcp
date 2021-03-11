<template>
  <!-- summary -->
  <div v-bind:style="background_color" class="custom_card">
    <div v-on:click="show_details = !show_details" class="card_header">
      <div class="card_header_item">
        <div class="card_header_item_header">Bahnhof</div>
        <div class="card_header_item_item">{{ summary.dp_station_display_name }}</div>
        <div class="card_header_item_item">{{ summary.ar_station_display_name }}</div>
      </div>
      <div class="card_header_item">
        <div class="card_header_item_header">Zeit</div>
        <div class="card_header_item_item" v-if="summary.dp_ct == summary.dp_pt">{{ summary.dp_ct }}</div>
        <div class="card_header_item_item" v-else>{{ summary.dp_ct }}  <del class="pt">{{ summary.dp_pt }}</del></div>

        <div class="card_header_item_item" v-if="summary.ar_ct == summary.ar_pt">{{ summary.ar_ct }}</div>
        <div class="card_header_item_item" v-else>{{ summary.ar_ct }}  <del class="pt">{{ summary.ar_pt }}</del></div>
      </div>
      <div class="card_header_item">
        <div class="card_header_item_header">Dauer</div>
        <div class="card_header_item_item">{{ summary.duration }}</div>
      </div>
      <div class="card_header_item">
        <div class="card_header_item_header">Umstiege</div>
        <div class="card_header_item_item">{{ summary.transfers }}</div>
      </div>
      <div class="card_header_item">
        <div class="card_header_item_header">Produkte</div>
        <div
          v-for="cat in summary.train_categories"
          :key="cat"
          class="card_header_item_item"
        >
          {{ cat }}
        </div>
      </div>
      <div class="card_header_item">
        <div class="card_header_item_header">Preis</div>
        <div class="card_header_item_item" v-if="summary.price === -1">-</div>
        <div class="card_header_item_item" v-else>{{ summary.price / 100 }}€</div>
      </div>
      <div class="card_header_item">
        <div class="card_header_item_header">Verbindungs-Score</div>
        <div class="card_header_item_item">{{ summary.score }}%</div>
      </div>
    </div>
    <!-- segments -->
    <transition name="open">
      <div v-if="show_details" class="card_contents">
        <div>
          <div class="details_grid">
            <segment
              v-for="(segment, index) in segments"
              v-bind:key="index"
              v-bind:segment="segment"
              v-bind:con_score="summary.score"
            ></segment>
          </div>
        </div>
      </div>
    </transition>
  </div>
</template>

<script>
import { rdylgr_colormap } from '../assets/js/colormap.js'
import segment from './segment.vue'

export default {
  name: 'connection',
  components: {
    segment
  },
  props: ['summary', 'segments'],
  data: function () {
    return {
      show_details: false,
      background_color: {
        'background-color': rdylgr_colormap(this.summary.score, 50, 100)
      }
    }
  }
}
</script>
<style>
  .pt {
    color: #686868;
  }
</style>
