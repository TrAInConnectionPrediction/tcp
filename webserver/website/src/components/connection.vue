<template>
  <!-- summary -->
  <div v-bind:style="background_color" class="custom_card">
    <div v-on:click="show_details = !show_details" class="card_header">
      <div class="card_header_item">
        <div class="card_header_item_header">Bahnhof</div>
        <div class="card_header_item_item">{{ summary.dp_station }}</div>
        <div class="card_header_item_item">{{ summary.ar_station }}</div>
      </div>
      <div class="card_header_item">
        <div class="card_header_item_header">Zeit</div>
        <div class="card_header_item_item">{{ summary.dp_pt }}</div>
        <div class="card_header_item_item">{{ summary.ar_pt }}</div>
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
              v-bind:con_score="con_score"
            ></segment>
          </div>
        </div>
      </div>
    </transition>
  </div>
</template>

<script>
import { rdylgrColormap } from '../assets/js/colormap.js'
import segment from './segment.vue'

export default {
  name: 'connection',
  components: {
    segment
  },
  props: ['summary', 'segments', 'con_score'],
  data: function () {
    return {
      show_details: false,
      background_color: {
        'background-color': rdylgrColormap(this.con_score, 50, 100)
      }
    }
  }
}
</script>
