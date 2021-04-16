<template>
  <!-- summary -->
  <div v-bind:style="[background_color, border_style]" class="custom_card">
    <div v-on:click="show_details = !show_details" class="card_header">
      <div class="p-3 col1">
        <div v-if="summary.dp_ct.isSame(summary.dp_pt)">{{ summary.dp_ct.format('HH:mm') }}</div>
        <div v-else>
          {{ summary.dp_ct.format('HH:mm') }} <del class="outdated">{{ summary.dp_pt.format('HH:mm') }}</del>
        </div>

        <div v-if="summary.ar_ct.isSame(summary.ar_pt)">{{ summary.ar_ct.format('HH:mm') }}</div>
        <div v-else>
          {{ summary.ar_ct.format('HH:mm') }} <del class="outdated">{{ summary.ar_pt.format('HH:mm') }}</del>
        </div>
      </div>
      <div class="p-3 col2">{{ summary.duration }}</div>
      <div class="p-3 col3">{{ summary.transfers }}</div>
      <div class="p-3 col4">
        {{ summary.train_categories.join(', ') }}
      </div>
      <div class="p-3 col5" v-bind:style="[text_color]">{{ summary.score }}%</div>
      <affiliateLink
        class="col6"
        :date="summary.dp_ct"
        :time="summary.dp_ct"
        :price="summary.price"
        :start="summary.dp_station_display_name"
        :destination="summary.ar_station_display_name"
      ></affiliateLink>
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
import affiliateLink from './affiliateLink.vue'

export default {
  name: 'connection',
  components: {
    segment,
    affiliateLink
  },
  props: ['summary', 'segments'],
  data: function () {
    return {
      show_details: false,
      background_color: {
        'background-color': '#212529'
      },
      border_style: {
        'border-left': '10px solid ' + rdylgr_colormap(this.summary.score, 50, 100, 200)
      },
      text_color: {
        color: rdylgr_colormap(this.summary.score, 50, 100, 200)
      }
    }
  }
}
</script>
