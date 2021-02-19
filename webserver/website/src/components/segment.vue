<template>
  <div style="display: contents">
    <div class="station" v-bind:style="dp_station_style">
      {{ segment.dp_station }}
    </div>
    <div class="time">ab {{ segment.dp_pt }}</div>
    <div class="platform">von Gl. {{ segment.dp_pp }}</div>

    <div class="train" style="grid-column-start: span 3;">
      <img
        v-if="segment.dp_c in train_icons"
        v-bind:src="train_icons[segment.dp_c]"
        height="20px"
      />
      {{ segment.train_name }} nach {{ segment.train_destination }}
    </div>

    <div class="station" v-bind:style="ar_station_style">
      {{ segment.ar_station }}
    </div>
    <div class="time">an {{ segment.ar_pt }}</div>
    <div class="platform">an Gl. {{ segment.ar_pp }}</div>

    <div v-if="'transfer_time' in segment" style="display: contents">
      <div class="transfer" v-bind:style="transfer_style">
        Umsteigezeit: {{ segment.transfer_time }} Min.
      </div>
      <div class="score" v-bind:style="transfer_style">
        Verbindungs-Score: {{ segment.score }}%
      </div>
    </div>

    <div v-if="segment.walk" class="walk" v-bind:style="transfer_style">
      <img src="../assets/img/pedestrian.svg" height="20px" />
      davon {{ segment.walk }} Min. Fußweg
    </div>
  </div>
</template>
<script>
import { rdylgr_colormap } from '../assets/js/colormap.js'

export default {
  name: 'segment',
  props: ['segment', 'con_score'],
  data: function () {
    return {
      show_details: false,
      dp_station_style: {
        'background-color': rdylgr_colormap(this.segment.dp_delay, 0.2, 0.8)
      },
      ar_station_style: {
        'background-color': rdylgr_colormap(this.segment.ar_delay, 0.2, 0.8)
      },
      transfer_style: {
        'background-color': rdylgr_colormap(this.con_score, 50, 100),
        'border-color': rdylgr_colormap(this.con_score, 50, 100)
      },
      train_icons: {
        ICE: require('../assets/img/ICE.svg'),
        IC: require('../assets/img/IC.svg'),
        RE: require('../assets/img/RE.svg'),
        S: require('../assets/img/S.svg'),
        RB: require('../assets/img/RB.svg')
      }
    }
  }
}
</script>