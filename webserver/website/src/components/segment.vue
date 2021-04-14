<template>
  <div style="display: contents">
    <div v-bind:style="gradient_line" class="station_delay_line"></div>
    <div class="station">
      {{ segment.dp_station_display_name }}
    </div>
    <div class="time" v-if="segment.dp_pt == segment.dp_ct">ab {{ segment.dp_ct }}</div>
    <div class="time" v-else>ab {{ segment.dp_ct }}  <del class="outdated">{{ segment.dp_pt }}</del></div>
    <div class="platform" v-if="segment.dp_pp == segment.dp_cp">von Gl. {{ segment.dp_cp }}</div>
    <div class="platform" v-else>von Gl. {{ segment.dp_cp }}  <del class="outdated">{{ segment.dp_pp }}</del></div>

    <div class="train" style="grid-column-start: span 3;">
      <img
        v-if="segment.dp_c in train_icons"
        v-bind:src="train_icons[segment.dp_c]"
        height="20px"
      />
      {{ segment.train_name }} nach {{ segment.train_destination }}
    </div>

    <div class="station">
      {{ segment.ar_station_display_name }}
    </div>
    <div class="time" v-if="segment.ar_pt == segment.ar_ct">an {{ segment.ar_ct }}</div>
    <div class="time" v-else>an {{ segment.ar_ct }}  <del class="outdated">{{ segment.ar_pt }}</del></div>
    <div class="platform" v-if="segment.ar_pp == segment.ar_cp">an Gl. {{ segment.ar_cp }}</div>
    <div class="platform" v-else>an Gl. {{ segment.ar_cp }}  <del class="outdated">{{ segment.dp_pp }}</del></div>

    <div v-if="'transfer_time' in segment" style="display: contents">
      <div style="background-color: #212529"></div>
      <div class="transfer" v-bind:style="transfer_style">
        Umsteigezeit: {{ segment.transfer_time }} Min.
      </div>
      <div class="score" v-bind:style="transfer_style">
        Verbindungs-Score:
        <span v-bind:style="text_color">{{ segment.score }}%</span>
      </div>
    </div>
    <div v-if="segment.walk" v-bind:style="transfer_style" style="display: contents;">
      <div style="background-color: #212529"></div>
      <div class="walk" v-bind:style="transfer_style">
        <div style="background-color: #212529"></div>
        <img src="../assets/img/pedestrian.svg" height="20px" style="color: lightgray" />
        davon {{ segment.walk }} Min. Fußweg
      </div>
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
        'background-color': rdylgr_colormap(this.segment.dp_delay, 0.2, 0.8, 200)
      },
      ar_station_style: {
        'background-color': rdylgr_colormap(this.segment.ar_delay, 0.2, 0.8, 200)
      },
      transfer_style: {
        'background-color': '#212529'
      },
      text_color: {
        color: rdylgr_colormap(this.segment.score, 50, 100, 200)
      },
      gradient_line: {
        'background-image': 'linear-gradient(' + rdylgr_colormap(this.segment.dp_delay, 0.2, 0.8, 200) + ', ' + rdylgr_colormap(this.segment.ar_delay, 0.2, 0.8, 200) + ')'
      },
      train_icons: {
        ICE: require('../assets/img/ICE.svg'),
        IC: require('../assets/img/IC.svg'),
        EC: require('../assets/img/IC.svg'),
        RE: require('../assets/img/RE.svg'),
        IRE: require('../assets/img/RE.svg'),
        S: require('../assets/img/S.svg'),
        RB: require('../assets/img/RB.svg')
      }
    }
  }
}
</script>
