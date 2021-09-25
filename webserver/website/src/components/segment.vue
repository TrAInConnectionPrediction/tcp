<template>
  <div style="display: contents">
    <div v-bind:style="gradient_line" class="station_delay_line"></div>
    <div class="station">
      {{ segment.dp_station_display_name }}
    </div>
    <div class="time" v-if="segment.dp_pt.isSame(segment.dp_ct)">ab {{ segment.dp_ct.format('HH:mm') }}</div>
    <div class="time" v-else>ab {{ segment.dp_ct.format('HH:mm') }}  <del class="outdated">{{ segment.dp_pt.format('HH:mm') }}</del></div>
    <div class="platform" v-if="segment.dp_pp == segment.dp_cp">von Gl. {{ segment.dp_cp }}</div>
    <div class="platform" v-else>von Gl. {{ segment.dp_cp }}  <del class="outdated">{{ segment.dp_pp }}</del></div>

    <div class="train">
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
    <div class="time" v-if="segment.ar_pt.isSame(segment.ar_ct)">an {{ segment.ar_ct.format('HH:mm') }}</div>
    <div class="time" v-else>an {{ segment.ar_ct.format('HH:mm') }}  <del class="outdated">{{ segment.ar_pt.format('HH:mm') }}</del></div>
    <div class="platform" v-if="segment.ar_pp == segment.ar_cp">an Gl. {{ segment.ar_cp }}</div>
    <div class="platform" v-else>an Gl. {{ segment.ar_cp }}  <del class="outdated">{{ segment.ar_pp }}</del></div>

    <div v-if="'transfer_time' in segment" style="display: contents">
      <div class="score" v-bind:style="transfer_style">
        Verbindungs-Score:
        <span v-bind:style="text_color">{{ segment.score }}%</span>
      </div>
      <div class="transfer" v-bind:style="transfer_style">
        Umsteigezeit: {{ segment.transfer_time }} Min.
      </div>
      <div class="walk" v-bind:style="transfer_style">
        <div v-if="segment.walk" style="display: contents">
          <i class="tcp-pedestrian" style="font-size: 1.2rem"></i>
          davon {{ segment.walk }} Min. Fußweg
        </div>
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

<style lang="scss">
@import 'src/assets/scss/variables';

.details_grid {
  margin: 20px;
  display: inline-grid;
  grid-template-columns: 15px minmax(max-content, auto) minmax(max-content, auto) minmax(max-content, auto);
  width: calc(100% - 40px);
  background-color: $page_lighter_gray;
}

.station {
  margin: 10px 10px 10px 0;
  padding: 5px 10px;
  background-color: $page_gray;
}

.time {
  margin: 15px;
}

.platform {
  margin: 15px;
}

.train {
  margin: 10px;
  grid-column-start: span 3;
}

.score {
  border: solid 10px transparent;
  grid-column-start: span 4;
}

.transfer {
  border: solid 10px transparent;
  grid-column-start: span 2;
}

.walk {
  border: solid 10px transparent;
  grid-column-start: span 2;
}

.station_delay_line {
  grid-row-start: span 3;
  width: 10px;
  margin: 10px 5px;
}

@media (max-width: 450px) {
  .details_grid {
    margin: 10px;
    display: inline-grid;
    grid-template-columns: 15px minmax(max-content, auto) minmax(max-content, auto);
    width: calc(100% - 20px);
  }

  .station {
    margin: 10px 10px 0 0;
    grid-column-start: span 2;
  }

  .time {
    margin: 0 0 10px 0;
    padding: 5px 10px;
    background-color: $page_gray;
  }

  .platform {
    margin: 0 10px 10px 0;
    padding: 5px 10px;
    background-color: $page_gray;
  }

  .train {
    margin: 10px;
    grid-column-start: span 2;
  }

  .score {
    grid-column-start: span 3;
  }

  .transfer {
    grid-column-start: span 3;
  }

  .walk {
    grid-column-start: span 3;
    border-top: 0;
  }

  .station_delay_line {
    grid-row-start: span 5;
  }

}
</style>
