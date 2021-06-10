<template>
  <transition name="fade">
    <div v-if="show" class="snackbar-center">
      <div id="snackbar" class="pretty_shadow">
        <div :class="[{ 'layout small': layout === 'small', 'layout multiline': layout === 'multiline'}, style_class ]">
          <div>
            <slot></slot>
          </div>
          <slot v-if="timeout === -1" name="action">
            <div class="click_text text-right" @click="show=false">SCHLIESSEN</div>
          </slot>
        </div>
      </div>
    </div>
  </transition>
</template>

<script lang="ts">
import Vue from 'vue'

export default Vue.extend({
  name: 'snackbar',
  props: {
    layout: {
      type: String,
      default: 'small',
      validator: function (value) {
        return ['small', 'multiline'].indexOf(value) !== -1
      }
    },
    timeout: {
      type: Number,
      default: -1
    },
    style_class: {
      type: String,
      default: ''
    }
  },
  data: function () {
    return {
      show: true
    }
  },
  updated () {
    if (this.timeout !== -1) {
      setTimeout(function () { this.show = false }.bind(this), this.timeout)
    }
  },
  mounted () {
    if (this.timeout !== -1) {
      setTimeout(function () { this.show = false }.bind(this), this.timeout)
    }
  }

})
</script>

<style lang="scss">
@import 'src/assets/scss/variables';

.snackbar-center {
  width: 100%;
  display: flex;
  justify-content: space-around;
  position: fixed; /* Sit on top of the screen */
  bottom: 30px; /* 30px from the bottom */
  z-index: 10;
}

#snackbar {
  width: 95%;
  max-width: 600px;
  background-color: $page_lighter_gray;

  .layout {
    padding: 20px 30px;
    display: grid;
    align-items: center;
  }

  .layout.small {
    grid-template-columns: auto min-content;
    column-gap: 20px;
  }

  .layout.multiline {
    grid-template-rows: auto min-content;
    row-gap: 20px;
  }
}

.click_text {
  font-size: 1.3em;
  font-weight: bold;
  cursor: pointer;
  color: $page_accent;
}

.click_text:hover {
  color: lighten($page_accent, 10)
}

.click_text:active {
  position: relative;
  top: 1px;
}

.fade-enter-active, .fade-leave-active {
  transition: opacity .5s;
}
.fade-enter, .fade-leave-to {
  opacity: 0;
}
</style>
