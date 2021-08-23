<template>
    <div class="toggle_container" @click="$emit('input', !value)">
      <span class="text" :style="{ 'font-weight': !value ? 'bold' : 'normal' }">Ab</span>
      <span class="toggler" :class="{'on': value}"></span>
      <span class="text" :style="{ 'font-weight': value ? 'bold' : 'normal' }">An</span>
    </div>
</template>

<script lang="ts">
import Vue from 'vue'
export default Vue.extend({
  name: 'toggleSwitch',
  props: {
    value: {
      type: Boolean,
      default: false
    }
  }
})
</script>

<style lang="scss">
@import 'src/assets/scss/variables';

.toggle_container {
  min-width: max-content;

  .text {
    transition: font-weight 0.5s;
  }
}

.toggler {
  width: 2.5rem;
  display: inline-block;
  height: 0.9rem;
  background-color: $page_even_lighter_gray;
  margin: 0 0.7rem;
  border-radius: 100px;
  position: relative;
  vertical-align: middle;

  &.on::after {
      left: 1.4rem;
  }
}

.toggler::after {
  content: "";
  width: 1.3rem;
  height: 1.3rem;
  display: inline-block;
  background-color: $page_accent;
  border-radius: 100px;
  position: absolute;
  top: -0.2rem;
  left: -0.2rem;
  box-shadow: 0 0 .5rem 0 $page_background;
  transition: left 0.5s;
}
</style>
