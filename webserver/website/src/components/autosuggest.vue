<template>
  <div class="dropdown" style="position:relative">
    <input
      class="pretty_textbox"
      type="text"
      autocomplete="off"
      :placeholder="placeholder"
      :name="name"
      v-model="user_search"
      @keydown.enter="enter"
      @keydown.down="down"
      @keydown.up="up"
      @keydown.tab="tab"
      @keydown.esc="loose_focus"
      @input="change"
      v-on:blur="loose_focus"
    />
    <ul
      class="dropdown-menu dropdown-menu-dark"
      v-bind:class="{ show: openSuggestion }"
      style="width:100%"
    >
      <li
        v-for="(suggestion, index) in matches"
        :key="index"
        @click="suggestionClick(index)"
        @mousedown="mousedown_prevent"
      >
        <a
          class="dropdown-item"
          v-bind:class="{ active: index === current }"
          href="#"
          >{{ suggestion }}</a
        >
      </li>
    </ul>
  </div>
</template>

<script>
import { mapState } from 'vuex'

export default {
  name: 'autosuggest',
  data () {
    return {
      open: false,
      current: 0,
      user_search: ''
    }
  },

  props: {
    placeholder: {
      type: String,
      required: false
    },
    name: {
      type: String,
      required: false
    }
  },

  computed: {
    matches () {
      return this.stations.filter(
        function (item) {
          if (this.count < 20 && item.toLowerCase().indexOf(this.search) >= 0) {
            this.count++
            return true
          }
          return false
        },
        { count: 0, search: this.user_search.toLowerCase() }
      )
    },

    openSuggestion () {
      return (
        this.user_search !== '' &&
        this.matches.length !== 0 &&
        this.open === true
      )
    },
    ...mapState(['stations'])
  },

  methods: {
    enter (event) {
      if (this.open) {
        event.preventDefault()
        this.user_search = this.matches[this.current]
        this.open = false
        this.$emit('input', this.user_search)
      }
    },

    up () {
      if (this.current > 0) this.current--
    },

    down () {
      if (this.current < this.matches.length - 1) this.current++
    },

    isActive (index) {
      return index === this.current
    },

    change () {
      if (this.open === false) {
        this.open = true
        this.current = 0
      }
      this.$emit('input', this.user_search)
    },

    tab (event) {
      if (this.open) {
        event.preventDefault()
        if (this.current < this.matches.length - 1) this.current++
        else this.current = 0
      }
    },

    mousedown_prevent (event) {
      // clicking on a suggestion should not trigger blur
      event.preventDefault()
    },

    loose_focus () {
      this.open = false
      this.$emit('input', this.user_search)
    },

    suggestionClick (index) {
      this.user_search = this.matches[index]
      this.open = false
      this.$emit('input', this.user_search)
    }
  }
}
</script>
