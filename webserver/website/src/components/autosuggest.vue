<template>
  <div class="dropdown" style="position:relative">
    <input
      class="pretty_textbox"
      type="text"
      autocomplete="off"
      :placeholder="placeholder"
      :name="name"
      v-model="user_search"
      v-on:blur="loose_focus"
      @keydown.enter="enter"
      @keydown.down="down"
      @keydown.up="up"
      @input="change"
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
function sleep (ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

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
    suggestions: {
      type: Array,
      required: true
    },
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
      return this.suggestions.filter(
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
    }
  },

  methods: {
    enter () {
      this.user_search = this.matches[this.current]
      this.open = false
      this.$emit('input', this.user_search)
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

    async loose_focus () {
      // When you click on one of the items, you will loose focus
      // on the text input, and the item you clicked on will be
      // closed befor the click was registert. To avoid this, we
      // are using a little delay.
      await sleep(1000)
      this.open = false
      this.$emit('input', this.user_search)
    },

    suggestionClick (index) {
      console.log(index)
      this.user_search = this.matches[index]
      this.open = false
      this.$emit('input', this.user_search)
    }
  }
}
</script>
