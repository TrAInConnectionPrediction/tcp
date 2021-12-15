<template>
  <body>
    <div class="fixed-top">
      <nav class="navbar navbar-expand-lg navbar-dark pretty_shadow">
        <div class="container-fluid">
          <router-link class="navbar-brand" to="/">TCP</router-link>
          <button
            class="navbar-toggler"
            type="button"
            data-bs-toggle="collapse"
            data-bs-target="#navbarSupportedContent"
            aria-controls="navbarSupportedContent"
            aria-expanded="false"
            aria-label="Toggle navigation"
          >
            <span class="navbar-toggler-icon"></span>
          </button>
          <div class="collapse navbar-collapse" id="navbarSupportedContent">
            <ul class="navbar-nav me-auto mb-2 mb-lg-0">
              <li class="nav-item">
                <router-link class="nav-link" to="/">Verbindungssuche</router-link>
              </li>
              <li class="nav-item">
                <router-link class="nav-link" :to="{ path: '/about', hash: '#content' }">Über TCP</router-link>
              </li>
              <li class="nav-item dropdown">
                <a class="nav-link dropdown-toggle" data-toggle="dropdown">Daten</a>
                <ul class="dropdown-menu">
                  <li>
                    <router-link class="dropdown-item" :to="{ path: '/data/stats', hash: '#content' }">Übersicht</router-link>
                  </li>
                  <li>
                    <router-link class="dropdown-item" :to="{ path: '/data/stations', hash: '#content' }"
                      >Stationen</router-link
                    >
                  </li>
                  <!-- <li>
                  <router-link class="dropdown-item" :to="{ path: '/data/obstacles', hash: '#stats' }"
                    >Zug-Hindernisse</router-link
                  >
                </li> -->
                </ul>
              </li>
            </ul>
            <div class="d-flex">
              <a
                class="pretty_button"
                href="https://github.com/TrAInConnectionPrediction/tcp"
                target="_blank"
                rel="noopener"
                ><i class="tcp-github"></i>TCP auf GitHub</a
              >
            </div>
          </div>
        </div>
      </nav>
      <div id="pgr_bar"></div>
    </div>
    <div id="intro" class="view">
      <div class="d-flex justify-content-center align-items-center mask" style="height: 100%">
        <div class="row" style="justify-content: center; min-width: 0; width: 100%">
          <div class="col white-text text-center">
            <h2 id="midheader" class="shadowheader">TrAIn_Connection_Prediction: TCP<br /></h2>
            <hr class="hr-light" />
            <p>
              <strong> Ihre Verbindungsvorhersage </strong>
            </p>
          </div>
          <div class="col">
            <searchform class="hover pretty_shadow"> </searchform>
          </div>
        </div>
      </div>
    </div>
    <div class="main_background">
      <main id="main">
        <router-view class="m-5" id="content" />
      </main>
    </div>
    <footer class="text-center p-3 pretty_shadow">
      <div class="fw-bold">
        <router-link class="footer_link" to="/imprint">Impressum</router-link> /
        <router-link class="footer_link" to="/privacy">Datenschutz</router-link>
      </div>
      <br />
      <span
        >© 2021 TrAIn_Connection_Prediction ist ein unabhängiger Service. Dieser steht in keiner Verbindung mit der
        Deutschen Bahn und ihren Tochter-Unternehmen.
      </span>
    </footer>
    <!-- Update Service worker -->
    <snackbar v-if="updateExists">
      Ein Update ist verfügbar
      <template v-slot:action>
        <div @click="refreshApp" class="click_text">UPDATE</div>
      </template>
    </snackbar>
    <!-- Error box -->
    <snackbar v-if="error" :timeout="15000" :layout="'multiline'" :style_class="'error_box'">
      <div>
        <div><b>Holy Guacamole!</b> {{ error.toString() }}</div>
        <div>
          Falls der Fehler weiterhin auftritt, verfassen Sie bitte einen Bugreport auf
          <a
            href="https://github.com/TrAInConnectionPrediction/tcp/issues"
            class="warning_link"
            target="_blank"
            rel="noopener"
            >GitHub</a
          >
        </div>
      </div>
    </snackbar>
  </body>
</template>

<script>
import searchform from './components/searchform.vue'
import update from './assets/js/update.js'
import snackbar from './components/snackbar.vue'
const ProgressBar = require('progressbar.js')
const dayjs = require('dayjs')

export default {
  name: 'App',
  data: function () {
    return {
      connections: [],
      progress: null,
      error: null
    }
  },
  components: {
    searchform, snackbar
  },
  mixins: [update],
  mounted () {
    console.log(`
                            ╔═══╗                           
   ╔════════════════════════╩═══╩════════════════════════╗  
   ║            ████████    ██████   ██████              ║  
   ║               ██      ██        ██   ██             ║  
   ║               ██      ██        ██████              ║  
   ║               ██      ██        ██                  ║  
   ║               ██       ██████   ██                  ║  
   ╚════════════════════════╦═══╦════════════════════════╝  
                    \\''''───║   ╟───''''/                   
                     )__,--─║   ╟─--,__(                    
                            ║   ║                           
      Your friendly         ║   ║                           
TrAIn_Connection_Prediction ║   ║                           
          Service           ║   ║                           
^^^^^^^^^^^^^^^^^^^^^^^^^^^^╜   ╙^^^^^^^^^^^^^^^^^^^^^^^^^^^
      `)
    // Progressbar init
    this.progress = new ProgressBar.Line('#pgr_bar', {
      strokeWidth: 0.8,
      color: '#3f51b5',
      trailColor: 'transparent',
      trailWidth: 0
    })
  },
  methods: {
    display_fetch_error: function (response) {
      if (!response.ok) {
        this.stop_progress()
        this.error = Error(response.statusText)
        console.log(response.url)
        console.log(this.error)
      }
      return response
    },
    display_img_load_error: function (event) {
      this.stop_progress()
      this.error = Error('Failed to load image')
      console.log(event)
      console.log(this.error)
    },
    hard_reload () {
      window.location.reload()
    },
    start_progress () {
      // start progress animation
      this.progress.animate(600, { duration: 300000, easing: 'linear' })
    },
    stop_progress () {
      // stop animation
      this.progress.animate(0, { duration: 10, easing: 'linear' })
    },
    parse_datetimes: function (connections) {
      for (let i = 0; i < connections.length; i++) {
        connections[i].summary.dp_pt = dayjs(connections[i].summary.dp_pt)
        connections[i].summary.ar_pt = dayjs(connections[i].summary.ar_pt)

        connections[i].summary.dp_ct = dayjs(connections[i].summary.dp_ct)
        connections[i].summary.ar_ct = dayjs(connections[i].summary.ar_ct)

        for (let u = 0; u < connections[i].segments.length; u++) {
          connections[i].segments[u].dp_pt = dayjs(connections[i].segments[u].dp_pt)
          connections[i].segments[u].ar_pt = dayjs(connections[i].segments[u].ar_pt)

          connections[i].segments[u].dp_ct = dayjs(connections[i].segments[u].dp_ct)
          connections[i].segments[u].ar_ct = dayjs(connections[i].segments[u].ar_ct)
        }
      }
      return connections
    },
    get_connections: function (search_data) {
      // remove current connections
      this.$store.commit('set_connections', [])
      this.start_progress()

      fetch(window.location.protocol + '//' + window.location.host + '/api/trip', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(search_data)
      })
        .then((response) => this.display_fetch_error(response))
        .then((response) => response.json())
        .then((connections) => this.parse_datetimes(connections))
        .then((connections) => {
          this.$store.commit('set_connections', connections)
          this.stop_progress()
          if (this.$route.path !== '/') {
            this.$router.push('/')
          }
          document.getElementById('content').scrollIntoView({ behavior: 'smooth', block: 'center' })
        })
    }
  }
}
</script>

<style lang="scss">
@import 'src/assets/scss/variables';

@font-face {
  font-family: 'tcp_custom_font';
  src:
    url('fonts/tcp_custom_font.ttf?vdwbwf') format('truetype'),
    url('fonts/tcp_custom_font.woff?vdwbwf') format('woff'),
    url('fonts/tcp_custom_font.svg?vdwbwf#tcp_custom_font') format('svg');
  font-weight: normal;
  font-style: normal;
  font-display: block;
}

i {
  /* use !important to prevent issues with browser extensions that change fonts */
  font-family: 'tcp_custom_font' !important;
  speak: never;
  font-style: normal;
  font-weight: normal;
  font-variant: normal;
  text-transform: none;
  line-height: 1;

  /* Better Font Rendering =========== */
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
}

.tcp-calendar:before {
  content: "\e901";
}
.tcp-train:before {
  content: "\e902";
}
.tcp-pedestrian:before {
  content: "\e900";
}
.tcp-github:before {
  content: "\eab0";
}

// Custom font (https://icomoon.io/app/#/select)
// @font-face {
//   font-family: 'tcp_custom_font';
//   src: url('./fonts/tcp_custom_font.ttf?1p3u8g') format('truetype'),
//     url('./fonts/tcp_custom_font.woff?1p3u8g') format('woff'),
//     url('./fonts/tcp_custom_font.svg?1p3u8g#tcp_custom_font') format('svg');
//   font-weight: normal;
//   font-style: normal;
//   font-display: block;
// }

// i {
//   /* use !important to prevent issues with browser extensions that change fonts */
//   font-family: 'tcp_custom_font' !important;
//   speak: never;
//   font-style: normal;
//   font-weight: normal;
//   font-variant: normal;
//   text-transform: none;
//   line-height: 1;

//   /* Better Font Rendering =========== */
//   -webkit-font-smoothing: antialiased;
//   -moz-osx-font-smoothing: grayscale;
// }

// .tcp-train:before {
//   content: '\e92b';
// }
// .tcp-calendar:before {
//   content: '\e953';
// }
// .tcp-github:before {
//   content: '\eab0';
// }

h1,
h2,
h3,
h4,
h5 {
  color: $page_light_text;
  text-align: center;
}

.custom_card {
  margin-bottom: 5px;
  color: $page_light_text;
}

.footer_link {
  color: inherit;
  font-weight: inherit;
  // text-decoration: none;
}

.warning_link {
  color: $page_dark_text;
  font-weight: bold;
  // text-decoration: none;
  cursor: pointer;
}

.warning_link:hover {
  color: lighten($page_dark_text, 40%);
}

#intro {
  min-height: 800px;
}

#main {
  max-width: 1200px;
}

.main_background {
  width: 100%;
  background-color: rgba(0, 0, 0, 0.8);
  box-shadow: 0px 0px 17px 50px rgba(0, 0, 0, 0.8);
  display: flex;
  flex-direction: column;
  align-items: center;
}

.dark_background {
  background-color: $page_background;
  box-shadow: 0px 0px 17px 50px $page_background;
  display: flex;
  flex-direction: column;
  align-items: center;
}

html,
body,
#intro {
  height: 100%;
}

body {
  background-image: url(./assets/img/background.webp);
  background-position-x: center;
  background-position-y: 70px;
  background-repeat: no-repeat;
  background-size: contain;
  background-attachment: fixed;
  background-color: $page_background;
  overflow: auto;
  color: $page_light_text;
}

@media (max-width: 740px) {
  h2 {
    font-size: 5vw;
  }
}

.col {
  width: 40vw;
  min-width: 350px;
  // max-width: 75vw;
  margin: 10px;
}

/* NAVBAR BEGIN */
.navbar {
  background-color: $page_lighter_gray;
}

.top-nav-collapse {
  background-color: $page_gray;
}

.navbar .nav-item .dropdown-item {
  color: $page_light_text;
}
.navbar .nav-item .dropdown-item:hover,
.dropdown-item:focus,
.dropdown-item:active {
  background-color: transparent;
}
.navbar .dropdown-menu {
  display: block;
  background-color: transparent;
}
@media all and (min-width: 992px) {
  .navbar .nav-item .dropdown-menu {
    display: none;
  }
  .navbar .nav-item:hover .dropdown-menu {
    display: block;
  }
  .navbar .nav-item .dropdown-menu {
    margin-top: 0;
    background-color: $page_gray !important;
  }
  .navbar .nav-item .dropdown-item:hover,
  .dropdown-item:focus,
  .dropdown-item:active {
    background-color: $page_gray;
  }
}

.btn-outline-accent {
  border-color: $page_accent;
}

.btn-outline-accent:hover {
  background-color: $page_accent;
  border-color: $page_accent;
}
/* NAVBAR END */

#pgr_bar {
 position: relative;
 top: -5px;
}

/* FOOTER BEGIN */
footer {
  background-color: $page_gray;
  color: $page_gray_text;
}
/* FOOTER END */

.shadowheader {
  text-shadow: 1px 1px #000, 2px 2px #000;
}

.invalid {
  border-left: solid 3px $page_warning;
}

.pretty_form {
  padding: 40px;
  margin: auto;
  display: grid;
  row-gap: 20px;
  background-color: $page_gray;
}

.pretty_textbox {
  color: $page_light_text !important;
  border-radius: 0px;
  background-color: $page_lighter_gray;
  padding: 6px 12px;
  width: 100%;
  border-width: 0;
  line-height: 1.6 !important;
  transition: border-color ease-in-out 0.15s, box-shadow ease-in-out 0.15s, -webkit-box-shadow ease-in-out 0.15s;
}

.pretty_textbox:focus {
  outline: 1px solid $page_accent;
}

.pretty_button {
  background-color: $page_accent;
  color: $page_light_text !important;
  padding: 12px 34px;
  border: none;
  border-radius: 0px;
  line-height: 1.6 !important;
  text-decoration: none;
  text-align: center;
}

.pretty_button:hover {
  background-color: $page_accent_darker;
}

.pretty_button:active {
  position: relative;
  top: 2px;
}

.dropdown-item.active {
  background-color: $page_accent !important;
}

.flatpickr-calendar,
.flatpickr-calendar.arrowTop,
.flatpickr-months .flatpickr-month,
.flatpickr-current-month,
span.flatpickr-weekday,
.flatpickr-current-month .flatpickr-monthDropdown-months {
  background: $page_gray;
}

.flatpickr-day.selected,
.flatpickr-day.startRange,
.flatpickr-day.endRange,
.flatpickr-day.selected.inRange,
.flatpickr-day.startRange.inRange,
.flatpickr-day.endRange.inRange,
.flatpickr-day.selected:focus,
.flatpickr-day.startRange:focus,
.flatpickr-day.endRange:focus,
.flatpickr-day.selected:hover,
.flatpickr-day.startRange:hover,
.flatpickr-day.endRange:hover,
.flatpickr-day.selected.prevMonthDay,
.flatpickr-day.startRange.prevMonthDay,
.flatpickr-day.endRange.prevMonthDay,
.flatpickr-day.selected.nextMonthDay,
.flatpickr-day.startRange.nextMonthDay,
.flatpickr-day.endRange.nextMonthDay {
  background: $page_accent;
  border-color: $page_accent;
}

.error_box {
  color: $page_dark_text;
  background-color: $page_warning;
}

.card_header {
  height: max-content;
  display: grid;
  grid-template-columns: 1fr 1fr 1fr 1fr 1fr minmax(190px, 1fr);

  .col1,
  .col3,
  .col5,
  .col6 {
    background-color: $page_gray;
  }

  .col2,
  .col4 {
    background-color: $page_lighter_gray;
  }
}

.connections_header {
  @extend .card_header;
  border-left: 10px solid transparent;

  div {
    padding: 5px 20px;
  }
}

.lighter {
  background-color: $page_lighter_gray;
}

.card_header_item {
  display: inline-grid;
  flex-grow: 1;
  flex-basis: 1px;
  width: min-content;
  grid-auto-rows: min-content;
}

.outdated {
  color: $page_outdated_text;
}

.affiliate_link_container {
  display: grid;
  flex-grow: 1;
  flex-basis: 0;

  .pretty_button {
    padding: 12px 12px;
  }
}

.card_contents {
  border-top: 1px solid $page_background;
  overflow: auto;
}

.open-enter-active,
.open-leave-active {
  transition: all 0.5s;
  max-height: 99em;
  overflow: hidden;
}

.open-enter,
.open-leave-to {
  display: block;
  max-height: 0px;
  overflow: hidden;
}

@media (max-width: 1000px) {
  .m-5 {
    margin: 3rem 0.5rem 3rem 0.5rem !important;
  }

  .card_header {
    grid-template-columns: 1fr 1fr minmax(190px, 1fr);

    .col1,
    .col3,
    .col4,
    .col6 {
      background-color: $page_gray;
    }

    .col2,
    .col5 {
      background-color: $page_lighter_gray;
    }
  }
}

@media (max-width: 450px) {
  .card_header {
    grid-template-columns: 1fr minmax(190px, 1fr);

    .col1,
    .col3,
    .col5 {
      background-color: $page_lighter_gray;
    }

    .col2,
    .col4,
    .col6 {
      background-color: $page_gray;
    }
  }

  .pretty_form {
    padding: 20px 8px;
  }
}

@media (max-width: 350px) {
  .card_header {
    grid-template-columns: minmax(190px, 1fr);
  }
}

.text_content {
  max-width: 800px;
  width: 90%;
  margin: auto !important;
  margin-bottom: 80px !important;
  display: grid;
  row-gap: 20px;
}

.stats {
  height: 100%;
  margin: 20px;
}

.stats-picker {
  display: flex;
  justify-content: space-around;
  margin: auto;
  width: 100%;
  gap: 20px;
  align-items: center;
}

.stats-image {
  width: 100%;
  max-width: 50em;
  margin: auto;
  display: block;
}

@media (max-width: 1000px) {
  .stats-picker {
    flex-wrap: wrap;
  }
}

.vue-slider-process {
  background-color: $page_accent !important;
}

.vue-slider-dot-tooltip-inner {
  border-color: $page_accent !important;
  background-color: $page_accent !important;
}

.vue-slider {
  margin: 50px 0;
  width: 100% !important;
}

.light_text {
  color: $page_light_text !important;
}

.arrow {
  border: solid $page_light_text;
  border-width: 0 3px 3px 0;
  display: inline-block;
  padding: 3px;
}

.right {
  transform: rotate(-45deg);
  -webkit-transform: rotate(-45deg);
}

.left {
  transform: rotate(135deg);
  -webkit-transform: rotate(135deg);
}

.up {
  transform: rotate(-135deg);
  -webkit-transform: rotate(-135deg);
}

.down {
  transform: rotate(45deg);
  -webkit-transform: rotate(45deg);
}

.sort_col {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  cursor: pointer;
}

.sort_col:hover {
  outline: solid 1px $page_accent;
  z-index: 1;
}

.sort_col:active {
  position: relative;
  top: 2px;
}

.pretty_shadow {
  box-shadow: 0px 0px 20px $page_background;
}
</style>
