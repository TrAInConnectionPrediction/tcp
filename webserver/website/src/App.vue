<template>
  <body>
    <navbar></navbar>
    <div id="intro" class="container-md">
      <div class="d-flex justify-content-center align-items-center" style="height: 100%">
        <div class="hero-layout">
            <div class="slogan d-flex flex-column justify-content-center align-items-center">
            <h2>Nächstes Mal pünktlich</h2>
            <p>
              <strong>Dank TrAIn_Connection_Prediction</strong>
            </p>
          </div>
          <searchform class="search hover shadow"> </searchform>
        </div>
      </div>
    </div>
    <div class="container-fluid d-flex flex-column align-items-center py-5 main_background">
      <main id="main">
        <router-view id="content" />
      </main>
    </div>
    <footer class="text-center text-info p-3 shadow bg-dark">
      <div class="fw-bold">
        <router-link class="link-info" to="/imprint">Impressum</router-link> /
        <router-link class="link-info" to="/privacy">Datenschutz</router-link>
      </div>
      <br />
      <span
        >© 2022 TrAIn_Connection_Prediction ist ein unabhängiger Service. Dieser steht in keiner Verbindung mit der
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
    <snackbar v-if="error" :timeout="15000" :layout="'multiline'" class="text-dark" :style_class="'bg-danger'">
      <div>
        <div><b>Holy Guacamole!</b> {{ error.toString() }}</div>
        <div>
          Falls der Fehler weiterhin auftritt, verfassen Sie bitte einen Bugreport auf
          <a
            href="https://github.com/TrAInConnectionPrediction/tcp/issues"
            class="link-dark fw-bold"
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
import navbar from './components/navbar.vue'
import searchform from './components/searchform.vue'
import update from './assets/js/update.js'
import snackbar from './components/snackbar.vue'
const dayjs = require('dayjs')

export default {
  name: 'App',
  data: function () {
    return {
      connections: [],
      error: null
    }
  },
  components: {
    searchform, snackbar, navbar
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
  },
  methods: {
    display_fetch_error: function (response) {
      if (!response.ok) {
        this.$store.commit('stop_progress')
        this.error = Error(response.statusText)
        console.log(response.url)
        console.log(this.error)
      }
      return response
    },
    display_img_load_error: function (event) {
      this.$store.commit('stop_progress')
      this.error = Error('Failed to load image')
      console.log(event)
      console.log(this.error)
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
      this.$store.commit('start_progress')

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
          this.$store.commit('stop_progress')
          if (this.$route.path !== '/connections') {
            this.$router.push('/connections')
          }
          document.getElementById('content').scrollIntoView({ behavior: 'smooth', block: 'center' })
        })
    }
  }
}
</script>

<style lang="scss">
@import "~bootstrap/scss/bootstrap";
@import "src/assets/scss/font.scss";

#intro {
  min-height: 800px;
}

#main {
  max-width: 1200px;
}

.shadow-xxxl {
  box-shadow: 0 0 3em 2em $page_background;
}

.main_background {
  background-color: rgba(0, 0, 0, 0.8);
  box-shadow: 0 0 2em 4em rgba(0, 0, 0, 0.8);
}

html,
body,
#intro {
  height: 100%;
}

body {
  background-image: url(./assets/img/background_bold_blur.webp);
  background-position-x: center;
  background-position-y: 70px;
  background-repeat: no-repeat;
  background-size: contain;
  background-attachment: fixed;
  background-color: $page_background;
  overflow: auto;
  color: $text_color;
}

.dropdown-item.active {
  background-color: $page_accent !important;
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
  max-height: 70vh;
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
  color: $text_color !important;
}

.hero-layout {
  display: grid;
  grid-template-columns: 1fr;
  grid-template-rows: max-content max-content;
  grid-template-areas: "slogan"
                       "search";
  grid-gap: 20px;
  margin: 0 auto;
  width: 100%;
}

.slogan {
  grid-area: slogan;
}

.search {
  grid-area: search;
  padding: 3em 1em;
}

@include media-breakpoint-up(sm) {
  .search{
    padding: 3em;
  }
}

@include media-breakpoint-up(lg) {
  .hero-layout {
    grid-template-columns: 1fr 1fr;
    grid-template-rows: 1fr;
    grid-template-areas: "slogan search";
  }
}
</style>
