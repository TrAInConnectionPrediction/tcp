<template>
  <body class="body" style="background-color: #000000; overflow: auto">
    <nav
      class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top"
      style="
        background-color: rgba(0, 0, 0, 0.5) !important;
        backdrop-filter: blur(5px);
      "
    >
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
              <router-link class="nav-link" to="/">Home</router-link>
            </li>
            <li class="nav-item">
              <router-link
                class="nav-link"
                :to="{ path: '/about', hash: '#about' }"
                >Über TCP</router-link
              >
            </li>
            <li class="nav-item">
              <router-link
                class="nav-link"
                :to="{ path: '/stats', hash: '#stats' }"
                >Statistiken</router-link
              >
            </li>
          </ul>
          <div class="d-flex">
            <a
              class="btn btn-outline-success"
              href="https://github.com/TrAInConnectionPrediction/tcp"
              target="_blank"
              ><i class="tcp-github"></i>TCP auf GitHub</a
            >
          </div>
        </div>
      </div>
    </nav>

    <div id="intro" class="view shadow" style="">
      <div
        class="d-flex justify-content-center align-items-center mask"
        style="height: 100%"
      >
        <div
          class="row"
          style="justify-content: center; min-width: 0; width: 100%"
        >
          <div class="col white-text text-center">
            <h2 id="midheader" class="shadowheader">
              TrAIn_Connection_Prediction: TCP<br />
            </h2>
            <hr class="hr-light" />
            <p>
              <strong> Ihr Verbindungsvorhersage </strong>
            </p>
          </div>
          <div class="col">
            <div class="card hover bg-dark">
              <div class="card-body bg-dark">
                <searchform> </searchform>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    <main id="main" style="margin-top: 0px; max-width: 100%; width: 100%">
      <div id="prg_bar_anchor"></div>
      <section id="pgr_bar" class="m-5" v-show="show_progress"></section>
      <div
        class="m-5 custom_card"
        id="error_box"
        style="background-color: rgb(255, 69, 69)"
      >
        <div v-if="error" @click="error = null" class="card_header">
          <b>Holy Guacamole</b>! Something went wrong: {{ error.toString() }}
        </div>
      </div>
      <router-view class="m-5" />
    </main>
    <footer
      class="text-center page-footer mt-4"
    >
      <hr style="margin-top: 0px" />
      <div class="d-flex justify-content-center align-items-xl-center pb-4">
        <a href="https://www.meteoblue.com/" target="_blank" style="margin: 5px"
          ><img
            src="https://www.meteoblue.com/favicon.ico"
            width="24"
            height="24"
        /></a>
        <div style="margin: 5px">Wetterdaten von Meteoblue</div>
        <a
          href="https://github.com/TrAInConnectionPrediction/tcp"
          target="_blank"
          style="margin: 5px; color: inherit; text-decoration: none"
          ><i class="tcp-github"></i
        ></a>
      </div>
      <div class="footer-copyright py-3">
        <a class="pretty_link" href="http://sfz-bw.de/eningen/">Impressum</a><br>
        © 2021
        <a class="pretty_link" href="mailto:marius@kepi.de">TrAIn_Connection_Prediction<br /></a>
        <span style="color: gray"
          >TrAIn_Connection_Prediction ist ein unabhängiger Service. Dieser
          steht in keiner Verbindung mit der Deutschen Bahn und ihren
          Tochter-Unternehmen.
        </span>
      </div>
    </footer>
  </body>
</template>

<script>
// import connectionDisplay from './components/connectionDisplay.vue'
import searchform from './components/searchform.vue'
const ProgressBar = require('progressbar.js')

export default {
  name: 'App',
  data: function () {
    return {
      show_progress: false,
      connections: [],
      progress: null,
      error: null
    }
  },
  components: {
    searchform
  },
  mounted () {
    // Progressbar init
    this.progress = new ProgressBar.Line('#pgr_bar', {
      strokeWidth: 4,
      easing: 'easeInOut',
      duration: 1400,
      color: '#FFEA82',
      trailColor: '#eee',
      trailWidth: 1,
      svgStyle: { width: '100%', height: '100%' }
    })
  },
  methods: {
    display_fetch_error: function (response) {
      if (!response.ok) {
        this.error = Error(response.statusText)
        console.log(response.url)
        console.log(this.error)
        document
          .getElementById('error_box')
          .scrollIntoView({ behavior: 'smooth' })
      }
      return response
    },

    get_connections: function (search_data) {
      // remove current connections
      this.$store.commit('set_connections', [])
      // start progress animation
      this.show_progress = true
      this.progress.animate(60, { duration: 30000, easing: 'linear' })
      document
        .getElementById('prg_bar_anchor')
        .scrollIntoView({ behavior: 'smooth' })

      fetch(
        window.location.protocol + '//' + window.location.host + '/api/trip',
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(search_data)
        }
      )
        .then((response) => this.display_fetch_error(response))
        .then((response) => response.json())
        .then((connections) => {
          // stop animation
          this.progress.animate(0, { duration: 10, easing: 'linear' })
          this.$store.commit('set_connections', connections)
          this.show_progress = false
          this.$router.push('/')
        })
    }
  }
}
</script>

<style>
/* Font auf https://icomoon.io/app/#/select generiert */
@font-face {
  font-family: 'tcp_custom_font';
  src: url('./fonts/tcp_custom_font.ttf?1p3u8g') format('truetype'),
    url('./fonts/tcp_custom_font.woff?1p3u8g') format('woff'),
    url('./fonts/tcp_custom_font.svg?1p3u8g#tcp_custom_font') format('svg');
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

.tcp-train:before {
  content: '\e92b';
}
.tcp-calendar:before {
  content: '\e953';
}
.tcp-github:before {
  content: '\eab0';
}

.custom_card {
  margin-bottom: 5px;
}

.card_header {
  padding: 20px;
  min-height: 60px;
  height: max-content;
  display: flex;
  flex-wrap: wrap;
  background-color: rgb(0, 0, 0, 0.03);
}

.pretty_link {
  color: gray;
}

#intro {
  background-image: url(./assets/img/ice.jpg);
  background-position: center;
  background-repeat: no-repeat;
  background-size: cover;
  background-attachment: fixed;
  min-height: 800px;
}

/* Required height of parents */

html,
body,
header,
.view {
  height: 100%;
}

/* Desing for mobile pages */

/* @media (max-width: 740px) {
  .full-page-intro {
    height: 1000px;
  }
} */

@media (max-width: 740px) {
  h2 {
    font-size: 5vw;
  }
}

/* Navbar animation */

.navbar {
  background-color: rgba(0, 0, 0, 0.3);
}

.top-nav-collapse {
  background-color: #202020;
}

/* Adding color to the Navbar on mobile */

@media only screen and (max-width: 768px) {
  .navbar {
    background-color: #202020;
  }
}

footer {
  background-color: #212529;
}

footer > div {
  color: white;
}

#pgr_bar {
  margin: 5px;
  height: 8px;
  margin-top: 20px;
  margin-bottom: 20px;
}

.autocomplete-suggestions {
  background: #212529;
  overflow: auto;
  color: #fff;
  box-shadow: 0px 0px 10px 4px black !important;
}

.autocomplete-suggestion {
  padding: 2px 5px;
  white-space: nowrap;
  overflow: hidden;
}

.autocomplete-selected {
  background: #000;
}

.autocomplete-suggestions strong {
  font-weight: normal;
  color: #3399ff;
}

.autocomplete-group {
  padding: 2px 5px;
}

.autocomplete-group strong {
  display: block;
  border-bottom: 1px solid #fff;
}

.shadow {
  -webkit-box-shadow: 0 -140px 70px -70px black inset !important;
  box-shadow: 0 -140px 70px -70px black inset !important;
}

.shadow .card {
  -webkit-box-shadow: 10px 10px 50px 5px black;
  box-shadow: 10px 10px 50px 5px black;
}

.hover:hover {
  position: relative;
  box-shadow: 10px 10px 50px 5px black, 11px 11px 50px 5px black;
}

.shadowheader2:hover {
  position: relative;
  top: -3px;
  left: -3px;
  text-shadow: 0px 1px var(--shadow-bg-color1), 2px 2px var(--shadow-bg-color1),
    3px 3px var(--shadow-bg-color1), 4px 4px var(--shadow-bg-color1),
    5px 5px var(--shadow-bg-color1), 6px 6px var(--shadow-bg-color1),
    7px 7px var(--shadow-bg-color1), 8px 8px var(--shadow-bg-color1) !important;
}

.backshadow:hover {
  box-shadow: 1px 1px #2b387c, 2px 2px #2b387c, 3px 3px #2b387c, 4px 4px #2b387c,
    5px 5px #2b387c, 6px 6px #2b387c;
}

.shadowheader {
  color: white;
  text-shadow: 1px 1px #000, 2px 2px #000;
}

:root {
  --shadow-bg-color1: #125163;
}

@media (max-width: 400px) {
  #brand_button {
    font-size: 4.6vw;
    margin: 0;
  }
}

@media (max-width: 300px) {
  #brand_button {
    font-size: 4vw;
    margin: 0;
  }
}

@media (max-width: 300px) {
  .navbar-dark .navbar-toggler {
    font-size: 4vw;
  }
}

@media (max-width: 400px) {
  .navbar-dark .navbar-toggler {
    font-size: 5vw;
  }
}

@media (min-width: 600px) {
  .navbar-expand-lg {
    -ms-flex-flow: row nowrap;
    flex-flow: row nowrap;
    -ms-flex-pack: start;
    justify-content: flex-start;
  }
}

@media (min-width: 600px) {
  .navbar-expand-lg .navbar-nav {
    -ms-flex-direction: row;
    flex-direction: row;
  }
}

@media (min-width: 600px) {
  .navbar-expand-lg .navbar-nav .dropdown-menu {
    position: absolute;
  }
}

@media (min-width: 600px) {
  .navbar-expand-lg .navbar-nav .nav-link {
    padding-right: 0.5rem;
    padding-left: 0.5rem;
  }
}

@media (min-width: 600px) {
  .navbar-expand-lg > .container,
  .navbar-expand-lg > .container-fluid,
  .navbar-expand-lg > .container-lg,
  .navbar-expand-lg > .container-md,
  .navbar-expand-lg > .container-sm,
  .navbar-expand-lg > .container-xl {
    -ms-flex-wrap: nowrap;
    flex-wrap: nowrap;
  }
}

@media (min-width: 600px) {
  .navbar-expand-lg .navbar-collapse {
    display: -ms-flexbox !important;
    display: flex !important;
    -ms-flex-preferred-size: auto;
    flex-basis: auto;
  }
}

@media (min-width: 600px) {
  .navbar-expand-lg .navbar-toggler {
    display: none;
  }
}

.col {
  width: 40vw;
  min-width: 350px;
  max-width: 75vw;
  margin: 30px;
}

#midheader {
  font-weight: bold;
  font-size: calc(12px + 1.5vw);
  white-space: nowrap;
}
</style>
