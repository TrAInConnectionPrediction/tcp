<template>
  <div class="fixed-top">
    <nav class="navbar navbar-expand-md navbar-dark bg-dark shadow">
      <div class="container-fluid">
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
            <router-link class="navbar-brand" to="#search"><img src="../assets/img/IC.svg" height="24"></router-link>
            <li class="nav-item">
              <router-link class="nav-link" :to="{ path: '/connections', hash: '#content' }">Verbindungen</router-link>
            </li>
            <li class="nav-item">
              <router-link class="nav-link" :to="{ path: '/about', hash: '#content' }">Über uns</router-link>
            </li>
            <li class="nav-item dropdown">
              <router-link
                class="nav-link dropdown-toggle"
                data-toggle="dropdown"
                :to="{ path: '/stats', hash: '#content' }"
                >Statistiken</router-link
              >
              <ul class="dropdown-menu dropdown-menu-dark bg-dark">
                <li>
                  <router-link class="nav-link" :to="{ path: '/stats/overview', hash: '#content' }"
                    >Übersicht</router-link
                  >
                </li>
                <li>
                  <router-link class="nav-link" :to="{ path: '/stats/stations', hash: '#content' }"
                    >Bahnhöfe</router-link
                  >
                </li>
              </ul>
            </li>
          </ul>
          <div class="d-flex gap-2">
            <install-button></install-button>
            <a
              class="btn btn-primary"
              href="https://github.com/TrAInConnectionPrediction/tcp"
              target="_blank"
              rel="noopener"
              ><i class="tcp-github"></i> Projekt auf GitHub</a
            >
          </div>
        </div>
      </div>
    </nav>
    <div id="pgr_bar"></div>
  </div>
</template>

<script>
import installButton from './installButton.vue'
import update from '../assets/js/update.js'
import { mapState } from 'vuex'
const ProgressBar = require('progressbar.js')

export default {
  name: 'navbar',
  components: { installButton },
  data: function () {
    return {
      progress: null
    }
  },
  computed: {
    ...mapState(['progressing'])
  },
  mounted: function () {
    if (window.location.hostname.indexOf('next.trainconnectionprediction.de') !== -1) {
      update.methods.clearCache()
      window.location.hostname = 'next.bahnvorhersage.de'
    }
    this.progress = new ProgressBar.Line('#pgr_bar', {
      strokeWidth: 0.8,
      color: '#3f51b5',
      trailColor: 'transparent',
      trailWidth: 0
    })
  },
  watch: {
    progressing: function (val) {
      if (val) {
        this.progress.animate(600, { duration: 300000, easing: 'linear' })
      } else {
        this.progress.animate(0, { duration: 10, easing: 'linear' })
      }
    }
  }
}
</script>

<style lang="scss">
.navbar .dropdown-menu {
  display: block;
}

.dropdown-menu .nav-link {
  padding-left: 1rem !important;
}

@include media-breakpoint-up(md) {
  .navbar .nav-item .dropdown-menu {
    display: none;
  }
  .navbar .nav-item:hover .dropdown-menu {
    display: block;
  }
  .navbar .nav-item .dropdown-menu {
    margin-top: 0;
  }
}

#pgr_bar {
  position: relative;
  top: -5px;
}
</style>
