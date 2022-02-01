<template>
    <div class="fixed-top">
      <nav class="navbar navbar-expand-lg navbar-dark shadow">
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
              <li class="nav-item">
                <router-link class="nav-link" to="/">Verbindungssuche</router-link>
              </li>
              <li class="nav-item">
                <router-link class="nav-link" :to="{ path: '/about', hash: '#content' }">Über uns</router-link>
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
import { mapState } from 'vuex'
const ProgressBar = require('progressbar.js')

export default {
  name: 'navbar',
  data: function () {
    return {
      progress: null
    }
  },
  computed: {
    ...mapState(['progressing'])
  },
  mounted: function () {
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
.navbar {
  background-color: $page_lighter_gray;
}

.top-nav-collapse {
  background-color: $page_gray;
}

.navbar .nav-item .dropdown-item {
  color: $text_color;
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

@include media-breakpoint-up(md) {
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

#pgr_bar {
 position: relative;
 top: -5px;
}
</style>
