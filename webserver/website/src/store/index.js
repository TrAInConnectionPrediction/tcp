import Vue from 'vue'
import Vuex from 'vuex'

Vue.use(Vuex)

export default new Vuex.Store({
  state: {
    stations: [],
    connections: [],
    progressing: false
  },
  mutations: {
    set_stations (state, stations) {
      state.stations = stations
    },
    set_connections (state, connections) {
      state.connections = connections
    },
    start_progress (state) {
      state.progressing = true
    },
    stop_progress (state) {
      state.progressing = false
    }
  },
  actions: {
  },
  modules: {
  }
})
