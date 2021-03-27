import Vue from 'vue'
import Vuex from 'vuex'

Vue.use(Vuex)

export default new Vuex.Store({
  state: {
    stations: [],
    connections: []
  },
  mutations: {
    set_stations (state, stations) {
      state.stations = stations
    },
    set_connections (state, connections) {
      state.connections = connections
    }
  },
  actions: {
  },
  modules: {
  }
})
