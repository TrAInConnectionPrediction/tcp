import Vue from 'vue'
import VueRouter from 'vue-router'
import connectionDisplay from '../views/connectionDisplay'

Vue.use(VueRouter)

const routes = [
  {
    path: '/',
    name: 'Home',
    component: connectionDisplay
  },
  {
    path: '/about',
    name: 'Über TCP',
    component: () => import('../views/About.vue')
  },
  {
    path: '/impressum',
    name: 'Impressum',
    component: () => import('../views/Impressum.vue')
  },
  {
    path: '/stats',
    name: 'Statistiken',
    component: () => import('../views/stats/Overview.vue')
  },
  {
    path: '/stats/stations',
    name: 'Stations Statistiken',
    component: () => import('../views/stats/Stations.vue')
  }
]

const router = new VueRouter({
  mode: 'history',
  routes: routes,
  scrollBehavior (to, from, savedPosition) {
    if (to.hash) {
      Vue.nextTick(() => {
        document.getElementById(to.hash.substring(1)).scrollIntoView({ behavior: 'smooth' })
      })
      // Does not work but it's the vue way
      return { selector: to.hash }
    }
  }
})

export default router
