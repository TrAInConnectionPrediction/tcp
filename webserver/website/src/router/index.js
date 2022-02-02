import Vue from 'vue'
import VueRouter from 'vue-router'
import connectionDisplay from '../views/connectionDisplay'

Vue.use(VueRouter)

const routes = [
  {
    path: '/connections',
    name: 'Verbindungen',
    component: connectionDisplay
  },
  {
    path: '/about',
    name: 'Über TCP',
    component: () => import('../views/About.vue')
  },
  {
    path: '/imprint',
    name: 'Impressum',
    component: () => import('../views/imprint.vue')
  },
  {
    path: '/privacy',
    name: 'Datenschutz',
    component: () => import('../views/privacy.vue')
  },
  {
    path: '/stats',
    name: 'Statistiken',
    component: () => import('../views/stats/Stats.vue')
  },
  {
    path: '/stats/overview',
    name: 'Statistiken',
    component: () => import('../views/stats/Overview.vue')
  },
  {
    path: '/stats/stations',
    name: 'Bahnhofs-Statistiken',
    component: () => import('../views/stats/Stations.vue')
  }
  // {
  //   path: '/data/obstacles',
  //   name: 'Zug-Behinderungen',
  //   component: () => import('../views/stats/Obstacles.vue')
  // }
]

const router = new VueRouter({
  mode: 'history',
  linkActiveClass: 'active',
  linkExactActiveClass: 'exact-active',
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
