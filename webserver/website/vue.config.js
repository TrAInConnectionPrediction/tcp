module.exports = {
  runtimeCompiler: true,

  css: {
    loaderOptions: {
      sass: {
        additionalData: `
          @import 'src/assets/scss/variables';
          @import 'bootstrap/scss/_functions.scss'; @import 'bootstrap/scss/_variables.scss'; @import 'bootstrap/scss/_mixins.scss';
        `
      }
    }
  },

  pwa: {
    name: 'TrAIn_Connection_Prediction',
    themeColor: '#3f51b5',
    msTileColor: '#212121',
    manifestOptions: {
      background_color: '#212121',
      short_name: 'TCP'
    },
    assetsVersion: '0.2.8'
  }
}
