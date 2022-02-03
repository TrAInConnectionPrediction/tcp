<template>
    <button v-if="showInstallButton" @click="install" class="btn btn-primary">Als App installieren</button>
</template>

<script>
export default {
  name: 'installButton',
  data: function () {
    return {
      showInstallButton: false,
      deferredPrompt: null
    }
  },
  mounted: function () {
    window.addEventListener('beforeinstallprompt', (e) => {
      e.preventDefault()
      this.deferredPrompt = e
      this.showInstallButton = true
    })
  },
  methods: {
    install: function () {
      this.showInstallButton = false
      this.deferredPrompt.prompt()
    }
  }
}
</script>
