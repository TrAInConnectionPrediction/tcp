<template>
  <div id="about" class="text_content">
    <h1 class="text-center">Statistiken</h1>
    <div>
      Wer wollte alles schon mal wissen wie pünktlich die Bahn gestern war? Oder welche Zugstrecken besonders
      zuverlässig sind? Wir sammeln rund um die Uhr die Verspätungsinformationen aus den Bahnverkehrsdaten, um diese in
      unsere Verbindungsvorhersage zu integrieren. Hier an dieser Stelle veröffentlichen wir auch unsere selbst
      erstellten Statistiken zur Pünktlichkeit im Bahnverkehr.
    </div>
    <div class="d-flex justify-content-center gap-5">
      <router-link class="btn btn-primary" :to="{ path: '/stats/overview', hash: '#content' }">Übersicht</router-link>
      <router-link class="btn btn-primary" :to="{ path: '/stats/stations', hash: '#content' }">Bahnhöfe</router-link>
    </div>
    <div>
      Vorsicht: Alle diese Daten sind eine Beobachtung von dritten und nicht von der Bahn selber. Wir können die
      Richtigkeit der Daten nicht garantieren. In den vielen von uns erstellten Auswertungen der Daten haben wir einige
      kleinere Fehler gefunden. Diese können wir nicht korrigieren, da wir nicht an bessere Daten herankommen. Wer
      belastbare Statistiken sucht, muss selber direkt bei der Bahn nachfragen.
    </div>
  </div>
</template>
