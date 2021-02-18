<template>
  <body class="body" style="background-color: #000000;overflow: auto;">
    <nav
      class="navbar navbar-dark navbar-expand-lg fixed-top scrolling-navbar"
      id="navbar"
      style="opacity: 1;"
    >
      <div class="container-fluid">
        <a class="navbar-brand home_button" id="brand_button" href="#"
          >TrAIn_Connection_Prediction: TCP<br /></a
        ><button
          data-toggle="collapse"
          class="navbar-toggler"
          data-target="#navcol-1"
        >
          <span class="sr-only">Toggle navigation</span
          ><span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse text-center" id="navcol-1">
          <ul class="nav navbar-nav" id="nav_buttons">
            <li class="nav-item" id="home_button">
              <a class="nav-link active" href="#">Home</a>
            </li>
            <li class="nav-item nav_button" id="about_button">
              <a class="nav-link" href="#about">Über TCP</a>
            </li>
            <li class="nav-item nav_button" id="stats_button">
              <a class="nav-link" href="#stats">Statistiken</a>
            </li>
          </ul>
        </div>
      </div>
    </nav>
    <div id="intro" class="view shadow" style="">
      <div
        class="d-flex justify-content-center align-items-center mask"
        style="background-color: rgba(0, 0, 0, 0.1);"
      >
        <div class="row" style="justify-content: center;">
          <div class="col mb-4 white-text text-center">
            <h2 id="midheader" class="shadowheader">
              TrAIn_Connection_Prediction: TCP<br />
            </h2>
            <hr class="hr-light" />
            <p>
              <strong>Ihre&nbsp;Verbindungsvorhersage<br /></strong>
            </p>
          </div>
          <div class="col mb-4">
            <div class="card hover">
              <div class="card-body bg-dark">
                <form id="input" method="post">
                  <!-- Heading -->
                  <h3 class="white-text text-center">
                    <strong>Verbindung finden:</strong>
                  </h3>
                  <hr />
                  <!-- Start Bhf Form-->
                  <div class="md-form">
                    <i class="fas fa-train prefix grey-text"></i>
                    <label class="form-control-label" for="startbhf"
                      >Start Bahnhof</label
                    >
                    <input
                      class="form-control white-text typeahead"
                      id="startbhf"
                      name="startbhf"
                      type="text"
                      value=""
                    />
                  </div>
                  <!-- End Bhf Form -->
                  <div class="md-form">
                    <i class="fas fa-train prefix grey-text"></i>
                    <label class="form-control-label" for="zielbhf"
                      >Ziel Bahnhof</label
                    >
                    <input
                      class="form-control white-text typeahead"
                      id="zielbhf"
                      name="zielbhf"
                      type="text"
                      value=""
                    />
                  </div>
                  <!-- Date Form -->
                  <div class="md-form">
                    <i class="fas fa-calendar prefix grey-text"></i>
                    <label class="form-control-label" for="datetime"
                      >Datum</label
                    >
                    <input
                      class="form-control white-text"
                      type="text"
                      id="datetime"
                      name="datetime"
                    />
                  </div>
                  <!-- Submit Button -->
                  <div class="text-center">
                    <input
                      class="btn btn-indigo backshadow"
                      id=""
                      name="submit"
                      type="submit"
                      value="SUCHEN"
                    />
                  </div>
                </form>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    <main id="main" style="margin-top: 0px;max-width: 100%;width: 100%;">
      <section id="pgr_bar" class="m-5 d-none"></section>
      <section id="results" class="m-5 d-none">
        <connection
          v-for="connection in connections"
          :key="connection"
          v-bind:summary="connection.summary"
          v-bind:segments="connection.segments"
          v-bind:con_score="connection.summary.score"
        >
        </connection>
      </section>
      <section id="about" class="d-none"></section>
      <section id="stats" class="d-none"></section>
    </main>
    <footer
      class="text-center page-footer mt-4"
      style="margin-top: 0px!important;"
    >
      <hr style="margin-top: 0px;" />
      <div class="d-flex justify-content-center align-items-xl-center pb-4">
        <a
          href="https://www.meteoblue.com/"
          target="_blank"
          style="margin: 5px;"
          ><img
            src="https://www.meteoblue.com/favicon.ico"
            width="24"
            height="24"
        /></a>
        <div style="margin: 5px">Wetterdaten von Meteoblue</div>
        <a
          href="https://github.com/TrAInConnectionPrediction/tcp"
          target="_blank"
          style="margin: 5px;"
          ><i class="fab fa-github"></i
        ></a>
      </div>
      <div class="footer-copyright py-3">
        © 2020 Copyright: <a href="mailto:marius@kepi.de">_®<br /></a>
      </div>
    </footer>
  </body>
</template>

<script>
import connection from "./components/connection.vue";

export default {
  name: "mymain",
  data: function() {
    return { connections: [] };
  },
  components: {
    connection
  },
  methods: {
    set_connections: function(connections) {
      this.connections = connections;
    }
  }
};
</script>

<style>
#intro {
  background-image: url(./assets/img/ice.jpg);
  background-position: center;
  background-repeat: no-repeat;
  background-size: cover;
  background-attachment: fixed;
}
</style>
