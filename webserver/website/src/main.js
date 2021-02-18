import Vue from "vue";
import mymain from "./mymain.vue";
import "./registerServiceWorker";
import flatpickr from "flatpickr";
// import { showSection } from "./assets/js/navigation.js";
// const bootstrap = require("bootstrap");

var $;
$ = require("jquery");

var ProgressBar = require("progressbar.js");

var bhfs = [];
var base_url = window.location.href.split("#")[0];
// var position_about = $("#about").offset().top - $("#navbar").innerHeight();
// var position_about = $("#about").offset().top - $("#navbar").innerHeight();

Vue.mixin({ delimiters: ["[[", "]]"] });

var App = new Vue({
  el: "#app",
  components: { mymain }
});

// var App_main = new Vue({
//   render: h => h(App)
// }).$mount("#app");

var position = $("#results").offset().top - 60;

//Sets labels over forms on top or on bottom
$("input[type=text]").each(function(i, element) {
  if (element.value.length > 0) {
    $(this)
      .siblings("label")
      .addClass("active");
  } else {
    $(this)
      .siblings("label")
      .removeClass("active");
  }
});

flatpickr("#datetime", {
  enableTime: true,
  time_24hr: true,
  dateFormat: "d.m.Y H:i",
  defaultDate: new Date().getTime()
});

// Progressbar init
var bar = new ProgressBar.Line("#pgr_bar", {
  strokeWidth: 4,
  easing: "easeInOut",
  duration: 1400,
  color: "#FFEA82",
  trailColor: "#eee",
  trailWidth: 1,
  svgStyle: { width: "100%", height: "100%" }
});

function con_error(msg) {
  alert(
    "No Connection to " +
      base_url +
      "! \nServer must be down. \nPlease try again later."
  );
  console.log(msg);
}

function error(msg) {
  console.log(msg);
  $("#results").html(
    '\
  <div class="alert alert-danger alert-dismissible fade show" role="alert">\
    <strong>Holy guacamole!</strong> Looks like something went wrong: ' +
      msg.status +
      '<br>\
    <small>For details see console<small>\
    <button type="button" class="close" data-dismiss="alert" aria-label="Close">\
      <span aria-hidden="true">&times;</span>\
    </button>\
  </div>'
  );
  //stop animation
  bar.animate(0, { duration: 10, easing: "linear" });
  showSection("results");
}

//called when form is submitted
$("#input").on("submit", function(event) {
  event.preventDefault();

  //First show and hide stuff
  document.querySelector("#datetime")._flatpickr.close();
  showSection("pgr_bar");
  window.location.hash = ""; //delete any # in the url
  //Clear old Data
  if (
    bhfs.includes($("#startbhf").val()) &&
    bhfs.includes($("#zielbhf").val())
  ) {
    App.$children[0].connections = [];
    var position = $("#pgr_bar").offset().top - window.innerHeight / 2;
    $("HTML, BODY").animate({ scrollTop: position }, 1000); //Scroll down
    //Start Progressbar for a long time (30s)
    bar.animate(60, { duration: 30000, easing: "linear" });
    $.ajax({
      type: "POST",
      cache: false,
      data: {
        startbhf: $("#startbhf").val(),
        zielbhf: $("#zielbhf").val(),
        date: $("#datetime").val()
      },
      url: base_url + "api/trip",
      dataType: "json",
      success: function(connections) {
        //stop animation
        bar.animate(0, { duration: 10, easing: "linear" });
        showSection("results");
        var position = $("#results").offset().top - 60;
        $("HTML, BODY").animate({ scrollTop: position }, 1000);
        console.log(connections);
        App.$children[0].set_connections(connections); //connections = connections;
      },
      error: function(jqXHR) {
        error(jqXHR);
      }
    });
    //return false so the page isn"t reloaded
    return false;
  }
  if (!bhfs.includes($("#startbhf").val()))
    $("#startbhf")
      .fadeOut(100)
      .fadeIn(100)
      .fadeOut(100)
      .fadeIn(100);
  if (!bhfs.includes($("#zielbhf").val()))
    $("#zielbhf")
      .fadeOut(100)
      .fadeIn(100)
      .fadeOut(100)
      .fadeIn(100);
  return false;
});

$(document).ready(function() {
  $.ajax({
    type: "POST",
    cache: false,
    data: {},
    url: base_url + "api/connect",
    dataType: "json",
    success: function(data) {
      bhfs = data.bhf;
      // $("#startbhf").autocomplete({
      //   lookup: bhfs
      // });
      // $("#zielbhf").autocomplete({
      //   lookup: bhfs
      // });
    },
    error: function(jqXHR) {
      con_error(jqXHR);
    }
  });

  // Mark allready filled text input fields as active
  $("input[type=text]").each(function(i, element) {
    if (element.value.length > 0) {
      $(this)
        .siblings("label")
        .addClass("active");
    } else {
      $(this)
        .siblings("label")
        .removeClass("active");
    }
  });

  //handle #"s
  $("#nav_buttons")
    .children()
    .each(function() {
      if (location.hash + "_button" == "#" + this.id) $(this).click();
    });
});

$(".nav_button").click(function() {
  var name = this.id.replace("_button", "");
  showSection(name);
  highlightButton(this.id);
  if ($("#" + name)[0].innerHTML == "") loadContent("#" + name, "wip.html");
  //loadContent("#" + name, name + ".html");

  $("body").css("overflow", "auto"); //make sure to enable global scrolling
  position =
    $("#" + name).offset().top +
    $("body").scrollTop() -
    $("#navbar").innerHeight();
  $("body").animate({ scrollTop: position }, 1000);
});

$("#home_button").click(function() {
  showSection("results");
  highlightButton("home");
  $("body").css("overflow", "auto");
  $("body").animate({ scrollTop: 0 }, 1000); //Scroll down
});

$("#brand_button").click(function() {
  $("#home_button").click();
});

function loadContent(target, url) {
  var selector = "#content";
  $.ajax({
    url: url,
    success: function(data) {
      $(target).html(
        $(data)
          .find(selector)
          .addBack(selector)
          .children()
      );
    }
  });
}

function showSection(id) {
  $("#main")
    .children()
    .each(function() {
      if (id == this.id) {
        $(this).removeClass("d-none");
      } else {
        $(this).addClass("d-none");
      }
    });
  // if(id == "about")
  //     $("footer").addClass("d-none");
  // else
  //     $("footer").removeClass("d-none");
}

function highlightButton(id) {
  $("#nav_buttons")
    .children()
    .each(function() {
      if (id == this.id + "_button") {
        $(this).addClass("active");
      } else {
        $(this).removeClass("active");
      }
    });
}
