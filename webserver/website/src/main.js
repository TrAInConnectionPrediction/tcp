import Vue from 'vue'
import App from './App.vue'
import './registerServiceWorker'

new Vue({
  render: h => h(App)
}).$mount('#app')

// function con_error(msg) {
//   alert(
//     "No Connection to " +
//       base_url +
//       "! \nServer must be down. \nPlease try again later."
//   );
//   console.log(msg);
// }

// function error(msg) {
//   console.log(msg);
//   $("#results").html(
//     '\
//   <div class="alert alert-danger alert-dismissible fade show" role="alert">\
//     <strong>Holy guacamole!</strong> Looks like something went wrong: ' +
//       msg.status +
//       '<br>\
//     <small>For details see console<small>\
//     <button type="button" class="close" data-dismiss="alert" aria-label="Close">\
//       <span aria-hidden="true">&times;</span>\
//     </button>\
//   </div>'
//   );
//   //stop animation
//   bar.animate(0, { duration: 10, easing: "linear" });
//   showSection("results");
// }
