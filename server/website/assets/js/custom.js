//Datetimepicker
$('#datetimepicker').datetimepicker({
  icons: {
    time: 'fas fa-clock',
    date: 'fas fa-calendar',
  }, format: 'DD.MM.YYYY HH:mm',
  widgetPositioning : {
    vertical: 'bottom'
  }
});

//We want to show the datetimepicker when in selected
$('#datetimepicker').select(function(){
  console.log($('#datetimepicker').is(":focus"));
  console.log("I select");
  $('#datetimepicker').datetimepicker("show");
});
  
//Sets labels over forms on top or on bottom
$('input[type=text]').each(function (i, element) {
  if (element.value.length > 0) {
    $(this).siblings('label').addClass('active');
  }
  else {
    $(this).siblings('label').removeClass('active');
  }
});

//Progressbar init
var bar = new ProgressBar.Line(pgr_bar, {
  strokeWidth: 4,
  easing: 'easeInOut',
  duration: 1400,
  color: '#FFEA82',
  trailColor: '#eee',
  trailWidth: 1,
  svgStyle: {width: '100%', height: '100%'}
});

//init socketio
// var socket = io(); 'http://dkm.homeip.net:4000'
var theadid = 0;
var bhfs = [];
var base_url = location.protocol + "//" +  location.hostname + ":5000/";
var base_url = "http://kepiserver.de:5000/";

//Fixed it?
function timeFromUnix(time){
  //https://stackoverflow.com/questions/847185/convert-a-unix-timestamp-to-time-in-javascript
  // Create a new JavaScript Date object based on the timestamp
  // multiplied by 1000 so that the argument is in milliseconds, not seconds.
  // the above is wrong since we already habe milliseconds
  var date = new Date(time);
  // Hours part from the timestamp
  var hours = date.getHours();
  // Minutes part from the timestamp
  var minutes = "0" + date.getMinutes();

  // Will display time in 10:30:23 format
  return hours + ':' + minutes.substr(-2);
}
function display_connection(index, data){
  $('#erg').append(
    '<div id="card'+ index +'" class="card custom-card" style="margin-bottom: 5px">\
        <h5 class="card-header" role="tab" id="heading' + index + '">\
          <a class="collapsed d-block text-white rounded-0" data-toggle="collapse" data-parent="#erg" href="#collapse'+ index +'" aria-expanded="false" aria-controls="collapse'+ index +'">\
            <table class="table table-sm" id="table' + index + '" style="color: inherit;">\
              <thead>\
                <tr>\
                  <th scope="col" style="width: 25%;">Bahnhof</th>\
                  <th scope="col" style="width: 10%;">Zeit</th>\
                  <th scope="col" style="width: 15%;">Dauer</th>\
                  <th scope="col" style="width: 20%;">Umsteigen</th>\
                  <th scope="col">Produkte</th>\
                  <th scope="col">Verbindungs-Score</th>\
                </tr>\
              </thead>\
              <tbody>\
                <tr>\
                  <td id="bhf" scope="col">'+ $("#startbhf").val() + '<br>' + $("#zielbhf").val() + '</td>\
                  <td id="time" scope="col">'+ timeFromUnix(data[0].departure.scheduledTime) + '<br>' + timeFromUnix(data.slice(-2)[0].arrival.scheduledTime)+'</td>\
                  <td id="totaltime" scope="col"><b>' + data.slice(-1)[0].totaltime  + '</b></td>\
                  <td id="trans" scope="col"><b>' + data.slice(-1)[0].transfers + '</b></td>\
                  <td id="prod" scope="col">' + '</td>\
                  <td id="prob" scope="col"><b>' + data.slice(-1)[0].total_score + '% '+ '</b> </td>\
                </tr>\
                </tbody>\
            </table>\
          </a>\
        </h5>\
        <div id="collapse'+ index +'" class="collapse" role="tabpanel" aria-labelledby="heading' + index + '">\
            <div class="card-body" id="details'+ index +'">\
            </div>\
        </div>\
    </div>');
  // make the trains beautiful
  $.each(data.slice(-1)[0].segmentTypes, function(_i,value){
    // console.log(data.slice(-1)[0].segmentTypes);
    $("#table" + index + " tbody td#prod").append(value + " ");
  });
  //header
  str = '<ul class="experiences rounded-0">\
        <table class="details_table">\
        <tr>\
          <th>Bahnhof</th>\
          <th>Uhrzeit</th>\
          <th>Gleis</th>\
        </tr>';
  //go through every segment of the connection
  for (i = 0; i < data.length-1; i++) {
    //if the segment is a Fußweg, do this
    if (data[i].train.name == "Fußweg"){
      var walking_time = Math.abs(data[i].departure.scheduledTime - data[i].arrival.scheduledTime) / 60000;
      console.log("data[i]:");
      console.log(data[i]);
      //If this is not befor the first or after the last Station
      if (i > 0 && i < data.length-2)
      {
        str = str.concat(
          '<tr>\
            <td colspan="2" class="transfer"> <img src="' + base_url + 'static/fußgänger.svg" height=20px>\
            davon '+ walking_time +' Min. Fußweg</td>\
          </tr>'
        );
      }
      else//If it's befor the first or after the last Station
      {
        str = str.concat(
          '<tr>\
            <td colspan="2" class="transfer"> <img src="' + base_url + 'static/fußgänger.svg" height=20px>\
            Fußweg '+ walking_time +' Min.</td>\
          </tr>'
        );
      }
    }
    //if the segment is a train
    else
    {
      //determin the color of the station depending on the 5 min delay propability
      var ddelay5 = data[i].ddelay5;
      var adelay5 = data[i].adelay5;
      abcolor = (ddelay5 < 0.08) ? 'green' : (ddelay5 < 0.25) ? 'orange' : 'red';
      ancolor = (adelay5 < 0.08) ? 'green' : (adelay5 < 0.25) ? 'orange' : 'red';
      
      //one segment
      str = str.concat(
        '<tr>\
          <td> <div class="bhf '+ abcolor +'">'+ data[i].segmentStart.title +'</div></td>\
          <td>ab ' + timeFromUnix(data[i].departure.scheduledTime) + '</td>\
          <td> von Gl.'+ data[i].departure.platform + '</td>\
        </tr>');
      if (data[i].train.d_type != 'unknown')
      {
        str +=
        '<tr>\
          <td colspan="3"><div class="train"> <img src="' + base_url + 'assets/img/'+ data[i].train.d_type +'.svg" height=20px>\
          </img>'+ data[i].train.name  + '</div></td>\
        </tr>';
      }
      else
      {
        str +=
          '<tr>\
            <td colspan="3"><div class="train">'+ data[i].train.name  + '</div></td>\
          </tr>'
      }

        str = str.concat(
        '<tr>\
          <td> <div class="bhf '+ ancolor +'">'+ data[i].segmentDestination.title  + '</div></td>\
          <td>an ' + timeFromUnix(data[i].arrival.scheduledTime) + '</td>\
          <td> an Gl.'+ data[i].arrival.platform + '</td>\
        </tr>'
      );
      //add the transfertime
      //do not run this after the last station
      if(i < data.length-2)
      {
        //check if next segment is a Fußweg and check if there is another segment afterwards
        if(data[i+1].train.name == "Fußweg" && i != data.length-3)
        {
          //calculate transfer time
          var tf_time = Math.abs(data[i+2].departure.scheduledTime - data[i].arrival.scheduledTime) / 60000
          str += '<tr>\
            <td colspan="1" class="transfer">Umsteigezeit: '+ tf_time +' Min. </td>\
            <td colspan="2" class="transfer">Verbindungs-Score: '+ Math.round(data[i].con_score * 100) +'%</td>\
          </tr>';
        }
        else
        {
          //calculat transfer time
          var tf_time = Math.abs(data[i+1].departure.scheduledTime - data[i].arrival.scheduledTime) / 60000
          str += '<tr>\
            <td colspan="1" class="transfer">Umsteigezeit: '+ tf_time +' Min. </td>\
            <td colspan="2" class="transfer">Verbindungs-Score: '+ Math.round(data[i].con_score * 100) +'%</td>\
          </tr>';
        }
      }
    }
  }
  //add foot
  str = str.concat(
      '</table>\
      </ul>');

  //determin the color of the connection depending on the con score
  $('#details' + index).html(str);
  if((data.slice(-1)[0].total_score) > 80){
    $("#card"+ index ).attr('class', 'card custom-card bg-success rounded-0');}
  else if((data.slice(-1)[0].total_score)  > 50){
    $("#card"+ index ).attr('class', 'card custom-card bg-warning rounded-0');}
  else if((data.slice(-1)[0].total_score) <= 50){
    $("#card"+ index ).attr('class', 'card custom-card bg-danger rounded-0');}
}

function connection(data) {
  console.log("Returned Data:");
  console.log(data);
  for (var i = 0; i < data.length; i++) {
    display_connection(i, data[i]);
  }
  //stop animation
  bar.animate(0, {duration: 10, easing: 'linear'})
  $("#pgr_bar").addClass("d-none");
  var position = $("#erg").offset().top - 60; 
  $("HTML, BODY").animate({ scrollTop: position }, 1000); //Scroll down
}

function error(msg) {
  console.log(msg);
  $('#erg').html('\
  <div class="alert alert-danger alert-dismissible fade show" role="alert">\
    <strong>Holy guacamole!</strong> Looks like something went wrong: ' + msg.status + '<br>\
    <small>For details see console<small>\
    <button type="button" class="close" data-dismiss="alert" aria-label="Close">\
      <span aria-hidden="true">&times;</span>\
    </button>\
  </div>');
  //stop animation
  bar.animate(0, {duration: 10, easing: 'linear'})
  $("#pgr_bar").addClass("d-none");
}

//called when form is submitted
$('#input').submit(function () {
  //First show and hide stuff
  $('#datetimepicker').datetimepicker('hide');
  $("#main").removeClass("d-none");
  $("#pgr_bar").removeClass("d-none");
  $("#about").addClass("d-none");
  window.location.hash = ""
  //Clear old Data
  $('#erg').html('');
  if (bhfs.includes($("#startbhf").val()) && bhfs.includes($("#zielbhf").val())){
    var position = $("#pgr_bar").offset().top - ( window.innerHeight / 2 ); 
    $("HTML, BODY").animate({ scrollTop: position }, 1000); //Scroll down
    //Start Progressbar for a long time (10s)
    bar.animate(20, {duration: 10000, easing: 'linear'})
    $.ajax({
      type: "POST",
      cache: false,
      data:{"startbhf": $("#startbhf").val(), "zielbhf": $("#zielbhf").val(), "date": $("#datetimepicker").val()},
      url:base_url + "api",
      dataType: "json",
      success: function(data) { connection(data)},
      error: function(jqXHR) { error(jqXHR) }
    })
    //return false so the page isn't reloaded
    return false;
  }
  if (!bhfs.includes($("#startbhf").val()))
    $("#startbhf").fadeOut(100).fadeIn(100).fadeOut(100).fadeIn(100);
  if (!bhfs.includes($("#zielbhf").val()))
    $("#zielbhf").fadeOut(100).fadeIn(100).fadeOut(100).fadeIn(100);
  return false;
});

$(document).ready(function() {   
  var search_word ="hi";
  // because we sometimes host on differen urls and our calc server is always on port 5000 (main server is on kepiserver.de:5000)
  // protocol + url + port + adress
  var qurl = base_url + "connect";
  $.ajax({
           type: "POST",
           cache: false,
           data:{keyword:search_word},
           url: qurl,
           dataType: "json",
           success: function(data) { 
             bhfs = data.bhf;
             console.log(data);  
             $('#startbhf').autocomplete({
               lookup: bhfs,
             });
             $('#zielbhf').autocomplete({
               lookup: bhfs,
             });                
           },
           error: function(jqXHR) {
               alert("error: " + jqXHR.status);
               console.log(jqXHR);            }
       })
});

$("#about_button").click(function(){
  $("#main").removeClass("d-none");
  $("#pgr_bar").addClass("d-none");
  $("#about").removeClass("d-none");
  var position = $("#about").offset().top - ( window.innerHeight / 2 ); 
    $("HTML, BODY").animate({ scrollTop: position }, 1000); //Scroll down
});

// Animations initialization
new WOW().init();
