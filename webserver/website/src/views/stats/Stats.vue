<template>
  <div id="stats" class="overview">
    <table class="table table-dark table-hover">
      <tbody>
        <tr>
          <td scope="row" colspan="3">Generiert am {{ stats.time }}</td>
        </tr>
        <tr>
          <th scope="col">Statistik</th>
          <th scope="col">Ankunft</th>
          <th scope="col">Abfahrt</th>
        </tr>
        <tr>
          <th class="text-center" colspan="3">Daten seit Oktober 2020</th>
        </tr>
        <tr>
          <th scope="row">Anzahl an Halten</th>
          <td>{{ stats.all.num_ar_data }}</td>
          <td>{{ stats.all.num_dp_data }}</td>
        </tr>
        <tr>
          <th scope="row">Maximale Verspätung</th>
          <td>{{ stats.all.max_ar_delay }} min</td>
          <td>{{ stats.all.max_dp_delay }} min</td>
        </tr>
        <tr>
          <th scope="row">Durchschnittliche Verspätung</th>
          <td>{{ stats.all.avg_ar_delay }} min</td>
          <td>{{ stats.all.avg_dp_delay }} min</td>
        </tr>
        <tr>
          <th scope="row">Prozent an Verspätungen</th>
          <td>{{ stats.all.perc_ar_delay }}%</td>
          <td>{{ stats.all.perc_dp_delay }}%</td>
        </tr>
        <tr>
          <th scope="row">Prozent an Ausfällen</th>
          <td>{{ stats.all.perc_ar_cancel }}%</td>
          <td>{{ stats.all.perc_dp_cancel }}%</td>
        </tr>
        <tr>
          <th class="text-center" colspan="3">Daten der letzten 24h</th>
        </tr>
        <tr>
          <th scope="row">Anzahl an Halten</th>
          <td>{{ stats.new.num_ar_data }}</td>
          <td>{{ stats.new.num_dp_data }}</td>
        </tr>
        <tr>
          <th scope="row">Maximale Verspätung</th>
          <td>{{ stats.new.max_ar_delay }} min</td>
          <td>{{ stats.new.max_dp_delay }} min</td>
        </tr>
        <tr>
          <th scope="row">Durchschnittliche Verspätung</th>
          <td>{{ stats.new.avg_ar_delay }} min</td>
          <td>{{ stats.new.avg_dp_delay }} min</td>
        </tr>
        <tr>
          <th scope="row">Prozent an Verspätungen</th>
          <td>{{ stats.new.perc_ar_delay }}%</td>
          <td>{{ stats.new.perc_dp_delay }}%</td>
        </tr>
        <tr>
          <th scope="row">Prozent an Ausfällen</th>
          <td>{{ stats.new.perc_ar_cancel }}%</td>
          <td>{{ stats.new.perc_dp_cancel }}%</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script>
export default {
  data: function () {
    return {
      stats: {}
    }
  },
  created () {
    fetch(window.location.protocol + '//' + window.location.host + '/api/stats', {
      type: 'GET',
      data: null,
      dataType: 'json'
    })
      .then((response) => this.$parent.display_fetch_error(response))
      .then((response) => response.json())
      .then((response) => {
        this.stats = response
      })
  },
  methods: {}
}
</script>
