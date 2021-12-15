<template>
  <div id="stats" class="overview">
    <table v-if="show" class="table table-dark table-hover">
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
          <td>{{ stats.all_num_ar_data.toLocaleString('de-DE') }}</td>
          <td>{{ stats.all_num_dp_data.toLocaleString('de-DE') }}</td>
        </tr>
        <tr>
          <th scope="row">Maximale Verspätung</th>
          <td>{{ stats.all_max_ar_delay.toLocaleString('de-DE') }} min</td>
          <td>{{ stats.all_max_dp_delay.toLocaleString('de-DE') }} min</td>
        </tr>
        <tr>
          <th scope="row">Durchschnittliche Verspätung</th>
          <td>{{ stats.all_avg_ar_delay.toLocaleString('de-DE') }} min</td>
          <td>{{ stats.all_avg_dp_delay.toLocaleString('de-DE') }} min</td>
        </tr>
        <tr>
          <th scope="row">Prozent an Verspätungen</th>
          <td>{{ stats.all_perc_ar_delay.toLocaleString('de-DE') }}%</td>
          <td>{{ stats.all_perc_dp_delay.toLocaleString('de-DE') }}%</td>
        </tr>
        <tr>
          <th scope="row">Prozent an Ausfällen</th>
          <td>{{ stats.all_perc_ar_cancel.toLocaleString('de-DE') }}%</td>
          <td>{{ stats.all_perc_dp_cancel.toLocaleString('de-DE') }}%</td>
        </tr>
        <tr>
          <th class="text-center" colspan="3">Daten der letzten 24h</th>
        </tr>
        <tr>
          <th scope="row">Anzahl an Halten</th>
          <td>{{ stats.new_num_ar_data.toLocaleString('de-DE') }}</td>
          <td>{{ stats.new_num_dp_data.toLocaleString('de-DE') }}</td>
        </tr>
        <tr>
          <th scope="row">Maximale Verspätung</th>
          <td>{{ stats.new_max_ar_delay.toLocaleString('de-DE') }} min</td>
          <td>{{ stats.new_max_dp_delay.toLocaleString('de-DE') }} min</td>
        </tr>
        <tr>
          <th scope="row">Durchschnittliche Verspätung</th>
          <td>{{ stats.new_avg_ar_delay.toLocaleString('de-DE') }} min</td>
          <td>{{ stats.new_avg_dp_delay.toLocaleString('de-DE') }} min</td>
        </tr>
        <tr>
          <th scope="row">Prozent an Verspätungen</th>
          <td>{{ stats.new_perc_ar_delay.toLocaleString('de-DE') }}%</td>
          <td>{{ stats.new_perc_dp_delay.toLocaleString('de-DE') }}%</td>
        </tr>
        <tr>
          <th scope="row">Prozent an Ausfällen</th>
          <td>{{ stats.new_perc_ar_cancel.toLocaleString('de-DE') }}%</td>
          <td>{{ stats.new_perc_dp_cancel.toLocaleString('de-DE') }}%</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script>
export default {
  data: function () {
    return {
      stats: {},
      show: false
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
        this.show = true
      })
  },
  methods: {}
}
</script>
