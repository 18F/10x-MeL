'use strict';


async function fetchCountsData(parameters, dataViewId, chart) {
  const url = buildRequest(services.countUnique, {
    'column': parameters.column,
    'data_view_id': dataViewId,
  });

  try {
    const response = await fetch(url);
    if (response.status !== HTTP_OK) {
      console.error('error processing request, status: ' + response.status);
      return;
    }

    let doSort = false;

    const result = await response.json();
    if (!result.error) {
      const counts = result.data;

      const x = [];
      const y = [];
      for (const key in counts) {
        let displayKey = key.trim().toLowerCase();
        console.info(displayKey, displayKey === 'very poor');
        if (displayKey === 'very poor') {
          displayKey = '1-' + key;
          doSort = true;
        } else if (displayKey === 'poor') {
          displayKey = '2-' + key;
          doSort = true;
        } else if (displayKey === 'fair') {
          displayKey = '3-' + key;
          doSort = true;
        } else if (displayKey === 'good') {
          displayKey = '4-' + key;
          doSort = true;
        } else if (displayKey === 'very good') {
          displayKey = '5-' + key;
          doSort = true;
        }

        x.push(displayKey);
        y.push(counts[key]);
      }
      chart.drawChart(x, y, doSort);

    } else {
      console.error('fetchBarData error', result.error);
    }
  } catch (err) {
    console.log('Fetch Error:', err);
  }
}


function processWordCountsOverTimeData(counts, totals, chart) {
  const limit = 20;
  app.counts = counts;
  app.totals = totals;

  const totalsList = [];
  for (const word in totals) {
    totalsList.push([word, totals[word]]);
  }

  const windowSize = 5;

  const sortedTotalsList = totalsList.sort((u, v) => { return v[1] - u[1] });

  const traces = [];
  for (let i = 0; i < limit; i++) {
    const word = sortedTotalsList[i][0];
    const c = counts[word];
    const smoothedCountsOverTime = [];
    for (let j = 0; j < c.length; j++) {
      let total = 0;
      for (let k = 0; k < windowSize; k++) {
        total += c[j - k] || 0;
      }
      smoothedCountsOverTime.push(total / windowSize);
    }
    const timePoints = Array.from(Array(counts[word].length).keys());

    traces.push({
      x: timePoints,
      y: smoothedCountsOverTime,
      type: 'line',
      name: word,
    });
  }

  chart.drawChartMultipleTraces(traces);
}


function processTfIdfOverValuesData(data, chart) {
  console.info("CHART 1", chart);
  app.myData = data;
  const traces = [];
  for (const category in data) {
    const x = [];
    const y = [];

    for (const entry in data[category]) {
      // console.info(entry, data[category][entry]);
      x.push(entry);
      // y.push(Math.log2(16 * data[category][entry]));
      y.push(data[category][entry]);
    }

    // console.info(x, y);

    const r = (Math.pow(Math.random(), 2) - 0.25) * 80 + 128;
    const g = (Math.pow(Math.random(), 2) - 0.25) * 80 + 128;
    const b = (Math.pow(Math.random(), 2) - 0.25) * 80 + 144;
    let color = 'rgb(' + r + ',' + g + ',' + b + ')';

    traces.push({
      x: x,
      y: y,
      type: 'bar',
      name: (category || "<blank>").substr(0, 10),

      marker: {
        color: color,
        line: {
          width: 2,
          color: color,
        }
      }
    });
  }

  chart.drawChartMultipleTraces(traces);
}


async function fetchWordCountsOverTimeData(parameters, dataViewId, chart) {
  console.info('!!!!!!', parameters);
  const url = buildRequest(services.wordCountsOverTime, {
    'text_column': parameters['column'],
    'date_time_column': parameters['columnNameDateTime'],
    'data_view_id': dataViewId,
  });

  try {
    const response = await fetch(url);
    if (response.status !== HTTP_OK) {
      console.error('error processing request, status: ' + response.status);
      return;
    }

    const result = await response.json();
    if (!result.error) {
      app.x_counts = result.data.counts;
      app.x_totals = result.data.totals;
      processWordCountsOverTimeData(result.data.counts, result.data.totals, chart);
    } else {
      console.error('fetchBarData error', result.error);
    }
  } catch(err) {
    console.log('Fetch Error:', err);
  }
}

async function fetchTfIdfOverValuesData(parameters, dataViewId, chart) {
  const url = buildRequest(services.tfIdfOverValuesData, {
    'text_column': parameters['columnNameText'],
    'category_column': parameters['columnNameCategory'],
    'data_view_id': dataViewId,
  });

  try {
    const response = await fetch(url);
    if (response.status !== HTTP_OK) {
      console.error('error processing request, status: ' + response.status);
      return;
    }

    const result = await response.json();
    if (!result.error) {
      const q = chart;
      console.info("RESULT", result, chart, q);
      processTfIdfOverValuesData(result.data, chart);
    } else {
      console.error('fetchBarData error', result.error);
    }
  } catch(err) {
    console.log('Fetch Error:', err);
  }
}

async function fetchBigramsData(columnName, dataViewId, chart) {
  const url = buildRequest(services.bigrams, {
    'text_column': columnName,
    'data_view_id': dataViewId,
  });

  try {
    const response = await fetch(url);
    if (response.status !== HTTP_OK) {
      console.error('error processing request, status: ' + response.status);
      return;
    }

    const result = await response.json();
    if (!result.error) {
      console.info("RESULT", result);
      processBigramsData(result.data.counts, result.data.totals, chart);
    } else {
      console.error('fetchBarData error', result.error);
    }
  } catch(err) {
    console.log('Fetch Error:', err);
  }
}

function processBigramsData(counts, chart) {
  const limit = 20;

  const totalsList = [];
  for (const word in totals) {
    totalsList.push([word, totals[word]]);
  }

  const sortedTotalsList = totalsList.sort((u, v) => { return v[1] - u[1] });

  const traces = [];
  for (let i = 0; i < limit; i++) {
    const word = sortedTotalsList[i][0];
    const countsOverTime = counts[word];
    const timePoints = Array.from(Array(10).keys());

    traces.push({
      x: timePoints,
      y: countsOverTime,
      type: 'line',
      name: word,
    });
  }

  chart.drawChartMultipleTraces(traces);

}

class Chart {
  static TYPE_BAR = 'bar';
  static TYPE_LINE = 'line';
  static TYPE_HISTORICAL_WORDS = 'historicalWords';
  static TYPE_BIGRAMS = 'bigrams';
  static TYPE_HISTORICAL_BIGRAMS = 'historicalBigrams';
  static TYPE_TF_IDF_OVER_VALUES = 'tf_idf_over_values';

  static defaultType = Chart.TYPE_BAR;

  constructor(element, type, parameters) {
    this.element = element;
    this.type = type || Chart.defaultType;
    this.parameters = parameters;
    this.active = true;
  }

  get isActive() {
    return this.active === true;
  }

  async update() {
    if (!app.dataView) {
      console.info('No DataView has been loaded, nothing to update');
      return;
    }

    if (this.type === Chart.TYPE_BAR) {
      console.info('type bar');
      await fetchCountsData(this.parameters, app.dataView.id, this);
    } else if (this.type === Chart.TYPE_LINE) {
      await fetchCountsData(this.parameters, app.dataView.id, this);
    } else if (this.type === Chart.TYPE_HISTORICAL_WORDS) {
      await fetchWordCountsOverTimeData(this.parameters, app.dataView.id, this);
    } else if (this.type === Chart.TYPE_BIGRAMS) {
      await fetchBigramsData(this.parameters, app.dataView.id, this);
    } else if (this.type === Chart.TYPE_HISTORICAL_BIGRAMS) {
      await fetchWordCountsOverTimeData(this.parameters, app.dataView.id, this);
    } else if (this.type === Chart.TYPE_TF_IDF_OVER_VALUES) {
      await fetchTfIdfOverValuesData(this.parameters, app.dataView.id, this);

    } else {
      console.error('Unrecognized chart type:', this.type)
    }
  }

  drawChart(x, y, doSort) {
    const chart = this.element;
    Plotly.newPlot(
      chart, [{
        x: x,
        y: y,
        type: this.type,
        marker: {
          color: 'rgba(120, 180, 200, 0.65)',
          line: {
            width: 2,
            color: 'rgba(160, 180, 200, 0.95)',
          }
        }
      }],
      {
        margin: { t: 0 },
        autosize: false,
        width: '150px',
        xaxis: {'categoryorder': doSort ? 'category ascending' : 'array'},
      },
      { showSendToCloud: true }
    ).then(response => {
      console.info(response);
    }).catch(err => {
      console.log('Fetch Error:', err);
    });
  }

  drawChartMultipleTraces(traces) {
    const chart = this.element;
    Plotly.newPlot(
      chart,
      traces,
      {
        margin: { t: 0 },
      },
      { showSendToCloud: true }
    ).then(response => {
      console.info(response);
    }).catch(err => {
      console.log('Fetch Error:', err);
    });
  }
}

class ChartManager {
  constructor(chartWindowId, categoryWindowId) {
    this.chartIdCounter = 0;
    this.chartWindowId = chartWindowId;
    this.chartWindow = null;
    this.categoryWindow = null;
    this.categoryWindowId = categoryWindowId;
    this.chartById = {};
    this.charts = [];
  }

  async init() {
    console.group('ChartManager.init');
    console.groupEnd();
    return Promise.all([]);
  }

  initDisplayCharts() {
    const display = () => {
      this.chartWindow = document.getElementById(this.chartWindowId);
      this.categoryWindow = document.getElementById(this.categoryWindowId);
      // this.createCategories(this.categoryWindow);

      if (Object.keys(app.chartManager.chartById).length) {
        this.showChartWindow();
      } else {
        this.hideChartWindow();
      }

      this.update();
    };

    return Promise.all([display()]);
  }

  get nextChartId() {
    return this.chartIdCounter++;
  }

  createCategories(parentElement) {
    app.chartManager.chartById[0] = 1;
    console.info('createCategories');

    const categoryTable = createTable({id: 'categoryTable'});
    const headingRow = categoryTable.insertRow();
    headingRow.insertCell().appendChild(createSpan({
      html: 'autocat',
      cls: 'categoryTableHeading',
    }));

    for (const category in categories) {
      for (const subcategory of categories[category]) {
        const row = categoryTable.insertRow();

        row.insertCell().appendChild(createSpan({
          text: category,
          cls: 'categoryTableCategory',
        }));

        row.insertCell().appendChild(createSpan({
          text: subcategory,
          cls: 'categoryTableSubcategory',
        }));
      }
    }

    parentElement.appendChild(createDiv({
      id: 'chartWorkingMsg',
      html: "working...",
      style: {
        padding: '20px',
      }
    }));


    setTimeout(() => {
      document.getElementById('chartWorkingMsg').style.display = 'none';
      parentElement.appendChild(categoryTable);
    }, 750 + Math.random() * 250);

  }

  createExampleCharts() {
    console.info("Creating sample charts");
    this.createChart(Chart.TYPE_BAR, {column: 'Q1'});
    this.createChart(Chart.TYPE_TF_IDF_OVER_VALUES, {
      columnNameText: 'Text',
      columnNameCategory: 'pageType',
    });
    this.createChart(Chart.TYPE_HISTORICAL_WORDS, {
      text_column_name: 'Text',
      date_time_column_name: 'StartDate',
    });
    this.createChart(Chart.TYPE_LINE, {column: 'Q2'});

    /*
    this.createChart(2, Chart.TYPE_HISTORICAL_WORDS, {
      textColumn: 'Q5', dateTimeColumn: 'StartDate',
    });
    this.createChart(3, Chart.TYPE_HISTORICAL_BIGRAMS, {
      textColumn: 'Q5', dataTimeColumn: 'StartDate'
    });
  */

  }

  createChart(type, parameters) {
    const index = this.nextChartId;
    const chartId = 'chart' + index;

    const container = createDiv({
      id: chartId + 'Container',
      cls: 'chartContainer',
    });

    const chartElement = createArticle({
      id: chartId,
    });

    container.appendChild(chartElement);

    for (const label in parameters) {
      const labelId = chartId + 'columnInput__' + label;
      const dataListId = chartId + 'columnNames__' +  label;

      const input = createInput({
        id: labelId,
        cls: 'chartColumnInput',
        type: 'text',
        value: parameters[label],
        name: labelId,
        list: dataListId,
        keydown: e => ifEnterPressed(e, e => {
          console.info('updating chart', label, index, e);
          const chart = app.chartManager.chartById[index];
          chart.parameters[label] = e.target.value;
          chart.update();
        }),
      });

      const columnNames = createDataList({id: dataListId});
      for (const label of app.dataView.labels) {
        columnNames.appendChild(createOption({value: label.name}));
      }

      container.appendChild(input);
      container.appendChild(columnNames);
    }

    const closeButton = createButton({
      id: chartId + 'CloseButton',
      html: "&times;",
      mousedown: e => ifPrimaryClick(e, () => { this.hideChartWindow(index); }),
    });

    container.appendChild(closeButton);
    this.chartWindow.appendChild(container);
    const chart = new Chart(chartElement, type, parameters);
    this.chartById[index] = chart;
    this.charts.push(chart);

    return chart;
  }

  get dataView() {
    return app.dataView;
  }

  showChartWindow() {
    console.info('showing chartWindow');
    show(this.chartWindow);
  }

  hideChartWindow() {
    console.info('hiding chartWindow');
    hide(this.chartWindow);
  }

  update() {
    for (const chartId in this.chartById) {
      const chart = this.chartById[chartId];
      if (chart.isActive) {
        console.info('updating chart', chart);
        this.chartById[chartId].update();
      }
    }
  }
}
