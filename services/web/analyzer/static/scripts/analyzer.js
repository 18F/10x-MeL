'use strict';

const PORT = 5000;
const HOST = 'http://localhost:' + PORT + '/';
const services = {
  heartbeat: 'heartbeat',
  listDatasets: 'list_datasets',
  setMostRecentDataset: 'set_most_recent_dataset',
  getDataViewId: 'get_data_view_id',
  getTransformDefs: 'get_transform_defs',
  mostRecentDataView: 'most_recent_data_view',
  rawDataForDataView: 'raw_data_for_data_view',
  rawEntriesAndTagsForDataView: 'raw_entries_and_tags_for_data_view',
  transformDataView: 'transform_data_view',
  countUnique: 'count_unique',
  addTags: 'add_tags',
  removeTags: 'remove_tags',
  wordCountsOverTime: 'word_counts_over_time',
  tfIdfOverValuesData: 'tf_idf_over_values',
};

const config = {
  datasetsLoaded: false,
  activeDatasetHeight: 30,
};
const AVAILABLE_CHART_TYPES = ['bar', 'line', 'historicalWords'];


const PAYLOAD_KEY = "q";


const TransformType = {
  FILTER: 'filter',
  ENRICHMENT: 'enrich',
};
Object.freeze(TransformType);

const DEFAULT_USER_ID = 1;


function buildRequest(service, payload) {
  if (payload) {
    return HOST + service + '?' + PAYLOAD_KEY + '=' + JSON.stringify(payload);
  } else {
    return HOST + service;
  }
}

async function isServerAvailable() {
  const url = buildRequest(services.heartbeat);

  try {
    const response = await fetch(url);
    const result = await response.text();
    console.info('heartbeat', result);

  } catch (err) {
    console.log('Fetch Error:', err);
    handleNoServerFound();
  }
}

function handleNoServerFound() {
  const container = createDiv({
    style: {
      'margin-top': '30%',
    },
  });

  container.appendChild(createH1({text: 'server could not be found'}));
  container.appendChild(createDiv({text: 'please ensure your docker container is running'}));

  const errorScreen = createDiv({id: 'working'});

  errorScreen.appendChild(container);
  document.body.appendChild(errorScreen);
}

class App {
  static transformListWindowId = 'transformListWindow';
  static transformListTableId = 'transformListTable';

  static datasetWindowId = 'datasetWindow';
  static activeDatasetWindowId = 'activeDatasetWindow';
  static datasetSelectionWindowId = 'datasetSelectionWindow';

  static chartWindowId = 'chartWindow';
  static categoryWindowId = 'categoryWindow';
  static chart0CloseButtonId = 'chart0__closeButton';
  static chart1CloseButtonId = 'chart1__closeButton';
  static chart2CloseButtonId = 'chart2__closeButton';
  static chart3CloseButtonId = 'chart3__closeButton';

  static buildTransformWindowId = 'buildTransformWindow';
  static buildVisualizationWindowId = 'buildVisualizationWindow';
  static addTransformWindowId = 'addTransformWindow';
  static addFilterButtonId = 'addFilterButton';
  static addEnrichmentButtonId = 'addEnrichmentButton';
  static addVisualizationButtonId = 'addVisualizationButton';

  constructor() {
    this.userId = DEFAULT_USER_ID;

    this.datasetManager = new DatasetManager(
      App.datasetWindowId,
      App.activeDatasetWindowId,
      App.datasetSelectionWindowId,
    );

    this.tagManager = new TagManager();

    this.chartManager = new ChartManager(
      App.chartWindowId,
      App.categoryWindowId,
      App.chart0CloseButtonId,
      App.chart1CloseButtonId,
      App.chart2CloseButtonId,
      App.chart3CloseButtonId,
    );
    Object.seal(this.chartManager);

    this.transformManager = new TransformManager(
      App.buildTransformWindowId,
      App.buildVisualizationWindowId,
      App.addTransformWindowId,
      App.addFilterButtonId,
      App.addEnrichmentButtonId,
      App.addVisualizationButtonId,
      App.transformListWindowId,
      App.transformListTableId,
    );
    Object.seal(this.transformManager);

    this.dataViewManager = new DataViewManager();
    Object.seal(this.dataViewManager);

    this.sortLabel = null;
    this.sortDir = "desc";
  }

  init() {
    console.info('init started');
    this._dataView = null;

    const datasetManager = this.datasetManager;
    const dataViewManager = this.dataViewManager;
    const chartManager = this.chartManager;
    const transformManager = this.transformManager;
    const tagManager = this.tagManager;

    const initDatasetManager = datasetManager.init.bind(datasetManager);
    const initDataViewManager = dataViewManager.init.bind(dataViewManager);
    const initChartManager = chartManager.init.bind(chartManager);
    const initTransformManager = transformManager.init.bind(transformManager);

    const fetchMostRecentDataView = dataViewManager.fetchMostRecentDataView.bind(dataViewManager);
    // const fetchRawData = dataViewManager.updateDataView.bind(dataViewManager);
    const displayCharts = chartManager.initDisplayCharts.bind(chartManager);

    const initAppearance = () => {
      hide(document.getElementById(App.buildTransformWindowId));
      transformManager.showAddWindow();
      const chartWindow = document.getElementById('chartWindow');

      chartWindow.ondblclick = () => {
        const dims = {};
        if (chartWindow.clientWidth === 50) {
          [dims.width, dims.height] = [750, 450];
        } else {
          [dims.width, dims.height] = [10, 10];
        }
        chartWindow.style.width = px(dims.width);
        chartWindow.style.height = px(dims.height);
      };

      return Promise.all([]);
    };

    const announce = () => { console.info(app.dataView); };

    const doInit = async () => {
      await isServerAvailable();
      await initDatasetManager();
      await initChartManager();
      await initTransformManager();
      await initDataViewManager();
      await initAppearance();
      await fetchMostRecentDataView();
      await displayCharts();
      await announce();
    };

    doInit().then(result => {
      console.info("doInit.complete", result);
    }).catch(err => {
      console.error("doInit - finished with error", err);
    });

    Object.seal(datasetManager);
    Object.seal(dataViewManager);
    Object.seal(chartManager);
    Object.seal(transformManager);
    console.info('init finished');
  }

  get dataView() {
    return this._dataView;
  }

  set dataView(dataView) {
    this._dataView = dataView;
    this.update();
  }

  update() {
    console.group('DataViewManager.update');
    const updateTransformList = this.transformManager.updateTransformList.bind(this.transformManager);
    const updateDataView = this.dataViewManager.updateDataView.bind(this.dataViewManager);
    const updateCharts = this.chartManager.update.bind(this.chartManager);

    const announce = () => { console.info(app.dataView); };

    const doUpdate = async () => {
      await updateTransformList();
      await Promise.all([updateDataView(), await updateCharts()]);
      await announce();
    };

    doUpdate().then(result => {
      console.info("doUpdate.complete", result);
    }).catch(err => {
      console.error("doUpdate - finished with error", err);
    });

    console.groupEnd();
  }
}


const app = new App();

document.addEventListener('DOMContentLoaded', () => { app.init(); }, false);
