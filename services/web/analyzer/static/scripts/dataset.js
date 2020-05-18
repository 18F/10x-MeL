'use strict';

class DatasetManager {
  constructor(
    datasetWindowId, activeDatasetWindowId, datasetSelectionWindowId
  ) {
    this.datasetWindowId = datasetWindowId;
    this.activeDatasetWindowId = activeDatasetWindowId;
    this.datasetSelectionWindowId = datasetSelectionWindowId;
    this.datasetWindow = null;
    this.activeDatasetWindow = null;
    this.datasetSelectionWindow = null;
  }

  async init() {
    console.group('DatasetManager.init');
    const initElements = () => {
      this.datasetWindow = document.getElementById(this.datasetWindowId);
      this.activeDatasetWindow = document.getElementById(this.activeDatasetWindowId);
      this.datasetSelectionWindow = document.getElementById(this.datasetSelectionWindowId);
    };

    const initAppearance = () => {
      this.windowHeight = config.activeDatasetHeight;
      show(this.activeDatasetWindow);
      hide(this.datasetSelectionWindow);
    };

    const initEventHandlers = () => {
      const datasetWindow = this.datasetWindow;
      const datasetSelectionWindow = this.datasetSelectionWindow;

      datasetWindow.addEventListener('mouseenter', () => {
        if (!config.datasetsLoaded) {
          console.info('loading selection');
          this.initDatasetSelection();
          config.datasetsLoaded = true;
        }

        this.setActiveDatasetColor(Color.WHITE, Color.GREY64);
        show(datasetSelectionWindow);
        this.windowHeight = datasetWindow.scrollHeight;
      }, true);

      datasetWindow.addEventListener('mouseout', () => {
        this.setActiveDatasetColor(Color.GREY32, Color.WHITE);
        this.windowHeight = config.activeDatasetHeight;
      }, true);
    };

    console.groupEnd();
    return Promise.all([initElements(), initAppearance(), initEventHandlers()]);
  }

  set windowHeight(height) {
    this.datasetWindow.style.height = px(height);
  }

  setActiveDatasetColor(fgColor, bgColor) {
    const windowStyle = this.activeDatasetWindow.style;
    fgColor && windowStyle.setProperty('color', fgColor);
    bgColor && windowStyle.setProperty('background-color', bgColor);
  }


  initDatasetSelection() {
    const url = HOST + services.listDatasets;

    fetch(url).then(response => {
      if (response.status !== HTTP_OK) {
        console.log('Error processing request, status: ' + response.status);
        return;
      }

      // Examine the text in the response
      response.json().then(data => {
        console.log(data);
        this.buildDatasetSelection(data);
      });
    }).catch(err => {
      console.log('Fetch Error:', err);
    });
  }

  buildDatasetSelection(datasets) {
    const datasetSelectionList = document.getElementById('datasetSelectionList');
    emptyElement(datasetSelectionList);

    for (const datasetName of datasets) {
      datasetSelectionList.appendChild(createDiv({
        text: datasetName,
        cls: 'datasetEntry',
        mousedown: e => ifPrimaryClick(e, () => {
          return this.setDataset(datasetName);
        }),
      }));
    }
  }

  async setDataset(datasetName) {
    console.info('setting data source to', datasetName);
    const url = buildRequest(services.setMostRecentDataset, {
      'filename': datasetName,
      'user_id': app.userId,
    });

    try {
      const response = await fetch(url);
      if (response.status !== HTTP_OK) {
        console.log('Error processing request, status: ' + response.status);
        return;
      }

      const result = await response.json();
      if (!result.error) {
        console.log(result);
        const dataset = Dataset.fromDict(result['dataset']);
        return app.dataViewManager.fetchMostRecentDataView(dataset.id);
      } else {
        console.error(result.error, result.msg);
      }

    } catch (err) {
      console.error('Fetch Error:', url, err);
    }
  }
}

