'use strict';

class DataViewManager {
  static defaultColumnWidth = 150;
  static defaultFontSize = 16;

  constructor() {
    this._entries = [];
    this.labelsHidden = new Set();
  }

  init() {
    console.group('DataViewManager.init');
    if (!this.dataView) {
      console.info("User has not loaded a Dataset");
      this.hideToolWindow();
      return;
    }

    console.groupEnd();
    return Promise.all([]);
  }

  get dataView() {
    return app.dataView;
  }

  get entries() {
    return this._entries;
  }

  set entries(newEntries) {
    this._entries = newEntries;
  }

  showToolWindow() {
    const toolWindow = document.getElementById('toolWindow');
    show(toolWindow);
  }

  hideToolWindow() {
    const toolWindow = document.getElementById('toolWindow');
    hide(toolWindow);
  }

  async updateDataView() {
    console.trace();
    console.info('updateDataView', this.dataView);
    if (!this.dataView) {
      console.info("User has not loaded a Dataset");
      this.hideToolWindow();
      return;
    }

    this.showToolWindow();

    const url = buildRequest(
      services.rawDataForDataView,
      { data_view_id: this.dataView.id },
    );

    console.info('updateDataView URL', url);

    try {
       const response = await fetch(url);
      if (response.status !== HTTP_OK) {
        console.log('Error processing request, status: ' + response.status);
        return;
      }

      const result = await response.json();
      if (!result.error) {
        this.entries = result['entries'];
        this.refreshDataView();
      } else {
        console.error(
          'error', result.error, 'retrieving data for DataView id =', this.dataView.id, ':', result.msg
        );
      }
    } catch(err) {
      console.log('Fetch Error:', err);
    }
  }

  refreshDataView() {
    const dataViewTable = document.getElementById('dataViewTable');

    console.info("DataViewManager.updateDataView");
    if (!this.dataView) {
      console.info("Cannot display data, dataView is ", this.dataView);
    }
    const entries = this.entries;
    const labelsHidden = this.labelsHidden;
    const labels = this.dataView.labels.filter(label => !labelsHidden.has(label));

    emptyElement(dataViewTable);

    if ((isIterable(labels) === false) || (objHasEntries(entries) === false)) {
      return;
    }

    const columnGroup = createColumnGroup({});
    let tableWidth = 0;
    for (const label of labels) {
      const columnWidth = (label.width || DataViewManager.defaultColumnWidth);

      columnGroup.appendChild(createColumn({
        id: 'col__' + label.hash,
        style: { width: px(columnWidth) },
      }));

      tableWidth += columnWidth;
    }

    dataViewTable.appendChild(columnGroup);
    console.info('tableWidth:', tableWidth);
    dataViewTable.style.width = px(tableWidth);

    const row = dataViewTable.createTHead().insertRow();
    for (const label of labels) {
      row.insertCell().appendChild(createLabel({
        text: label.name,
        mousedown: e => ifPrimaryClick(e, () => {
          console.info('label', label);
          this.createColumnHeaderEditWindow(e, label);
        }),
      }));
    }

    const entryKeys = Object.keys(entries);
    const tableBody = dataViewTable.createTBody();
    let maxIndex = 0;
    for (const [index, entryKey] of entryKeys.entries()) {
      const entry = entries[entryKey];
      const row = tableBody.insertRow();

      for (const label of labels) {
        let value = entry[label.name];
        if (value === undefined) {
          value = '';
        }
        if (entry)
        row.insertCell().appendChild(createSpan({
          cls: 'cellText',
          text: value,
          style: {
            'font-size': px(label.fontSize || DataViewManager.defaultFontSize),
            'overflow-wrap': 'anywhere',
          },
        }));
      }

      maxIndex = Math.max(maxIndex, index);
    }

    console.info("processed items:", maxIndex);
    // app.chartManager.update();
  }

  createColumnHeaderEditWindow(e, label) {
    const container = createDiv({
      cls: ['headerEditorContainer', 'modal'],
      style: {
        left: px(e.pageX - 40),
        top: px(e.pageY - 30),
      },
      mouseleave: cleanup,
    });

    const window = createDiv({cls: 'headerEditorWindow'});

    function cleanup() {
      hide(container);
      document.body.removeChild(container);
    }

    // column font size

    const setFontSizeContainer = createDiv({});

    setFontSizeContainer.appendChild(createDiv({
      cls: 'headerEditorButton',
      text: 'font size',
      mousedown: e => ifPrimaryClick(e, () => {
        setFontSizeInput.focus();
        show(setFontSizeInput);
        setTimeout(() => { setFontSizeInput.focus(); }, 10);
        setFontSizeInput.focus();
      })
    }));

    const setFontSizeInput = createInput({
      cls: 'headerEditorInput',
      type: 'number', min: '2', max: '120',
      value: '' + (label.fontSize || DataViewManager.defaultFontSize),
      keydown: e => ifEnterPressed(e, () => {
        label.fontSize = parseFloat(setFontSizeInput.value);
        cleanup();
        this.refreshDataView();
      }),
    });

    setFontSizeContainer.appendChild(setFontSizeInput);
    hide(setFontSizeInput);

    // column width
    const setColumnWidthContainer = createDiv({});

    setColumnWidthContainer.appendChild(createDiv({
      cls: 'headerEditorButton',
      text: 'column width',
      mousedown: e => ifPrimaryClick(e, () => {
        show(setColumnWidthInput);
        setTimeout(() => { setColumnWidthInput.focus(); }, 10);
        setColumnWidthInput.focus();
      }),
    }));

    const setColumnWidthInput = createInput({
      cls: 'headerEditorInput',
      type: 'number',  min: '10', max: '10000',
      value: '' + (label.width || DataViewManager.defaultColumnWidth),
      keydown: e => ifEnterPressed(e, () => {
        cleanup();
        label.width = parseInt(setColumnWidthInput.value);
        this.refreshDataView();
      }),
    });

    setColumnWidthContainer.appendChild(setColumnWidthInput);
    hide(setColumnWidthInput);

    // column hiding
    const hideColumnContainer = createDiv({});
    hideColumnContainer.appendChild(
      createButton({
        cls: 'headerEditorButton',
        text: 'hide',
        mousedown: e => ifPrimaryClick(e, () => {
          cleanup();
          this.labelsHidden.add(label);
          this.refreshDataView();
        }),
      })
    );

    window.appendChild(setFontSizeContainer);
    window.appendChild(setColumnWidthContainer);
    window.appendChild(hideColumnContainer);

    container.appendChild(window);

    document.body.appendChild(container);
  }

  /**
   * Fetch the most recent DataView, according to the specified parameters
   * @async
   * @function fetchMostRecentDataView
   * @param {str} [datasetId=null] the id of the dataset, for the desired DataView
   * @return {Promise<str>}
   */
  async fetchMostRecentDataView(datasetId) {
    const userId = app.userId;
    console.info('fetchMostRecentDataView userId', userId, 'datasetId', datasetId);

    const payload = { user_id: userId };
    if (datasetId) {
      payload.dataset_id = datasetId;
    }

    const url = buildRequest(services.mostRecentDataView, payload);

    try {
      const response = await fetch(url);
      if (response.status !== HTTP_OK) {
        console.log('Error processing request, status: ' + response.status);
        return Promise.reject('Error processing request, status: ' + response.status);
      }

      const result = await response.json();
      if (!result.error) {
        console.info('MostRecent', result, result['data_view']);
        app.dataView = DataView.deserialize(result['data_view']);
      } else {

      }

    } catch (err) {
      console.log('Fetch Error:', err);
    }
  }
}
