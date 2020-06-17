'use strict';

class DataViewManager {
  static defaultColumnWidth = 150;
  static defaultFontSize = 16;

  constructor() {
    this._entries = [];
  }

  init() {
    console.group('DataViewManager.init');
    if (!this.dataView) {
      console.info('User has not loaded a Dataset');
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
      console.info('User has not loaded a Dataset');
      this.hideToolWindow();
      return;
    }

    this.showToolWindow();

    const payload = {
      data_view_id: this.dataView.id,
    };

    if (app.sortLabel) {
      console.info('setting payload.sort_label');
      payload.sort_label = app.sortLabel.name;
      payload.sort_dir = app.sortDir;
    }

    const url = buildRequest(
      services.rawEntriesAndTagsForDataView,
      payload,
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
        const tagsByKey = result['tags_by_key'];
        if (tagsByKey) {
          app.tagManager.updateMap(tagsByKey);
        }
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

    console.info('DataViewManager.updateDataView');
    if (!this.dataView) {
      console.info('Cannot display data, dataView is', this.dataView);
    }
    const entries = this.entries;
    const labels = app.dataView.activeLabels;
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

    const renderer = new DataViewRenderer();
    for (const [index, entryKey] of entryKeys.entries()) {
      const entry = entries[entryKey];
      const row = tableBody.insertRow();

      for (const label of labels) {
        const parentElement = row.insertCell();
        renderer.renderCell(entry, label, parentElement);
      }

      maxIndex = Math.max(maxIndex, index);
    }

    console.info('processed items:', maxIndex);
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

    // sort by column

    const sortByColumnContainer = createDiv({});

    sortByColumnContainer.appendChild(createDiv({
      cls: 'headerEditorButton',
      text: 'sort by',
      mousedown: e => ifPrimaryClick(e, () => {
        app.sortLabel = label;
        show(sortByColumnAscendingButton);
        show(sortByColumnDescendingButton);
      })
    }));

    const sortByColumnAscendingButton = createSpan({
      cls: 'headerEditorButton',
      html: '&uarr;',
      mousedown: e => ifPrimaryClick(e, () => {
        app.sortDir = 'asc';
        cleanup();
        this.updateDataView();
      }),
    });

    const sortByColumnDescendingButton = createSpan({
      cls: 'headerEditorButton',
      html: '&darr;',
      mousedown: e => ifPrimaryClick(e, () => {
        app.sortDir = 'desc';
        cleanup();
        this.updateDataView();
      }),
    });

    sortByColumnContainer.appendChild(sortByColumnAscendingButton);
    sortByColumnContainer.appendChild(sortByColumnDescendingButton);
    hide(sortByColumnAscendingButton);
    hide(sortByColumnDescendingButton);

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
          console.info("HIDE", this);
          this.hideLabel(label);
          this.refreshDataView();
        }),
      })
    );

    window.appendChild(sortByColumnContainer);
    window.appendChild(setFontSizeContainer);
    window.appendChild(setColumnWidthContainer);
    window.appendChild(hideColumnContainer);

    container.appendChild(window);

    document.body.appendChild(container);
  }

  hideLabel(label) {
    app.dataView.hiddenLabels.add(label);
    return this.updateLabels();
  }

  showLabel(label) {
    app.dataView.hiddenLabels.delete(label);
    return this.updateLabels();
  }

  async updateLabels() {

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

class DataViewRenderer {
  renderCell(entry, label, parentElement) {
    if (label.name === 'tag') {
      app.tagManager.renderTagCell(entry, label, parentElement);
    } else {
      this.renderDefaultCell(entry, label, parentElement);
    }
  }

  renderDefaultCell(entry, label, parentElement) {
    const value = entry[label.name] ? entry[label.name] : '';

    parentElement.appendChild(createDiv({
      cls: 'cellText',
      html: value,
      style: {
        'font-size': px(label.fontSize || DataViewManager.defaultFontSize),
        'overflow-wrap': 'anywhere',
      },
    }));
  }
}


class TagManager {
  TAG_TYPE = 'Tag';
  PARAMETER_PRIMARY_KEY_NAME = 'primary_key_column_label';

  constructor() {
    this._primaryKeyName = null;
    this._tagsByKey = {}
  }

  get primaryKeyName() {
    if (this._primaryKeyName === null) {
      this._primaryKeyName = this.extractPrimaryKeyName();
    }
    return this._primaryKeyName;
  }

  extractPrimaryKeyName() {
    for (const transform of app.dataView.transforms) {
      if (transform.type !== this.TAG_TYPE) {
        continue;
      }

      const primaryKeyName = transform.parameters[this.PARAMETER_PRIMARY_KEY_NAME];
      console.info('Found PrimaryKey', primaryKeyName);
      return primaryKeyName;
    }

    console.error('Failed to find valid PrimaryKey:', this._primaryKeyName);
    return null;
  }

  updateMap(tagsByKey) {
    this._tagsByKey = tagsByKey;
  }

  renderTagCell(entry, label, parentElement) {
    const primaryKey = entry[this.primaryKeyName];

    const container = createDiv({
      cls: 'tagAction',
    });

    const tags = this._tagsByKey[primaryKey] || [];
    const tagsDisplay = this.tagDataToHtml(tags, primaryKey);

    container.appendChild(tagsDisplay);

    const addTagInput = createInput({
      cls: 'tagAction',
      type: 'text',
      keydown: e => {
        console.info("key", e.key);
        if (e.key === 'Enter') {
          // clean up
          hide(addTagInput);
          const tags = addTagInput.value.split(",").map(s => s.trim());
          this.addTags(tags, primaryKey);
          // this.refreshRow(primaryKey);
          app.dataViewManager.updateDataView();

        } else if (e.key === 'Escape') {
          hide(addTagInput);
          addTagInput.value = '';
        }
      },
    });

    container.appendChild(addTagInput);
    hide(addTagInput);

    parentElement.appendChild(container);

    parentElement.onmousedown = e => ifPrimaryClick(e, () => {
      addTagInput.focus();
      show(addTagInput);
      setTimeout(() => { addTagInput.focus(); }, 10);
      addTagInput.focus();
    });
  }

  tagDataToHtml(tags, primaryKey) {
    const mainContainer = createSpan({});
    for (const rawTag of tags) {
      const tagContainer = createSpan({});
      const tag = rawTag.trim();

      tagContainer.appendChild(createButton({
        html: '&times;',
        mousedown: e => ifPrimaryClick(e, () => {
          console.info(e);
          hide(tagContainer);
          this.removeTags([tag], primaryKey);
          e.stopPropagation();
          e.stopImmediatePropagation();
        }),
      }));

      tagContainer.appendChild(createLink({
        text: tag,
      }));

      mainContainer.appendChild(tagContainer);
    }
    return mainContainer;
  }

  async addTags(tagNames, primaryKey) {
    const url = buildRequest(services.addTags, {
      'tags': tagNames,
      'primary_key': primaryKey,
      'primary_key_name': this.primaryKeyName,
      'data_view_id': app.dataView.id,
    });

    try {
      const response = await fetch(url);
      if (response.status !== HTTP_OK) {
        console.error('error processing request, status: ' + response.status);
        return;
      }

      const result = await response.json();
      if (!result.error) {
        console.info("Successfully added", tagNames, "to", primaryKey, "in", this.primaryKeyName);

      } else {
        console.error('fetchBarData error', result.error);
      }
    } catch(err) {
      console.log('Fetch Error:', err);
    }
  }

  async removeTags(tagNames, primaryKey) {
    const url = buildRequest(services.removeTags, {
      'tags': tagNames,
      'primary_key': primaryKey,
      'primary_key_name': this.primaryKeyName,
      'data_view_id': app.dataView.id,
    });

    try {
      const response = await fetch(url);
      if (response.status !== HTTP_OK) {
        console.error('error processing request, status: ' + response.status);
        return;
      }

      const result = await response.json();
      if (!result.error) {
        console.info("Successfully removed", tagNames, "from", primaryKey, "in", this.primaryKeyName);

        await app.dataViewManager.updateDataView();
      } else {
        console.error('fetchBarData error', result.error);
      }
    } catch(err) {
      console.log('Fetch Error:', err);
    }
  }
}
