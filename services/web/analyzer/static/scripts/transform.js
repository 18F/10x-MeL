
class TransformManager {
  constructor(
    buildTransformWindowId,
    buildVisualizationWindowId,
    addTransformWindowId,
    addFilterButtonId,
    addEnrichmentButtonId,
    addVisualizationButtonId,
    transformListWindowId,
    transformListTableId,
  ) {
    this.buildTransformWindowId = buildTransformWindowId;
    this.buildVisualizationWindowId = buildVisualizationWindowId;
    this.addTransformWindowId = addTransformWindowId;
    this.addFilterButtonId = addFilterButtonId;
    this.addEnrichmentButtonId = addEnrichmentButtonId;
    this.addVisualizationButtonId = addVisualizationButtonId;
    this.transformListWindowId = transformListWindowId;
    this.transformListTableId = transformListTableId;

    this.buildTransformWindow = null;
    this.buildVisualizationWindow = null;
    this.addWindow = null;
    this.addFilterButton = null;
    this.addEnrichmentButton = null;
    this.addVisualizationButton = null;
    this.transformListWindow = null;

    this._transformDefs = [];
    this._transformDefByType = {};
  }

  init() {
    console.group('TransformManager.init');
    const initElements = () => {
      this.buildTransformWindow = document.getElementById(this.buildTransformWindowId);
      this.buildVisualizationWindow = document.getElementById(this.buildVisualizationWindowId);
      this.addWindow = document.getElementById(this.addTransformWindowId);
      this.addFilterButton = document.getElementById(this.addFilterButtonId);
      this.addEnrichmentButton = document.getElementById(this.addEnrichmentButtonId);
      this.addVisualizationButton = document.getElementById(this.addVisualizationButtonId);
      this.transformListWindow = document.getElementById(this.transformListWindowId);
    };

    const initEventHandler = () => {
      this.addFilterButton.onmousedown = e => ifPrimaryClick(e, () => {
        console.info('addFilterButton mousedown', e);
        this.hideAddWindow();
        this.createNewBuildWindow(TransformType.FILTER);
        show(this.buildTransformWindow);
      });

      this.addEnrichmentButton.onmousedown = e => ifPrimaryClick(e, () => {
        console.info('addEnrichmentButton mousedown', e);
        this.hideAddWindow();
        this.createNewBuildWindow(TransformType.ENRICHMENT);
        show(this.buildTransformWindow);
      });

      this.addVisualizationButton.onmousedown = e => ifPrimaryClick(e, () => {
        console.info('addVisualizationButton mousedown', e);
        this.hideAddWindow();
        this.createNewVisualizationWindow();
        show(this.buildVisualizationWindow);
      });
    };

    console.groupEnd();
    return Promise.all([
      initElements(),
      initEventHandler(),
      this.fetchTransformDefs(),
    ]);
  }

  createNewBuildWindow(transformType) {
    emptyElement(this.buildTransformWindow);
    this.addTransformNameSpecification(this.buildTransformWindow, transformType);
  }

  get transformDefs() {
    return this._transformDefs;
  }

  set transformDefs(transformDefs) {
    this._transformDefs = transformDefs;
    for (const transformDef of this._transformDefs) {
      this._transformDefByType[transformDef.type] = transformDef;
    }
  }

  async fetchTransformDefs() {
    const url = HOST + services.getTransformDefs;

    const response = await fetch(url);
    if (response.status !== HTTP_OK) {
      console.log('Error processing request, status: ' + response.status);
      return;
    }

    try {
      const result = await response.json();
      this.transformDefs = result.map(def => TransformDef.fromDict(def));
    } catch(err) {
      console.log('Fetch Error:', err);
    }
  }

  hideAddWindow() {
    hide(this.addWindow);
  }

  showAddWindow() {
    show(this.addWindow);
  }

  get transformDefTypes() {
    return this.transformDefs.map(transform => transform.type);
  }

  transformDefByType(transformType) {
    return this._transformDefByType[transformType];
  }

  addTransformNameSpecification(parentElement, transformType) {
    console.info(transformType);
    const parameterSpecificationContainerId = 'transformArgumentSpecificationContainer';
    const transformNameContainer = createDiv({id: 'transformNameContainer'});

    const transformNames = this.transformDefTypes;
    const transformNameSet = new Set(transformNames);

    const addTransformArguments = e => {
      console.info("XXXXX addTransformArguments");
      const currentValue = e.currentTarget.value;
      console.info('current value', currentValue);

      function createContainer() {
        return parentElement.appendChild(
          createDiv({id: parameterSpecificationContainerId})
        );
      }

      const parameterSpecificationContainer = (
        document.getElementById(parameterSpecificationContainerId) || createContainer()
      );

      emptyElement(parameterSpecificationContainer);

      if (transformNameSet.has(currentValue)) {
        this.addArgumentsToBuildWindow(currentValue, parameterSpecificationContainer, transformType);
      }
    };

    const transformTypeInput = createInput({
      id: 'transformTypeInput',
      type: 'text',
      name: 'transformType',
      list: 'transformTypes',
      change: addTransformArguments,
    });

    const datalist = createDataList({id: 'transformTypes'});
    for (const transformName of transformNames) {
      const transformDef = this.transformDefByType(transformName);
      if (transformDef.operations.indexOf(transformType) === -1) {
        continue;
      }

      datalist.appendChild(createOption({value: transformName}));
    }

    const closeButton = createButton({
      id: 'closeAddTransformButton',
      html: '&times;',
      mousedown: () => {
        hide(this.buildTransformWindow);
        this.showAddWindow();
      },
    });

    parentElement.appendChild(transformNameContainer);
    transformNameContainer.appendChild(closeButton);
    transformNameContainer.appendChild(transformTypeInput);
    transformNameContainer.appendChild(datalist);
  }


  addArgumentsToBuildWindow(transformName, parentElement, transformType) {
    const exampleClass = 'transformArgumentExample';
    const labelClass = 'transformArgumentLabel';
    const parametersTableClassName = 'transformArgumentTable';
    const parameterDefs = this.transformDefByType(transformName).parameters;

    const argumentBuilder = new ArgumentBuilder();

    const parametersTable = createTable({cls: parametersTableClassName});
    for (const parameterDef of parameterDefs) {

      console.info(transformType, transformName, "parameterDef.type", parameterDef.type);

      const labelId = ArgumentBuilder.inputIdFromDef(parameterDef);

      const onkeydown = e => ifEnterPressed(e, () => this.completeTransform(transformName, transformType));

      const row = parametersTable.insertRow();
      const inputCell = row.insertCell();

      if (parameterDef.type === ParameterDef.TYPE_COLUMN_NAME) {
        argumentBuilder.buildColumnNameInput(labelId, parameterDef, onkeydown, inputCell);
      } else if (parameterDef.type === ParameterDef.TYPE_DATE_RANGE) {
        argumentBuilder.buildDateRangeInput(labelId, parameterDef, onkeydown, inputCell);
      } else if (parameterDef.type === ParameterDef.TYPE_DATE_RANGE_LIST) {
        argumentBuilder.buildDateRangeInputList(labelId, parameterDef, onkeydown, inputCell);
      } else if (parameterDef.type === ParameterDef.TYPE_COLUMN_NAME_LIST) {
        argumentBuilder.buildColumnNameInputList(labelId, parameterDef, onkeydown, inputCell);
      } else {
        argumentBuilder.buildTextInput(labelId, parameterDef, onkeydown, inputCell);
      }

      row.insertCell().appendChild(createLabel({
        cls: labelClass, text: parameterDef.label
      }));

      if (parameterDef.example) {
        row.insertCell().appendChild(createLabel({
          cls: exampleClass, text: parameterDef.example,
        }));
      }
    }

    parentElement.appendChild(parametersTable);
  }

  async completeTransform(transformName, transformType) {
    const buildWindow = document.getElementById('buildTransformWindow');
    const addWindow = document.getElementById('addTransformWindow');
    const transformTypeInput = document.getElementById('transformTypeInput');
    const parameterDefs = this.transformDefByType(transformName).parameters;

    function validate() {
      console.warn('Validation not implemented');
      return true;
    }

    if (validate()) {
      const parameters = {};

      for (const parameterDef of parameterDefs) {
        const inputId = ArgumentBuilder.inputIdFromDef(parameterDef);
        const indexId = ArgumentBuilder.indexIdFromLabelId(inputId);

        if (ParameterDef.COLLECTION_TYPES.has(parameterDef.type)) {
          const indexElement = document.getElementById(indexId);
          const numEntries = indexElement ? parseInt(indexElement.value) : 0;
          console.info("numEntries", numEntries);
          const entries = [];
          for (const i of Array(numEntries).keys()) {
            const index = i + 1;
            const indexedInputId = inputId + '_' + index;

            console.info(">>>", inputId, indexedInputId, i, index, numEntries);

            entries.push(document.getElementById(indexedInputId).value);
          }

          console.info("!!!", numEntries, "!!!", entries);

          if (parameterDef.type === ParameterDef.TYPE_DATE_RANGE_LIST) {

            const numPairs = (entries.length / 2) >> 0;

            console.info("NUMPAIRS", numPairs);

            const dateRanges = [];
            for (const i of Array(numPairs).keys()) {
              const dateRange = [entries[2 * i], entries[2 * i + 1]];
              dateRanges.push(dateRange.join(ParameterDef.DATE_RANGE_SEPARATOR));
            }

            console.info("@", dateRanges);

            parameters[parameterDef.name] = dateRanges.join(ParameterDef.LIST_SEPARATOR);

          } else {
            parameters[parameterDef.name] = entries.join(ParameterDef.LIST_SEPARATOR);
          }

        } else {
          if (parameterDef.type === ParameterDef.TYPE_DATE_RANGE) {
            const prefixes = ['from', 'to'];
            const dateStrings = [];
            for (const prefix of prefixes) {
              const prefixedInputId = prefix + '_' + inputId;
              const dateString = document.getElementById(prefixedInputId).value;
              dateStrings.push(dateString);
            }

            parameters[parameterDef.name] = dateStrings.join(ParameterDef.DATE_RANGE_SEPARATOR);

          } else {
            console.info("###", parameterDef.name, inputId);
            parameters[parameterDef.name] = document.getElementById(inputId).value;
          }
        }
      }

      console.info('save', transformTypeInput.value, parameters);
      const transformDefType = transformTypeInput.value;
      const transformDef = this.transformDefByType(transformDefType);
      const transform = new Transform(transformDef, parameters, transformType);

      await this.transformDataView({dataView: app.dataView, addTransforms: [transform]});
      hide(buildWindow);

      emptyElement(buildWindow);
      show(addWindow);

    } else {
      console.error('should handle validation errors here');
    }
  }

  async updateTransformList() {
    emptyElement(this.transformListWindow);

    const transformTable = createDiv({
      id: this.transformListTableId,
    });

    let i = 0;
    for (const transform of app.dataView.transforms) {
      const baseId = 'transform' + i;

      const entry = createDiv({
        cls: 'transformEntry',
        style: { 'display': 'inline-flex' },
      });

      const button = createButton({
        id: baseId + 'RemoveButton',
        cls: 'removeTransformButton',
        html: '&times;',
        mousedown: e => ifPrimaryClick(e, () => {
          console.info('removing transform', i, transform);
          return this.transformDataView({dataView: app.dataView, deleteTransforms: [transform]});
        }),
      });

      const transformType = createDiv({
        id: baseId + 'Type',
        cls: 'transformListType',
      });

      transformType.appendChild(transform.richTextDescription);
      entry.appendChild(button);
      entry.appendChild(transformType);
      transformTable.appendChild(entry);

      i += 1;
    }

    this.transformListWindow.appendChild(transformTable);
  }


  createNewVisualizationWindow() {
    const argSpecContainerId = 'visualizationArgumentSpecificationContainer';

    const visualizationWindow = document.getElementById('buildVisualizationWindow');
    emptyElement(visualizationWindow);

    const chartNameContainer = createDiv({id: 'chartNameContainer'});

    visualizationWindow.appendChild(chartNameContainer);

    const availableChartTypeSet = new Set(AVAILABLE_CHART_TYPES);

    const addVisualizationArguments = e => {
      const currentValue = e.currentTarget.value;
      console.info('current value', currentValue);

      function createContainer() {
        return visualizationWindow.appendChild(
          createDiv({id: argSpecContainerId})
        );
      }

      const argSpecContainer = (
        document.getElementById(argSpecContainerId) || createContainer()
      );

      emptyElement(argSpecContainer);

      if (availableChartTypeSet.has(currentValue)) {
        this.addArgumentsToVisualizationWindow(currentValue, argSpecContainer);
      }
    };

    const chartTypes = createDataList({id: 'chartTypes'});
    for (const chartType of AVAILABLE_CHART_TYPES) {
      chartTypes.appendChild(createOption({value: chartType}));
    }

    visualizationWindow.appendChild(createButton({
      id: 'closeAddVisualizationButton',
      html: '&times;',
      mousedown: () => {
        hide(visualizationWindow);
        this.showAddWindow();
      },
    }));

    visualizationWindow.appendChild(createInput({
      type: 'text',
      name: 'chartType',
      id: 'chartTypeInput',
      list: 'chartTypes',
      change: addVisualizationArguments,
    }));

    visualizationWindow.appendChild(chartTypes);
  }

  async transformDataView({dataView, addTransforms = [], deleteTransforms = []}) {
    const url = buildRequest(
      services.transformDataView,
      {
        data_view_id: dataView.id,
        add_transforms: addTransforms.map(transform => transform.serialize()),
        del_transforms: deleteTransforms.map(transform => transform.serialize()),
      },
    );
    console.info('transformDataView URL', url);

    try {
      const response = await fetch(url);
      if (response.status !== HTTP_OK) {
        console.error('error processing request, status: ' + response.status);
        return;
      }

      const result = await response.json();
      console.info('result: ', result);

      if (!result.error) {
        app.dataView = DataView.deserialize(result['data_view']);
        const dataViewId = result['data_view_id'];
        console.info("transformDataView", app.dataView.id, dataViewId);
        console.info(app.dataView);

      } else {
        console.error(
          'DataView transform error', result.error,
          'add:', addTransforms,
          'del:', deleteTransforms,
          'error:', result.msg,
        );
      }
    } catch(err) {
      console.warn('Fetch Error:', err);
    }
  }

  addArgumentsToVisualizationWindow(chartType, parentElement) {
    const parametersTableClassName = 'transformArgumentTable';
    const labelClassName = 'transformArgumentLabel';

    const parametersTable = createTable({cls: parametersTableClassName });
    const row = parametersTable.insertRow();

    const cell = row.insertCell();

    cell.appendChild(createInput({
      id: 'columnNameInput',
      type: 'text',
      name: 'columnName',
      list: 'columnNames',
      change: e => {
        const columnName = e.currentTarget.value;
        const chartType = document.getElementById('chartTypeInput').value;

        const parameters = {'column': columnName};

        app.chartManager.createChart(chartType, parameters);
        app.chartManager.showChartWindow();
        app.chartManager.update();
      },
    }));

    const columnNames = createDataList({id: 'columnNames'});
    for (const label of app.dataView.labels) {
      columnNames.appendChild(createOption({value: label.name}));
    }

    cell.appendChild(columnNames);

    row.insertCell().appendChild(
      createLabel({text: 'column', cls: labelClassName})
    );

    parentElement.appendChild(parametersTable);
  }
}


class ArgumentBuilder {
  static inputIdFromDef(parameterDef) {
    return 'parameterInput__' + parameterDef.name;
  }

  static indexIdFromLabelId(labelId) {
    return 'index_' + labelId;
  }

  buildColumnNameInput(labelId, parameterDef, onkeydown, parentElement) {
    console.info('buildColumnNameInput');
    const container = createSpan({});
    const inputId = ArgumentBuilder.inputIdFromDef(parameterDef);

    const columnNames = createSelect({
      id: inputId,
      keydown: onkeydown,
    });

    for (const label of app.dataView.labels) {
      columnNames.appendChild(createOption({
        value: label.name,
        text: label.name,
        keydown: onkeydown,
      }));
    }
    container.appendChild(columnNames);
    parentElement.appendChild(container);
  }

  buildDateRangeInput(labelId, parameterDef, onkeydown, parentElement) {
    const container = createSpan({});

    container.appendChild(createInput({
      id: 'from_' + labelId,
      type: 'date',
      keydown: onkeydown,
    }));

    container.appendChild(createInput({
      id: 'to_' + labelId,
      type: 'date',
      keydown: onkeydown,
    }));

    parentElement.appendChild(container);
  }

  buildDateRangeInputList(labelId, parameterDef, onkeydown, parentElement) {
    const indexId = ArgumentBuilder.indexIdFromLabelId(labelId);
    const initialIndex = 2;
    const indexDelta = 2;

    function getOrCreateIndex(indexId) {
      if (document.getElementById(indexId)) {
        return document.getElementById(indexId);
      } else {
        const indexElement = createInput({
          id: indexId,
          value: initialIndex,
          style: {
            display: 'none',
          },
        });
        parentElement.appendChild(indexElement);
        return indexElement;
      }
    }

    const indexElement = getOrCreateIndex(indexId, parentElement);

    const index = parseInt(indexElement.value);
    console.info("INFO", index, indexElement.value);

    const container = createDiv({});

    container.appendChild(createDiv({
      id: ArgumentBuilder.indexIdFromLabelId(labelId)
    }));

    container.appendChild(createInput({
      id: labelId + '_' + (index - 1),
      type: 'date',
      keydown: onkeydown,
    }));

    container.appendChild(createInput({
      id: labelId + '_' + index,
      type: 'date',
      keydown: onkeydown,
    }));

    container.appendChild(createButton({
      cls: 'transformAddArgumentButton',
      text: '+',
      mousedown: e => ifPrimaryClick(e, () => {
        const indexElement = getOrCreateIndex(indexId, parentElement);
        indexElement.value = parseInt(indexElement.value) + indexDelta;
        this.buildDateRangeInputList(
          labelId, parameterDef, onkeydown, parentElement
        );
      })
    }));

    container.appendChild(createButton({
      cls: 'transformAddArgumentButton',
      text: '-',
      mousedown: e => ifPrimaryClick(e, () => {
        const indexElement = getOrCreateIndex(indexId, parentElement);
        const index = indexElement ? parseInt(indexElement.value) : 0;
        if (index / indexDelta > 1) {
          indexElement.value = index - indexDelta;
          container.parentElement.removeChild(container);
        }
      })
    }));

    parentElement.appendChild(container);
  }

  buildColumnNameInputList(labelId, parameterDef, onkeydown, parentElement) {
    const indexId = ArgumentBuilder.indexIdFromLabelId(labelId);

    function getOrCreateIndex(indexId) {
      if (document.getElementById(indexId)) {
        return document.getElementById(indexId);
      } else {
        const indexElement = createInput({
        id: indexId,
        value: 1,
        style: {
          display: 'none',
          },
        });
        parentElement.appendChild(indexElement);
        return indexElement;
      }
    }

    const indexElement = getOrCreateIndex(indexId, parentElement);

    const index = indexElement ? parseInt(indexElement.value) : 0;
    const indexedLabelId = labelId + '_' + index;
    console.info("INFO", index, indexElement.value);

    const container = createDiv({});

    const columnNames = createSelect({
      id: indexedLabelId,
      keydown: onkeydown,
    });

    for (const label of app.dataView.labels) {
      columnNames.appendChild(createOption({
        value: label.name,
        text: label.name,
        keydown: onkeydown,
      }));
    }

    container.appendChild(columnNames);

    container.appendChild(createButton({
      cls: 'transformAddArgumentButton',
      text: '+',
      mousedown: e => ifPrimaryClick(e, () => {
        const indexElement = getOrCreateIndex(indexId, parentElement);
        indexElement.value = parseInt(indexElement.value) + 1;
        this.buildColumnNameInputList(
          labelId, parameterDef, onkeydown, parentElement
        );
      })
    }));

    container.appendChild(createButton({
      cls: 'transformAddArgumentButton',
      text: '-',
      mousedown: e => ifPrimaryClick(e, () => {
        const indexElement = getOrCreateIndex(indexId, parentElement);
        const index = parseInt(indexElement.value);
        if (index > 1) {
          indexElement.value = index - 1;
          container.parentElement.removeChild(container);
        }
      })
    }));

    parentElement.appendChild(container);
  }

  buildColumnNameDataListInput(labelId, parameterDef, onkeydown, parentElement) {
    console.info('buildColumnNameInput');
    const container = createSpan({});
    const datalistId = 'parameterDatalist__' + parameterDef.label;

    const columnNames = createDataList({id: datalistId});
    for (const label of app.dataView.labels) {
      columnNames.appendChild(createOption({value: label.name}));
    }
    container.appendChild(columnNames);

    container.appendChild(createInput({
      id: labelId,
      type: 'text',
      list: datalistId,
      keydown: onkeydown,
    }));

    parentElement.appendChild(container);
  }

  buildTextInput(labelId, parameterDef, onkeydown, parentElement) {
    const input = createInput({
      id: labelId,
      type: 'text',
      keydown: onkeydown,
    });

    parentElement.appendChild(input);
  }
}