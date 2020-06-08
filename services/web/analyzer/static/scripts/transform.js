
class TransformManager {
  constructor(
    buildConstraintWindowId,
    buildVisualizationWindowId,
    addConstraintWindowId,
    addFilterButtonId,
    addEnrichmentButtonId,
    addVisualizationButtonId,
    constraintListWindowId,
    constraintListTableId,
  ) {
    this.buildConstraintWindowId = buildConstraintWindowId;
    this.buildVisualizationWindowId = buildVisualizationWindowId;
    this.addConstraintWindowId = addConstraintWindowId;
    this.addFilterButtonId = addFilterButtonId;
    this.addEnrichmentButtonId = addEnrichmentButtonId;
    this.addVisualizationButtonId = addVisualizationButtonId;
    this.constraintListWindowId = constraintListWindowId;
    this.constraintListTableId = constraintListTableId;

    this.buildConstraintWindow = null;
    this.buildVisualizationWindow = null;
    this.addWindow = null;
    this.addFilterButton = null;
    this.addEnrichmentButton = null;
    this.addVisualizationButton = null;
    this.constraintListWindow = null;

    this._constraintDefs = [];
    this._constraintDefByType = {};
  }

  init() {
    console.group('TransformManager.init');
    const initElements = () => {
      this.buildConstraintWindow = document.getElementById(this.buildConstraintWindowId);
      this.buildVisualizationWindow = document.getElementById(this.buildVisualizationWindowId);
      this.addWindow = document.getElementById(this.addConstraintWindowId);
      this.addFilterButton = document.getElementById(this.addFilterButtonId);
      this.addEnrichmentButton = document.getElementById(this.addEnrichmentButtonId);
      this.addVisualizationButton = document.getElementById(this.addVisualizationButtonId);
      this.constraintListWindow = document.getElementById(this.constraintListWindowId);
    };

    const initEventHandler = () => {
      this.addFilterButton.onmousedown = e => ifPrimaryClick(e, () => {
        console.info('addFilterButton mousedown', e);
        this.hideAddWindow();
        this.createNewBuildWindow(ConstraintType.FILTER);
        show(this.buildConstraintWindow);
      });

      this.addEnrichmentButton.onmousedown = e => ifPrimaryClick(e, () => {
        console.info('addEnrichmentButton mousedown', e);
        this.hideAddWindow();
        this.createNewBuildWindow(ConstraintType.ENRICHMENT);
        show(this.buildConstraintWindow);
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
      this.fetchConstraintDefs(),
    ]);
  }

  createNewBuildWindow(constraintType) {
    emptyElement(this.buildConstraintWindow);
    this.addConstraintNameSpecification(this.buildConstraintWindow, constraintType);
  }

  get constraintDefs() {
    return this._constraintDefs;
  }

  set constraintDefs(constraintDefs) {
    this._constraintDefs = constraintDefs;
    for (const constraintDef of this._constraintDefs) {
      this._constraintDefByType[constraintDef.type] = constraintDef;
    }
  }

  async fetchConstraintDefs() {
    const url = HOST + services.getConstraintDefs;

    const response = await fetch(url);
    if (response.status !== HTTP_OK) {
      console.log('Error processing request, status: ' + response.status);
      return;
    }

    try {
      const result = await response.json();
      console.log('constraintDefs: ', result, 'this', this);
      this.constraintDefs = result.map(def => ConstraintDef.fromDict(def));
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

  get constraintDefTypes() {
    return this.constraintDefs.map(constraint => constraint.type);
  }

  constraintDefByType(constraintType) {
    console.info('constraintDefByType', Object.keys(this._constraintDefByType));
    return this._constraintDefByType[constraintType];
  }

  addConstraintNameSpecification(parentElement, constraintType) {
    console.info(constraintType);
    const parameterSpecificationContainerId = 'constraintArgumentSpecificationContainer';
    const constraintNameContainer = createDiv({id: 'constraintNameContainer'});

    const constraintNames = this.constraintDefTypes;
    const constraintNameSet = new Set(constraintNames);

    const addConstraintArguments = e => {
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

      if (constraintNameSet.has(currentValue)) {
        this.addArgumentsToBuildWindow(currentValue, parameterSpecificationContainer, constraintType);
      }
    };

    const constraintTypeInput = createInput({
      id: 'constraintTypeInput',
      type: 'text',
      name: 'constraintType',
      list: 'constraintTypes',
      change: addConstraintArguments,
    });

    const datalist = createDataList({id: 'constraintTypes'});
    for (const constraintName of constraintNames) {
      const constraintDef = this.constraintDefByType(constraintName);
      if (constraintDef.operations.indexOf(constraintType) === -1) {
        continue;
      }

      datalist.appendChild(createOption({value: constraintName}));
    }

    const closeButton = createButton({
      id: 'closeAddConstraintButton',
      html: '&times;',
      mousedown: () => {
        hide(this.buildConstraintWindow);
        this.showAddWindow();
      },
    });

    parentElement.appendChild(constraintNameContainer);
    constraintNameContainer.appendChild(closeButton);
    constraintNameContainer.appendChild(constraintTypeInput);
    constraintNameContainer.appendChild(datalist);
  }


  addArgumentsToBuildWindow(constraintName, parentElement, constraintType) {
    const exampleClass = 'constraintArgumentExample';
    const labelClass = 'constraintArgumentLabel';
    const parametersTableClassName = 'constraintArgumentTable';
    const parameterDefs = this.constraintDefByType(constraintName).parameters;

    const parametersTable = createTable({cls: parametersTableClassName});
    for (const parameterDef of parameterDefs) {

      console.info("parameterDef.type", parameterDef.type);

      const labelId = 'parameterInput__' + parameterDef.label;
      const datalistId = 'parameterDatalist__' + parameterDef.label;

      const elements = [];
      if (parameterDef.type === ParameterDef.TYPE_COLUMN_NAME) {
        const container = createSpan({});

        const columnNames = createDataList({id: datalistId});
        for (const label of app.dataView.labels) {
          columnNames.appendChild(createOption({value: label.name}));
        }
        container.appendChild(columnNames);

        container.appendChild(createInput({
          id: labelId,
          type: 'text',
          list: datalistId,
          keydown: e => ifEnterPressed(e, () => this.completeTransform(constraintName, constraintType)),
        }));

        elements.push(container);
      } else {
        elements.push(createInput({
          id: labelId,
          type: 'text',
          keydown: e => ifEnterPressed(e, () => this.completeTransform(constraintName, constraintType)),
        }));

      }

      elements.push(createLabel({cls: labelClass, text: parameterDef.label}));

      if (parameterDef.example) {
        elements.push(createLabel({
          cls: exampleClass, text: parameterDef.example,
        }));
      }

      const row = parametersTable.insertRow();
      for (const elem of elements) {
        elem && row.insertCell().appendChild(elem)
      }
    }

    parentElement.appendChild(parametersTable);
  }

  async completeTransform(constraintName, constraintType) {
    const buildWindow = document.getElementById('buildConstraintWindow');
    const addWindow = document.getElementById('addConstraintWindow');
    const constraintTypeInput = document.getElementById('constraintTypeInput');
    const parameterDefs = this.constraintDefByType(constraintName).parameters;

    function validate() {
      console.warn('Validation not implemented');
      return true;
    }

    if (validate()) {
      const parameters = {};

      for (const parameterDef of parameterDefs) {
        const inputId = 'parameterInput__' + parameterDef.label;
        parameters[parameterDef.name] = document.getElementById(inputId).value;
      }
      console.info('save', constraintTypeInput.value, parameters);
      const constraintDefType = constraintTypeInput.value;
      const constraintDef = this.constraintDefByType(constraintDefType);
      const constraint = new Constraint(constraintDef, parameters, constraintType);

      await this.transformDataView({dataView: app.dataView, addTransforms: [constraint]});
      hide(buildWindow);

      emptyElement(buildWindow);
      show(addWindow);

    } else {
      console.error('should handle validation errors here');
    }
  }

  async updateConstraintList() {
    emptyElement(this.constraintListWindow);

    const constraintTable = createDiv({
      id: this.constraintListTableId,
    });

    let i = 0;
    for (const constraint of app.dataView.transforms) {
      const baseId = 'constraint' + i;

      const entry = createDiv({
        cls: 'constraintEntry',
        style: { 'display': 'inline-flex' },
      });

      const button = createButton({
        id: baseId + 'RemoveButton',
        cls: 'removeConstraintButton',
        html: '&times;',
        mousedown: e => ifPrimaryClick(e, () => {
          console.info('removing constraint', i, constraint);
          return this.transformDataView({dataView: app.dataView, deleteTransforms: [constraint]});
        }),
      });

      const constraintType = createDiv({
        id: baseId + 'Type',
        cls: 'constraintListType',
      });

      constraintType.appendChild(constraint.richTextDescription);
      entry.appendChild(button);
      entry.appendChild(constraintType);
      constraintTable.appendChild(entry);

      i += 1;
    }

    this.constraintListWindow.appendChild(constraintTable);
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
    const parametersTableClassName = 'constraintArgumentTable';
    const labelClassName = 'constraintArgumentLabel';

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
