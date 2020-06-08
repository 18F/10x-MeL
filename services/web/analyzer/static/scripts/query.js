'use strict';

class ParameterDef {
  static TYPE_TEXT = 'text';
  static TYPE_TEXT_LIST = 'text_list';
  static TYPE_INTEGER = 'int';
  static TYPE_FLOAT = 'float';
  static TYPE_COLUMN_NAME = 'column_name';

  static SEPARATOR = ',';

  static KEY_TYPE = 'type';
  static KEY_NAME = 'name';
  static KEY_LABEL = 'label';
  static KEY_EXAMPLE = 'example';

  constructor(type, name, label, example) {
    this.type = type;
    this.name = name;
    this.label = label;
    this.example = example;
  }

  static fromDict(dict) {
    return new ParameterDef(
      dict[ParameterDef.KEY_TYPE],
      dict[ParameterDef.KEY_NAME],
      dict[ParameterDef.KEY_LABEL],
      dict[ParameterDef.KEY_EXAMPLE],
    );
  }

  static processValue(rawValue, type) {
    if (type === ParameterDef.TYPE_TEXT) {
      return rawValue.toString();

    } else if (type === ParameterDef.TYPE_COLUMN_NAME) {
      return rawValue.toString();

    } else if (type === ParameterDef.TYPE_INTEGER) {
      return parseInt(rawValue);

    } else if (type === ParameterDef.TYPE_FLOAT) {
      return parseFloat(rawValue);

    } else if (type === ParameterDef.TYPE_TEXT_LIST) {
      if (typeof rawValue === 'string') {
        const sep = ParameterDef.SEPARATOR;
        console.info('processValue TEXT_LIST', rawValue.split(sep).map(s => s.trim()));
        return rawValue.split(sep).map(s => s.trim())
      } else {
        return rawValue;
      }
    } else {
      throw 'Unrecognized Parameter type:' + type;
    }
  }
}

class ConstraintDef {
  static KEY_TYPE = 'type';
  static KEY_DESC = 'description';
  static KEY_PARAMS = 'params';
  static KEY_OPS = 'ops';

  constructor(type, description, paramDefs, operations) {
    this._type = type;
    this._description = description;
    this._parameters = paramDefs;
    this._operations = operations;
  }

  get type() {
    return this._type;
  }

  get description() {
    return this._description;
  }

  get parameters() {
    return this._parameters;
  }

  get operations() {
    return this._operations;
  }

  static fromDict(dict) {
    return new ConstraintDef(
      dict[ConstraintDef.KEY_TYPE],
      dict[ConstraintDef.KEY_DESC],
      dict[ConstraintDef.KEY_PARAMS].map(p => ParameterDef.fromDict(p)),
      dict[ConstraintDef.KEY_OPS],
    );
  }
}


class Constraint {
  constructor(constraintDef, parameters, operation) {
    console.info(constraintDef, parameters);
    this._def = constraintDef;
    this.parameters = parameters;
    this.operation = operation;
  }

  get type() {
    return this._def.type;
  }

  get typeWithSpaces() {
    return this.type.replace(/([A-Z])/g, ' $1').toLocaleLowerCase().trim();
  }

  get description() {
    return this._def.description;
  }

  /*
  TODO: Add when tests exist
  get textDescription() {
    const tokens = [];
    for (const field of this.description) {
      if (field.startsWith('{') && field.endsWith('}')) {
        const key = field.substr(1, field.length - 2);
        tokens.push(this.parameters[key]);
      } else {
        tokens.push(field);
      }
    }

    return tokens.join('');
  }
   */

  get richTextDescription() {
    const VALUE_SPEC = '{}';
    const paragraph = document.createElement('p');

    for (const field of this.description) {
      if (field.startsWith(VALUE_SPEC[0]) && field.endsWith(VALUE_SPEC[1])) {

        const key = field.substr(1, field.length - 2);
        paragraph.appendChild(createSpan({
          cls: 'constraintDescriptionFieldValue',
          text: this.parameters[key],
        }));

      } else {
        paragraph.appendChild(createSpan({
          cls: 'constraintDescriptionFieldOperator',
          text: field,
        }));
      }
    }

    return paragraph;
  }

  toDict() {
    return {
      name: this.type,
      args: this.parameters,
    };
  }

  serialize() {
    const parameterDefs = this._def.parameters;
    const parameters = this.parameters;

    const values = [this.type, this.operation];
    for (const parameterDef of parameterDefs) {
      const parameterType = parameterDef.type;
      const parameterName = parameterDef.name;

      const value = parameters[parameterName];
      values.push(ParameterDef.processValue(value, parameterType));
    }

    return values;
  }

  static deserialize(values) {
    /*
    Constraints are serialized as follows:
      [constraintType, param_0, param_1, ..., param_n]
     */
    const constraintType = values.shift();
    const operation = values.shift();

    const constraintDef = app.transformManager.constraintDefByType(constraintType);
    const parameterDefs = constraintDef.parameters;
    const parameters = {};

    for (const parameterDef of parameterDefs) {
      const parameterType = parameterDef.type;
      const parameterName = parameterDef.name;

      const value = values.shift();
      parameters[parameterName] = ParameterDef.processValue(value, parameterType);
      //parameters[parameterName] = values.shift();
    }

    return new Constraint(
      constraintDef,
      parameters,
      operation,
    )
  }
}


class Dataset {
  constructor(id, filename, name) {
    this.id = id;
    this.filename = filename;
    this.name = name;
  }

  static fromDict(dict) {
    return new Dataset(dict[0], dict[1], dict[2]);
  }
}

class Label {
  constructor(name, width, fontSize) {
    this.name = name;
    this.width = parseInt(width);
    this.fontSize = parseFloat(fontSize);
  }

  get hash() {
    return this.name.replace(/ /g, '_');
  }

  static deserialize(dict) {
    const KEY_NAME = 'n';
    const KEY_WIDTH = 'w';
    const KEY_FONT_SIZE = 's';
    return new Label(
      dict[KEY_NAME],
      parseInt(dict[KEY_WIDTH]),
      parseFloat(dict[KEY_FONT_SIZE]),
    );
  }
}

class DataView {
  constructor(id, parentId, datasetId, userId, labels, transforms) {
    this.id = id;
    this.parentId = parentId;
    this.userId = userId;
    this.datasetId = datasetId;
    this.labels = labels;
    this.transforms = transforms;
  }

  static deserialize(dict) {
    const KEY_COLUMN_LABELS = 'column_labels';
    const KEY_DATASET_ID = 'dataset_id';
    const KEY_DATA_VIEW_ID = 'id';
    const KEY_PARENT_DATA_VIEW_ID = 'parent_id';
    const KEY_TRANSFORMS = 'transforms';
    const KEY_USER_ID = 'user_id';

    const dataViewId = dict[KEY_DATA_VIEW_ID];
    const parentDataViewId = dict[KEY_PARENT_DATA_VIEW_ID];
    const userId = dict[KEY_USER_ID];
    const datasetId = dict[KEY_DATASET_ID];

    const columnLabelsListDict = dict[KEY_COLUMN_LABELS] || [];
    const columnLabels = columnLabelsListDict.map(dict => Label.deserialize(dict));

    const transformsListDict = dict[KEY_TRANSFORMS] || [];
    console.info('transformsListDict', transformsListDict);
    const transforms = transformsListDict.map(dict => Constraint.deserialize(dict));

    return new DataView(
      dataViewId,
      parentDataViewId,
      userId,
      datasetId,
      columnLabels,
      transforms,
    );
  }
}
