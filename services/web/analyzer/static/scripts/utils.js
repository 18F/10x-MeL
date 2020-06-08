'use strict';

const HTTP_OK = 200;

const Color = {
  WHITE: 'rgb(255,255,255)',
  GREY32: 'rgb(32,32,32)',
  GREY64: 'rgb(64,64,64)',
};
Object.freeze(Color);


function ifPrimaryClick(e, func) {
  if (e.button === 0) {
    func(e);
  }
}


function ifEnterPressed(e, func) {
  if (e.key === 'Enter') {
    func(e);
  }
}

function ifEscPressed(e, func) {
  if (e.key === 'Escape') {
    func(e);
  }
}



const px = value => value + 'px';


function emptyElement(element) {
  const numNodesToRemove = element.childElementCount;
  for (let i = 0; i < numNodesToRemove; i++) {
    element.removeChild(element.childNodes[0]);
  }
}


function isIterable(obj) {
  // checks for null and undefined
  if (obj === null) {
    return false;
  }
  return typeof obj[Symbol.iterator] === 'function';
}


function objHasEntries(obj) {
  for (const x in obj) {
    return true;
  }
  return false;
}


function hide(element) {
  element.style.display = 'none';
}


function show(element) {
  element.style.display = 'block';
}

function createElement(tag, args) {
  const e = document.createElement(tag);

  if (args.cls) {
    if (typeof args.cls === 'string') {
      args.cls = [args.cls];
    }

    for (const className of args.cls) {
      e.classList.add(className);
    }

    delete args.cls;
  }

  if (args.id) {
    e.setAttribute('id', args.id);
    delete args.id;
  }

  if (args.html !== undefined) {
    e.innerHTML = args.html;
    delete args.html;
  }

  if (args.text !== undefined) {
    e.textContent = args.text;
    delete args.text;
  }

  if (args.mouseleave) {
    e.onmouseleave = args.mouseleave;
    delete args.mouseleave;
  }

  if (args.mousedown) {
    e.onmousedown = args.mousedown;
    delete args.mousedown;
  }

  if (args.keydown) {
    e.onkeydown = args.keydown;
    delete args.keydown;
  }

  if (args.change) {
    e.onchange = args.change;
    delete args.change;
  }

  if (args.value) {
    e.setAttribute('value', args.value);
    delete args.value;
  }

  if (args.name) {
    e.setAttribute('name', args.name);
    delete args.name;
  }

  if (args.type) {
    e.setAttribute('type', args.type);
    delete args.type;
  }

  if (args.list) {
    e.setAttribute('list', args.list);
    delete args.list;
  }

  if (args.min) {
    e.setAttribute('min', args.min);
    delete args.min;
  }

  if (args.max) {
    e.setAttribute('max', args.max);
    delete args.max;
  }

  for (const property in args.style) {
    e.style.setProperty(property, args.style[property]);
    delete args.style[property];
  }

  if (args.style && Object.keys(args.style).length !== 0) {
      console.error('style has unprocessed properties:', args.style);
  }
  delete args.style;

  if (Object.keys(args).length !== 0) {
    console.error('unprocessed arguments:', args)
  }

  return e;
}

function createDiv() {
  return createElement('div', arguments[0]);
}

function createDetails() {
  return createElement('details', arguments[0]);
}

function createNav() {
  return createElement('nav', arguments[0]);
}

function createSection() {
  return createElement('section', arguments[0]);
}

function createH1() {
  return createElement('h1', arguments[0]);
}

function createArticle() {
  return createElement('article', arguments[0]);
}

function createH2() {
  return createElement('h2', arguments[0]);
}

function createH3() {
  return createElement('h3', arguments[0]);
}

function createH4() {
  return createElement('h4', arguments[0]);
}

function createOption() {
  return createElement('option', arguments[0]);
}

function createInput() {
  return createElement('input', arguments[0]);
}

function createLabel() {
  return createElement('label', arguments[0]);
}

function createButton() {
  return createElement('button', arguments[0]);
}

function createDataList() {
  return createElement('datalist', arguments[0]);
}

function createSpan() {
  return createElement('span', arguments[0]);
}

function createLink() {
  return createElement('a', arguments[0]);
}

function createTable() {
  return createElement('table', arguments[0]);
}

function createColumn() {
  return createElement('col', arguments[0]);
}

function createColumnGroup() {
  return createElement('colgroup', arguments[0]);
}
