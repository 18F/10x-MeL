from pathlib import Path
from typing import List

from flask import Flask, jsonify, request
from flask_cors import CORS

from analyzer.session import Session

import logging
from logging.handlers import RotatingFileHandler


LOG_FILENAME = "log/app.log"
LOG_MAX_SIZE_BYTES = 500000
LOG_BACKUP_COUNT = 5

DATA_DIR = Path("/data")
CONFIG_DIR = Path("/config")

CONFIG_FILENAME = "config.json"
DATA_VIEWS_FILENAME = "data_views.json"
USERS_FILENAME = "users.json"
DATASETS_FILENAME = "datasets.json"
DATA_VIEW_HISTORY_FILENAME = "data_view_history.json"

INDEX_FILENAME = "index.html"

IGNORE_FILENAMES = {"README.md"}


handler = RotatingFileHandler(
    filename=LOG_FILENAME,
    maxBytes=LOG_MAX_SIZE_BYTES,
    backupCount=LOG_BACKUP_COUNT,
)

logging.basicConfig(
    level=logging.DEBUG,
    filename=LOG_FILENAME,
)

log = logging.getLogger(__name__)
log.addHandler(handler)


session = Session(
    config_dir=CONFIG_DIR,
    data_dir=DATA_DIR,
    users_filename=USERS_FILENAME,
    datasets_filename=DATASETS_FILENAME,
    data_views_filename=DATA_VIEWS_FILENAME,
    data_view_history_filename=DATA_VIEW_HISTORY_FILENAME,
)

app = Flask(__name__, static_folder="static")
CORS(app)


@app.route("/")
def index():
    return app.send_static_file(INDEX_FILENAME)


@app.route("/list_datasets")
def show_data_dir() -> str:
    """The list of viewable files in the data directory"""
    def filter_filenames(filenames: List[str]) -> List[str]:
        filtered_filenames = []
        for filename in filenames:
            if filename in IGNORE_FILENAMES:
                continue
            if filename.startswith("."):
                continue
            filtered_filenames.append(filename)
        return filtered_filenames

    return jsonify(
        filter_filenames(
            [p.name for p in sorted(Path(DATA_DIR).iterdir())]
        )
    )


@app.route("/list_data_views_for_active_user")
def list_data_views_for_active_user() -> str:
    """The DataViews associated with the active User"""
    data_views = session.data_views_for_active_user()

    return jsonify([data_view.serialize() for data_view in data_views])


@app.route("/list_data_views_for_active_user_and_dataset")
def list_data_views_for_active_user_and_dataset() -> str:
    """The DataViews associated with the active User and active Dataset"""
    data_views = session.data_views_for_active_user(active_dataset=True)
    return jsonify([data_view.serialize() for data_view in data_views])


@app.route("/list_users")
def list_users() -> str:
    """The list of Users"""
    return jsonify(
        [user.serialize() for user in session.user_handler.find()]
    )


@app.route("/show_datasets", methods=["GET"])
def show_datasets() -> str:
    """The list of Datasets"""
    match_string = request.args.get("match", "")

    return jsonify(
        [dataset.serialize() for dataset in session.dataset_handler.find(match_string)]
    )


@app.route("/active_dataset", methods=["GET"])
def active_dataset() -> str:
    # capture the filename of a new dataset, if provided
    new_dataset_key = "new_dataset"

    if new_dataset_key in request.args:
        new_dataset = request.args.get("new_dataset", "")
        session.active_dataset = new_dataset
        log.info("setting active dataset to %s", new_dataset)

    if session.active_dataset:
        filename = session.active_dataset.filename
    else:
        filename = ""

    return jsonify({"active_dataset": filename})


@app.route("/active_user_id", methods=["GET"])
def active_user_id() -> str:
    log.info("Session user_id = %s", session.active_user.id)
    return jsonify({"user_id": session.active_user.id})


@app.route("/active_data_view")
def active_data_view() -> str:
    data_view = session.active_data_view
    if data_view:
        return jsonify(data_view.serialize())
    else:
        return jsonify({})


@app.route("/refresh_data_views")
def refresh_data_views():
    session.refresh_data_views()
    return active_data_view()


@app.route("/get_labels_from_active_dataset", methods=["GET"])
def get_labels_from_active_dataset():
    default_label_type = "original"
    label_type = request.args.get("type", default_label_type)

    try:
        labels = session.get_labels_from_active_dataset(label_type)
        return jsonify(labels.serialize())

    except Exception as exc:
        log.error(exc)
        return jsonify([])


@app.route("/get_data_from_active_dataset")
def get_data_from_active_dataset():
    labels, entries = session.get_data_from_active_dataset()
    return jsonify(dict(labels=labels.serialize(), entries=entries))


@app.route("/hello_world")
def hello_world():
    return jsonify(hello="world")
