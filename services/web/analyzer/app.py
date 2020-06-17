from pathlib import Path
from typing import Dict, List, Optional
import json

from flask import Flask, jsonify, request
from flask_cors import CORS

from analyzer.session import (
    Session, UserId, DatasetId, DataViewId, TransformList,
)

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
TAGS_PREFIX = "tags"

PAYLOAD_KEY = "q"

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
    tag_prefix=TAGS_PREFIX,
)


app = Flask(__name__, static_folder="static")
CORS(app)


def extract_payload() -> Optional[Dict]:
    json_payload = request.args.get(PAYLOAD_KEY)
    try:
        return json.loads(json_payload)
    except json.JSONDecodeError as exc:
        log.error("%s: could not decode payload: %s", exc, json_payload)
        return None


@app.route("/")
def index():
    return app.send_static_file(INDEX_FILENAME)


@app.route("/heartbeat")
def ping():
    return "heartbeat"


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


@app.route("/most_recent_data_view", methods=["GET"])
def most_recent_data_view() -> str:
    key_user_id = "user_id"
    key_dataset_id = "dataset_id"
    payload = extract_payload()

    user_id_str = payload.get(key_user_id, None)

    if not user_id_str:
        return jsonify(
            dict(error=1, msg=f'user_id must be specified, found "{user_id_str}"')
        )

    user_id = UserId(user_id_str)
    dataset_id_str = payload.get(key_dataset_id, None)

    try:
        if not dataset_id_str:
            data_view = session.get_most_recent_data_view(user_id=user_id)
        else:
            data_view = session.get_most_recent_data_view(
                user_id=user_id,
                dataset_id=DatasetId(dataset_id_str)
            )

        return jsonify(dict(error=0, data_view=data_view.serialize()))
    except ValueError as exc:
        return jsonify(dict(error=1, data_view=None, msg=str(exc)))


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


@app.route("/set_most_recent_dataset", methods=["GET"])
def set_most_recent_dataset() -> str:
    key_user_id = "user_id"
    key_filename = "filename"

    payload = extract_payload()

    user_id_str = payload.get(key_user_id, None)
    filename = payload.get(key_filename, "").strip()

    if not user_id_str:
        return jsonify(dict(error=1, msg="no user_id specified"))
    if not filename:
        return jsonify(dict(error=2, msg="no filename specified"))

    user_id = UserId(user_id_str)

    try:
        dataset = session.set_most_recent_dataset(user_id, filename)
        return jsonify(dict(dataset=dataset.serialize(), user_id=user_id))
    except ValueError as exc:
        return jsonify(dict(error=3, msg=str(exc), dataset=None, user_id=user_id))


@app.route("/add_tags", methods=["GET"])
def add_tags() -> str:
    key_data_view_id = "data_view_id"
    key_primary_key = "primary_key"
    key_primary_key_name = "primary_key_name"
    key_tags = "tags"

    payload = extract_payload()

    data_view_id_str: str = payload.get(key_data_view_id, None)
    primary_key: str = payload.get(key_primary_key, None)
    primary_key_name: str = payload.get(key_primary_key_name, None)
    tags: List[str] = payload.get(key_tags, None)

    if not data_view_id_str:
        return jsonify(dict(error=1, msg="no data_view_id specified"))
    elif not primary_key:
        return jsonify(dict(error=2, msg="no primary_key specified"))
    elif not primary_key_name:
        return jsonify(dict(error=3, msg="no primary_key_name specified"))
    elif not tags:
        return jsonify(dict(error=4, msg="no tags specified"))

    data_view_id = DataViewId(data_view_id_str)

    try:
        updated_tags = session.add_tags(
            tags=tags,
            primary_keys=[primary_key],
            primary_key_name=primary_key_name,
            data_view_id=data_view_id,
        )
        return jsonify(dict(primary_key=primary_key, tags=list(updated_tags)))
    except ValueError as exc:
        return jsonify(dict(error=5, msg=str(exc)))


@app.route("/remove_tags", methods=["GET"])
def remove_tags() -> str:
    key_data_view_id = "data_view_id"
    key_primary_key = "primary_key"
    key_primary_key_name = "primary_key_name"
    key_tags = "tags"

    payload = extract_payload()

    data_view_id_str: str = payload.get(key_data_view_id, None)
    primary_key: str = payload.get(key_primary_key, None)
    primary_key_name: str = payload.get(key_primary_key_name, None)
    tags: List[str] = payload.get(key_tags, None)

    if not data_view_id_str:
        return jsonify(dict(error=1, msg="no data_view_id specified"))
    elif not primary_key:
        return jsonify(dict(error=2, msg="no primary_key specified"))
    elif not primary_key_name:
        return jsonify(dict(error=3, msg="no primary_key_name specified"))
    elif not tags:
        return jsonify(dict(error=4, msg="no tags specified"))

    data_view_id = DataViewId(data_view_id_str)

    try:
        updated_tags = session.remove_tags(
            tags=tags,
            primary_keys=[primary_key],
            primary_key_name=primary_key_name,
            data_view_id=data_view_id,
        )
        return jsonify(dict(primary_key=primary_key, tags=list(updated_tags)))
    except ValueError as exc:
        return jsonify(dict(error=5, msg=str(exc)))


@app.route("/get_tags", methods=["GET"])
def get_tags() -> str:
    key_data_view_id = "data_view_id"
    key_primary_keys = "primary_keys"

    payload = extract_payload()

    data_view_id_str: str = payload.get(key_data_view_id, None)
    primary_keys: List[str] = payload.get(key_primary_keys, [])

    data_view_id = DataViewId(data_view_id_str)

    try:
        tag_map = session.get_tags(primary_keys, data_view_id)
        return jsonify(tag_map)
    except ValueError as exc:
        return jsonify(dict(error=5, msg=str(exc)))


@app.route("/raw_data_for_data_view")
def raw_data_for_data_view():
    key_data_view_id = "data_view_id"
    key_sort_label = "sort_label"
    key_sort_direction = "sort_dir"
    try:
        payload = extract_payload()
        data_view_id = DataViewId(payload[key_data_view_id])
        sort_label = payload[key_sort_label] if key_sort_label in payload else None

        sort_dir = payload.get(key_sort_direction, None)
        if sort_dir == "asc":
            sort_asc = True
        elif sort_dir == "desc":
            sort_asc = False
        else:
            sort_asc = None

        entries = session.raw_data_for_data_view(data_view_id, sort_label, sort_asc)
        return jsonify(dict(entries=entries))
    except ValueError as exc:
        return jsonify(dict(error=1, msg=str(exc), entries=-1))


@app.route("/raw_entries_and_tags_for_data_view")
def raw_entries_and_tags_for_data_view():
    key_data_view_id = "data_view_id"
    key_sort_label = "sort_label"
    key_sort_direction = "sort_dir"
    try:
        payload = extract_payload()
        data_view_id = DataViewId(payload[key_data_view_id])
        sort_label = payload[key_sort_label] if key_sort_label in payload else None

        sort_dir = payload.get(key_sort_direction, None)
        if sort_dir == "asc":
            sort_asc = True
        elif sort_dir == "desc":
            sort_asc = False
        else:
            sort_asc = None

        entries, tags_by_key = session.raw_entries_and_tags(data_view_id, sort_label, sort_asc)

        return jsonify(dict(entries=entries, tags_by_key=tags_by_key))
    except ValueError as exc:
        return jsonify(dict(error=1, msg=str(exc), entries=-1))


@app.route("/get_transform_defs")
def get_transform_defs():
    return jsonify([c.serialize() for c in session.get_transform_defs()])


@app.route("/transform_data_view", methods=["GET"])
def transform_data_view():
    key_data_view_id = "data_view_id"
    key_add_transforms = "add_transforms"
    key_del_transforms = "del_transforms"

    payload = extract_payload()

    try:
        add_transforms = TransformList.deserialize(payload[key_add_transforms])
        del_transforms = TransformList.deserialize(payload[key_del_transforms])
        data_view_id = DataViewId(payload[key_data_view_id])

        data_view = session.transform_data_view(data_view_id, add_transforms, del_transforms)
        data_view_id = data_view.id

        return jsonify(
            dict(
                data_view_id=data_view_id,
                data_view=data_view.serialize(),
                error=0,
                msg="",
            )
        )
    except ValueError as exc:
        return jsonify(
            dict(
                data_view_id=-1,
                data_view=-1,
                error=1,
                msg=str(exc),
            )
        )


@app.route("/count_unique", methods=["GET"])
def count_unique():
    key_column = "column"
    key_data_view_id = "data_view_id"

    payload = extract_payload()

    column_name = payload[key_column]
    data_view_id = payload[key_data_view_id]

    log.info(f"column_name {column_name} data_view_id {data_view_id}")

    response = session.count_uniques(column_name, data_view_id)

    return jsonify(response.serialize())


@app.route("/tf_idf_over_values", methods=["GET"])
def tf_idf_over_values():
    key_text_column = "text_column"
    key_category_column = "category_column"
    key_data_view_id = "data_view_id"

    payload = extract_payload()

    text_column_name = payload[key_text_column]
    category_column_name = payload[key_category_column]
    data_view_id = payload[key_data_view_id]

    response = session.tf_idf_over_values(
        text_column_name=text_column_name,
        category_column_name=category_column_name,
        data_view_id=data_view_id,
    )

    return jsonify(response.serialize())


@app.route("/word_counts_over_time", methods=["GET"])
def word_counts_over_time():
    key_text_column = "text_column"
    key_date_time_column = "date_time_column"
    key_data_view_id = "data_view_id"

    payload = extract_payload()

    text_column_name = payload[key_text_column]
    date_time_column_name = payload[key_date_time_column]
    data_view_id = payload[key_data_view_id]

    response = session.word_counts_over_time(
        text_column_name=text_column_name,
        date_time_column_name=date_time_column_name,
        data_view_id=data_view_id,
    )

    return jsonify(response.serialize())


@app.route("/categories")
def categories():
    sample_categories = {
        "passports": [
            "application",
            "renewal",
            "forms",
            "process",
            "visas",
            "help",
            "vacation",
            "travel",
            "wizard",
            "expiration",
            "post office",
        ],
        "coronavirus": [
            "stimulus/check",
            "outbreak",
            "local",
            "concern",
            "task force",
            "closed",
            "businesses",
        ],
        "voting": [
            "elections",
            "registration",
            "officials",
            "selective service",
            "absentee",
            "driver license",
        ],
        "jobs": [
            "small business",
            "fraud",
            "scams",
            "applications",
            "unemployment",
            "compensation",
            "benefits",
        ],
        "money": [
            "unclaimed",
            "credit report",
            "social security",
            "fraud",
            "scam",
            "phishing",
        ],
        "license": [
            "marriage",
            "birth",
            "replacement birth certificate",
            "social security card",
            "replacement",
        ],
        "tax": [
            "refund",
            "direct deposit",
            "state",
            "website",
            "federal tax refund",
            "clear instruction",
            "credit card",
            "consumer action",
            "low income",

        ]
    }
    return jsonify(sample_categories)


@app.route("/hello_world")
def hello_world():
    return jsonify(hello="world")
