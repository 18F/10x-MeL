from __future__ import annotations

from typing import List, Dict, Tuple, Optional, Union
from pathlib import Path
import logging

from analyzer.utils import Serializable, SerializableHandler
from analyzer.users import UserId
from analyzer.dataset import DatasetId
from analyzer.dataset.dataset_lib import Dataset


log = logging.getLogger(__name__)


class User(Serializable):
    def __init__(self, user_id: UserId, name: str):
        self.id = user_id
        self.name = name

    def serialize(self) -> List:
        return [self.id, self.name]

    @classmethod
    def deserialize(cls, d: List) -> User:
        return User(user_id=d[0], name=d[1])

    def __repr__(self) -> str:
        return "<User {id}: {name}>".format(id=self.id, name=self.name)


class UserHandler(SerializableHandler):
    KEY_USERS = "users"
    KEY_HISTORY = "history"

    default_user = User(user_id=UserId(1), name="owner")

    def __init__(self, path: Path):
        self._path = path
        self._users: List[User] = []
        self._history: Dict[UserId, DatasetId] = {}

        self._by_id: Dict[UserId, User] = {}
        self.load()

    def initialization_data(self):
        return {
            self.KEY_USERS: [self.default_user.serialize()],
            self.KEY_HISTORY: self._history,
        }

    def load(self):
        try:
            users, history = self._load(self._path)
        except Exception as exc:
            log.error("Could not load users '%s': %s", self._path, exc)
            data = self.initialization_data()
            users, history = data[self.KEY_USERS], data[self.KEY_HISTORY]

        self._users = users
        self._history = history

        self._by_id.clear()
        for user in self._users:
            self._index_user(user)

    def save(self):
        if self._users is None:
            log.warning("Attempting to save Users that have not been loaded")
            return
        self._save(self._path)

    def serialize(self) -> Dict:
        return dict(
            users=[user.serialize() for user in self._users],
            history=self._history,
        )

    @classmethod
    def deserialize(cls, d: Dict) -> Tuple[List[User], Dict[UserId, DatasetId]]:
        if len(d) == 0:
            return [], {}

        users = [User.deserialize(u) for u in d[cls.KEY_USERS]]
        history: Dict[UserId, DatasetId] = d.get(cls.KEY_HISTORY)

        return (
            users,
            {UserId(u): DatasetId(d) for u, d in history.items()},
        )

    def _index_user(self, user: User):
        self._by_id[user.id] = user

    def find(self, name: Optional[str] = None) -> List[User]:
        users = []
        for user in self._users:
            if name and name not in user.name:
                continue
            users.append(user)
        return users

    def by_id(self, user_id: UserId) -> Optional[User]:
        return self._by_id.get(user_id)

    def get_last_dataset_id(self, user_id: UserId) -> Optional[DatasetId]:
        return self._history.get(user_id)

    def set_last_dataset(self, user_id: UserId, dataset_id: DatasetId):
        self._history[user_id] = dataset_id
        self.save()
