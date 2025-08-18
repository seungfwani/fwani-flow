from abc import abstractmethod, ABC
from typing import Generic, TypeVar, Optional

from core.database import get_db_context
from models.db.system_function import SystemFunction

T = TypeVar('T')
ID = TypeVar('ID')


class Repository(ABC, Generic[T, ID]):
    @abstractmethod
    def find_by_id(self, id_: ID) -> Optional[T]:
        pass

    @abstractmethod
    def save(self, entity: T) -> T:
        pass

    @abstractmethod
    def delete(self, id_: ID) -> None:
        pass


class SystemFunctionRepo(Repository[SystemFunction, str]):
    def __init__(self):
        self.db_context = get_db_context()

    def find_by_id(self, id_: str) -> Optional[SystemFunction]:
        with self.db_context as db:
            return db.query(SystemFunction).filter(SystemFunction.id == id_).first()

    def save(self, entity: SystemFunction) -> SystemFunction:
        with self.db_context as db:
            db.add(entity)
            db.commit()
            db.refresh(entity)
        return entity

    def delete(self, id_: str) -> None:
        entity = self.find_by_id(id_)
        if entity:
            with self.db_context as db:
                db.delete(entity)
                db.commit()
