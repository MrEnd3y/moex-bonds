from datetime import datetime, timedelta
from importlib import resources

from sqlalchemy import create_engine, func, desc, and_, or_
from sqlalchemy.orm import sessionmaker

from inc.Models import Bond
import pandas as pd
import os
from typing import List


class Db:
    def __init__(self):
        # Создаем папку _db если её нет
        db_folder = "_db"
        if not os.path.exists(db_folder):
            os.makedirs(db_folder)
            print(f"📁 Создана папка: {db_folder}")

        # Получаем путь к БД
        with resources.path("_db", "db.db") as path:
            db_path = str(path)
            engine = create_engine(f"sqlite:///{db_path}")

            # Создаем таблицы только если БД не существует
            if not os.path.exists(db_path):
                Bond.metadata.create_all(engine)
                print(f"✅ База данных создана: {db_path}")
            else:
                print(f"📊 База данных уже существует: {db_path}")

            _session = sessionmaker()
            _session.configure(bind=engine)
            self.session = _session()

    def get_df(self):
        return pd.read_sql(self.session.query(Bond).statement, self.session.bind)

    def add_bond(self, j):
        """
        Добавляю новую облигу
        или обновляю ту что уже в базе
        :param j:
        :return:
        """
        o = self.session.query(Bond).filter_by(secid=j['secid']).first()
        if not o:
            o = Bond()

        o.from_json(j)
        self.session.add(o)

    def update_bond_from_json(self, bond: Bond, j: dict):
        """
        Обновление облиги
        запись спеков и доходностей
        :param bond:
        :param j:
        :return:
        """
        bond.from_json(j)
        bond.updated = datetime.now()
        self.session.add(bond)

    def get_random_bond(self) -> Bond:
        return self.session.query(Bond).filter_by(is_traded=True).order_by(func.random()).first()

    def get_next_bond(self, seconds=18000) -> Bond:
        before = (datetime.now() - timedelta(seconds=seconds))
        return self.session.query(Bond).filter(and_(or_(Bond.updated == None, Bond.updated < before), Bond.is_traded == True)).order_by(desc(Bond.updated)).first()

    def reset_all_updated(self):
        """
        Устанавливает все значения в колонке Bond.updated равными None
        """
        self.session.query(Bond).update({Bond.updated: None})
        self.session.commit()

    # Если нужны только определенные поля
    def get_upd_none_bonds_ids(self) -> List[str]:
        """Возвращает только secid облигаций без обновлений"""
        return [bond.secid for bond in self.session.query(Bond).filter(Bond.updated == None).all()]

    # Если нужно количество
    def count_upd_none_bonds(self) -> int:
        """Возвращает количество облигаций без обновлений"""
        return self.session.query(Bond).filter(Bond.updated == None).count()

    def get_all_bonds(self) -> List[Bond]:
        """
        Получить все облигации из базы данных
        :return: Список всех объектов Bond
        """
        return self.session.query(Bond).all()

    def get_all_bonds_count(self) -> int:
        """
        Получить общее количество облигаций в базе
        :return: Количество облигаций
        """
        return self.session.query(Bond).count()

    def get_all_bonds_filtered(self, **filters) -> List[Bond]:
        """
        Получить все облигации с фильтрами
        :param filters: Параметры фильтрации (например, is_traded=True, faceunit='RUB')
        :return: Отфильтрованный список объектов Bond
        """
        query = self.session.query(Bond)
        for attr, value in filters.items():
            query = query.filter(getattr(Bond, attr) == value)
        return query.all()

    def get_all_bonds_as_dicts(self) -> List[dict]:
        """
        Получить все облигации в виде словарей
        Удобно для конвертации в DataFrame
        :return: Список словарей с данными облигаций
        """
        bonds = self.get_all_bonds()
        bonds_data = []
        for bond in bonds:
            bond_dict = {}
            for column in bond.__table__.columns:
                bond_dict[column.name] = getattr(bond, column.name)
            bonds_data.append(bond_dict)
        return bonds_data

    def get_all_bonds_dataframe(self) -> pd.DataFrame:
        """
        Получить все облигации в виде DataFrame
        :return: DataFrame со всеми облигациями
        """
        return pd.read_sql(self.session.query(Bond).statement, self.session.bind)

    @property
    def engine(self):
        """
        Получить engine базы данных для прямых SQL запросов
        """
        return self.session.bind
