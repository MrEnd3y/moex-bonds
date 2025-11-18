from datetime import datetime

from sqlalchemy import Column, Integer, String, Boolean, Float, DateTime
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Bond(Base):
    """
    https://www.moex.com/ru/listing/securities.aspx
    https://www.moex.com/ru/issue.aspx?code=secid
    https://www.moex.com/ru/issue.aspx?code=RU000A1047S3

    К сож. в беспл ISS MOEX не доступны orderbook ни в каком видео
    """
    __tablename__ = "bonds"
    id = Column(Integer, primary_key=True)
    is_traded = Column(Boolean)
    secid = Column(String)
    shortname = Column(String)
    price = Column(Float)  # цена в проц от номинала
    yieldsec = Column(Float)  # расчитанная мосбиржей доходность (неточная)
    calc_yield = Column(Float)  # Ручной расчет доходности
    month_percent = Column(Float)  # Месячный ручной расчет
    total_percent = Column(Float)  # Обший ручной процент
    volume = Column(Integer)  # объем торгов на посл сессиий в выбраном режиме (board)
    remaining_coupons = Column(Integer)  # Количество оставшихся купонов
    days_to_buyback = Column(Float)    # Кол дней до офферты
    days_to_coupondate = Column(Float)  # Кол дней до купона
    days_since_prev_coupon = Column(Float)  # Кол дней с предыдущего купона
    days_to_finish = Column(Float)     # Колл дней до закрытия
    couponvalue = Column(Float)  # купон нв деньгах
    couponpercent = Column(Float)  # купон %
    accruedint = Column(Float)  # НКД
    listlevel = Column(Integer)  # Уровень листинга - 1 круто, 3 - нет
    bondtype = Column(String)
    buybackdate = Column(DateTime)  # Дата офферты
    matdate = Column(DateTime)  # Дата погашения
    # дата посл торгов, # если при последней проверке торгов не было - заношу дату проверки
    tradedate = Column(DateTime)
    couponfrequency = Column(Integer)  # частота выплаты купона
    coupondate = Column(DateTime)  # дата след купона
    updated = Column(DateTime)
    emitent_id = Column(Integer)
    type = Column(String)  # тип облиги (корп, офз, муниц)
    typename = Column(String)
    primary_boardid = Column(String)  # осн. режим торгов (board)
    issuedate = Column(DateTime)  # Дата начала торгов
    initialfacevalue = Column(Float)  # Первоначальная номинальная стоимость
    faceunit = Column(String)  # валюта
    issuesize = Column(Integer)  # объем выпуска
    facevalue = Column(Float)  # Номинальная стоимость
    isqualifiedinvestors = Column(Boolean)  # только для квалов
    earlyrepayment = Column(Boolean)  # Возможен досрочный выкуп
    # Проценты в месяц до даты офферты или завершения
    _month_percent = Column(Float)
    # Общие проценты до даты офферты или завершения
    _total_percent = Column(Float)

    def cast(self, val, _type, _key):
        """
        Приведение типов
        ISS не всегда отдает number в нужном видео, иногда number в ответе - это str, соотв нужно привести
        Даты тоже нужно привести к datetime
        :param val:
        :param _type:
        :param _key:
        :return:
        """
        try:
            if not val:
                return val

            if isinstance(_type, Integer):
                val = int(val)
            elif isinstance(_type, String):
                val = str(val)
            elif isinstance(_type, Float):
                val = float(val)
            elif isinstance(_type, Boolean):
                val = int(val)  # для sqllite так
            elif isinstance(_type, DateTime):
                val = datetime.strptime(val, "%Y-%m-%d")
        except Exception as e:
            # для дебага
            print([val, _type, _key, self.secid])
            print(f"Ошибка преобразования типов: {str(e)}")
            exit(0)

        return val

    def from_json(self, j):
        """
        Данные из json формата в модель по аттрибутам
        минус способа - аттрибуты должно одинаково именоваться json => model => table
        это не всегда удобно
        :param j:
        :return:
        """
        for col in self.__table__.columns:
            if col.key in j:
                val = self.cast(j[col.key], col.type, col.key)
                setattr(self, col.key, val)

    def get_date_str(self, field='issuedate', _format='%Y-%m-%d'):
        """
        Формат поля даты из timestamp в строку формата _format
        :param field:
        :param _format:
        :return:
        """
        val = getattr(self, field)
        if not val:
            return "n/a"
        return val.strftime('%Y-%m-%d')

    def get_url(self):
        """
        Инфа по выпуску
        https://www.tinkoff.ru/invest/bonds/{secid}/

        :return:
        """
        return f"https://www.moex.com/ru/issue.aspx?code={self.secid}"

    def __str__(self):
        issuedate = self.get_date_str()
        tradedate = self.get_date_str('tradedate')
        return f"{self.secid} / {self.shortname}, {issuedate} = {self.is_traded} / {tradedate} = {self.yieldsec}"
