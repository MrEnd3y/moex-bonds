import datetime
import time
from urllib import parse
import requests
from bs4 import BeautifulSoup


class Moex:
    def query(self, method: str, **kwargs):
        """
        Отправка запроса к ISS MOEX
        """
        for attempt in range(3):
            try:
                # Формируем URL
                url = f"https://iss.moex.com/iss/{method}.json"
                # if kwargs:
                #    url += "?" + parse.urlencode(kwargs)

                # Выполняем запрос
                response = requests.get(url, params=kwargs, timeout=1)
                response.raise_for_status()
                return response.json()

            except Exception as e:
                print(f"Попытка {attempt + 1}/3 ошибка: {e}")
                if attempt >= 2:
                    time.sleep(10)
        return None

    def flatten_old(self, data: dict, blockname: str):
        """
        Собираю двумерный словарь - название поля: значение
        :param data:
        :param blockname:
        :return:
        """
        securities = data.get(blockname)
        if securities is None:
            print(
                f"securities раздел не был найден в data для блока {blockname}")
            return []

        if not securities or 'columns' not in securities or 'data' not in securities:
            print(f"Блок {blockname} имеет неправильную структуру")
            return []

        try:
            # Формируем список словарей - название поля: значение
            flattened_data = [{str.lower(k): item[i] for i, k in enumerate(securities.get('columns'))}
                              for item in securities.get('data')]
            return flattened_data
        except Exception as e:
            print(f"Ошибка при обработке блока {blockname}: {e}")
            return []

    def flatten(self, data: dict, blockname: str):
        """
        Преобразует блок MOEX (columns + data) в список словарей.
        Оптимизировано: минимум аллокаций, обработка None, str.lower() один раз.
        """
        block = data.get(blockname)
        if not block or 'columns' not in block or 'data' not in block:
            print(f"Блок {blockname} отсутствует или пуст")
            return []

        columns = block['columns']
        rows = block['data']

        if not columns or not rows:
            return []

        # Предвычисляем нижний регистр для колонок один раз
        lower_columns = [col.lower() if col else '' for col in columns]

        result = []
        for row in rows:
            # Проверяем длину строки (на случай битых данных)
            if len(row) < len(columns):
                row += [None] * (len(columns) - len(row))
            elif len(row) > len(columns):
                row = row[:len(columns)]

            # Один проход: zip + dict
            result.append(dict(zip(lower_columns, row)))

        return result

    def rows_to_dict(self, data: dict, blockname: str, field_key='name', field_value='value'):
        """
        Для преобразования запросов типа /securities/:secid.json (спецификация бумаги)
        в словарь значений
        :param data:
        :param blockname:
        :param field_key:
        :param field_value:
        :return:
        """
        flattened_list = self.flatten(data, blockname)
        if not flattened_list:
            return {}

        try:
            return {str.lower(item.get(field_key)): item.get(field_value) for item in flattened_list}
        except KeyError as e:
            print(f"Отсутствует ключ в данных: {e}")
            return {}
        except Exception as e:
            print(f"Ошибка при преобразовании в словарь: {e}")
            return {}

    def get_bonds(self, page=1, limit=10):
        """
        Получаю облигации торгуемые на Мосбирже (stock_bonds)
        без данных по облигации, только исин, эмитент и т.п.
        :param page:
        :param limit:
        :return:
        """
        data_dict = self.query("securities",
                               group_by="group",
                               group_by_filter="stock_bonds",
                               limit=limit,
                               start=(page-1)*limit)

        if data_dict is None:
            print(f"Не удалось получить данные для страницы {page}")
            return []

        flattened_data = self.flatten(data_dict, 'securities')
        print(f"📊 Страница {page}: получено {len(flattened_data)} облигаций")
        return flattened_data

    def get_bond_type_from_smartlab(self, secid):
        """
        Получает тип облигации с сайта Smart-Lab по ISIN
        Возвращает: переменный, плавающий, фиксированный купон, амортизирующий долг, индексируемый номинал
        """
        for attempt in range(3):
            try:
                url = f"https://smart-lab.ru/q/bonds/{secid}/"
                # headers = {
                #    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                # }

                response = requests.get(url, timeout=30)
                # response.raise_for_status()
                if response.status_code != 200:
                    return None

                soup = BeautifulSoup(response.text, 'html.parser')

                # Ищем заголовок h1 с классом qn-menu__title
                title_tag = soup.find('h1', class_='qn-menu__title')

                if title_tag:
                    title_text = title_tag.get_text().lower()

                    if 'плавающим' in title_text:
                        return 'Плавающий купон'
                    elif 'переменным' in title_text:
                        return 'Переменный купон'
                    elif 'фиксированным' in title_text:
                        return 'Фиксированный купон'
                    elif 'амортизацией' in title_text:
                        return 'Амортизирующий долг'
                    elif 'индексируемым' in title_text:
                        return 'Индексируемый номинал'
                    return None

            except Exception as e:
                print(f"Попытка {attempt + 1}/3 ошибка: {e}")
                if attempt < 2:
                    time.sleep(2)
        return None

    def get_specs(self, secid: str):
        data_dict = self.query(f"securities/{secid}")
        if data_dict is None:
            print(f"Не удалось получить спецификации для {secid}")
            return {}
        specs = self.rows_to_dict(data_dict, 'description')
        specs["accruedint"] = self.get_nkd(secid)
        specs["remaining_coupons"] = self._get_remaining_coupons(specs)
        specs["days_to_buyback"] = (datetime.datetime.strptime(specs.get(
            "buybackdate"), "%Y-%m-%d").date() - datetime.datetime.now().date()).days if specs.get("buybackdate") else None
        specs["days_to_coupondate"] = (datetime.datetime.strptime(specs.get(
            "coupondate"), "%Y-%m-%d").date() - datetime.datetime.now().date()).days if specs.get("coupondate") else None
        specs["days_to_finish"] = (datetime.datetime.strptime(specs.get(
            "matdate"), "%Y-%m-%d").date() - datetime.datetime.now().date()).days if specs.get("matdate") else None
        yield_dict = self.get_yield(secid)
        specs["price"] = yield_dict.get("price")
        specs["yieldsec"] = yield_dict.get("yieldsec")
        specs["volume"] = yield_dict.get("volume")
        calc_yield_dict = self._get_calc_yield_params(specs)
        specs["calc_yield"] = calc_yield_dict.get("year_percent")
        specs["total_percent"] = calc_yield_dict.get("total_percent")
        specs["month_percent"] = calc_yield_dict.get("month_percent")
        specs["days_since_prev_coupon"] = self._calc_days_since_prev_coupon(
            specs)
        calc_yield_dict_ = self._get_calc_yield_params_(specs)
        specs["_total_percent"] = calc_yield_dict_.get("_total_percent")
        specs["_month_percent"] = calc_yield_dict_.get("_month_percent")
        if specs.get("faceunit") in ['SUR', 'RUB']:
            specs["bondtype"] = self.get_bond_type_from_smartlab(secid)
        else:
            specs["bondtype"] = None
        return specs

    def _get_calc_yield_params_(self, specs):
        yield_commission = 0.87
        yield_sec = specs.get("yieldsec")
        days_to_buyback = specs.get("days_to_buyback")
        days_to_finish = specs.get("days_to_finish")

        if days_to_buyback:
            finish_days = days_to_buyback
        elif days_to_finish:
            finish_days = days_to_finish
        else:
            finish_days = None

        if finish_days and yield_sec:
            try:
                yield_sec = float(yield_sec)
                total_percent = yield_sec * yield_commission * finish_days / 365
                month_percent = yield_sec * yield_commission * \
                    30 / 365 if finish_days > 30 else 0

                return {
                    "_total_percent": round(total_percent, 2),
                    "_month_percent": round(month_percent, 2),
                }

            except Exception as e:
                print(
                    f"Ошибка при расчете доходности от старых данных в {specs.get("secid")}: {str(e)}")
        return {
            "_total_percent": 0,
            "_month_percent": 0,
        }

    def _calc_days_since_prev_coupon(self, specs):
        coupon_date_str = specs.get("coupondate")
        coupon_frequency = specs.get("couponfrequency")

        if not coupon_date_str or not coupon_frequency:
            return 0
        try:
            coupon_date = datetime.datetime.strptime(
                coupon_date_str, "%Y-%m-%d").date()
            coupon_period_days = 365 / float(coupon_frequency)
            previous_coupon_date = coupon_date - \
                datetime.timedelta(days=coupon_period_days)
            days_since_prev_coupon = (
                datetime.datetime.now().date() - previous_coupon_date).days
            return max(0, days_since_prev_coupon)
        except Exception as e:
            print(f"Ошибка при расчете дней с предыдущего купона: {str(e)}")
            return 0

    def get_nkd(self, secid: str):
        """
        Получает ТОЛЬКО НКД облигации.
        """
        params = {
            "iss.only": "securities",
            "iss.meta": "off",
            "securities.columns": "ACCRUEDINT"
        }
        data = self.query(
            f"engines/stock/markets/bonds/securities/{secid}",
            **params
        )

        # Безопасное извлечение данных
        if not data or 'securities' not in data:
            return None

        securities_data = data['securities'].get('data', [])

        # Проверяем, что список не пустой и содержит хотя бы один элемент
        if not securities_data or len(securities_data[0]) == 0:
            return None

        return securities_data[0][0]

    def _get_remaining_coupons(self, specs: dict) -> int:
        buyback_date_str = specs.get("buybackdate")
        coupon_date_str = specs.get("coupondate")
        coupon_freq = specs.get("couponfrequency")
        mat_date_str = specs.get("matdate")

        if coupon_date_str and coupon_freq:
            today = datetime.datetime.now().date()
            coupon_date = datetime.datetime.strptime(
                coupon_date_str, "%Y-%m-%d").date()

            if buyback_date_str:
                finish_date = datetime.datetime.strptime(
                    buyback_date_str, "%Y-%m-%d").date()
            elif mat_date_str:
                finish_date = datetime.datetime.strptime(
                    mat_date_str, "%Y-%m-%d").date()
            else:
                return 0

            coupon_dates = []
            step_days = 365 / int(coupon_freq)

            while coupon_date <= finish_date:
                if coupon_date >= today:
                    coupon_dates.append(coupon_date)
                    coupon_date = coupon_date + \
                        datetime.timedelta(days=step_days)
            return len(coupon_dates)

        else:
            return 0

    def _get_calc_yield_params(self, specs: dict) -> dict:
        # Годовая: (доход_после_налогов / цена_покупки * 100 / дней_до_окончания * 365)
        # Месячная: (доход_после_налогов / цена_покупки * 100 / дней_до_окончания / 30)
        # Общая: (доход_после_налогов / цена_покупки * 100)

        commission = 2.94
        # 13 процентов при выводе (считается от ДОХОДА)
        yield_commission = 0.87
        ret_none = {
            "total_percent": None,
            "year_percent": None,
            "month_percent": None
        }
        coupon_frequency = specs.get("couponfrequency")
        # Не могут быть None
        initial_face_value = specs.get("initialfacevalue")
        price_percent = specs.get("price")
        coupon_value = specs.get("couponvalue")
        remaining_coupons = specs.get("remaining_coupons")
        days_to_buyback = specs.get("days_to_buyback")
        if any(value is None for value in [initial_face_value, price_percent, coupon_value, remaining_coupons]):
            return ret_none
        # Могу быть None 1 из
        days_to_buyback = specs.get("days_to_buyback")
        days_to_finish = specs.get("days_to_finish")
        # Может быть None то тогда надо считать вручную
        nkd = specs.get("accruedint")

        if days_to_buyback:
            finish_days = days_to_buyback
        elif days_to_finish:
            finish_days = days_to_finish
        else:
            finish_days = None

        if finish_days:
            try:
                coupon_value = float(coupon_value)
                remaining_coupons = int(remaining_coupons)
                initial_face_value = float(initial_face_value)
                price_percent = float(price_percent)
                if finish_days <= 0:
                    return ret_none

                if not nkd:
                    # Расчет не точный *
                    if coupon_frequency:
                        coupon_frequency = float(coupon_frequency)
                        if coupon_frequency == 0:
                            return ret_none
                        nkd = finish_days*coupon_value / \
                            (365/coupon_frequency)
                    else:
                        return ret_none
                else:
                    nkd = float(nkd)

                real_price = initial_face_value * price_percent / 100
                if real_price <= 0:
                    return ret_none
                total_percent = (initial_face_value - real_price + coupon_value - nkd - commission + (
                    coupon_value*(remaining_coupons-1))) * yield_commission / real_price * 100
                year_percent = total_percent / finish_days * 365
                month_percent = total_percent / finish_days * 30 if finish_days > 30 else 0

                return {
                    "total_percent": round(total_percent, 2),
                    "year_percent": round(year_percent, 2),
                    "month_percent": round(month_percent, 2)
                }
            except Exception as e:
                print(
                    f"Ошибка при расчете доходности облигации {specs.get("secid")}: {str(e)}")
        return ret_none

    def get_yield(self, secid: str):
        """Получение доходности по secid"""
        path = f"history/engines/stock/markets/bonds/sessions/3/securities/{secid}"
        from_date = (datetime.datetime.now() -
                     datetime.timedelta(days=7)).strftime("%Y-%m-%d")

        data_dict = self.query(path, **{"from": from_date})
        if data_dict is None:
            print(f"Не удалось получить доходность для {secid}")
            return self._get_empty_yield_data()

        flattened_data = self.flatten(data_dict, 'history')

        # если сделок не было, то что-то нужно записать в базу чтобы не запрашивать облигу сегодня ещё
        if len(flattened_data) < 1:
            return self._get_empty_yield_data()

        try:
            return {
                'price': flattened_data[-1]['close'],
                'yieldsec': flattened_data[-1]['yieldclose'],
                'tradedate': flattened_data[-1]['tradedate'],
                'volume': flattened_data[-1]['volume']*1000,
            }
        except KeyError as e:
            print(f"Отсутствует ключ в данных доходности для {secid}: {e}")
            return self._get_empty_yield_data()

    def _get_empty_yield_data(self):
        """Возвращает пустые данные о доходности"""
        return {
            'price': 0,
            'yieldsec': 0,
            'tradedate': datetime.datetime.now().strftime("%Y-%m-%d"),
            'volume': 0
        }

    def get_last_yield(self, secid: str):
        """
        !!! Сейчас не использую, вместо него см.
        https://iss.moex.com/iss/reference/793
        Очень кривой способ
        - расчет вчерашним днем
        - нет объемов (стакан платный)
        - не ко всем бумагам
        - глючит

        price = Column(Float)
        tradedate = Column(DateTime)
        effectiveyield = Column(Float)

        :param secid:
        :return:
        """
        path = f"history/engines/stock/markets/bonds/yields/{secid}"
        _from = (datetime.datetime.now() -
                 datetime.timedelta(days=3)).strftime("%Y-%m-%d")

        j = self.query(path, _from=_from)
        if j is None:
            return self._get_empty_last_yield_data()

        _r = self.flatten(j, 'history_yields')

        # не по всем облигам (особ не публичным) вообще есть такая инфа
        r = {} if _r is None or len(_r) < 1 else _r[-1]

        # не для всех облиг есть торговля, но нужно в базе как то отмечать что проверка была, поэтому костыль ниже
        k = 'tradedate'
        if k not in r or r[k] is None:
            r[k] = (datetime.datetime.now() -
                    datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        return r

    def _get_empty_last_yield_data(self):
        """Возвращает пустые данные для get_last_yield"""
        return {'tradedate': (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")}
