from datetime import datetime, timedelta

import pandas as pd

from inc.Db import Db


class Analytics:
    def __init__(self, db: Db):
        self.df = db.get_df()

        # Безопасное вычисление matdays
        if not self.df.empty and 'matdate' in self.df.columns:
            try:
                # Преобразуем matdate в datetime если это еще не сделано
                if not pd.api.types.is_datetime64_any_dtype(self.df['matdate']):
                    self.df['matdate'] = pd.to_datetime(self.df['matdate'])

                # Вычисляем matdays только для корректных дат
                self.df['matdays'] = self.df['matdate'] - datetime.now()
            except Exception as e:
                print(f"⚠️ Ошибка при вычислении matdays: {e}")
                # Создаем пустую колонку в случае ошибки
                self.df['matdays'] = pd.NaT
        else:
            # Если DataFrame пустой или нет колонки matdate
            self.df['matdays'] = pd.NaT

    def get_main_stats(self):
        # Проверяем что DataFrame не пустой
        if self.df.empty:
            return {
                'всего облиг': 0,
                'торгуемых': 0,
                'сообщение': 'База данных пустая, запустите "python main.py get-bonds"'
            }

        date_2022 = datetime.strptime("2022-01-01", "%Y-%m-%d")
        date_2021 = datetime.strptime("2021-01-01", "%Y-%m-%d")
        date_2020 = datetime.strptime("2020-01-01", "%Y-%m-%d")
        date_2019 = datetime.strptime("2019-01-01", "%Y-%m-%d")
        delta365 = timedelta(days=365)
        df = self.df

        # Безопасные вычисления с проверкой наличия колонок
        stats = {
            'всего облиг': len(df.index),
            'торгуемых': len(df[df['is_traded'] == 1].index) if 'is_traded' in df.columns else 0,
            'для квалов': len(df[df['isqualifiedinvestors'] == True].index) if 'isqualifiedinvestors' in df.columns else 0,

            'выпущенных в 2021': len(df[(df['issuedate'] >= date_2021) & (df['issuedate'] <= date_2022)].index) if 'issuedate' in df.columns else 0,
            'выпущенных в 2020': len(df[(df['issuedate'] >= date_2020) & (df['issuedate'] <= date_2021)].index) if 'issuedate' in df.columns else 0,
            'выпущенных в 2019': len(df[(df['issuedate'] >= date_2019) & (df['issuedate'] <= date_2020)].index) if 'issuedate' in df.columns else 0,
        }

        # Добавляем статистику по доходностям если есть колонка
        if 'effectiveyield' in df.columns:
            stats.update({
                'с доходностью > 1%': len(df[df['effectiveyield'] >= 1].index),
                'с доходностью > 8%': len(df[df['effectiveyield'] >= 8].index),
                'с доходностью > 11%': len(df[df['effectiveyield'] >= 11].index),
            })
        else:
            stats.update({
                'с доходностью > 1%': 0,
                'с доходностью > 8%': 0,
                'с доходностью > 11%': 0,
            })

        # Добавляем статистику по листингу если есть колонки
        if 'listlevel' in df.columns and 'effectiveyield' in df.columns:
            traded_df = df[df['is_traded'] ==
                           1] if 'is_traded' in df.columns else pd.DataFrame()

            if not traded_df.empty:
                stats.update({
                    'листинг 1': len(traded_df[traded_df['listlevel'] == 1].index),
                    'медианная доходность, листинг 1, %': round(traded_df[traded_df['listlevel'] == 1]['effectiveyield'].median(), 2) if len(traded_df[traded_df['listlevel'] == 1]) > 0 else 0,
                    'листинг 2': len(traded_df[traded_df['listlevel'] == 2].index),
                    'медианная доходность, листинг 2, %': round(traded_df[traded_df['listlevel'] == 2]['effectiveyield'].median(), 2) if len(traded_df[traded_df['listlevel'] == 2]) > 0 else 0,
                    'листинг 3': len(traded_df[traded_df['listlevel'] == 3].index),
                    'медианная доходность, листинг 3, %': round(traded_df[traded_df['listlevel'] == 3]['effectiveyield'].median(), 2) if len(traded_df[traded_df['listlevel'] == 3]) > 0 else 0,
                })
            else:
                stats.update({
                    'листинг 1': 0, 'листинг 2': 0, 'листинг 3': 0,
                    'медианная доходность, листинг 1, %': 0,
                    'медианная доходность, листинг 2, %': 0,
                    'медианная доходность, листинг 3, %': 0,
                })
        else:
            stats.update({
                'листинг 1': 0, 'листинг 2': 0, 'листинг 3': 0,
                'медианная доходность, листинг 1, %': 0,
                'медианная доходность, листинг 2, %': 0,
                'медианная доходность, листинг 3, %': 0,
            })

        # Добавляем статистику по ценам
        if 'price' in df.columns:
            stats.update({
                'медианная цена, %': round(df['price'].mean(), 2),
            })

            if 'effectiveyield' in df.columns:
                stats.update({
                    'медианная цена, с дох >= 11, %': round(df[df['effectiveyield'] >= 11]['price'].median(), 2) if len(df[df['effectiveyield'] >= 11]) > 0 else 0,
                    'медианная цена, с дох >= 8 & < 11, %': round(df[(df['effectiveyield'] >= 8) & (df['effectiveyield'] < 11)]['price'].median(), 2) if len(df[(df['effectiveyield'] >= 8) & (df['effectiveyield'] < 11)]) > 0 else 0,
                    'медианная цена, с дох >= 1 & < 8, %': round(df[(df['effectiveyield'] >= 1) & (df['effectiveyield'] < 8)]['price'].median(), 2) if len(df[(df['effectiveyield'] >= 1) & (df['effectiveyield'] < 8)]) > 0 else 0,
                })

            if 'listlevel' in df.columns:
                traded_df = df[df['is_traded'] ==
                               1] if 'is_traded' in df.columns else pd.DataFrame()
                if not traded_df.empty:
                    stats.update({
                        'медианная цена, листинг 1, %': round(traded_df[traded_df['listlevel'] == 1]['price'].median(), 2) if len(traded_df[traded_df['listlevel'] == 1]) > 0 else 0,
                        'медианная цена, листинг 2, %': round(traded_df[traded_df['listlevel'] == 2]['price'].median(), 2) if len(traded_df[traded_df['listlevel'] == 2]) > 0 else 0,
                        'медианная цена, листинг 3, %': round(traded_df[traded_df['listlevel'] == 3]['price'].median(), 2) if len(traded_df[traded_df['listlevel'] == 3]) > 0 else 0,
                    })

        # Добавляем статистику по сроку погашения
        if 'matdays' in df.columns and 'effectiveyield' in df.columns:
            matdays_df = df[df['matdays'] < delta365]
            if not matdays_df.empty:
                stats.update({
                    'медианная цена, matday < 365, %': round(matdays_df['price'].median(), 2),
                    'медианная доходность, matday < 365, %': round(matdays_df['effectiveyield'].median(), 2),
                })
            else:
                stats.update({
                    'медианная цена, matday < 365, %': 0,
                    'медианная доходность, matday < 365, %': 0,
                })

        return stats

    def report_lowest_price(self, min_normal=90):
        if self.df.empty or 'price' not in self.df.columns or 'effectiveyield' not in self.df.columns:
            return pd.DataFrame()
        return self.df[self.df['price'] < min_normal].sort_values(by=['effectiveyield'], ascending=False)

    def report_365_cheap_ll21(self):
        """
        Облиги с ценой ниже медианы, в лл 1-2 и с погашением в сл 365 дней
        """
        if self.df.empty:
            return pd.DataFrame()

        delta365 = timedelta(days=365)
        required_columns = ['is_traded', 'listlevel', 'matdays', 'price']
        if not all(col in self.df.columns for col in required_columns):
            return pd.DataFrame()

        df = self.df[(self.df['is_traded'] == 1) & (
            self.df['listlevel'] <= 2) & (self.df['matdays'] < delta365)]
        if df.empty:
            return pd.DataFrame()

        med = df['price'].median()
        return df[df['price'] < med].sort_values(by=['price'], ascending=True)

    def report_365_yieldest(self):
        """
        Облиги погашаемые в след 365 дней и в листингах 1 и 2
        доходностью выше медианной в этой группе
        """
        if self.df.empty:
            return pd.DataFrame()

        delta365 = timedelta(days=365)
        required_columns = ['matdays', 'listlevel', 'effectiveyield']
        if not all(col in self.df.columns for col in required_columns):
            return pd.DataFrame()

        df = self.df[(self.df['matdays'] < delta365)
                     & (self.df['listlevel'] <= 2)]
        if df.empty:
            return pd.DataFrame()

        med = df['effectiveyield'].median()
        return df[df['effectiveyield'] > med].sort_values(by=['effectiveyield'], ascending=False)
