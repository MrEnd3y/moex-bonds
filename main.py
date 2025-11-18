import datetime
import click
from inc import moex, db, an
import pandas as pd
import os


def timediff(start: datetime):
    d = datetime.datetime.now() - start
    return datetime.datetime.fromtimestamp(d.total_seconds()).strftime("%M:%S")


def _update_bonds():
    # добаляю спеки облиги (их тоже нужно обновлять, напр за дату след купона)
    # добалвю расчет доходностей yields (кот мосбиржа считает раз в сутки по пред дню)
    # считаю только те что is_traded = True, это ~2700 из 8000 облиг
    while True:
        # получает облигу которая не обновлялась посл 24 часа
        bond = db.get_next_bond(60*60*24)
        if not bond:
            click.secho(f"Закончила обновлять", fg='green')
            break

        db.update_bond_from_json(bond, moex.get_specs(bond.secid))
        # db.update_bond_from_json(bond, moex.get_yield(bond.secid))
        db.session.commit()

         click.echo(click.style(timediff(start_time),
                   fg='yellow') + " / " + str(bond))


@click.command()
def get_bonds():
    """
    Парсинг всех (вкл не торгуемые) облигаций, кот отдает ISS MOEX
    и добавление в базу или обновление в базе
    минимальное колво инфы по каждой их облиг
    :return:
    """
    start_time = datetime.datetime.now()

    # обновление списка облиг
    # добавление новых, смена статуса и т.д.
    # без спеков и доходностей - только secid, isin, boiard id n etc.

    for page in range(1, 1000):
        bonds = moex.get_bonds(page, 100)

        if len(bonds) < 1:
            click.secho(
                f"Закончила обновлять список облигаций на стр. № {page}", fg='green')
            break

        [db.add_bond(bond) for bond in bonds]
        db.session.commit()
        click.echo(click.style(timediff(start_time),
                   fg='yellow') + f" / page {page}")
    _update_bonds()


@click.command()
def update_bonds():
    db.reset_all_updated()
    _update_bonds()


@click.command()
def stats():
    for k, v in an.get_main_stats().items():
        click.echo(click.style(k, fg='bright_white') +
                   " .. " + click.style(v, fg='green'))


@click.command()
@click.option('--rep', '-r', default='lowest_price', show_default=True, required=False)
def report(rep="lowest_price"):
    method = getattr(an, f"report_{rep}")
    df = method()

    # df = an.report_lowest_price()
    # df = an.report_365_yieldest()
    # df = an.report_365_cheap_ll21()

    for i, r in df.iterrows():
        print(f"{r['shortname']}, {r['matdays'].days} : {r['price']}, {r['effectiveyield']} / https://www.moex.com/ru/issue.aspx?code={r['secid']}")

    click.echo("report %s, нашла %s облиг" % (
        click.style(f"{rep}", fg='green'),
        click.style(f"{len(df)}", fg='green')
    ))


@click.command()
def test():
    b = db.get_random_bond()
    j = moex.get_yield(b.secid)

    db.update_bond_from_json(b, j)
    db.session.commit()
    click.echo([j, b.primary_boardid])


@click.group()
def cli_group():
    pass


@click.command()
@click.option('--filename', '-f', default='bonds.xlsx', show_default=True,
              help='Имя файла для экспорта отчета по оффертам')
@click.option('--only-buyback', '-b', is_flag=True, default=False,
              help='Экспортировать только облигации с оффертой (buybackdate NOT NULL)')
@click.option('--all_data', '-a', is_flag=True, default=False,
              help='Экспортировать все облигации без фильтров')
def export_bonds(filename, only_buyback, all_data):

    if all_data:
        """
        Экспорт всех данных об облигациях из БД в Excel файл
        """
        try:
            # Получаем все облигации из базы данных
            query = "SELECT * FROM bonds"
            df = pd.read_sql_query(query, db.engine)

            if len(df) == 0:
                click.secho(
                    "В базе данных нет облигаций для экспорта", fg='red')
                return

            reports_dir = "reports"
            if not os.path.exists("reports"):
                os.makedirs(reports_dir)

            reports_dir = "reports"
            if not os.path.exists("reports"):
                os.makedirs(reports_dir)

            # Сохраняем в Excel
            df.to_excel(os.path.join(reports_dir, "bonds_all.xlsx"),
                        index=False, engine='openpyxl')

            click.secho(
                f"Успешно экспортировано {len(df)} облигаций в файл: {filename}", fg='green')
            click.echo(f"Колонки в экспорте: {', '.join(df.columns.tolist())}")

        except Exception as e:
            click.secho(f"Ошибка при экспорте: {str(e)}", fg='red')
    else:
        """
        Отчет по облигациям с оффертами (buyback)
        """
        try:
            # Базовый SQL запрос
            query = """
            SELECT * FROM bonds 
            WHERE 
                is_traded = 1 AND
                bonds.isqualifiedinvestors != 1 AND
                bonds.issuedate NOT NULL AND
                bonds.couponpercent > 1 AND
                bonds.matdate > date('now') AND
                bonds.faceunit IN ('SUR', 'RUB')
            """

            # Добавляем условие фильтрации по оффертам, если указан параметр
            if only_buyback:
                query += " AND bonds.buybackdate NOT NULL"
                report_type = "с оффертами"
            else:
                report_type = "всех облигаций"

            # Добавляем сортировку
            query += """
            ORDER BY 
                CASE WHEN bonds.price <= 100 THEN 0 ELSE 1 END,
                calc_yield DESC,
                days_to_buyback ASC
            """

            # Выполняем запрос через SQLAlchemy или используем pandas
            df = pd.read_sql_query(query, db.engine)

            # Сохраняем в Excel
            reports_dir = "reports"
            if not os.path.exists("reports"):
                os.makedirs(reports_dir)

            if only_buyback:
                filename = "bonds_with_filter_buyback.xlsx"
            else:
                filename = "bonds_with_filter.xlsx"

            df.to_excel(os.path.join(reports_dir, filename),
                        index=False, engine='openpyxl')

            click.secho(
                f"Успешно создан отчет {report_type}: {filename}", fg='green')
            click.echo(f"Найдено облигаций: {len(df)}")
            click.echo(f"Колонки в отчете: {', '.join(df.columns.tolist())}")

            # Выводим первые несколько строк для предварительного просмотра
            if len(df) <= 0:
                click.secho(
                    "Не найдено облигаций, соответствующих критериям", fg='yellow')

        except Exception as e:
            click.secho(f"Ошибка при создании отчета: {str(e)}", fg='red')




if __name__ == '__main__':
    cli_group.add_command(report)
    cli_group.add_command(stats)
    cli_group.add_command(get_bonds)
    cli_group.add_command(export_bonds)
    cli_group.add_command(test)
    cli_group.add_command(update_bonds)
    cli_group()