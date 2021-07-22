"""
Skrypt ściągający archiwalne dane pogodowe udostępniane przez IMGW i łączący je w jeden plik CSV.
UWAGA: dane są objęte licencją zabraniającą wykorzystywania ich komercyjnie. 
Szczegóły licencji znajdziesz na stronie dane.imgw.pl
"""

from bs4 import BeautifulSoup
import urllib.request
import re
import os
from icecream import ic
import zipfile
from io import StringIO
import pandas as pd


BASE_URL = "https://dane.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/synop/"
DEST_DIR = "/data/pogoda/"


def save_files(dir_url, refresh=False):
    dir_page = table_of_contents = urllib.request.urlopen(dir_url)
    soup = BeautifulSoup(dir_page)
    zip_urls = [
        link.get("href")
        for link in soup.findAll("a")
        if re.match(r".+\.zip$", link.get("href"))
    ]
    for z in zip_urls:
        if os.path.exists(os.path.join(DEST_DIR, z)) and not refresh:
            continue
        ic(z)
        urllib.request.urlretrieve(f"{dir_url}/{z}", os.path.join(DEST_DIR, z))


def process_zip(zip_file, show_errors=False):

    z = zipfile.ZipFile(os.path.join(DEST_DIR, zip_file))
    inner_fnames = [n for n in z.namelist() if n.startswith("s_d_t")]
    processed_csvs = [get_csv_content(z, i) for i in inner_fnames]
    n_errors = len([p for p in processed_csvs if p[0] != "OK"])
    if show_errors:
        for err in list(set([p[1] for p in processed_csvs if p[0] != "OK"])):
            ic(err)

    return pd.concat([p[1] for p in processed_csvs if p[0] == "OK"])


def get_csv_content(zip_object, inner_fname):
    sdt_col_names = [
        "kod_stacji",
        "nazwa_stacji",
        "rok",
        "miesiac",
        "dzien",
        "zachmurzenie_ogolne",
        "status_zachmurzenie",
        "srednia_predkosc_wiatru",
        "status_srednia_predkosc_wiatru",
        "srednia_temperatura",
        "status_srednia_temperatura",
        "srednie_cisnienie_pary_wodnej",
        "status_srednie_cisnienie_pary_wodnej",
        "srednia_wilgotnosc_wzgledna",
        "status_srednia_wilgotnosc_wzgledna",
        "srednie_cisnienie",
        "status_srednie_cisnienie",
        "srednie_cisnienie_na_poziomie_morza",
        "status_srednie_cisnienie_na_poziomie_morza",
        "suma_opadu_dzien",
        "status_suma_opadu_dzien",
        "suma_opadu_noc",
        "status_suma_opadu_noc",
    ]

    csv_content = zip_object.read(inner_fname).decode("cp1250")
    try:
        df_object = pd.read_csv(StringIO(csv_content), header=None, names=sdt_col_names)
    except Exception as err:
        return ("Error", err)
    return ("OK", df_object)


def concat_all_data(directory=DEST_DIR):
    df = pd.concat(
        [process_zip(z) for z in os.listdir(directory) if z.endswith(".zip")]
    )
    df["data"] = pd.to_datetime(
        dict(year=df["rok"], month=df["miesiac"], day=df["dzien"])
    )
    df = df.drop(columns=["rok", "miesiac", "dzien"])
    return df


def output_fname(df, date_col="data"):
    min_date = df[date_col].min()
    max_date = df[date_col].max()
    return f"pogoda_{min_date:%Y%m%d}-{max_date:%Y%m%d}.csv"


if __name__ == "__main__":

    table_of_contents = urllib.request.urlopen(BASE_URL)
    soup = BeautifulSoup(table_of_contents)

    yearly_archives = [
        f"{BASE_URL}{link.get('href')}"
        for link in soup.findAll("a")
        if re.match(r"\d+(\_\d+)*\/", link.get("href"))
    ]

    os.makedirs(DEST_DIR, exist_ok=True)
    for ya in yearly_archives:
        save_files(ya)

    full_data = concat_all_data()
    output_file = os.path.join(DEST_DIR, output_fname(full_data))
    full_data.to_csv(output_file, index=False)
