import requests
from bs4 import BeautifulSoup
from time import sleep
from concurrent.futures.thread import ThreadPoolExecutor
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


# получаем урлы объявлений со сгенерированных страниц
def get_urls_from_page(url, flats):
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    pages = soup.find_all(attrs={"class": "item-description-title-link"})
    for page in pages:
        flats.urls.append(page.attrs['href'])


# создается словарь с указанными ключами и значениями из ссылки карточки
def get_info_from_page(url, flats_obj):
    try:
        # получаем реквест для сгенерированного урла
        html = requests.get("https://www.avito.ru" + url).text
        # создаем объект супа, пытаемся вытащить нужные параметры,
        # складываем их в словарь, добавляем словарь в класс Flats
        ads = BeautifulSoup(html, 'html.parser')
        try:
            title = ads.find(attrs={"class": "title-info-title-text"}).text
        except:
            title = ''
        try:
            price = ads.find(attrs={'class': 'js-item-price'}).text
        except:
            price = ''
        html_mobile = requests.get("https://m.avito.ru" + url).text
        ads_mobile = BeautifulSoup(html_mobile, 'html.parser')
        try:
            phone_number = ads_mobile.find(attrs={'class': '_3vWKQ'}).find('a').get('href').split('+')[1]
        except:
            phone_number = ''

        ads_ = {"phone": phone_number,
                "title": title,
                "price": price,
                "url": url}
        print(ads_)

        flats_obj.flats.append(ads_)

    except Exception as e:
        print(e)
        sleep(10)


#объявляем класс в который будут складываться словарь с объявлений
# для потоков
class Flats:
    def __init__(self):
        self.flats = []
        self.counter = 0
        self.urls = []

    def ret_flats(self):
        return self.flats


# объяляем метакласс базы данных
Base = declarative_base()

# объявляем класс БД, указываем столбцы, прописываем бизнес-логику
# задаем название таблицы
class InformationFromAds(Base):
    __tablename__ = 'Ресницы, города милионники'
    id = Column(Integer, primary_key=True)
    phone = Column(String)
    title = Column(String)
    price = Column(String)
    url = Column(String)

    def __repr__(self):
        return f'Квартира ID: {self.id}, имя: {self.title[:5]}'

    def __str__(self):
        return f'Квартира ID: {self.id}, имя: {self.title[:5]}'


zapros = ''
engine = create_engine('sqlite:///Russia_beauty_services' + zapros + '.db')
session_object = sessionmaker()
session_object.configure(bind=engine)
Base.metadata.create_all(engine)
session = session_object()


def main():
    zapros = 'Ресницы'
    region = 'moskva'
    url = "https://www.avito.ru/" + region + "/predlozheniya_uslug/krasota_zdorove?p="
    flats_obj = Flats()
    with ThreadPoolExecutor(max_workers=5) as executor:
        for i in range(0, 50):
            url_p = url + str(i) + '&q=' + zapros
            threads_ads = executor.submit(get_urls_from_page, url_p, flats_obj)

    with ThreadPoolExecutor(max_workers=20) as executor:
        for url in flats_obj.urls:
            future = executor.submit(get_info_from_page, url, flats_obj)

    flats = flats_obj.ret_flats()
    counter = 0
    for flat in flats_obj.flats:
        counter +=1
        flat_db = InformationFromAds(**flat)
        session.add(flat_db)
        if counter % 10 == 0:
            session.commit()
    session.commit()
    print(flats)


if __name__ == '__main__':
    main()