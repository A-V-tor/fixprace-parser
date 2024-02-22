from os import times
from pathlib import Path
import time
import scrapy


class ProductsSpider(scrapy.Spider):
    name = 'product'

    def start_requests(self):
        ROOT_URL = 'https://fix-price.com/catalog/'
        input_list_categories = [
            getattr(self, 'category-1', None),
            getattr(self, 'category-2', None),
            getattr(self, 'category-3', None),
        ]
        urls = [ROOT_URL + url for url in input_list_categories if url]

        cookies = {
            'i18n_redirected': 'ru',
            '_cfuvid': '5cxrtC7Yd8F_hhGEwkrSyPseVdzgHQeJ36Rxpo5XR6I-1708441191369-0.0-604800000',
            'cf_clearance': 'v14SIkCMzspA_WFWptYqbsXsgDD5haal0KgUWVcPfbo-1708441196-1.0-Ae5IqVoL6zOF2VLcaj6LyDVyJopHONZs2iYlqmtEfaO4MmqknDEbaG0igoxLH7B8Jb84SHRgNxjEvQqkvM69qOk=',
            'token': '5f135625eec9a78e9d3e23ec13f17341',
            'is-logged': '',
            'tmr_lvid': '374109357f1dbcbfb940bb67a99b698c',
            'tmr_lvidTS': '1708441268530',
            '_ym_uid': '1708441269646417098',
            '_ym_d': '1708441269',
            '_ymab_param': 'KzIDypDNuebpJO4dqyvJeEwzyX4pMEdhleH2Sk_I7PnBqCPKk2nsl8aeOxeRFcgiERUclKh270JjFIQLzyJQadSe5Ws',
            '_ym_isad': '2',
            'skip-city': 'true',
            'tmr_detect': '0%7C1708442595199',
            'locality': '%7B%22city%22%3A%22%D0%95%D0%BA%D0%B0%D1%82%D0%B5%D1%80%D0%B8%D0%BD%D0%B1%D1%83%D1%80%D0%B3%22%2C%22cityId%22%3A55%2C%22longitude%22%3A60.597474%2C%22latitude%22%3A56.838011%2C%22prefix%22%3A%22%D0%B3%22%7D',
        }

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, cookies=cookies)

    def detail_product(self, response):
        timestamp = int(time.time())
        rpc = response.css('span.value::text')[0].get()
        url = response.url
        title = response.css('h1.title::text').get()
        marketing_tag = response.css('p.special-auth::text').get()
        price_data = (
            response.css('div.price-quantity-block')
            .css('meta::attr(content)')[1]
            .get()
        )
        assets = {
            'main_image': response.css('img.zoom::attr(src)').get(),
            'set_images': response.css('div.swiper-slide')
            .css('img::attr(src)')
            .getall(),
        }
        section = response.url.split('/')[-2]
        brand = response.css('p.property').css('a.link::text').get()
        metadata = {
            '__description': response.css('div.description::text').getall()[
                -1
            ],
            'brand': brand,
        }

        try:
            # отрезать лишний ключ "бренд"
            data = response.css('p.property').css('span::text').getall()[1:]
            result = {data[i]: data[i + 1] for i in range(0, len(data), 2)}
        except IndexError:
            data = response.css('p.property').css('span::text').getall()
            result = {data[i]: data[i + 1] for i in range(0, len(data), 2)}

        metadata.update(result)

        return {
            'timestamp': timestamp,
            'RPC': rpc,
            'url': url,
            'title': title,
            'marketing_tag': marketing_tag,
            'brand': brand,
            'section': section,
            'price_data': price_data,
            'assets': assets,
            'metadata': metadata,
        }


    def current_page_parse(self, response):
        response.css('div.product__wrapper')
        entities = response.css('div.product__wrapper')

        # print('КАРТОЧЕК НА СТРАНИЦЕ: ',len(entities))

        # проверка наличия карточек товара
        if len(entities) == 0:
            self.logger.info('Товаров для парсинга закончились!')
            raise scrapy.exceptions.CloseSpider('Stopping parsing.')

        for i in entities:
            url_part = (
                i.css('div.product__wrapper')[0].css('a::attr(href)').get()
            )
            next_page = response.urljoin(url_part)
            yield scrapy.Request(next_page, callback=self.detail_product)

    def parse(self, response):
        # print(f"Город {response.css('div.default-layout')[0].css('span')[0].css('::text').get()}")
        current_page = response.meta.get('page', 1)
        pagination = f'?page={current_page + 1}'

        #  URL для следующей страницы
        next_page_url = response.urljoin(pagination)

        # запрос к следующей странице пагинации
        yield scrapy.Request(
            next_page_url, callback=self.parse, meta={'page': current_page + 1}
        )

        # обход товаров на текущей странице
        yield from self.current_page_parse(response)
