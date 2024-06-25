from datetime import datetime
import sys
sys.path.append('../../')
from tools.s3_token.token import upload_to_s3
from tools.save_json.save import save_json
import os
from typing import Iterable
import scrapy


class KopiSpider(scrapy.Spider):
    name = "kopi"
    allowed_domains = ["www.indonetwork.co.id"]
    start_urls = ["https://www.indonetwork.co.id/k/kopi"]
    page_index = 2

    def parse(self, response):
        div_links_page = response.xpath('/html/body/div[1]/div/div[4]/div[2]/div/div/div[2]/div')
        for link in div_links_page:
            link_page = link.xpath('.//div/div[1]/div[2]/div[1]/div[1]/a/@href').get()
            if link_page and 'http' not in link_page:
                link_page = response.urljoin(link_page)
            yield scrapy.Request(url=link_page, callback=self.parse_detail)

        
        next_page = f'https://www.indonetwork.co.id/k/kopi/{self.page_index}'
        if next_page:
            if self.page_index != 10:
                self.page_index += 1
            yield scrapy.Request(next_page, callback=self.parse)
            


    def parse_detail(self, response):
        title = response.xpath('//h1/text()').get().strip()
        kategori = response.xpath('/html/body/div[1]/div[3]/div[2]/div[2]/div/div[1]/div/div[2]/div[2]/a/text()').get().strip()
        last_update = response.xpath('/html/body/div[1]/div[3]/div[2]/div[2]/div/div[1]/div/div[3]/div[2]/text()').get().strip()
        minim_buy = response.xpath('/html/body/div[1]/div[3]/div[2]/div[2]/div/div[1]/div/div[4]/div[2]/text()').get().strip()
        total_view = response.xpath('/html/body/div[1]/div[3]/div[2]/div[2]/div/div[1]/div/div[5]/div[2]/text()').get().strip()
        harga = response.xpath('/html/body/div[1]/div[3]/div[2]/div[2]/div/div[1]/div/div[7]/div[2]/text()').get().strip()
        desc_produk = response.xpath('//*[@id="product-description-box"]/div/div//text()').getall()
        desc_produk = [d.strip() for d in desc_produk if d.strip()]
        desc_comp = response.xpath('//*[@id="company-info-box"]/div[2]/div/div/div//text() | //*[@id="company-info-box"]/div[1]/div/h3/text()').getall()
        desc_comp = [d.strip() for d in desc_comp if d.strip()]

        data = {
            'title': title,
            'kategori' :kategori,
            'update_terakhir': last_update,
            'minimal_pembelian': minim_buy,
            'dilihat_sebanyak': total_view,
            'harga' : harga,
            'deskripsi_produk' : desc_produk,
            'tentang_perusahaan' : desc_comp
        }


        path = 'data'
        os.makedirs(path, exist_ok=True)
        filename = f'{title.lower().replace(' ','_').replace('/','')}.json'
        s3_path = f''
        local_path = f''


        data_json = {
            'link' : 'https://www.indonetwork.co.id/k/kopi',
            'tag' : [
                response.url.split('/')[2],
                'perusahaan biji kopi dan biji kopi',
                'BIJI KOPI'
            ],
            'link_detail' : response.url,
            'domain' : response.url.split('/')[2],
            'crawling_time' : datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'crawling_time_epoch' : int(datetime.now().timestamp()),
            'path_data_raw' : s3_path,
            'path_data_clean' : None,
            'data' : data
        }

        save_json(data_json, os.path.join(path, filename))
        # upload_to_s3(local_path, s3_path.replace('s3://','')) 