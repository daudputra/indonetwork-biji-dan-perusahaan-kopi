from datetime import datetime
import sys
sys.path.append('../../')
from tools.s3_token.token import upload_to_s3
from tools.save_json.save import save_json
import os
from typing import Iterable
import scrapy

class PerusahaanKopiSpider(scrapy.Spider):
    name = "perusahaan"
    allowed_domains = ["www.indonetwork.co.id"]
    start_urls = ["https://www.indonetwork.co.id/k/kopi/perusahaan"]
    page_index = 2


    def parse(self, response):
        links = response.xpath('/html/body/div[1]/div/div[4]/div[2]/div/div/div[2]/div/div/div/div[2]/div[1]/div/div[2]/a/@href').getall()
        for link in links:
            if link and 'http' not in link:
                link = response.urljoin(link)
            yield scrapy.Request(url=link, callback=self.parse_detail)

        next_page = f'https://www.indonetwork.co.id/k/kopi/perusahaan/{self.page_index}'
        if next_page:
            if self.page_index < 6:
                self.page_index += 1
            yield scrapy.Request(next_page, callback=self.parse)
            
            

    def parse_detail(self, response):
        title = response.xpath('/html/body/div[1]/div[2]/div[1]/div[1]/div/div/div/div/div[1]/div/div[2]/div[1]/div/div/div[2]/h1/text()').get()
        if title is not None:
            title = response.xpath('/html/body/div[1]/div[2]/div[1]/div[1]/div/div/div/div/div[1]/div/div[2]/div[1]/div/div/div[2]/h1/text()').get().strip()
        membership_rank = response.xpath('/html/body/div[1]/div[2]/div[1]/div[1]/div/div/div/div/div[1]/div/div[2]/div[1]/div/div/div[2]/div/span/text()').get()
        if membership_rank is not None:
            membership_rank = response.xpath('/html/body/div[1]/div[2]/div[1]/div[1]/div/div/div/div/div[1]/div/div[2]/div[1]/div/div/div[2]/div/span/text()').get().strip()
        membership_year = response.xpath('/html/body/div[1]/div[2]/div[1]/div[1]/div/div/div/div/div[1]/div/div[2]/div[1]/div/div/div[2]/div/div/text()').get()
        if membership_year is not None:   
            membership_year = response.xpath('/html/body/div[1]/div[2]/div[1]/div[1]/div/div/div/div/div[1]/div/div[2]/div[1]/div/div/div[2]/div/div/text()').get().strip()
        alamat = response.xpath('/html/body/div[1]/div[2]/div[1]/div[1]/div/div/div/div/div[1]/div/div[2]/div[2]/div[1]/div/div[2]/span/text()').get()
        if alamat is not None:
            alamat = alamat.strip()
        profil_perusahaan = response.xpath('/html/body/div[1]/div[2]/div[1]/div[2]/div/div/div/div[2]/div/text()').get()
        if profil_perusahaan is not None:
            profil_perusahaan = profil_perusahaan.strip()

        filename_clean = title.lower().replace(' ','_').replace('/','').replace(':','_').replace('.','_').replace('__','_')
        filename = f'{filename_clean}.json'
        s3_path = f''

        data_perusahaan = {
            'link' : 'https://www.indonetwork.co.id/k/kopi/perusahaan',
            'tag' : [
                response.url.split('/')[2],
                'perusahaan biji kopi dan biji kopi',
                'PERUSAHAAN BIJI KOPI'
            ],
            'link_detail' : response.url,
            'domain' : response.url.split('/')[2],
            'crawling_time' : datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'crawling_time_epoch' : int(datetime.now().timestamp()),
            'path_data_raw' : s3_path,
            'path_data_clean' : None,
            'data' : {
            'title' : title,
            'membership_rank' : membership_rank,
            'membership_year' : membership_year,
            'alamat' : alamat,
            'profil_perusahaan' : profil_perusahaan,
            'katalog_produk' : {}
        }
    }
        links_produk = response.xpath('/html/body/div[1]/div[2]/div[2]/div[2]/div/div/div/div[2]/div/div/div[2]/div/div/div[1]/div/h3/a/@href').getall()
        for link_produk in links_produk:
            link_produk = response.urljoin(link_produk)
            yield scrapy.Request(link_produk, callback=self.parse_produk, meta={'data_perusahaan' :data_perusahaan, 'title_per' : title})
            

        
        selected_li = response.xpath('//ul[@class="paging d-flex justify-content-center"]/li[span[@class="selected align-middle"]]')[0]
        next_page = selected_li.xpath('./following-sibling::li[1]/a/@href').get()
        if next_page and '?page=12' in next_page:
            return
        if next_page:
            yield scrapy.Request(next_page, callback=self.parse_detail)


    def parse_produk(self, response):
        title_per = response.meta['title_per']
        data_perusahaan = response.meta['data_perusahaan']
        data_perusahaan_s = response.meta['data_perusahaan']['data']
        katalog_produk = data_perusahaan['data']['katalog_produk']

        title = response.xpath('//h1/text()').get().strip()
        kategori = response.xpath('/html/body/div[1]/div[3]/div[2]/div[2]/div/div[1]/div/div[2]/div[2]/a/text()').get()
        if kategori is not None:
            kategori = response.xpath('/html/body/div[1]/div[3]/div[2]/div[2]/div/div[1]/div/div[2]/div[2]/a/text()').get().strip()
        last_update = response.xpath('/html/body/div[1]/div[3]/div[2]/div[2]/div/div[1]/div/div[3]/div[2]/text()').get().strip()
        minim_buy = response.xpath('/html/body/div[1]/div[3]/div[2]/div[2]/div/div[1]/div/div[4]/div[2]/text()').get().strip()
        total_view = response.xpath('/html/body/div[1]/div[3]/div[2]/div[2]/div/div[1]/div/div[5]/div[2]/text()').get()
        if total_view is not None:
            total_view = total_view.strip()
        harga = response.xpath('/html/body/div[1]/div[3]/div[2]/div[2]/div/div[1]/div/div[7]/div[2]/text()').get()
        if harga is not None:
            harga = harga.strip()
        desc_produk = response.xpath('//*[@id="product-description-box"]/div/div//text()').getall()
        desc_produk = [d.strip() for d in desc_produk if d.strip()]
        desc_comp = response.xpath('//*[@id="company-info-box"]/div[2]/div/div/div//text() | //*[@id="company-info-box"]/div[1]/div/h3/text()').getall()
        desc_comp = [d.strip() for d in desc_comp if d.strip()]

        path = 'data'
        os.makedirs(path, exist_ok=True)
        filename_clean = title_per.lower().replace(' ','_').replace('/','').replace(':','_').replace('.','_').replace('__','_')
        filename = f'{filename_clean}.json'

        data_produk = {
            'title': title,
            'kategori' :kategori,
            'update_terakhir': last_update,
            'minimal_pembelian': minim_buy,
            'dilihat_sebanyak': total_view,
            'harga' : harga,
            'deskripsi_produk' : desc_produk,
            # 'tentang_perusahaan' : desc_comp
        }

    
        katalog_produk[title.replace(' ','_').lower()] = data_produk

        data_perusahaan_s['katalog_produk'] = katalog_produk


        save_json(data_perusahaan, os.path.join(path, filename))