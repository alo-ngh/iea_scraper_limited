# definitions from instance overrides values from settings
from iea_scraper.instance import *
from iea_scraper.instance import EXT_DB_STR
import pathlib
import platform
import pycountry
import numpy as np

# Refinitive APP Key
REFINITIVE_APP_KEY = f'0b32d069a93849488b1f24f70342382cf1e26523'

ROOT_PATH = pathlib.Path(__file__).absolute().parent.parent
FILE_STORE_PATH = ROOT_PATH / 'filestore'
SSL_CERTIFICATE_PATH = ROOT_PATH / 'ssl_cert' / 'ssl_verify.crt'

# Mail configuration
MAIL_DEFAULT_SENDER = 'oil-scraper@iea.org'
MAIL_EDC_SENDER = 'real-time@iea.org'
MAIL_SERVER = '172.20.29.150'
MAIL_RECIPIENT = ['luisfernando.rosa@iea.org',
                  'Olaoye.OLOYEDE@iea.org',
                  'tsuyoshi.deguchi@iea.org']

EDC_MAILING_LIST = ['Aloys.NGHIEM@iea.org',
                    'Louis.CHAMBEAU@iea.org',
                    'Martin.TAV@iea.org']

EDC_GOOGLE_TRENDS_MAILING_LIST = ['Aloys.NGHIEM@iea.org',
                                  'amani.al-saidi@iea.org']

NEW_DATA_DAILY_RECIPIENT = ['toril.bosoni@iea.org',
                            'peg.mackey@iea.org',
                            'Jacob.MESSING@iea.org',
                            'kristine.petrosyan@iea.org',
                            'Jennifer.THOMSON@iea.org',
                            'Ciaran.HEALY@iea.org',
                            'Yuya.AKIZUKI@iea.org',
                            'joel.couse@iea.org'
                            ] + MAIL_RECIPIENT
NEW_DATA_WEEKLY_RECIPIENT = ['kristine.petrosyan@iea.org',
                             'Jennifer.THOMSON@iea.org',
                             'joel.couse@iea.org'] + MAIL_RECIPIENT
NEW_DATA_PAJ_WEEKLY_RECIPIENT = ['kristine.petrosyan@iea.org',
                                 'Yuya.AKIZUKI@iea.org',
                                 'Jennifer.THOMSON@iea.org',
                                 'joel.couse@iea.org'] \
                                + MAIL_RECIPIENT
NEW_DATA_CN_CUSTOMS_RECIPIENT = ['kristine.petrosyan@iea.org',
                                 'Ciaran.HEALY@iea.org',
                                 'Yuya.AKIZUKI@iea.org',
                                 'Jennifer.THOMSON@iea.org',
                                 'joel.couse@iea.org'
                                 ] \
                                + MAIL_RECIPIENT

MAIL_PORT = 25
MAIL_USE_TLS = True

# IEA Proxy
PROXY_DICT = {"http": "http://proxy.iea.org:8080",
              "https": "http://proxy.iea.org:8080",
              "ftp": "ftp://proxy.iea.org:8080"}

# Browser driver path for selenium
PLATFORM = platform.system()
CHROME_DRIVER = f"chromedriver{'.exe' if PLATFORM == 'Windows' else ''}"
FIREFOX_DRIVER = f"geckodriver{'.exe' if PLATFORM == 'Windows' else ''}"
WEBDRIVER_PATH = ROOT_PATH / 'drivers'

# for compatibility with previous versions
BROWSERDRIVER_PATH = WEBDRIVER_PATH / CHROME_DRIVER

LOGGING_DAILY = {
    'version': 1,
    'formatters': {
        'default': {
            'format': '%(asctime)s %(levelname)-8s %(funcName)20s()-%(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'default'
        },
        'file': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'default',
            'filename': ROOT_PATH / 'logs' / 'daily_master.log',
            'when': 'D',
            'backupCount': 5
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console', 'file'],
        'disable_existing_loggers': False
    },
}

LOGGING_WEEKLY = {
    'version': 1,
    'formatters': {
        'default': {
            'format': '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler'
        },
        'file': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'default',
            'filename': ROOT_PATH / 'logs' / 'weekly_master.log',
            'when': 'W0',
            'backupCount': 4
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console', 'file']
    },
}

LOGGING_MERIT = {
    'version': 1,
    'formatters': {
        'default': {
            'format': '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler'
        },
        'file': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'default',
            'filename': ROOT_PATH / 'logs' / 'in_meritindia_master.log',
            'when': 'D',
            'backupCount': 5
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console', 'file'],
        'disable_existing_loggers': False
    },
}

LOGGING_EDC_ELEC_DAILY = {
    'version': 1,
    'formatters': {
        'default': {
            'format': '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler'
        },
        'file': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'default',
            'filename': ROOT_PATH / 'logs' / 'edc_daily_elec_master.log',
            'when': 'D',
            'backupCount': 5
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console', 'file'],
        'disable_existing_loggers': False
    },
}

LOGGING_EDC_GAS_DAILY = {
    'version': 1,
    'formatters': {
        'default': {
            'format': '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler'
        },
        'file': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'default',
            'filename': ROOT_PATH / 'logs' / 'edc_daily_gas_master.log',
            'when': 'D',
            'backupCount': 5
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console', 'file'],
        'disable_existing_loggers': False
    },
}

LOGGING_EDC_GOOGLE_TRENDS_DAILY = {
    'version': 1,
    'formatters': {
        'default': {
            'format': '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler'
        },
        'file': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'default',
            'filename': ROOT_PATH / 'logs' / 'edc_google_trends_daily_master.log',
            'when': 'D',
            'backupCount': 5
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console', 'file'],
        'disable_existing_loggers': False
    },
}

LOGGING_EDC_HOURLY = {
    'version': 1,
    'formatters': {
        'default': {
            'format': '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler'
        },
        'file': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'default',
            'filename': ROOT_PATH / 'logs' / 'hourly_master.log',
            'when': 'D',
            'backupCount': 5
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console', 'file'],
        'disable_existing_loggers': False
    },
}


LOGGING_EDC_POPULATE_DB = {
    'version': 1,
    'formatters': {
        'default': {
            'format': '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler'
        },
        'file': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'default',
            'filename': ROOT_PATH / 'logs' / 'edc_populate_db.log',
            'when': 'D',
            'backupCount': 5
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console', 'file'],
        'disable_existing_loggers': False
    },
}


EDC_TIMEOUT = 1000  # time out for runs

EDC_DAILY_ELECTRICITY_JOBS = {'Filestore': {'provider_code': 'elec_filestore', 'source_code': 'electricity_filestore'},
                              'Nigeria': {'provider_code': 'org_niggrid','source_code': 'nigerian_daily_generation_stats'},
                              'Japan Tepco_demand': {'provider_code': 'jp_tepco','source_code': 'japanese_tepco_demand_stats'},
                              'Japan Tepco_generation': {'provider_code': 'jp_tepco', 'source_code': 'japanese_tepco_generation_stats'},
                              'Japan Hokuriku_demand': {'provider_code': 'jp_rikuden', 'source_code': 'japanese_hokuriku_demand_stats'},
                              'Japan Hokuriku_generation': {'provider_code': 'jp_rikuden',  'source_code': 'japanese_hokuriku_generation_stats'},
                              'Japan Okinawa_demand': {'provider_code': 'jp_okiden', 'source_code': 'japanese_okinawa_demand_stats'},
                              'Japan Okinawa_generation': {'provider_code': 'jp_okiden', 'source_code': 'japanese_okinawa_generation_stats'},
                              'Japan Chugoku_demand': {'provider_code': 'jp_energia','source_code': 'japanese_chugoku_demand_stats'},
                              'Japan Chugoku_generation': {'provider_code': 'jp_energia','source_code': 'japanese_chugoku_generation_stats'},
                              'Japan Tohoku_demand': {'provider_code': 'jp_tohoku', 'source_code': 'japanese_tohoku_demand_stats'},
                              'Japan Tohoku_generation': {'provider_code': 'jp_tohoku', 'source_code': 'japanese_tohoku_generation_stats'},
                              'Japan Kansai_demand': {'provider_code': 'jp_kansai_td', 'source_code': 'japanese_kansai_demand_stats'},
                              'Japan Kansai_generation': {'provider_code': 'jp_kansai_td', 'source_code': 'japanese_kansai_generation_stats'},
                              'Japan Shikoku_demand': {'provider_code': 'jp_yonden', 'source_code': 'japanese_shikoku_demand_stats'},
                              'Japan Shikoku_generation': {'provider_code': 'jp_yonden', 'source_code': 'japanese_shikoku_generation_stats'},
                              'Japan Kyushu_demand': {'provider_code': 'jp_kyuden', 'source_code': 'japanese_kyushu_demand_stats'},
                              'Japan Kyushu_generation': {'provider_code': 'jp_kyuden', 'source_code': 'japanese_kyushu_generation_stats'},
                              'Japan Chubu_demand': {'provider_code': 'jp_chubu', 'source_code': 'japanese_chubu_demand_stats'},
                              'Japan Chubu_generation': {'provider_code': 'jp_chubu','source_code': 'japanese_chubu_generation_stats'},
                              'Japan Hokkaido_demand': {'provider_code': 'jp_hepco', 'source_code': 'japanese_hokkaido_demand_stats'},
                              'Japan Hokkaido_generation': {'provider_code': 'jp_hepco', 'source_code': 'japanese_hokkaido_generation_stats'},
                              'South Africa': {'provider_code': 'za_eksom', 'source_code': 'south_african_power_stats'},
                              'Taiwan': {'provider_code': 'tw_taipower', 'source_code': 'taiwanese_power_stats'},
                              'Brazil': {'provider_code': 'br_ons', 'source_code': 'brazilian_power_stats'},
                              'Europe': {'provider_code': 'eu_entsoe', 'source_code': 'european_power_stats'},
                              'Japan': {'provider_code': 'org_jepx', 'source_code': 'japanese_prices_stats'},
                              'US': {'provider_code': 'gov_eia', 'source_code': 'us_power_stats'},
                              'Turkey': {'provider_code': 'tr_epias', 'source_code': 'turkish_power_stats'},
                              'Australia': {'provider_code': 'au_aemo', 'source_code': 'australian_power_stats'},
                              'Iran': {'provider_code': 'ir_irema', 'source_code': 'iranian_power_stats'},
                              'Australia West': {'provider_code': 'au_wem','source_code': 'west_australian_power_stats'},
                              'Malaysia': {'provider_code': 'my_gso', 'source_code': 'malaysian_power_stats'},
                              'New Zealand': {'provider_code': 'nz_transpower', 'source_code': 'nz_daily_power_stats'},
                              'Colombia': {'provider_code': 'co_xm', 'source_code': 'colombian_power_stats'},
                              'Argentina_power': {'provider_code': 'ar_cammesa', 'source_code': 'argentina_power_stats'},
                              'Argentina_generation': {'provider_code': 'ar_cammesa', 'source_code': 'argentina_generation_stats'},
                              'Singapore': {'provider_code': 'sg_ema', 'source_code': 'singapore_power_stats'},
                              'Russia': {'provider_code': 'ru_atc', 'source_code': 'russian_power_stats'},
                              'Russia ups': {'provider_code': 'ru_ups', 'source_code': 'russian_power_stats_ups'},
                              'Bolivia': {'provider_code': 'bo_cndc', 'source_code': 'bolivia_power_stats'},
                              'Great Britain': {'provider_code': 'gb_elexon', 'source_code': 'great_britain_power_stats'},
                              'Mexican_generation': {'provider_code': 'mx_cenace', 'source_code': 'mexican_generation_stats'},
                              'Mexican_demand': {'provider_code': 'mx_cenace', 'source_code': 'mexican_demand_stats'},
                              'Mexican_prices': {'provider_code': 'mx_cenace', 'source_code': 'mexican_prices_stats'},
                              'Australia_generation': {'provider_code': 'au_aemo','source_code': 'australian_generation_stats'},
                              'Peru': {'provider_code': 'pe_coes', 'source_code': 'peruvian_power_stats'},
                              'Philippines': {'provider_code': 'ph_iemop', 'source_code': 'philippines_power_stats'},
                              'Costa Rica': {'provider_code': 'cr_cence', 'source_code': 'costarican_power_stats'},
                              'Chile': {'provider_code': 'cl_coordinador', 'source_code': 'chilean_power_stats'},
                              'USA_MISO': {'provider_code': 'org_miso', 'source_code': 'usa_miso_prices_stats'},
                              'USA_PJM': {'provider_code': 'com_pjm', 'source_code': 'usa_pjm_prices_stats'},
                              'USA_CAISO': {'provider_code': 'com_caiso', 'source_code': 'usa_caiso_prices_stats'},
                              'USA_NYISO': {'provider_code': 'com_nyiso', 'source_code': 'usa_nyiso_prices_stats'},
                              'USA_ERCOT': {'provider_code': 'com_ercot', 'source_code': 'usa_ercot_prices_stats'},
                              'Ukraine': {'provider_code': 'ua_energo', 'source_code': 'ukrainian_power_stats'},
                              'Moldova': {'provider_code': 'md_moldelectrica', 'source_code': 'moldova_power_stats'},
                              'Ireland': {'provider_code': 'com_eirgrid', 'source_code': 'irish_power_stats'},
                              'Uruguay': {'provider_code': 'uy_adme', 'source_code': 'uruguayan_power_stats'},
                              }

EDC_DAILY_GAS_JOBS = {'Europe': {'provider_code': 'eu_entsog', 'source_code': 'european_gas_stats'},
                      'USA': {'provider_code': 'gov_eia', 'source_code': 'us_gas_stats'},
                      'Norway':{'provider_code': 'no_gassco', 'source_code': 'norwegian_gas_stats'}}

EDC_DAILY_OTHER_JOBS = {'IMF_exchange_rates': {'provider_code': 'org_imf', 'source_code': 'imf_exchange_rates'},
                        'FXTOP_exchange_rates': {'provider_code': 'com_fxtop', 'source_code': 'fxtop_exchange_rates'}
                        }

EDC_DAILY_GOOGLE_TRENDS_JOBS = {'google_trends': {'provider_code': 'com_google_trends', 'source_code': 'google_trends'}
                                }

EDC_DAILY_JAPAN_JOBS = {
    'Japan Tepco_demand': {'provider_code': 'jp_tepco', 'source_code': 'japanese_tepco_demand_stats'},
    'Japan Tepco_generation': {'provider_code': 'jp_tepco', 'source_code': 'japanese_tepco_generation_stats'},
    'Japan Hokuriku_demand': {'provider_code': 'jp_rikuden', 'source_code': 'japanese_hokuriku_demand_stats'},
    'Japan Hokuriku_generation': {'provider_code': 'jp_rikuden', 'source_code': 'japanese_hokuriku_generation_stats'},
    'Japan Okinawa_demand': {'provider_code': 'jp_okiden', 'source_code': 'japanese_okinawa_demand_stats'},
    'Japan Okinawa_generation': {'provider_code': 'jp_okiden', 'source_code': 'japanese_okinawa_generation_stats'},
    'Japan Chugoku_demand': {'provider_code': 'jp_energia', 'source_code': 'japanese_chugoku_demand_stats'},
    'Japan Chugoku_generation': {'provider_code': 'jp_energia', 'source_code': 'japanese_chugoku_generation_stats'},
    'Japan Tohoku_demand': {'provider_code': 'jp_tohoku', 'source_code': 'japanese_tohoku_demand_stats'},
    'Japan Tohoku_generation': {'provider_code': 'jp_tohoku', 'source_code': 'japanese_tohoku_generation_stats'},
    'Japan Kansai_demand': {'provider_code': 'jp_kansai_td', 'source_code': 'japanese_kansai_demand_stats'},
    'Japan Kansai_generation': {'provider_code': 'jp_kansai_td', 'source_code': 'japanese_kansai_generation_stats'},
    'Japan Shikoku_demand': {'provider_code': 'jp_yonden', 'source_code': 'japanese_shikoku_demand_stats'},
    'Japan Shikoku_generation': {'provider_code': 'jp_yonden', 'source_code': 'japanese_shikoku_generation_stats'},
    'Japan Kyushu_demand': {'provider_code': 'jp_kyuden', 'source_code': 'japanese_kyushu_demand_stats'},
    'Japan Kyushu_generation': {'provider_code': 'jp_kyuden', 'source_code': 'japanese_kyushu_generation_stats'},
    'Japan Chubu_demand': {'provider_code': 'jp_chubu', 'source_code': 'japanese_chubu_demand_stats'},
    'Japan Chubu_generation': {'provider_code': 'jp_chubu', 'source_code': 'japanese_chubu_generation_stats'},
    'Japan Hokkaido_demand': {'provider_code': 'jp_hepco', 'source_code': 'japanese_hokkaido_demand_stats'},
    'Japan Hokkaido_generation': {'provider_code': 'jp_hepco', 'source_code': 'japanese_hokkaido_generation_stats'}
    }

EDC_DAILY_USA_PRICES_JOBS = {'USA_MISO': {'provider_code': 'org_miso', 'source_code': 'usa_miso_prices_stats'},
                              'USA_PJM': {'provider_code': 'com_pjm', 'source_code': 'usa_pjm_prices_stats'},
                              'USA_CAISO': {'provider_code': 'com_caiso', 'source_code': 'usa_caiso_prices_stats'},
                              'USA_NYISO': {'provider_code': 'com_nyiso', 'source_code': 'usa_nyiso_prices_stats'},
                              'USA_ERCOT': {'provider_code': 'com_ercot', 'source_code': 'usa_ercot_prices_stats'}}

EDC_INACTIVE_DAILY_JOBS = {
    'Philippines_historical': {'provider_code': 'ph_iemop', 'source_code': 'philippines_historical_power_stats'},
    'India_historical': {'provider_code': 'in_meritindia', 'source_code': 'indian_historical_power_stats'}
}

EDC_DAILY_JOBS_BATCH_ELEC = EDC_DAILY_ELECTRICITY_JOBS
EDC_DAILY_JOBS_BATCH_GAS_OTHERS = d5 = {**EDC_DAILY_GAS_JOBS, **EDC_DAILY_OTHER_JOBS}
EDC_ALL_DAILY_JOBS = d5 = {**EDC_DAILY_JOBS_BATCH_ELEC, 
                           **EDC_DAILY_JOBS_BATCH_GAS_OTHERS,
                           **EDC_INACTIVE_DAILY_JOBS}

EDC_HOURLY_JOBS = {'New Zealand': {'provider_code': 'nz_transpower', 'source_code': 'nz_hourly_power_stats'},
                   'Nigeria': {'provider_code': 'org_niggrid', 'source_code': 'nigerian_hourly_demand_stats'},
                   'India': {'provider_code': 'in_meritindia', 'source_code': 'indian_hourly_power_stats'}}

REQUESTS_HEADERS = {'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
                                   + ' AppleWebKit/537.36 (KHTML, like Gecko) '
                                   + 'Chrome/80.0.3987.87 Safari/537.36')}

EDC_TOLERATED_LISTS = {
        'country_list': [country.alpha_3 for country in pycountry.countries],
        'gas_country_partner_list': [country.alpha_3 for country in pycountry.countries] + ['LNG Partner'],
        'gas_trade_location_list': ['Remich ITP-00072', 'Hora Svaté Kateřiny (CZ) / Deutschneudorf (Sayda) (DE) ITP-00015', 'Waidhaus ITP-00139', 'Lanžhot ITP-00051', 'Brandov STEGAL (CZ) / Stegal (DE) ITP-00123', 'Olbernhau (DE) / Hora Svaté Kateřiny (CZ) ITP-00150', 'Cieszyn (PL) / Český Těšín (CZ) ITP-00158', 'Deutschneudorf EUGAL Brandov  ITP-00535', 'VIP PIRINEOS ITP-00304', 'Bocholtz (Fluxys TENP) ITP-00068', 'Eynatten (BE) // Lichtenbusch / Raeren (DE) (Fluxys TENP) ITP-00057', 'Überackern ABG (AT) / Überackern (DE) ITP-00019', 'Überackern SUDAL (AT) / Überackern 2 (DE) ITP-00007', 'VIP Kiefersfelden-Pfronten ITP-00291', 'RC Lindau ITP-00227', 'BALTICCONNECTOR ITP-00550', 'Tarvisio (IT) / Arnoldstein (AT) ITP-00040', 'Gorizia (IT) /Šempeter (SI) ITP-00049', 'Melendugno - IT / TAP ITP-00008', 'VIP France - Germany ITP-00540', 'Medelsheim (DE) / Obergailbach (FR) (GRTgaz D) ITP-00083', 'Oberkappel (GRTgaz D) ITP-00056', 'Waidhaus (GRTgaz D) ITP-00073', 'Mallnow ITP-00096', 'VIP Brandov ITP-00537', 'Bunde (DE) / Oude Statenzijl (H) (NL) (GASCADE) ITP-00076', 'Eynatten 1 (BE) // Lichtenbusch / Raeren (DE) ITP-00112', 'Brandov / OPAL ITP-00452', 'Kulata (BG) / Sidirokastron (GR) ITP-00128', 'Nea Mesimvria ITP-00427', 'VIP DK-THE ITP-10011', 'GCP GAZ-SYSTEM/ONTRAS ITP-00497', 'VIP IBERICO ITP-00286', 'Balassagyarmat (HU) / Velké Zlievce (SK) ITP-00027', 'Baumgarten ITP-00168', 'Kiemenai ITP-00054', 'Murfeld (AT) / Ceršak (SI) ITP-00079', 'Rogatec ITP-00042', 'Baumgarten (TAG) ITP-00037', 'Negru Voda II, III (RO) / Kardam (BG) ITP-00059', 'Bacton (BBL) ITP-00207', 'Zelzate (Zebra Pijpleiding) ITP-00110', 'Zevenaar ITP-00259', 'Winterswijk ITP-00078', 'Bunde (DE) / Oude Statenzijl (H) (NL) (GUD) ITP-00102', 'Bunde (DE) / Oude Statenzijl (L) (NL) (GUD) ITP-00107', 'Bunde (DE) / Oude Statenzijl (L) (NL) (GTG Nord) ITP-00118', 'Vlieghuis ITP-00151', 'Bocholtz-Vetschau ITP-00025', 'Dinxperlo ITP-00305', 'VIP-BENE ITP-00555', 'Bunde (DE) / Oude Statenzijl (H) (NL) (GTG Nord) ITP-00507', 'Zandvliet H-gas ITP-00088', 'Tegelen ITP-00244', "'s Gravenvoeren Dilsen (BE) // 's Gravenvoeren/Obbicht (NL) ITP-00258", 'Hilvarenbeek ITP-00038', 'VIP TTF-NCG H ITP-00551', 'Zelzate ITP-00101', 'Bunde (DE) / Oude Statenzijl (H) (NL) I (OGE) ITP-00103', 'Haanrade ITP-00071', 'VIP TTF-GASPOOL H ITP-00554', 'VIP TTF-THE-L ITP-10010', 'Bocholtz ITP-00169', 'Dravaszerdahely ITP-00011', 'Csanadpalota ITP-00032', 'Mosonmagyarovar ITP-00043', 'VIRTUALYS ITP-00526', 'Zeebrugge IZT ITP-00061', 'Blaregnies L (BE) / Taisnières B (FR) ITP-00115', 'VIP Belgium - NCG ITP-00542', 'Obergailbach (FR) / Medelsheim (DE) ITP-00137', 'Ellund (GUD) ITP-00109', 'Zevenaar (Thyssengas) ITP-00026', 'Baumgarten (WAG) ITP-00162', 'Petrzalka ITP-00255', 'Oberkappel ITP-00140', 'Baumgarten (Gas Connect Austria) ITP-00062', 'South North CSEP ITP-00222', 'VIP Waidhaus ITP-00538', 'Bacton (IUK) ITP-00005', 'Bacton IPs ITP-00492', 'Moffat ITP-00090', 'Zevenaar (OGE) ITP-00060', 'Bocholtz (OGE) ITP-00066', 'VIP Oberkappel ITP-00539', 'Waidhaus (OGE) ITP-00069', 'Ellund (OGE) ITP-00031', 'Medelsheim (DE) / Obergailbach (FR) (OGE) ITP-00047', 'Oberkappel (OGE) ITP-00006', 'Negru Voda I (RO) / Kardam (BG) ITP-00058', 'Ruse (BG) / Giurgiu (RO) ITP-00153', 'Lubmin II ITP-00501', 'Greifswald / Fluxys Deutschland ITP-00297', 'Värska ITP-00187', 'Narva ITP-00243', 'Gela ITP-00074', 'Mazara del Vallo ITP-00093', 'Greifswald / OPAL ITP-00251', 'Greifswald / NEL ITP-00247', 'Greifswald / LBTG ITP-00454', 'Kipi (TR) / Kipi (GR) ITP-00046', 'Tarifa ITP-00082', 'Almería ITP-00048', 'Uzhgorod (UA) - Velké Kapušany (SK) ITP-00117', 'Kotlovka ITP-00085', 'Kipoi ITP-00274', 'Strandzha 2 (BG) / Malkoclar (TR) ITP-00549', 'Emden (EPT1) (GTS) ITP-00160', 'VIP Bereg (HU) / VIP Bereg (UA) ITP-10006', 'Zeebrugge ZPT ITP-00106', 'Kondratki ITP-00104', 'Wysokoje ITP-00092', 'GCP GAZ-SYSTEM/UA TSO ITP-10008', 'Tieterowka ITP-00094', 'Dunkerque ITP-00045', 'Emden (EPT1) (GUD) ITP-00081', 'Greifswald / GUD ITP-00491', 'Dornum / NETRA (GUD) ITP-00188', 'Emden (EPT1) (Thyssengas) ITP-00105', 'Emden (EPT1) (OGE) ITP-00080', 'Dornum / NETRA (OGE) ITP-00126', 'Dornum GASPOOL ITP-00525', 'Imatra ITP-00024', 'Panigaglia LNG-00019', 'OLT LNG / Livorno LNG-00004', 'Cavarzere (Porto Levante / Adriatic LNG) LNG-00015', 'Klaipeda (LNG) LNG-00030', 'Gate Terminal (I) LNG-00027', 'Zeebrugge LNG LNG-00017', 'Swinoujscie LNG-00006', 'Sines LNG-00026', 'Wallbach ITP-00294', 'VIP Germany-CH ITP-00544', 'Griespass (CH) / Passo Gries (IT) ITP-00136', 'Bizzarone ITP-00278', 'Budince ITP-00421', 'Sakiai ITP-00050', 'Kyustendil (BG) / Zidilovo (MK) ITP-00036', 'Kireevo (BG) / Zaychar (RS) ITP-00529', 'Kiskundorozsma-2 (HU) / Horgos (RS) ITP-10013', 'Kiskundorozsma (HU>RS) ITP-00055', 'Oltingue (FR) / Rodersdorf (CH) ITP-00039', 'RC Basel ITP-00228', 'RC Thayngen-Fallentor ITP-00229', 'Strandzha (BG) / Malkoclar (TR) ITP-00041','Dornum', 'Emden', 'Dunkerque', 'Zeebrugge', 'Easington', 'St.Fergus', 'Fields Delivering into SEGAL', 'Other Exit Nominations'],
        'gas_unit_list': ['kWh/h', 'Bcm', 'Bcf', 'TJ', 'MSm3'],
        'price_node_list':[np.nan,
                           'Illinois', 'Michigan', 'Minnesota', 'Indiana', 'Arkansas', 'Louisiana', 'Texas', 'Mississippi', 'Houston Area', 'North Area', 'Panhandle Area', 'South Area', 'West Area',
                           'California Oregon Intertie', 'Palo Verde Intertie', 'ZP26- Central Generation Area', 'NP15- San Francisco Area', 'SP15- Los Angeles Area',
                           'Capital Area', 'Central Area', 'Dunwoodie', 'Genesee', 'Hydro Quebec Intertie', 'Hudson Valley', 'Long Island', 'Mohawk Valley', 'Millwood', 'New York City', 'ISO-NE Intertie', 'IESO Intertie', 'PJM Intertie',
                           'East Pennsylvnaia', 'West Pennsylvania', 'New Jersey', 'Chicago', 'North Illinois', 'American Electric Power', 'American Electric Power Dayton', 'Ohio', 'Virginia', 'American Transmission System Inc.']
        }
