# -*- coding: utf-8 -*-
"""
Created on Thu Sep  9 17:14:53 2021

@author: NGHIEM_A
Source: https://gist.github.com/systemcatch/13a1b301471861458bef8a105a6f30a9#file-ph_plant_map-py
and some research:
Mindanao: https://www.doe.gov.ph/sites/default/files/pdf/electric_power/electric_power_plants_mindanao_december_2020-03_may-2021.pdf

"""

Luzon = {
'1GNPD_U01': 'coal',
'3SBPL_G01': 'coal',
'1SUAL_G01': 'coal',
'3ILIJAN_G02': 'gas',
'3ILIJAN_G01': 'gas',
'3QPPL_G01': 'coal',
'1MARVEL_G02': 'coal',
'3PAGBIL_G01': 'coal',
'3CALACA_G02': 'coal',
'3CALACA_G01': 'coal',
'1MSINLO_G02': 'coal',
'1MSINLO_G01': 'coal',
'1MSINLO_G03': 'coal',
'3STA-RI_G05': 'gas',
'3STA-RI_G06': 'gas',
'3STA-RI_G03': 'gas',
'3STA-RI_G04': 'gas',
'3STA-RI_G01': 'gas',
'3STA-RI_G02': 'gas',
'3SNGAB_G01': 'gas',
'3SLPGC_G02': 'coal',
'1CASECN_G01': 'hydro',
'3SLTEC_G02': 'coal',
'3SLTEC_G01': 'coal',
'3BACMAN_G01': 'geothermal',
'1SMC_G01': 'coal',
'3TIWI_C': 'geothermal',
'1MARVEL_G01': 'coal',
'3PAGBIL_G03': 'coal',
'1SMC_G02': 'coal',
'3MKBN_A': 'geothermal',
'1BAKUN_G01': 'hydro',
'1MAGAT_U03': 'hydro',
'1ANDA_G01': 'coal',
'1MAGAT_U04': 'hydro',
'1MAGAT_U02': 'hydro',
'3MKBN_B': 'geothermal',
'3KAL_G02': 'hydro storage',
'1BURGOS_G01': 'wind',
'3KAL_G01': 'hydro storage',
'1SROQUE_U02': 'hydro',
'1MAGAT_U01': 'hydro',
'3MALAYA_G02': 'gas',
'3MKBN_D': 'geothermal',
'1HEDCOR_G01': 'hydro',
'3TIWI_A': 'geothermal',
'3SLPGC_G01': 'coal',
'1SROQUE_U03': 'hydro',
'1AMBUK_U01': 'hydro',
'3MKBN_E': 'geothermal',
'1BINGA_U02': 'hydro',
'1BINGA_U01': 'hydro',
'1BINGA_U03': 'hydro',
'3BACMAN_G02': 'geothermal',
'1BINGA_U04': 'hydro',
'3MGPP_G01': 'geothermal',
'1CAPRIS_G01': 'wind',
'1AMBUK_U03': 'hydro',
'1IBEC_G01': 'biomass',
'1AMBUK_U02': 'hydro',
'1ANGAT_M': 'hydro',
'1SROQUE_U01': 'hydro',
'1APEC_G01': 'coal',
'3AVION_U01': 'gas',
'3CALIRY_G01': 'hydro',
'1ANGAT_A': 'hydro',
'3AWOC_G01': 'wind',
'3AVION_U02': 'gas',
'1GIFT_G01': 'biomass',
'1BT2020_G01': 'biomass',
'1IPOWER_G01': 'biomass',
'1PNTBNG_U01': 'hydro',
'1PETSOL_G01': 'solar',
'1SABANG_G01': 'hydro',
'1PNTBNG_U02': 'hydro',
'3CALSOL_G01': 'solar',
'1LIMAY_B': 'gas',
'1BAUANG_G01': 'oil',
'2TMO_G03': 'oil',
'3MEC_G01': 'solar',
'2TMO_G01': 'oil',
'1S_ENRO_G01': 'oil',
'1SUBSOL_G01': 'solar',
'1NWIND_G02': 'wind',
'1MASIWA_G01': 'hydro',
'2TMO_G02': 'oil',
'1NIABAL_G01': 'hydro',
'1NWIND_G01': 'wind',
'3BBEC_G01': 'biomass',
'1MAEC_G01': 'solar',
'2TMO_G04': 'oil',
'1CLASOL_G01': 'solar',
'1NMHC_G01': 'hydro',
'1MARSOL_G01': 'solar',
'1PETRON_G01': 'unknown',
'3BOTOCA_G01': 'hydro',
'1BULSOL_G01': 'solar',
'1RASLAG_G02': 'solar',
'1SLANGN_G01': 'hydro',
'1RASLAG_G01': 'solar',
'1LIMAY_A': 'gas',
'3ORMAT_G01': 'geothermal',
'1CABSOL_G01': 'solar',
'1YHGRN_G01': 'solar',
'1CIP2_G01': 'oil',
'2VALSOL_G01': 'solar',
'2MMPP_G01': 'gas',
'3KAL_G03': 'hydro storage',
'1NMHC_G03': 'hydro',
'1SMBELL_G01': 'hydro',
'1ARMSOL_G01': 'solar',
'2PNGEA_G01': 'gas',
'1DALSOL_G01': 'solar',
'1ZAMSOL_G01': 'solar',
'1BURGOS_G02': 'solar',
'1SPABUL_G01': 'solar',
'1BURGOS_G03': 'solar',
'3ADISOL_G01': 'solar',
'1BTNSOL_G01': 'solar',
'3RCBMI_G01': 'oil',
'3RCBMI_G02': 'oil',
'3PAGBIL_G02': 'coal',
'3LIAN_G01': 'biomass',
'1BOSUNG_G01': 'solar',
'2SMNRTH_G01': 'solar',
'1ACNPC_G01': 'biomass',
'1GFII_G01': 'biomass',
'1IPOWER_G02': 'unknown',
'3MKBN_C': 'geothermal',
'3MALAYA_G01': 'gas',
'3BART_G01': 'hydro',
'3SLPGC_G03': 'coal',
'1MSNLO_BATG': 'battery storage',
'3HDEPOT_G01': 'solar',
'1SMC_G03': 'coal',
'1SMC_G04': 'coal',
'3TIWI_B': 'geothermal',
'3SLPGC_G04': 'coal',
'1SUAL_G02': 'coal',
'1T_ASIA_G01': 'oil',
'1UPPC_G01': 'coal',
'3KAL_G04': 'hydro storage'
}

Visayas = {
'4LEYTE_A': 'geothermal',
'5THVI_U01': 'coal',
'5THVI_U02': 'coal',
'5TPC_G02': 'coal',
'8PEDC_U03': 'coal',
'6PAL1A_G01': 'geothermal',
'5KSPC_G02': 'coal',
'5KSPC_G01': 'coal',
'8PALM_G01': 'coal',
'5CEDC_U03': 'coal',
'5CEDC_U01': 'coal',
'5CEDC_U02': 'coal',
'8PEDC_U02': 'coal',
'8PEDC_U01': 'coal',
'6NASULO_G01': 'geothermal',
'4LGPP_G01': 'geothermal',
'6HELIOS_G01': 'solar',
'8PDPP3_G01': 'oil',
'5CPPC_G01': 'oil',
'6PAL2A_U01': 'geothermal',
'6PAL2A_U02': 'geothermal',
'8PWIND_G01': 'wind',
'5TOLSOL_G01': 'solar',
'6PAL2A_U03': 'geothermal',
'6MANSOL_G01': 'solar',
'4SEPSOL_G01': 'solar',
'6CARSOL_G01': 'solar',
'6SACASL_G02': 'solar',
'8SUWECO_G01': 'hydro',
'6SACASL_G01': 'solar',
'5CDPPI_G02': 'oil',
'5EAUC_G01': 'oil',
'4PHSOL_G01': 'solar',
'8PDPP_G01': 'oil',
'6SACSUN_G01': 'solar',
'6SLYSOL_G01': 'solar',
'8SLWIND_G01': 'wind',
'7BDPP_G01': 'oil',
'6MNTSOL_G01': 'solar',
'5TPC_G01': 'oil',
'5CDPPI_G01': 'oil',
'7JANOPO_G01': 'hydro',
'6FFHC_G01': 'biomass',
'8GLOBAL_G01': 'unknown',
'7LOBOC_G01': 'hydro',
'8COSMO_G01': 'solar',
'8STBAR_PB': 'oil',
'6AMLA_G01': 'hydro',
'8STBAR_PB2': 'oil',
'6SCBE_G01': 'oil',
'5PHNPB3_G01': 'oil',
'8PDPP3_G': 'oil',
'8CASA_G01': 'biomass',
'8AVON_G01': 'oil',
'6VMC_G01': 'biomass',
'6URC_G01': 'biomass',
'8PPC_G01': 'oil',
'6PAL2A_U04': 'geothermal',
'8PDPP3_H': 'oil',
'8PDPP3_D': 'oil',
'5LBGT_G01': 'gas',
'6HPCO_G01': 'biomass',
'8GUIM_G01': 'wind',
'6PAL2A_G01': 'geothermal',
'8PDPP3_F': 'oil',
'8PDPP3_C': 'oil',
'6CENPRI_U01': 'oil',
'6CENPRI_U02': 'oil',
'6CENPRI_U03': 'oil',
'6CENPRI_U04': 'oil',
'8PDPP3_E': 'oil',
'5LBGT_G02': 'gas'
}

Mindanao = {
'9AGUS4_G03': 'hydro',
'9AGUS1_G02': 'hydro',
'9AGUS1_G01': 'hydro',
'9AGUS2_G02': 'hydro',
'9AGUS4_G01': 'hydro',
'9AGUS5_G01': 'hydro',
'9AGUS5_G02': 'hydro',
'9AGUS6_G03': 'hydro',
'9AGUS6_G04': 'hydro',
'9AGUS2_G03': 'hydro',
'9AGUS7_G03': 'hydro',
'9AGUS6_G01': 'hydro',
'9AGUS6_G02': 'hydro',
'9AGUS7_G01': 'hydro',
'9AGUS7_G02': 'hydro',
'9AGUS2_G01': 'hydro',
'9AGUS4_G02': 'hydro',
'10AGUS1_U02': 'hydro',
'10AGUS2_U02': 'hydro', 
'10AGUS4_U01': 'hydro',
'10AGUS4_U02': 'hydro',
'10AGUS4_U03': 'hydro',
'10AGUS4_U02': 'hydro',
'10AGUS5_U01': 'hydro',
'10AGUS5_U02': 'hydro',
'10AGUS6_U01': 'hydro',
'10AGUS6_U02': 'hydro',
'10AGUS5_U03': 'hydro',
'10AGUS6_U04': 'hydro',
'10AGUS6_U05': 'hydro',
'10AGUS7_U01': 'hydro',
'9SPPC_G01': 'diesel',
'9SPPC_G02': 'diesel',
'9WMPC_G01': 'diesel',
'9WMPC_G02': 'diesel',
'9WMPC_G03': 'diesel',
'9WMPC_G04': 'diesel',
'9WMPC_G05': 'diesel',
'9WMPC_G06': 'diesel',
'9WMPC_G07': 'diesel',
'9WMPC_G08': 'diesel',
'9WMPC_G09': 'diesel',
'9WMPC_G10': 'diesel',
'9KIDPAW_G01': 'geothermal',
'9KIDPAW_G02': 'geothermal',
'9PICOP_G01': 'diesel',
'13DCPP_U01': 'coal',
'13DCPP_U02': 'coal',
'10GNPK_U01': 'coal',
'10GNPK_U02': 'coal',
'13SMC_U01': 'coal',
'13SMC_U02': 'coal',
'11FDC_U01': 'coal',
'11FDC_U01': 'coal',
'STEAG_U01': 'coal',
'STEAG_U02': 'coal',
}

ph_fuel_mapping = {
    'battery storage': 'Other',
    'biomass': 'Biomass',
    'gas': 'Natural Gas',
    'geothermal': 'Geothermal',
    'hydro': 'Hydro',
    'hydro storage': 'Hydro Reservoir',
    'oil': 'Oil', 
    'solar': 'Solar',
    'unknown': 'Other',
    'wind': 'Wind Onshore',
    'diesel': 'Oil',    
    'coal': 'Coal'
}

POWER_PLANTS_MAPPING = {**Luzon, **Visayas, **Mindanao}
POWER_PLANTS_MAPPING_EDC = {power_plant: ph_fuel_mapping[fuel] 
                            for power_plant, fuel in POWER_PLANTS_MAPPING.items()}