# ---------------------------------------------------------------------------------
# Option Quote Parser 
#      Extracts option prices from the High Yield Credit Default Swap Index (HYCDX)
#
# (C) 2022 Stephen Olano, Jersey City, NJ
# email poweribo@yahoo.com
# ---------------------------------------------------------------------------------

import datetime as dt
import pandas as pd
import glob
import os
import re
from abc import ABC, abstractmethod

## Parsing Strategy - Abstract Base Class
class ParsingStrategy(ABC):    

    # common header date format    
    __header_date_format = '%m/%d/%y'

    # common header regex
    @staticmethod
    def get_header_rx_dict():
        return {
            'header' : re.compile(r'^From: (?P<firm_sender>\w+) At: (?P<date>\d{2}\/\d{2}\/\d{2}) (?P<time>\d{2}\:\d{2}\:\d{2}) (\w{3}[-+]\d?\d\:\d\d)'),
        }

    @abstractmethod
    def get_rx_dict(self):
        pass

    @abstractmethod    
    def on_header(self, match):
        self.pq = ProductQuote()
        self.pq.firm_sender = match.group('firm_sender')
        date_str = match.group('date')
        self.pq.date = dt.datetime.strptime(date_str, self.__header_date_format).strftime('%d-%b-%y')
        self.pq.time = match.group('time')                

    @abstractmethod    
    def on_subject(self, match):
        self.pq.ref_px = match.group('ref_px')
    
    @abstractmethod    
    def on_expiry(self, match):    
        exp_date_str = match.group('expiration_date')
        self.pq.expire_date = dt.datetime.strptime(exp_date_str, self.exp_date_format).strftime('%d-%b-%y')

    @abstractmethod    
    def on_table_header(self, match):    
        self.col_idx = dict()
        for index, col_name in enumerate(match.groupdict()):
            self.col_idx[col_name] = index            

    @abstractmethod    
    def on_table_row(self, line):    
        pass

## Object to hold row values
class ProductQuote:

    def reset_table_row_values(self):
        self.strike_px   = ''
        self.strike_spd  = ''
        self.option_type = ''
        self.bid_price   = ''
        self.ask_price   = ''    
        self.delta       = ''
        self.iv_spd      = ''
        self.iv_bps      = ''
        self.iv_px       = ''

    def get_values(self):        
        return [self.date, self.time, self.firm_sender, self.expire_date, self.option_type, self.strike_px, self.strike_spd, 
                self.bid_price, self.ask_price, self.delta, self.iv_spd, self.iv_bps, self.iv_px, self.ref_px]        

## Concrete strategies for XXX, YYY, ZZZ, WWW
class ParserForXXX(ParsingStrategy):
    __rx_dict = {    
        # Subject: HY37 5y SWAPTION UPDATE - Ref 108 (320.43)
        'subject'     : re.compile(r'^\Subject: .*?\w+ (?P<ref_px>[\d]+.[\d]+)'),
        # Expiry 15Dec21 (107.78 323.85)
        'table_expiry': re.compile(r'^Expiry (?P<expiration_date>\d{2}\w{3}\d{2})'),    
        # Stk   Sprd  |     Pay      Delta       Rec      Vol   Vol Chg  Vol Bpd  Tail  |
        'table_header': re.compile(r'^(?P<strike_px>\w+)\ +(?P<strike_spd>\w+)\ +\|\ +(?P<p_price>\w+)\ +(?P<delta>\w+)\ +(?P<c_price>\w+)\ +(?P<iv_spd>\w+) +(?P<vol_chg>\w+\ \w+)\ +(?P<iv_bps>\w+\ \w+)\ +(?P<tail>\w+)'),
        # 110.5 266.8 | 2.650/2.800  -99.9      --/--     30.2    1.6      6.2    99.9  |
        'table_row'   : re.compile(r'^\ *\d+.\d+')
    }

    def __init__(self):
        self.exp_date_format = '%d%b%y'
        self.translate_dict = str.maketrans({'|':'', '-':''})

    def on_table_row(self, line):
        row_pair = []        
        row_data = line.translate(self.translate_dict).split()

        self.pq.strike_px   = row_data[self.col_idx['strike_px']]
        self.pq.strike_spd  = row_data[self.col_idx['strike_spd']]            
        self.pq.delta       = row_data[self.col_idx['delta']]
        self.pq.iv_spd      = row_data[self.col_idx['iv_spd']]
        self.pq.iv_bps      = row_data[self.col_idx['iv_bps']]
        self.pq.iv_px       = ''

        self.pq.option_type = 'P'
        self.pq.bid_price, self.pq.ask_price = row_data[self.col_idx['p_price']].split('/')
        row_pair.append(self.pq.get_values())                       
        
        self.pq.option_type = 'C'
        self.pq.bid_price, self.pq.ask_price = row_data[self.col_idx['c_price']].split('/')
        row_pair.append(self.pq.get_values())

        self.pq.reset_table_row_values()

        return row_pair

    def get_rx_dict(self):
        return self.__rx_dict    

    def on_header(self, match):
        super().on_header(match)        
        
    def on_subject(self, match):
        super().on_subject(match)

    def on_expiry(self, match):
        super().on_expiry(match);
        
    def on_table_header(self, match):
        super().on_table_header(match)        

class ParserForYYY(ParsingStrategy):
    __rx_dict = {    
        # Subject: $$ CDX OPTIONS: HY37 5Y UPDATE - REF 108.125
        'subject'     : re.compile(r'^\Subject: .*?\w+ (?P<ref_px>[\d]+.[\d]+)'),    
        # EXPIRY: 15-DEC-2021 Fwd 107.89 / 320.8 Dv01 4.67
        'table_expiry': re.compile(r'^EXPIRY\: (?P<expiration_date>\d{2}\-\w{3}\-\d{4})'),    
        # K [~Sprd]  |DEC21>PAY   Dlt |DEC21>RCV   Dlt |MidVol [SprdVol] Chg    b/e
        'table_header': re.compile(r'^\ *(?P<strike_px>\w) +\[.(?P<strike_spd>\w+)\]\ +\|(?P<p_bid_price>\w+)\>(?P<p_ask_price>\w+)\ +(?P<p_delta>\w+) +\|(?P<c_bid_price>\w+)\>(?P<c_ask_price>\w+)\ +(?P<c_delta>\w+) +\|(?P<mid_vol>\w+)\ +\[(?P<iv_spd>\w+)\]\ +(?P<chg>\w+)\ +(?P<iv_bps>.+)'),
        # 109.5 [287] |155.5 170.5 95% |  0.0 9.6    5% |  4.5% [ 32%]    -1.3%  6.05
        'table_row'   : re.compile(r'^\ *\d+.\d+')
    }

    def __init__(self):
        self.exp_date_format = '%d-%b-%Y'
        self.translate_dict = str.maketrans({'|':'', '[':'', ']':'', '%':''})

    def on_table_row(self, line):
        row_pair = []        
        row_data = line.translate(self.translate_dict).split()

        self.pq.strike_px   = row_data[self.col_idx['strike_px']]
        self.pq.strike_spd  = row_data[self.col_idx['strike_spd']]            
        self.pq.delta       = ''
        self.pq.iv_spd      = row_data[self.col_idx['iv_spd']]
        self.pq.iv_bps      = row_data[self.col_idx['iv_bps']]
        self.pq.iv_px       = ''

        self.pq.option_type = 'P'
        self.pq.bid_price = row_data[self.col_idx['p_bid_price']]
        self.pq.ask_price = row_data[self.col_idx['p_ask_price']]
        self.pq.delta     = row_data[self.col_idx['p_delta']]
        row_pair.append(self.pq.get_values())

        self.pq.option_type = 'C'
        self.pq.bid_price = row_data[self.col_idx['c_bid_price']]
        self.pq.ask_price = row_data[self.col_idx['c_ask_price']]
        self.pq.delta     = row_data[self.col_idx['c_delta']]
        row_pair.append(self.pq.get_values())

        self.pq.reset_table_row_values()

        return row_pair

    def get_rx_dict(self):
        return self.__rx_dict

    def on_header(self, match):
        super().on_header(match)
        
    def on_subject(self, match):
        super().on_subject(match)

    def on_expiry(self, match):
        super().on_expiry(match);
        
    def on_table_header(self, match):
        super().on_table_header(match)

class ParserForZZZ(ParsingStrategy):
    __rx_dict = {    
        # No Subject
        # Exp: 15-Dec-21 Swaptions Ref: 108.1    CDX HY37
        'table_expiry': re.compile(r'^Exp\: (?P<expiration_date>\d{2}\-\w{3}\-\d{2}) .*?\w+ \w+\: (?P<ref_px>[\d]+.[\d]+)'),    
        #    K    |     Puts    Del |    Calls    Del |   Vol    Chg |  Prc Vol
        'table_header': re.compile(r'^\s+(?P<strike_px>\w+)\s+\|\s+(?P<p_price>\w+)\s+(?P<p_delta>\w+)\s+\|\s+(?P<c_price>\w+)\s+(?P<c_delta>\w+)\s+\|\s+(?P<iv_spd>\w+)\s+(?P<chg>\w+)\s+\|\s+(?P<iv_px>\w+\s\w+)'),
        #     108 |   52 /  70   55 |   40 /  58  -45 |  41.7   +1.5 |    6.1 
        'table_row'   : re.compile(r'^\s*\d+[.]?\d*')
    }

    def __init__(self):
        self.exp_date_format = '%d-%b-%y'
        self.translate_dict = str.maketrans({'|':'', '[':'', ']':'', '\xa0':' '})

    def on_table_row(self, line):
        row_pair = []        
        row_data = line.translate(self.translate_dict).replace('  ',' ').replace(' / ','/').strip().split()

        self.pq.strike_px   = row_data[self.col_idx['strike_px']]
        self.pq.strike_spd  = ''
        self.pq.delta       = ''
        self.pq.iv_spd      = row_data[self.col_idx['iv_spd']]
        self.pq.iv_bps      = ''
        self.pq.iv_px       = row_data[self.col_idx['iv_px']]

        self.pq.option_type = 'P'
        self.pq.delta       = row_data[self.col_idx['p_delta']]
        self.pq.bid_price, self.pq.ask_price = row_data[self.col_idx['p_price']].split('/')                
        row_pair.append(self.pq.get_values())

        self.pq.option_type = 'C'                
        self.pq.delta       = row_data[self.col_idx['c_delta']]
        self.pq.bid_price, self.pq.ask_price = row_data[self.col_idx['c_price']].split('/')        
        row_pair.append(self.pq.get_values())

        self.pq.reset_table_row_values()

        return row_pair

    def get_rx_dict(self):
        return self.__rx_dict

    def on_header(self, match):
        super().on_header(match)
        
    def on_subject(self, match):
        super().on_subject(match)        

    def on_expiry(self, match):
        super().on_expiry(match);
        self.pq.ref_px = match.group('ref_px')
        
    def on_table_header(self, match):
        super().on_table_header(match)

class ParserForWWW(ParsingStrategy):
    __rx_dict = {    
        # Subject: CDX Options: CDX.HY S37/36 5Y Dec-Jun [ref 108.1] - Update
        'subject'     : re.compile(r'^\Subject: .*?\w+ (?P<ref_px>[\d]+.[\d]+)'),    
        # CDX Options: HY (S37V1) 15-Dec-21 ** Fwd @107.881, Delta @108.1
        'table_expiry': re.compile(r'^CDX \w+\: \w+ \(\w+\) (?P<expiration_date>\d{2}\-\w{3}\-\d{2})'),    
        #   K  |    Rec    Delta Vol  Chg B/E|   K  |     Pay     Delta Vol  Chg  B/E
        'table_header': re.compile(r'^\ +(?P<c_strike_px>\w+)\ +\|\ +(?P<c_price>\w+)\ +(?P<c_delta>\w+)\ +(?P<c_iv_spd>\w+)\ +(?P<c_chg>\w+)\ +(?P<c_iv_bps>\w+\/\w+)\|\ +(?P<p_strike_px>\w+)\ +\|\ +(?P<p_price>\w+)\ +(?P<p_delta>\w+)\ +(?P<p_iv_spd>\w+)\ +(?P<p_chg>\w+)\ +(?P<p_iv_bps>\w+\/\w+)'),
        #  111 |  0.0/10.0   0%   32  0.4 5.3|  109 | 114.1/130.1  86%   37 -0.1  6.8
        'table_row'   : re.compile(r'^\ *\d+[.]?\d*|^\ +\-\ +')
    }

    def __init__(self):
        self.exp_date_format = '%d-%b-%y'
        self.translate_dict = str.maketrans({'|':'', '%':''})

    def on_table_row(self, line):
        row_pair = []        
        row_data = line.translate(self.translate_dict).strip().split()

        self.pq.strike_spd  = ''
        self.pq.iv_px       = ''        

        self.pq.option_type = 'P'
        self.pq.bid_price, self.pq.ask_price = row_data[self.col_idx['p_price']].split('/')                
        self.pq.strike_px   = row_data[self.col_idx['p_strike_px']]        
        self.pq.delta       = row_data[self.col_idx['p_delta']]        
        self.pq.iv_spd      = row_data[self.col_idx['p_iv_spd']]
        self.pq.iv_bps      = row_data[self.col_idx['p_iv_bps']]        
        row_pair.append(self.pq.get_values())

        self.pq.option_type = 'C'        
        if (row_data[self.col_idx['c_strike_px']] != '-'):
            self.pq.bid_price, self.pq.ask_price = row_data[self.col_idx['c_price']].split('/')
            self.pq.strike_px   = row_data[self.col_idx['c_strike_px']]
            self.pq.delta       = row_data[self.col_idx['c_delta']]            
            self.pq.iv_spd      = row_data[self.col_idx['c_iv_spd']]
            self.pq.iv_bps      = row_data[self.col_idx['c_iv_bps']]        
            row_pair.append(self.pq.get_values())        
        else:    
            self.pq.bid_price, self.pq.ask_price = ['','']
            self.pq.strike_px   = ''
            self.pq.delta       = ''
            self.pq.iv_spd      = ''
            self.pq.iv_bps      = ''
            row_pair.append(self.pq.get_values())        

        self.pq.reset_table_row_values()

        return row_pair

    def get_rx_dict(self):
        return self.__rx_dict

    def on_header(self, match):
        super().on_header(match)
        
    def on_subject(self, match):
        super().on_subject(match)        

    def on_expiry(self, match):
        super().on_expiry(match);        
        
    def on_table_header(self, match):
        super().on_table_header(match)

## Context - This is the object whose behavior will change
class QuoteParser:    
    strategy: ParsingStrategy

    def set_strategy(self, strategy: ParsingStrategy):        
        self.strategy = strategy

    def get_header_rx_dict(self):
        return ParsingStrategy.get_header_rx_dict()

    def get_rx_dict(self):
        return self.strategy.get_rx_dict()    

    def on_header(self, match):
        self.strategy.on_header(match)

    def on_subject(self, match):
        self.strategy.on_subject(match)
    
    def on_expiry(self, match):    
        self.strategy.on_expiry(match)

    def on_table_header(self, match):    
        self.strategy.on_table_header(match)

    def on_table_row(self, line):    
        return self.strategy.on_table_row(line)        

## Do a regex search against all defined regex's and return the key and match result of the first match
def parse_line(regex_dict, line):
    for key, rx in regex_dict.items():
        match = rx.search(line)
        if match:
            return key, match    
    return None, None

## factory method
def get_parser(firm_sender):
    if firm_sender == 'XXX':
        return ParserForXXX()    
    elif firm_sender == 'YYY':
        return ParserForYYY()
    elif firm_sender == 'ZZZ':
        return ParserForZZZ()        
    elif firm_sender == 'WWW':
        return ParserForWWW()                
    return None

## Heart of the parser - parse each line with regex
def parse_file(file):
    file_rows = []
    matched_table_header = False

    parser = QuoteParser()
    curr_regex_dict = parser.get_header_rx_dict()

    for line in file:
        key, match = parse_line(curr_regex_dict, line)        
        if key == None:
            continue
        elif key == 'header':    
            parser_strategy = get_parser(match.group('firm_sender'))        
            if (parser_strategy == None):
                break            
            parser.set_strategy(parser_strategy)
            parser.on_header(match)            
            curr_regex_dict = parser.get_rx_dict()
        elif key == 'subject':
            parser.on_subject(match)
        elif key == 'table_expiry':
            parser.on_expiry(match)            
        elif key == 'table_header' and matched_table_header == False:
            parser.on_table_header(match)
            matched_table_header = True
        elif key == 'table_row':  
            row_pair = parser.on_table_row(line)
            file_rows.extend(row_pair)

    return file_rows

## read all files based on path and file pattern
def read_feed_files(path, file_pattern):
    file_location = os.path.join(path, file_pattern)
    filenames = glob.glob(file_location)

    all_data = []

    for filename in filenames:
        file = open(filename, encoding='utf-8')        
        file_rows = parse_file(file)        
        all_data.extend(file_rows)
        file.close()              

    print(f'Total records added : {len(all_data)}')

    return pd.DataFrame(all_data, columns=['Date', 'Time', 'Firm', 'Expiration', 'Option Type', 'Strike Px',
                                           'Strike Spd', 'Bid Price', 'Ask Price', 'Delta', 'Implied Vol Spd', 
                                           'Implied Vol bps', 'Implied Vol px', 'Ref Px'])

## convert numeric column to numbers and normalize prices
def transform_data(df):
    df['Strike Px']  = pd.to_numeric(df['Strike Px'], errors='coerce')
    df['Strike Spd'] = pd.to_numeric(df['Strike Spd'], errors='coerce')
    df['Bid Price']  = pd.to_numeric(df['Bid Price'], errors='coerce')
    df['Ask Price']  = pd.to_numeric(df['Ask Price'], errors='coerce')
    df['Delta']      = pd.to_numeric(df['Delta'], errors='coerce')
    df['Ref Px']     = pd.to_numeric(df['Ref Px'], errors='coerce')
    df['Implied Vol Spd'] = pd.to_numeric(df['Implied Vol Spd'], errors='coerce')
    df['Implied Vol bps'] = pd.to_numeric(df['Implied Vol bps'], errors='coerce')
    df['Implied Vol px']  = pd.to_numeric(df['Implied Vol px'], errors='coerce')    
    
    # Normalize all prices in cents    
    mask = df['Firm'] != 'XXX' 
    df.loc[mask, 'Bid Price'] = df[mask].apply(lambda x: x['Bid Price'] / 100, axis=1)        
    df.loc[mask, 'Ask Price'] = df[mask].apply(lambda x: x['Ask Price'] / 100, axis=1)        

## write output to excel
def write_data_to_excel(df, output_file):
    with pd.ExcelWriter(output_file) as writer:
        df.to_excel(writer, sheet_name='out', index=False)

def main():
    df = read_feed_files('', 'hycdx_option_quotes_*.txt')
    transform_data(df)
    write_data_to_excel(df, 'output_smo.xlsx')    

main()