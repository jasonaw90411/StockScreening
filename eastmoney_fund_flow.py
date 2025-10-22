import os
import json
import subprocess
import logging
from datetime import datetime, timedelta
import requests
import re
from bs4 import BeautifulSoup
import time
import random
import pandas as pd
from io import StringIO
from urllib.parse import quote
from typing import Dict, List, Optional, Tuple

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('eastmoney_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# API配置常量
API_CONFIG = {
    'api_url': "http://push2.eastmoney.com/api/qt/clist/get",
    'html_url': "https://data.eastmoney.com/bkzj/hy.html",
    'headers': {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Referer': 'https://data.eastmoney.com/bkzj/hy.html',
    },
    'params': {
        'pn': 1,  # 页码
        'pz': 100,  # 每页数量
        'po': 1,  # 排序方式
        'np': 1,
        'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
        'fltt': 2,  # 过滤条件
        'invt': 2,
        'fid': 'f62',  # 按主力净流入排序
        'fs': 'm:90 t:2',  # 板块类型：行业板块
        'fields': 'f1,f14,f62,f66,f69,f72,f75,f184,f128,f136',  # 所需字段
    }
}

# 股票数据API配置
STOCK_API_CONFIG = {
    'base_url': 'https://push2.eastmoney.com/api/qt/clist/get',
    'kline_url': 'http://push2his.eastmoney.com/api/qt/stock/kline/get',
    'headers': {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://data.eastmoney.com/',
    },
    'stock_fields': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f23,f24,f25,f26,f62,f66,f69,f72,f75,f128',
    'kline_fields': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
    'default_params': {
        'pn': 1,
        'pz': 30,
        'po': 1,
        'np': 1,
        'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
        'fltt': 2,
        'invt': 2,
        'fid': 'f62',
    },
    'kline_params': {
        'fields1': 'f1,f2,f3,f4,f5,f6',
        'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
        'klt': '101',  # 日K线
        'fqt': '1',    # 前复权
    }
}

# 数据提取配置
EXTRACTION_CONFIG = {
    'min_data_rows': 5,  # 最小数据行数
    'max_retries': 3,    # 最大重试次数
    'table_min_rows': 10,  # 表格最小行数
    'table_min_cols': 8,   # 表格最小列数
    'pandas_table_min_rows': 10,  # pandas表格最小行数
    'pandas_table_min_cols': 8,   # pandas表格最小列数
    'delay_range': (0.5, 2.0),    # 延迟范围
    'retry_delay_range': (3.0, 5.0),  # 重试延迟范围
    'timeout': 15,  # 请求超时时间
    'debug_print_limit': 5,  # 调试打印限制
    'regex_match_limit': 20,   # 正则匹配限制
    'extraction_methods': ['api', 'html_table', 'page_text', 'pandas']  # 提取方法顺序
}

# 字段映射配置
FIELD_MAPPINGS = {
    'name': ['名称', '板块', '行业', '板块名称'],
    'change_rate': ['涨跌幅', '涨', '跌幅', '涨跌'],
    'super_large_inflow': ['超大单净流入', '超大单流入', '超大单'],
    'super_large_ratio': ['超大单净流入净占比', '超大单净占比', '超大单占比'],
    'large_inflow': ['大单净流入', '大单流入', '大单'],
    'large_ratio': ['大单净流入净占比', '大单净占比', '大单占比'],
    'max_stock': ['主力净流入最大股', '最大股', '主力股']
}

def retry_with_backoff(max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 10.0):
    """
    重试装饰器，支持指数退避
    
    Args:
        max_retries: 最大重试次数
        base_delay: 基础延迟时间（秒）
        max_delay: 最大延迟时间（秒）
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"函数 {func.__name__} 在 {max_retries} 次尝试后失败: {e}")
                        raise
                    
                    # 计算退避延迟
                    delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                    logger.warning(f"函数 {func.__name__} 第 {attempt + 1} 次尝试失败: {e}, 等待 {delay:.1f} 秒后重试")
                    time.sleep(delay)
            
            return None
        return wrapper
    return decorator

# 获取东方财富网板块资金流入数据
@retry_with_backoff(max_retries=3, base_delay=1.0, max_delay=10.0)
def crawl_eastmoney_fund_flow(max_retries: int = 3) -> Tuple[List[Dict], List[Dict]]:
    """
    从东方财富网爬取板块资金流向数据
    URL: https://data.eastmoney.com/bkzj/hy.html
    获取今日超大单和大单都是净流入的前五个板块
    
    Returns:
        Tuple[List[Dict], List[Dict]]: (top_sectors, all_sectors)
    """
    logger.info("开始爬取东方财富网板块资金流入数据")
    
    # 添加随机延迟避免被封
    time.sleep(random.uniform(0.5, 2.0))
    
    # 尝试API方式获取数据
    all_sectors = fetch_sectors_from_api()
    
    # 如果API获取数据不足，尝试HTML解析方式
    if len(all_sectors) < 5:
        logger.warning("API获取数据不足，尝试HTML解析方式...")
        all_sectors = fetch_sectors_from_html()
    
    # 如果还是数据不足，抛出异常触发重试
    if len(all_sectors) < 5:
        raise ValueError("无法获取足够数据，需要重试")
    
    # 处理获取到的数据
    return process_sectors_data(all_sectors)

def fetch_sectors_from_api() -> List[Dict]:
    """从API获取板块数据"""
    logger.info("尝试通过API获取板块数据...")
    
    try:
        # 构建API参数
        params = API_CONFIG['params'].copy()
        params['_'] = str(int(time.time() * 1000))  # 时间戳防止缓存
        
        response = requests.get(
            API_CONFIG['api_url'], 
            params=params, 
            headers=API_CONFIG['headers'], 
            timeout=15
        )
        response.raise_for_status()
        
        logger.info(f"API响应状态码: {response.status_code}")
        return parse_api_response(response.json())
        
    except requests.Timeout as e:
        logger.error(f"API请求超时: {e}")
        return []
    except requests.ConnectionError as e:
        logger.error(f"API连接错误: {e}")
        return []
    except requests.HTTPError as e:
        logger.error(f"API HTTP错误 (状态码: {response.status_code}): {e}")
        return []
    except requests.RequestException as e:
        logger.error(f"API请求失败: {e}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"API响应解析失败: {e}")
        return []
    except Exception as e:
        logger.error(f"API获取数据时发生未知错误: {e}")
        return []

def parse_api_response(data: Dict) -> List[Dict]:
    """解析API响应数据"""
    all_sectors = []
    
    if not data.get('data') or not data['data'].get('diff'):
        logger.warning("API响应数据格式不正确")
        return all_sectors
    
    diff_data = data['data']['diff']
    logger.info(f"API响应数据字段: {list(diff_data[0].keys()) if diff_data else '无数据'}")
    
    for item in diff_data:
        try:
            sector_data = {
                'name': item.get('f14', '未知'),  # 板块名称
                'change_rate': float(item.get('f3', 0)),  # 涨跌幅
                'super_large_inflow': float(item.get('f66', 0)) / 10000,  # 超大单净流入（转换为亿元）
                'super_large_ratio': float(item.get('f69', 0)),  # 超大单净占比
                'large_inflow': float(item.get('f72', 0)) / 10000,  # 大单净流入（转换为亿元）
                'large_ratio': float(item.get('f75', 0)),  # 大单净占比
                'max_stock': item.get('f128', '未知')  # 主力净流入最大股
            }
            
            all_sectors.append(sector_data)
            
            # 只打印前5个用于调试
            if len(all_sectors) <= 5:
                logger.debug(f"API提取板块数据: {sector_data['name']}, 超大单流入: {sector_data['super_large_inflow']}亿, 大单流入: {sector_data['large_inflow']}亿")
        
        except (ValueError, TypeError, KeyError) as e:
            logger.warning(f"解析API数据项失败: {e}, 数据项: {item}")
            continue
    
    logger.info(f"API成功获取到{len(all_sectors)}个板块数据")
    return all_sectors

def fetch_sectors_from_html() -> List[Dict]:
    """从HTML页面获取板块数据"""
    logger.info("尝试通过HTML解析获取板块数据...")
    
    try:
        response = requests.get(
            API_CONFIG['html_url'], 
            headers=API_CONFIG['headers'], 
            timeout=15
        )
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 打印页面信息用于调试
        logger.debug(f"页面中div数量: {len(soup.find_all('div'))}")
        logger.debug(f"页面中table数量: {len(soup.find_all('table'))}")
        
        # 尝试多种数据提取方法
        all_sectors = try_multiple_extraction_methods(soup, response.text)
        
        logger.info(f"HTML解析成功获取到{len(all_sectors)}个板块数据")
        return all_sectors
        
    except requests.RequestException as e:
        logger.error(f"HTML页面请求失败: {e}")
        return []

def try_multiple_extraction_methods(soup: BeautifulSoup, page_text: str) -> List[Dict]:
    """尝试多种数据提取方法"""
    all_sectors = []
    
    # 按照配置的提取方法顺序尝试
    for method in EXTRACTION_CONFIG['extraction_methods']:
        if method == 'html_table':
            all_sectors = extract_data_from_tables(soup)
        elif method == 'page_text':
            all_sectors = extract_data_from_page_text(page_text)
        elif method == 'pandas':
            all_sectors = extract_with_pandas(page_text)
        
        # 如果获取到足够数据，停止尝试其他方法
        if len(all_sectors) >= EXTRACTION_CONFIG['min_data_rows']:
            logger.info(f"使用 {method} 方法成功获取到 {len(all_sectors)} 个板块数据")
            break
        else:
            logger.info(f"{method} 方法获取数据不足 ({len(all_sectors)} < {EXTRACTION_CONFIG['min_data_rows']})，尝试下一种方法")
    
    return all_sectors

def extract_with_pandas(page_text: str) -> List[Dict]:
    """使用pandas提取表格数据"""
    try:
        tables = pd.read_html(StringIO(page_text))
        for i, table in enumerate(tables):
            logger.debug(f"Pandas找到表格{i+1}，形状: {table.shape}")
            # 尝试处理表格数据
            if table.shape[0] > 10 and table.shape[1] > 8:  # 合理大小的表格
                sectors_from_pandas = process_pandas_table(table)
                if sectors_from_pandas:
                    return sectors_from_pandas
    except Exception as e:
        logger.error(f"Pandas读取表格失败: {e}")
    
    return []

def process_sectors_data(all_sectors: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """处理板块数据，排序并提取前五个"""
    logger.info(f"总共提取到{len(all_sectors)}个板块数据")
    
    # 按主力净流入（超大单+大单）排序
    all_sectors.sort(key=lambda x: (x['super_large_inflow'] + x['large_inflow']), reverse=True)
    
    # 获取前五个板块
    top_sectors = all_sectors[:5]
    
    logger.info(f"按主力净流入排序，前五个板块: {[s['name'] for s in top_sectors]}")
    
    # 保存爬取的数据到JSON文件
    save_crawl_data(all_sectors, top_sectors)
    
    return top_sectors, all_sectors

# 从表格中提取数据
def extract_data_from_tables(soup):
    """从页面的表格中提取板块资金流向数据"""
    all_sectors = []
    
    # 尝试查找特定的表格容器
    table_container = soup.find('div', {'class': 'data-list'}) or soup.find('div', {'id': 'dt_1'})
    if table_container:
        logger.debug("找到表格容器")
        tables = table_container.find_all('table')
    else:
        tables = soup.find_all('table')
    
    for table in tables:
        # 查找表格的行
        rows = table.find_all('tr')
        
        # 跳过空表或只有表头的表
        if len(rows) <= 1:
            continue
        
        # 尝试识别表头，确定列的位置
        header_row = rows[0]
        header_cells = header_row.find_all(['th', 'td'])
        header_texts = [cell.get_text(strip=True) for cell in header_cells]
        
        logger.debug(f"表头文本: {header_texts[:5]}...")
        
        # 根据表头内容确定数据列的位置
        col_map = build_column_mapping(header_texts)
        logger.debug(f"列映射: {col_map}")
        
        # 处理数据行
        data_rows = rows[1:]
        for row in data_rows:
            cells = row.find_all(['td', 'th'])
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            
            # 跳过空行或不符合条件的行
            if len(cell_texts) < EXTRACTION_CONFIG['table_min_cols']:
                continue
            
            try:
                # 提取数据
                sector_data = extract_sector_data_from_row(cell_texts, col_map)
                
                # 验证数据有效性
                if is_valid_sector_data(sector_data):
                    all_sectors.append(sector_data)
                    if len(all_sectors) <= EXTRACTION_CONFIG['debug_print_limit']:
                        logger.debug(f"提取板块数据: {sector_data['name']}, 超大单流入: {sector_data['super_large_inflow']}亿, 大单流入: {sector_data['large_inflow']}亿")
            
            except Exception as e:
                logger.warning(f"解析行数据失败: {e}, 行内容: {cell_texts[:5]}")
                continue
    
    return all_sectors

def build_column_mapping(header_texts: List[str]) -> Dict[str, int]:
    """根据表头文本构建列映射"""
    col_map = {}
    
    for field, keywords in FIELD_MAPPINGS.items():
        for i, text in enumerate(header_texts):
            if any(keyword in text for keyword in keywords):
                col_map[field] = i
                break
    
    # 为缺失的字段设置默认值
    default_positions = {
        'name': 1,
        'change_rate': 2,
        'super_large_inflow': 4,
        'super_large_ratio': 5,
        'large_inflow': 6,
        'large_ratio': 7,
        'max_stock': 9
    }
    
    for field, default_pos in default_positions.items():
        if field not in col_map:
            col_map[field] = default_pos
    
    return col_map

def extract_sector_data_from_row(cell_texts: List[str], col_map: Dict[str, int]) -> Dict[str, any]:
    """从行数据中提取板块信息"""
    return {
        'name': safe_get_cell_text(cell_texts, col_map['name'], '未知'),
        'change_rate': extract_float_value(safe_get_cell_text(cell_texts, col_map['change_rate'], '0%')),
        'super_large_inflow': extract_float_value(safe_get_cell_text(cell_texts, col_map['super_large_inflow'], '0亿')),
        'super_large_ratio': extract_float_value(safe_get_cell_text(cell_texts, col_map['super_large_ratio'], '0%')),
        'large_inflow': extract_float_value(safe_get_cell_text(cell_texts, col_map['large_inflow'], '0亿')),
        'large_ratio': extract_float_value(safe_get_cell_text(cell_texts, col_map['large_ratio'], '0%')),
        'max_stock': safe_get_cell_text(cell_texts, col_map['max_stock'], '未知')
    }

def safe_get_cell_text(cell_texts: List[str], index: int, default: str = '') -> str:
    """安全获取单元格文本"""
    return cell_texts[index] if index < len(cell_texts) else default

def is_valid_sector_data(sector_data: Dict[str, any]) -> bool:
    """验证板块数据是否有效"""
    if not sector_data.get('name') or sector_data['name'] in ['净占比', '名称', '板块', '行业']:
        return False
    
    # 检查关键字段是否有有效值
    required_fields = ['super_large_inflow', 'large_inflow']
    for field in required_fields:
        if not isinstance(sector_data.get(field), (int, float)):
            return False
    
    return True

# 从页面文本中提取数据
def extract_data_from_page_text(page_text):
    """使用正则表达式从页面文本中提取数据"""
    all_sectors = []
    
    # 尝试匹配板块数据的模式
    # 模式示例: 序号 名称 涨跌幅 主力净流入 超大单净流入 超大单净占比 大单净流入 大单净占比 中单净流入 中单净占比 小单净流入 小单净占比 主力净流入最大股
    pattern = r'(\d+)\s+([^\s]+)\s+([-+]?\d+\.\d+)%\s+([-+]?\d+\.\d+)亿[^\s]*\s+([-+]?\d+\.\d+)亿\s+([-+]?\d+\.\d+)%\s+([-+]?\d+\.\d+)亿\s+([-+]?\d+\.\d+)%'
    
    matches = re.findall(pattern, page_text)
    print(f"正则表达式找到{len(matches)}个匹配")
    
    for match in matches[:20]:  # 限制处理数量
        try:
            sector_data = {
                'name': match[1],
                'change_rate': float(match[2]),
                'super_large_inflow': float(match[4]),
                'super_large_ratio': float(match[5]),
                'large_inflow': float(match[6]),
                'large_ratio': float(match[7]),
                'max_stock': '未知'  # 正则表达式中没有捕获这个字段
            }
            
            all_sectors.append(sector_data)
            if len(all_sectors) <= 3:
                print(f"文本提取数据: {sector_data['name']}, 超大单流入: {sector_data['super_large_inflow']}亿")
        except Exception as e:
            print(f"解析匹配数据失败: {e}")
            continue
    
    return all_sectors

# 使用pandas处理表格数据
def process_pandas_table(table):
    """处理pandas读取的表格数据"""
    all_sectors = []
    
    try:
        # 遍历表格行
        for _, row in table.iterrows():
            # 转换为列表并处理
            row_values = row.tolist()
            
            # 跳过空行或不相关行
            if len(row_values) < 8 or pd.isna(row_values[0]):
                continue
            
            # 尝试提取数据
            try:
                # 根据常见的表格结构推断列的位置
                sector_data = {
                    'name': str(row_values[1]) if len(row_values) > 1 else '未知',
                    'change_rate': extract_float_value(str(row_values[2])) if len(row_values) > 2 else 0.0,
                    'super_large_inflow': extract_float_value(str(row_values[4])) if len(row_values) > 4 else 0.0,
                    'super_large_ratio': extract_float_value(str(row_values[5])) if len(row_values) > 5 else 0.0,
                    'large_inflow': extract_float_value(str(row_values[6])) if len(row_values) > 6 else 0.0,
                    'large_ratio': extract_float_value(str(row_values[7])) if len(row_values) > 7 else 0.0,
                    'max_stock': str(row_values[9]) if len(row_values) > 9 else '未知'
                }
                
                # 过滤掉无效数据
                if sector_data['name'] and not any(keyword in sector_data['name'] for keyword in ['名称', '板块', '行业', 'nan']):
                    all_sectors.append(sector_data)
            except Exception as e:
                print(f"处理pandas行数据失败: {e}")
                continue
    
    except Exception as e:
        print(f"处理pandas表格失败: {e}")
    
    return all_sectors

# 从字符串中提取浮点数
def extract_float_value(text):
    """从包含数字的文本中提取浮点数"""
    # 尝试匹配数字，包括正负号
    match = re.search(r'([-+]?\d+(?:\.\d+)?)', text)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return 0.0
    return 0.0

# 数据清洗函数
def clean_stock_data(stock_info):
    """
    清洗和转换股票数据，确保数据格式正确
    """
    # 定义需要转换为浮点数的字段
    float_fields = [
        'price', 'change_rate', 'change_amount', 'volume', 'volume_amount',
        'turnover_rate', 'volume_ratio', 'pe_ratio', 'pb_ratio', 
        'market_cap', 'circulation_cap', 'high_price', 'low_price',
        'open_price', 'pre_close_price', 'amplitude', 'main_inflow',
        'main_ratio', 'super_large_inflow', 'super_large_ratio',
        'large_inflow', 'large_ratio', 'rsi', 'ma5', 'ma10', 'ma20',
        'ma30', 'ma60'
    ]
    
    # 转换字段为浮点数，处理无效值
    for field in float_fields:
        if field in stock_info and stock_info[field] != '':
            try:
                # 处理特殊值（如-表示无数据）
                if stock_info[field] in ['-', '--', 'None']:
                    stock_info[field] = 0.0
                else:
                    value = float(stock_info[field])
                    # 处理异常大的值（可能是时间戳）
                    if value > 999999:  # 假设价格不会超过100万
                        stock_info[field] = 0.0
                    else:
                        stock_info[field] = value
            except (ValueError, TypeError):
                stock_info[field] = 0.0
        else:
            stock_info[field] = 0.0
    
    # 计算一些衍生指标
    try:
        # 计算价格相对位置（当前价相对于高低价的位置）
        if stock_info['high_price'] > stock_info['low_price']:
            price_position = (stock_info['price'] - stock_info['low_price']) / (stock_info['high_price'] - stock_info['low_price'])
            stock_info['price_position'] = round(price_position * 100, 2)
        else:
            stock_info['price_position'] = 50.0
        
        # 计算市值单位转换（万元转亿元）
        stock_info['market_cap_billion'] = round(stock_info['market_cap'] / 10000, 2)
        stock_info['circulation_cap_billion'] = round(stock_info['circulation_cap'] / 10000, 2)
        
        # 计算资金流向强度（主力净流入相对于流通市值的比例）
        if stock_info['circulation_cap'] > 0:
            fund_intensity = (stock_info['main_inflow'] / stock_info['circulation_cap']) * 100
            stock_info['fund_intensity'] = round(fund_intensity, 4)
        else:
            stock_info['fund_intensity'] = 0.0
        
        # 计算量比状态（大于1为放量）
        stock_info['volume_status'] = '放量' if stock_info['volume_ratio'] > 1 else '缩量'
        
        # 计算换手率状态
        if stock_info['turnover_rate'] > 10:
            stock_info['turnover_status'] = '高换手'
        elif stock_info['turnover_rate'] > 5:
            stock_info['turnover_status'] = '中换手'
        else:
            stock_info['turnover_status'] = '低换手'
            
    except Exception as e:
        print(f"计算衍生指标时出错: {e}")
        # 设置默认值
        stock_info['price_position'] = 50.0
        stock_info['market_cap_billion'] = 0.0
        stock_info['circulation_cap_billion'] = 0.0
        stock_info['fund_intensity'] = 0.0
        stock_info['volume_status'] = '未知'
        stock_info['turnover_status'] = '未知'
    
    return stock_info

# 获取板块跳转URL
def get_sector_stocks(sector_code, sector_name, limit=30):
    """
    获取板块中的个股数据，按资金流入排序
    包含丰富的因子计算数据：成交量、量比、换手率、市盈率、市值等
    """
    logger.info(f"[函数调用] get_sector_stocks(sector_code={sector_code}, sector_name={sector_name}, limit={limit})")
    
    # 使用配置中的API端点
    api_endpoints = [STOCK_API_CONFIG['base_url']] * 5
    
    # 尝试不同的过滤条件格式
    fs_param_variations = [
        f'b:{sector_code}',
        f'b:BK{sector_code}',
        f'm:90 t:2 f:BK{sector_code}',
    ]
    
    # 使用配置中的请求头
    headers = STOCK_API_CONFIG['headers'].copy()
    headers['Referer'] = f'https://data.eastmoney.com/bkzj/{sector_code}.html'
    
    # 使用配置中的字段列表
    fields = STOCK_API_CONFIG['stock_fields']
    
    # 尝试所有API端点和过滤条件组合
    for api_url in api_endpoints:
        for fs_param in fs_param_variations:
            try:
                # 添加随机延迟避免请求过快
                time.sleep(random.uniform(0.5, 2.0))
                
                logger.info(f"尝试API: {api_url}, 过滤条件: {fs_param}")
                
                # 构建API参数
                params = STOCK_API_CONFIG['default_params'].copy()
                params.update({
                    'pn': 1,  # 页码
                    'pz': limit,  # 每页数量
                    'fs': fs_param,  # 板块过滤条件
                    'fields': fields,
                    '_': str(int(time.time() * 1000))  # 时间戳防止缓存
                })
                
                response = requests.get(api_url, params=params, headers=headers, timeout=15)
                response.raise_for_status()
                
                logger.info(f"API响应状态码: {response.status_code}")
                
                data = response.json()
                
                # 检查API响应结构
                if not data.get('data'):
                    logger.warning(f"API响应缺少data字段: {data.keys()}")
                    continue
                    
                if not data['data'].get('diff'):
                    logger.warning(f"API响应data中缺少diff字段: {data['data'].keys() if 'data' in data else '无'}")
                    continue
                
                # 解析股票数据
                stocks = []
                diff_data = data['data']['diff']
                logger.info(f"获取到{len(diff_data)}条股票数据")
                
                # 打印第一条数据的字段，用于调试（限制调试输出数量）
                if diff_data and len(diff_data) <= EXTRACTION_CONFIG['debug_print_limit']:
                    logger.debug(f"第一条数据的字段: {list(diff_data[0].keys())}")
                
                for stock in diff_data:
                    try:
                        # 提取股票数据
                        stock_info = {
                            # 基础信息
                            'code': stock.get('f12', ''),  # 股票代码
                            'name': stock.get('f14', ''),  # 股票名称
                            'price': stock.get('f2', 0),   # 最新价
                            'change_rate': stock.get('f3', 0),  # 涨跌幅(%)
                            'change_amount': stock.get('f4', 0),  # 涨跌额
                            
                            # 成交量相关
                            'volume': stock.get('f5', 0),  # 成交量(手)
                            'amount': stock.get('f6', 0),  # 成交额(元)
                            'turnover_rate': stock.get('f8', 0),  # 换手率(%)
                            'volume_ratio': stock.get('f10', 0),  # 量比
                            
                            # 估值指标
                            'pe_ratio': stock.get('f9', 0),  # 市盈率
                            'pb_ratio': stock.get('f11', 0),  # 市净率
                            'market_cap': stock.get('f20', 0),  # 总市值
                            'circulation_cap': stock.get('f21', 0),  # 流通市值
                            
                            # 资金流向数据
                            'main_inflow': float(stock.get('f62', 0)) / 10000,  # 主力净流入(亿元)
                            'main_ratio': float(stock.get('f128', 0)) if stock.get('f128') != '-' else 0,  # 主力净占比
                            'super_large_inflow': float(stock.get('f66', 0)) / 10000,  # 超大单净流入(亿元)
                            'super_large_ratio': float(stock.get('f69', 0)) if stock.get('f69') != '-' else 0,  # 超大单净占比
                            'large_inflow': float(stock.get('f72', 0)) / 10000,  # 大单净流入(亿元)
                            'large_ratio': float(stock.get('f75', 0)) if stock.get('f75') != '-' else 0,  # 大单净占比
                        }
                        
                        # 确保代码和名称不为空
                        if stock_info['code'] and stock_info['name']:
                            # 数据清洗和转换
                            stock_info = clean_stock_data(stock_info)
                            stocks.append(stock_info)
                            
                            # 打印前5只股票信息（限制调试输出数量）
                            if len(stocks) <= EXTRACTION_CONFIG['debug_print_limit']:
                                logger.info(f"股票数据: {stock_info['name']}({stock_info['code']}), 主力净流入: {stock_info['main_inflow']}亿")
                    
                    except (ValueError, TypeError, KeyError) as e:
                        logger.error(f"解析股票数据失败: {e}, 股票数据: {stock}")
                        continue
                
                # 如果获取到足够的数据，返回结果
                if stocks:
                    logger.info(f"成功获取板块 '{sector_name}' 的 {len(stocks)} 只个股数据")
                    return stocks
                
            except Exception as e:
                logger.error(f"使用API端点{api_url}和过滤条件{fs_param}获取数据失败: {e}")
                # 继续尝试下一个组合
                continue
    
    # 如果所有API调用都失败，返回空列表
    logger.error(f"所有API调用都失败，未能获取板块 '{sector_name}' 的个股数据")
    return []

def get_sector_urls(top_sectors):
    """
    获取板块在东方财富网上的跳转URL
    基于板块名称构建URL或从页面中提取
    """
    urls = {}
    
    try:
        # 使用配置中的请求头
        headers = API_CONFIG['headers'].copy()
        
        # 添加随机延迟避免请求过快
        time.sleep(random.uniform(1.0, 3.0))
        
        # 获取页面内容
        response = requests.get('https://data.eastmoney.com/bkzj/hy.html', headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 获取板块名称列表
        sector_names = [s['name'] for s in top_sectors]
        
        # 查找所有链接
        for link in soup.find_all('a'):
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # 查找匹配的板块名称
            if href.startswith('/bkzj/BK') and text in sector_names:
                # 构建完整的URL
                full_url = f"https://data.eastmoney.com{href}"
                urls[text] = full_url
                logger.info(f"找到板块 '{text}' 的URL: {full_url}")
        
    except Exception as e:
        logger.error(f"获取板块URL时出错: {e}")
    
    # 为每个板块添加URL信息
    sectors_with_urls = []
    base_url = "https://data.eastmoney.com/bkzj/hy.html"
    
    for sector in top_sectors:
        sector_with_url = sector.copy()
        sector_with_url['url'] = urls.get(sector['name'], base_url)
        sectors_with_urls.append(sector_with_url)
        # 移除重复的日志输出，已在上面记录过URL信息
    
    return sectors_with_urls



# 保存爬取的数据到JSON文件
def save_crawl_data(all_sectors, top_sectors, sector_stocks=None):
    """保存爬取的数据到JSON文件"""
    json_filename = 'eastmoney_crawl_data.json'
    crawl_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    data = {
        'crawl_time': crawl_time,
        'top_sectors': top_sectors
    }
    
    # 如果有个股数据，也保存到JSON中
    if sector_stocks:
        data['sector_stocks'] = sector_stocks
        total_stocks = sum(len(stocks) for stocks in sector_stocks.values())
        print(f"个股数据已添加到JSON文件，总共 {total_stocks} 只股票")
    
    try:
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"爬取数据已保存到: {json_filename}")
        
        # 触发选股策略脚本
        try:
            print("正在执行选股策略...")
            # 使用subprocess调用选股脚本
            result = subprocess.run(['python', 'stock_selection_strategy.py'], 
                                   capture_output=True, text=True)
            print("选股策略输出:")
            print(result.stdout)
            if result.stderr:
                print("选股策略错误:")
                print(result.stderr)
            print("选股策略执行完成")
            
            # 重新生成HTML报告以包含最新的选股结果
            print("正在更新HTML报告以包含选股结果...")
            # 重新加载top_sectors数据用于HTML生成
            if os.path.exists(json_filename):
                try:
                    with open(json_filename, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    # 重新生成HTML报告
                    generate_html_report(data.get('top_sectors', []), [])
                except Exception as e:
                    print(f"更新HTML报告失败: {e}")
                    
        except Exception as e:
            print(f"执行选股策略时出错: {e}")
            
    except Exception as e:
        print(f"保存数据失败: {e}")

# 生成HTML页面
def generate_selected_stocks_html(selected_stocks):
    """
    生成选股结果的HTML内容
    """
    if not selected_stocks:
        return "<p>暂无选股结果</p>"
    
    # 检查是否有15天动量得分字段，确定使用哪个标题和字段
    has_15day_score = any('15day_momentum_score' in stock for stock in selected_stocks)
    
    if has_15day_score:
        html = """
    <div class="selected-stocks">
        <h2>推荐前10个股</h2>
        <table class="stocks-table">
            <thead>
                <tr>
                    <th>排名</th>
                    <th>股票代码</th>
                    <th>股票名称</th>
                    <th>价格</th>
                    <th>涨跌幅(%)</th>
                    <th>主力净流入(亿)</th>
                    <th>15天动量得分</th>
                    <th>原动量得分</th>
                </tr>
            </thead>
            <tbody>
    """
    else:
        html = """
    <div class="selected-stocks">
        <h2>推荐前10个股</h2>
        <table class="stocks-table">
            <thead>
                <tr>
                    <th>排名</th>
                    <th>股票代码</th>
                    <th>股票名称</th>
                    <th>价格</th>
                    <th>涨跌幅(%)</th>
                    <th>主力净流入(亿)</th>
                    <th>动量得分</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for stock in selected_stocks:
        # 格式化数据
        main_inflow_yi = round(stock.get('main_inflow', 0) / 1e8, 2)
        
        # 判断涨跌幅的颜色类
        change_class = "positive" if stock.get('change_rate', 0) >= 0 else "negative"
        change_sign = "+" if stock.get('change_rate', 0) >= 0 else ""
        
        # 判断资金流入的颜色类
        inflow_class = "positive" if main_inflow_yi >= 0 else "negative"
        inflow_sign = "+" if main_inflow_yi >= 0 else ""
        
        ratio_class = "positive" if stock.get('main_ratio', 0) >= 0 else "negative"
        ratio_sign = "+" if stock.get('main_ratio', 0) >= 0 else ""
        
        if has_15day_score:
            # 使用新结构：包含15天动量得分和原动量得分
            html += f"""
                <tr>
                    <td>{stock.get('rank', '')}</td>
                    <td>{stock.get('code', '')}</td>
                    <td>{stock.get('name', '')}</td>
                    <td>{stock.get('sector', '')}</td>
                    <td>{stock.get('price', '')}</td>
                    <td class="{change_class}">{change_sign}{stock.get('change_rate', 0):.2f}</td>
                    <td class="{inflow_class}">{inflow_sign}{main_inflow_yi}</td>
                    <td class="positive">{stock.get('15day_momentum_score', 0):.2f}</td>
                    <td class="positive">{stock.get('old_momentum_score', 0):.2f}</td>
                </tr>
            """
        else:
            # 使用旧结构：只有动量得分
            html += f"""
                <tr>
                    <td>{stock.get('rank', '')}</td>
                    <td>{stock.get('code', '')}</td>
                    <td>{stock.get('name', '')}</td>
                    <td>{stock.get('sector', '')}</td>
                    <td>{stock.get('price', '')}</td>
                    <td class="{change_class}">{change_sign}{stock.get('change_rate', 0):.2f}</td>
                    <td class="{inflow_class}">{inflow_sign}{main_inflow_yi}</td>
                    <td class="positive">{stock.get('momentum_score', 0):.2f}</td>
                </tr>
            """
    
    html += """
            </tbody>
        </table>
    </div>
    """
    
    return html

def load_selected_stocks():
    """
    加载选股结果
    """
    selected_stocks_file = 'selected_stocks.json'
    if os.path.exists(selected_stocks_file):
        try:
            with open(selected_stocks_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            # 新的JSON结构包含两个因子类型的选股结果
            # 优先显示15天动量反转因子的选股结果
            if '15day_momentum_reversal_stocks' in data:
                return data.get('15day_momentum_reversal_stocks', [])
            # 如果新结构不存在，尝试旧结构
            elif 'selected_stocks' in data:
                return data.get('selected_stocks', [])
            else:
                return []
        except Exception as e:
            print(f"加载选股结果失败: {e}")
    return []

def generate_html_report(top_sectors, all_sectors):
    """
    生成HTML报告页面
    显示超大单和大单都是净流入的前三个板块以及选股结果
    """
    html_filename = 'eastmoney_fund_flow_report.html'
    crawl_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # 加载选股结果
    selected_stocks = load_selected_stocks()
    
    html_content = f"""
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>东方财富网板块资金流入报告</title>
        <style>
            body {{
                font-family: 'Microsoft YaHei', Arial, sans-serif;
                line-height: 1.4;
                color: #333;
                max-width: 1200px;
                margin: 0 auto;
                padding: 10px;
                background-color: #f5f5f5;
            }}
            h1 {{
                color: #1a1a1a;
                text-align: center;
                margin-bottom: 15px;
                padding-bottom: 10px;
                border-bottom: 1px solid #e0e0e0;
                font-size: 1.5em;
            }}
            h2 {{
                color: #2c3e50;
                margin-top: 20px;
                margin-bottom: 15px;
                font-size: 1.3em;
            }}
            .update-time {{
                text-align: center;
                color: #666;
                margin-bottom: 15px;
                font-style: italic;
                font-size: 0.9em;
            }}
            .top-sectors {{
                background-color: #fff;
                border-radius: 6px;
                padding: 15px;
                margin-bottom: 15px;
                box-shadow: 0 1px 5px rgba(0, 0, 0, 0.1);
            }}
            .selected-stocks {{
                background-color: #fff;
                border-radius: 6px;
                padding: 15px;
                margin-bottom: 15px;
                box-shadow: 0 1px 5px rgba(0, 0, 0, 0.1);
                border-left: 4px solid #007bff;
            }}
            .sector-card {{
                background-color: #f8f9fa;
                border-radius: 4px;
                padding: 12px;
                margin-bottom: 10px;
                border-left: 3px solid #28a745;
            }}
            .sector-card h2 {{
                margin-top: 0;
                margin-bottom: 8px;
                color: #2c3e50;
                display: flex;
                justify-content: space-between;
                align-items: center;
                font-size: 1.2em;
            }}
            .change-rate {{
                color: #dc3545;
                font-weight: bold;
                padding: 1px 6px;
                border-radius: 3px;
                background-color: #fee;
                font-size: 0.9em;
            }}
            .data-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                gap: 10px;
                margin-top: 8px;
            }}
            .data-item {{
                background-color: #fff;
                padding: 8px;
                border-radius: 3px;
                border: 1px solid #e9ecef;
            }}
            .data-item .label {{
                font-size: 0.8em;
                color: #666;
                margin-bottom: 3px;
            }}
            .data-item .value {{
                font-size: 1em;
                font-weight: bold;
                color: #28a745;
            }}
            .value.negative {{
                color: #dc3545;
            }}
            .all-sectors {{
                background-color: #fff;
                border-radius: 8px;
                padding: 25px;
                box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 20px;
            }}
            th, td {{
                padding: 12px;
                text-align: left;
                border-bottom: 1px solid #ddd;
            }}
            th {{
                background-color: #f8f9fa;
                font-weight: bold;
                color: #495057;
            }}
            tr:hover {{
                background-color: #f8f9fa;
            }}
            .positive {{
                color: #28a745;
            }}
            .negative {{
                color: #dc3545;
            }}
            /* 响应式表格 */
            @media (max-width: 768px) {{
                .stocks-table {{
                    display: block;
                    overflow-x: auto;
                    white-space: nowrap;
                }}
            }}
        </style>
    </head>
    <body>
        <h1>东方财富网板块资金流入报告</h1>
        <div class="update-time">更新时间: {crawl_time}</div>
        
        <div class="top-sectors">
            <h2>主力净流入前五个板块</h2>
            {generate_top_sectors_html(top_sectors)}
        </div>
        
        {generate_selected_stocks_html(selected_stocks)}
    </body>
    </html>
    """
    
    try:
        with open(html_filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"HTML报告已生成: {html_filename}")
        return html_filename
    except Exception as e:
        print(f"生成HTML报告失败: {e}")
        return None

# 生成前五个板块的HTML内容，包含URL链接
def generate_top_sectors_html(top_sectors):
    if not top_sectors:
        return "<p>暂无符合条件的板块数据</p>"
    
    html = ""
    for i, sector in enumerate(top_sectors, 1):
        # 检查是否有URL字段
        url = sector.get('url', '#')
        html += f"""
        <div class="sector-card">
            <h2>
                    <span>#{i} <a href="{url}" target="_blank" style="color: #2c3e50; text-decoration: none;">{sector['name']}</a></span>
                    <span class="change-rate {'' if sector['change_rate'] >= 0 else 'negative'}">{"+" if sector['change_rate'] >= 0 else ""}{sector['change_rate']}%</span>
                </h2>
            <div class="data-grid">
                <div class="data-item">
                    <div class="label">超大单净流入</div>
                    <div class="value {'' if sector['super_large_inflow'] >= 0 else 'negative'}">{"+" if sector['super_large_inflow'] >= 0 else ""}{sector['super_large_inflow']}亿</div>
                </div>
                <div class="data-item">
                    <div class="label">超大单净流入占比</div>
                    <div class="value {'' if sector['super_large_ratio'] >= 0 else 'negative'}">{"+" if sector['super_large_ratio'] >= 0 else ""}{sector['super_large_ratio']}%</div>
                </div>
                <div class="data-item">
                    <div class="label">大单净流入</div>
                    <div class="value {'' if sector['large_inflow'] >= 0 else 'negative'}">{"+" if sector['large_inflow'] >= 0 else ""}{sector['large_inflow']}亿</div>
                </div>
                <div class="data-item">
                    <div class="label">大单净流入占比</div>
                    <div class="value {'' if sector['large_ratio'] >= 0 else 'negative'}">{"+" if sector['large_ratio'] >= 0 else ""}{sector['large_ratio']}%</div>
                </div>
                <div class="data-item">
                    <div class="label">主力净流入最大股</div>
                    <div class="value">{sector['max_stock']}</div>
                </div>
                <div class="data-item">
                    <div class="label">板块链接</div>
                    <div class="value" style="word-break: break-all;"><a href="{url}" target="_blank" style="color: #0066cc;">查看详情</a></div>
                </div>
            </div>
        </div>
        """
    
    return html

# 生成所有板块的表格HTML内容
def generate_all_sectors_table(all_sectors):
    if not all_sectors:
        return "<p>暂无板块数据</p>"
    
    html = """
    <table>
        <thead>
            <tr>
                <th>板块名称</th>
                <th>涨跌幅</th>
                <th>超大单净流入(亿)</th>
                <th>超大单净流入占比</th>
                <th>大单净流入(亿)</th>
                <th>大单净流入占比</th>
                <th>主力净流入最大股</th>
            </tr>
        </thead>
        <tbody>
    """
    
    for sector in all_sectors:
        # 判断涨跌幅的颜色类
        change_class = "positive" if sector['change_rate'] >= 0 else "negative"
        
        # 判断资金流入的颜色类
        super_large_class = "positive" if sector['super_large_inflow'] >= 0 else "negative"
        large_class = "positive" if sector['large_inflow'] >= 0 else "negative"
        
        # 添加符号
        change_sign = "+" if sector['change_rate'] >= 0 else ""
        super_large_sign = "+" if sector['super_large_inflow'] >= 0 else ""
        super_large_ratio_sign = "+" if sector['super_large_ratio'] >= 0 else ""
        large_sign = "+" if sector['large_inflow'] >= 0 else ""
        large_ratio_sign = "+" if sector['large_ratio'] >= 0 else ""
        
        html += f"""
        <tr>
            <td>{sector['name']}</td>
            <td class="{change_class}">{change_sign}{sector['change_rate']}%</td>
            <td class="{super_large_class}">{super_large_sign}{sector['super_large_inflow']}</td>
            <td class="{super_large_class}">{super_large_ratio_sign}{sector['super_large_ratio']}%</td>
            <td class="{large_class}">{large_sign}{sector['large_inflow']}</td>
            <td class="{large_class}">{large_ratio_sign}{sector['large_ratio']}%</td>
            <td>{sector['max_stock']}</td>
        </tr>
        """
    
    html += """
        </tbody>
    </table>
    """
    
    return html

# 获取股票历史价格数据
def get_stock_history_prices(stock_code, days=30):
    """
    获取股票近N个交易日的收盘价数据
    
    Args:
        stock_code: 股票代码（如：000001）
        days: 获取的交易天数，默认30个交易日
    
    Returns:
        list: 包含日期和收盘价的字典列表
    """
    # 东方财富历史数据API
    # 对于A股，需要添加市场前缀：0-深市，1-沪市
    if stock_code.startswith('6'):
        market_code = '1'  # 沪市
    else:
        market_code = '0'  # 深市
    
    full_code = f"{market_code}.{stock_code}"
    
    # 计算开始日期（考虑非交易日，多取一些日期）
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days * 2)  # 考虑非交易日，多取一些日期
    
    # 使用配置中的历史K线数据API
    url = STOCK_API_CONFIG['kline_url']
    
    params = STOCK_API_CONFIG['kline_params'].copy()
    params.update({
        'secid': full_code,
        'beg': start_date.strftime('%Y%m%d'),
        'end': end_date.strftime('%Y%m%d'),
        'lmt': days + 20,  # 多取一些数据，确保有足够交易日
        '_': str(int(time.time() * 1000))
    })
    
    headers = STOCK_API_CONFIG['headers'].copy()
    
    try:
        logger.info(f"正在获取股票 {stock_code} 的 {days} 个交易日历史价格数据...")
        
        # 添加随机延迟避免请求过快
        time.sleep(random.uniform(0.5, 1.5))
        
        response = requests.get(url, params=params, headers=headers, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('data') and data['data'].get('klines'):
            history_prices = []
            klines = data['data']['klines']
            
            # 解析K线数据，按日期倒序排列（最新的在前）
            for kline in klines[:days]:  # 只取前N个交易日
                parts = kline.split(',')
                if len(parts) >= 6:  # 确保有足够的字段
                    date_str = parts[0]  # 日期
                    open_price = float(parts[1])  # 开盘价
                    close_price = float(parts[2])  # 收盘价
                    high_price = float(parts[3])  # 最高价
                    low_price = float(parts[4])  # 最低价
                    volume = float(parts[5])  # 成交量
                    
                    history_prices.append({
                        'date': date_str,
                        'open_price': open_price,
                        'close_price': close_price,
                        'high_price': high_price,
                        'low_price': low_price,
                        'volume': volume
                    })
            
            logger.info(f"成功获取股票 {stock_code} 的 {len(history_prices)} 个交易日历史价格数据")
            return history_prices
        else:
            logger.warning(f"未获取到股票 {stock_code} 的历史价格数据")
            return []
            
    except Exception as e:
        logger.error(f"获取股票 {stock_code} 历史价格数据失败: {e}")
        return []

# 为股票数据添加历史价格信息
def add_history_prices_to_stocks(stocks, days=30):
    """
    为股票列表中的每只股票添加历史价格数据
    
    Args:
        stocks: 股票数据列表
        days: 获取的历史交易日数，默认30个交易日
    
    Returns:
        list: 包含历史价格数据的股票列表
    """
    if not stocks:
        return stocks
    
    logger.info(f"开始为 {len(stocks)} 只股票添加 {days} 个交易日历史价格数据...")
    
    for i, stock in enumerate(stocks):
        stock_code = stock.get('code', '')
        if stock_code:
            # 获取历史价格数据
            history_prices = get_stock_history_prices(stock_code, days)
            
            # 添加到股票数据中
            stock['history_prices'] = history_prices
            
            # 计算技术指标
            if history_prices:
                # 提取价格数据
                close_prices = [price['close_price'] for price in history_prices]
                open_prices = [price['open_price'] for price in history_prices]
                high_prices = [price['high_price'] for price in history_prices]
                low_prices = [price['low_price'] for price in history_prices]
                volumes = [price['volume'] for price in history_prices]
                
                # 计算移动平均线
                if len(close_prices) >= 5:
                    stock['ma5'] = sum(close_prices[:5]) / 5
                if len(close_prices) >= 10:
                    stock['ma10'] = sum(close_prices[:10]) / 10
                if len(close_prices) >= 20:
                    stock['ma20'] = sum(close_prices[:20]) / 20
                if len(close_prices) >= 30:
                    stock['ma30'] = sum(close_prices[:30]) / 30
                
                # 计算涨跌幅
                if len(close_prices) >= 2:
                    latest_price = close_prices[0]
                    prev_price = close_prices[1]
                    stock['history_change_rate'] = ((latest_price - prev_price) / prev_price) * 100
                    
                    # 计算30日涨跌幅
                    if len(close_prices) >= 30:
                        stock['history_change_rate_30d'] = ((latest_price - close_prices[29]) / close_prices[29]) * 100
                
                # 计算最高价和最低价
                stock['history_high'] = max(close_prices) if close_prices else 0
                stock['history_low'] = min(close_prices) if close_prices else 0
                
                # 计算波动率（标准差）
                if len(close_prices) >= 10:
                    mean_price = sum(close_prices) / len(close_prices)
                    variance = sum((price - mean_price) ** 2 for price in close_prices) / len(close_prices)
                    stock['volatility'] = variance ** 0.5
                
                # 计算成交量指标
                if len(volumes) >= 5:
                    stock['volume_ma5'] = sum(volumes[:5]) / 5
                if len(volumes) >= 10:
                    stock['volume_ma10'] = sum(volumes[:10]) / 10
                
                # 计算相对强弱指标（RSI）
                if len(close_prices) >= 14:
                    gains = []
                    losses = []
                    for j in range(1, min(15, len(close_prices))):
                        change = close_prices[j-1] - close_prices[j]
                        if change > 0:
                            gains.append(change)
                        else:
                            losses.append(abs(change))
                    
                    if gains and losses:
                        avg_gain = sum(gains) / len(gains)
                        avg_loss = sum(losses) / len(losses)
                        if avg_loss != 0:
                            rs = avg_gain / avg_loss
                            stock['rsi'] = 100 - (100 / (1 + rs))
                
                logger.info(f"  [{i+1}/{len(stocks)}] {stock['name']}({stock_code}) - {days}日历史价格数据已添加")
            
            # 添加延迟避免请求过快
            time.sleep(0.5)
    
    logger.info(f"{days}日历史价格数据添加完成！")
    return stocks

# 主函数
def main():
    logger.info("开始爬取东方财富网板块资金流入数据...")
    
    # 爬取数据
    top_sectors, all_sectors = crawl_eastmoney_fund_flow()
    
    # 初始化变量
    top_sectors_with_urls = []
    all_sector_stocks = {}
    
    # 获取板块跳转URL和个股数据
    if top_sectors:
        logger.info("\n获取板块跳转URL...")
        top_sectors_with_urls = get_sector_urls(top_sectors)
        
        # 获取每个板块的个股数据
        logger.info("\n获取板块个股数据...")
        all_sector_stocks = fetch_sector_stocks_data(top_sectors_with_urls)
        
        # 重新保存数据，包含URL和个股信息
        save_crawl_data(all_sectors, top_sectors_with_urls, all_sector_stocks)
    else:
        logger.warning("\n未能获取到符合条件的板块数据")
    
    # 生成HTML报告
    html_file = generate_html_report(top_sectors_with_urls, all_sectors)
    
    # 输出结果
    print_results_summary(top_sectors_with_urls, all_sector_stocks, html_file)

def fetch_sector_stocks_data(top_sectors_with_urls: List[Dict]) -> Dict[str, List[Dict]]:
    """获取所有板块的个股数据"""
    all_sector_stocks = {}
    
    for sector in top_sectors_with_urls:
        sector_name = sector['name']
        sector_url = sector.get('url', '')
        
        # 从URL中提取板块代码
        if '/bkzj/BK' in sector_url:
            sector_code = sector_url.split('/bkzj/')[1].replace('.html', '')
            
            logger.info(f"正在获取 '{sector_name}' 板块的个股数据...")
            stocks = get_sector_stocks(sector_code, sector_name, limit=30)
            
            # 为个股添加历史价格数据
            if stocks:
                stocks_with_history = add_history_prices_to_stocks(stocks, days=30)
                all_sector_stocks[sector_name] = stocks_with_history
                logger.info(f"  成功获取 {len(stocks_with_history)} 只个股数据（包含30日历史价格）")
            else:
                logger.warning(f"  未能获取到个股数据")
                all_sector_stocks[sector_name] = []
            
            # 添加延迟避免请求过快
            time.sleep(2)
        else:
            logger.warning(f"  无法从URL中提取板块代码: {sector_url}")
            all_sector_stocks[sector_name] = []
    
    return all_sector_stocks

def print_results_summary(top_sectors_with_urls: List[Dict], all_sector_stocks: Dict[str, List[Dict]], html_file: str):
    """输出爬取结果摘要"""
    if html_file:
        logger.info(f"\n爬取和报告生成完成！")
        logger.info(f"1. 主力净流入前五个板块：")
        if top_sectors_with_urls:
            for i, sector in enumerate(top_sectors_with_urls, 1):
                url = sector.get('url', 'N/A')
                stock_count = len(all_sector_stocks.get(sector['name'], []))
                logger.info(f"   {i}. {sector['name']} - 超大单流入: {sector['super_large_inflow']}亿, 大单流入: {sector['large_inflow']}亿, URL: {url}")
                logger.info(f"      个股数据: {stock_count} 只（包含30日历史价格）")
        else:
            logger.warning("   暂无符合条件的板块")
        
        total_stocks = sum(len(stocks) for stocks in all_sector_stocks.values())
        logger.info(f"2. 总共获取个股数据: {total_stocks} 只（包含30日历史价格）")
        logger.info(f"3. HTML报告已保存至: {html_file}")
        logger.info(f"4. 详细数据已保存至: eastmoney_crawl_data.json")
    else:
        logger.error("报告生成失败")

if __name__ == "__main__":
    main()