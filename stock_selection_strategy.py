import json
import os
import pandas as pd
import numpy as np
from datetime import datetime
from eastmoney_fund_flow import generate_html_report

# 阶段类型配置
PHASE_CONFIG = {
    "上涨阶段": {
        "description": "市场处于上涨趋势，适合动量策略",
        "weights": {
            "momentum_factor": 0.4,      # 动量因子权重
            "trend_factor": 0.3,        # 趋势因子权重
            "volume_factor": 0.15,      # 成交量因子权重
            "fund_flow_factor": 0.15    # 资金流向因子权重
        }
    },
    "震荡阶段": {
        "description": "市场处于震荡整理，适合反转策略",
        "weights": {
            "momentum_factor": 0.2,      # 动量因子权重
            "trend_factor": 0.25,       # 趋势因子权重
            "volume_factor": 0.25,      # 成交量因子权重
            "fund_flow_factor": 0.3     # 资金流向因子权重
        }
    },
    "下跌阶段": {
        "description": "市场处于下跌趋势，适合防御性策略",
        "weights": {
            "momentum_factor": 0.15,     # 动量因子权重
            "trend_factor": 0.35,       # 趋势因子权重
            "volume_factor": 0.2,       # 成交量因子权重
            "fund_flow_factor": 0.3     # 资金流向因子权重
        }
    }
}

def load_stock_data(json_file_path):
    """
    从JSON文件加载股票数据
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"加载JSON文件失败: {e}")
        return None

def collect_all_stocks(sector_stocks):
    """
    收集所有行业的股票到一个列表中
    """
    all_stocks = []
    for sector_name, stocks in sector_stocks.items():
        for stock in stocks:
            # 添加行业信息
            stock['sector'] = sector_name
            all_stocks.append(stock)
    return all_stocks

def calculate_15day_momentum_reversal_factor(stock):
    """
    计算短期15天动量反转因子
    
    基于15个交易日的历史价格数据，结合多种技术指标：
    1. 短期动量：最近5天 vs 前10天的表现
    2. 价格位置：当前价格在15天内的相对位置
    3. 波动率调整：考虑价格波动对动量的影响
    4. 成交量确认：结合量比因子确认动量强度
    5. 资金流向：主力资金流向作为辅助确认
    
    返回：综合动量反转得分，正值表示动量策略，负值表示反转策略
    """
    # 检查是否有历史价格数据
    if 'history_prices' not in stock or not stock['history_prices']:
        return 0.0
    
    history_prices = stock['history_prices']
    
    # 确保有足够的历史数据（至少15个交易日）
    if len(history_prices) < 15:
        return 0.0
    
    # 提取收盘价序列（从最新到最旧）
    close_prices = [price['close_price'] for price in history_prices]
    
    # 计算关键价格点
    current_price = close_prices[0]  # 最新价格
    price_5days_ago = close_prices[4]  # 5天前价格
    price_10days_ago = close_prices[9]  # 10天前价格
    price_15days_ago = close_prices[14]  # 15天前价格
    
    # 计算历史高点和低点
    history_high = max(close_prices)
    history_low = min(close_prices)
    
    # 1. 短期动量计算（最近5天 vs 前10天）
    momentum_5day = (current_price - price_5days_ago) / price_5days_ago * 100
    momentum_10day = (price_5days_ago - price_15days_ago) / price_15days_ago * 100
    
    # 2. 价格位置因子（当前价格在15天内的相对位置）
    if history_high != history_low:
        price_position = (current_price - history_low) / (history_high - history_low) * 100
    else:
        price_position = 50.0
    
    # 3. 波动率调整因子（15天价格波动率）
    price_returns = []
    for i in range(len(close_prices) - 1):
        if close_prices[i+1] > 0:
            daily_return = (close_prices[i] - close_prices[i+1]) / close_prices[i+1]
            price_returns.append(daily_return)
    
    if price_returns:
        volatility = np.std(price_returns) * np.sqrt(252)  # 年化波动率
        volatility_factor = min(1.0, 0.3 / (volatility + 0.1))  # 波动率越大，因子越小
    else:
        volatility_factor = 1.0
    
    # 4. 成交量确认因子
    volume_ratio = stock.get('volume_ratio', 1.0)
    volume_factor = min(2.0, volume_ratio)  # 量比因子，上限为2
    
    # 5. 资金流向因子
    main_ratio = stock.get('main_ratio', 0)
    fund_factor = 1.0 + main_ratio * 0.1  # 主力净占比每1%增加0.1的因子
    
    # 6. 综合动量反转逻辑
    # 如果短期动量很强且价格处于高位，倾向于反转
    # 如果短期动量适中且价格处于中低位，倾向于动量
    
    if momentum_5day > 15:  # 短期涨幅过大
        # 反转策略：短期涨幅过大，预期回调
        if price_position > 80:  # 价格处于高位
            reversal_score = -momentum_5day * 0.8
        else:
            reversal_score = -momentum_5day * 0.5
    elif momentum_5day < -10:  # 短期跌幅过大
        # 反转策略：短期跌幅过大，预期反弹
        if price_position < 20:  # 价格处于低位
            reversal_score = abs(momentum_5day) * 0.8
        else:
            reversal_score = abs(momentum_5day) * 0.5
    else:
        # 动量策略：短期动量适中
        if momentum_10day > 0:  # 中期趋势向上
            reversal_score = momentum_5day * 1.2
        else:  # 中期趋势向下
            reversal_score = momentum_5day * 0.8
    
    # 应用调整因子
    final_score = (reversal_score * volatility_factor * volume_factor * fund_factor + 
                  price_position * 0.1)  # 价格位置作为辅助因子
    
    # 归一化到合理范围
    final_score = max(-50, min(50, final_score))
    
    return final_score

def calculate_momentum_factor(stock, market_median_change=None):
    """
    计算动态调整动量反转因子（保留原有函数，但标记为旧版本）
    """
    # 基础动量因子（基于涨跌幅）
    change_rate = stock.get('change_rate', 0)
    
    # 资金流向因子
    main_inflow = stock.get('main_inflow', 0)
    main_ratio = stock.get('main_ratio', 0)
    
    # 超大单流入因子
    super_large_inflow = stock.get('super_large_inflow', 0)
    super_large_ratio = stock.get('super_large_ratio', 0)
    
    # 量比因子 - 新增
    volume_ratio = stock.get('volume_ratio', 1.0)
    
    # 计算量比因子得分（非线性转换，量比越大得分越高，但有上限）
    volume_factor = min(3.0, volume_ratio) - 1.0  # 基础量比因子，大于1表示放量
    if volume_ratio > 3.0:
        # 量比超过3视为异常放量，给予额外奖励但增速放缓
        volume_factor = 2.0 + (volume_ratio - 3.0) * 0.2
    
    # 计算资金强度综合得分
    fund_strength = 0.6 * (main_inflow / 1e8) + 0.4 * main_ratio
    
    # 计算超大单强度得分
    super_large_strength = 0.5 * (super_large_inflow / 1e8) + 0.5 * super_large_ratio
    
    # 如果提供了市场中位数涨跌幅，用于动态调整反转阈值
    if market_median_change is not None:
        # 动态调整反转阈值，市场波动大时阈值提高
        reversal_threshold = max(3.0, abs(market_median_change) * 2)
    else:
        reversal_threshold = 5.0  # 默认反转阈值
    
    # 根据涨跌幅的不同区间应用不同策略，并引入量比因子
    if change_rate > reversal_threshold:
        # 涨幅过大，应用反转策略，得分降低
        # 量比大的股票可能会继续上涨，因此反转力度减弱
        momentum_score = -0.5 * change_rate + 1.5 * fund_strength + 1.0 * super_large_strength + 0.8 * volume_factor
    elif change_rate < -2.0:
        # 跌幅较大，应用反转策略，得分提高
        # 量比大的下跌股票可能存在超卖情况，反转潜力更大
        momentum_score = 0.8 * abs(change_rate) + 1.0 * fund_strength + 0.8 * super_large_strength + 1.2 * volume_factor
    else:
        # 涨幅适中，应用动量策略
        # 量比大的股票动量更强
        momentum_score = 1.2 * change_rate + 1.5 * fund_strength + 1.2 * super_large_strength + 1.0 * volume_factor
    
    # 添加价格因素的调整（价格较低的股票可能有更大的上涨空间）
    price = stock.get('price', 100)
    price_factor = 100 / (price + 50)  # 非线性价格调整因子
    momentum_score *= (1 + 0.2 * price_factor)
    
    # 确保得分不会过高或过低
    momentum_score = max(-100, min(100, momentum_score))
    
    return momentum_score

def calculate_trend_factor(stock):
    """
    计算均线趋势因子
    
    基于MA5、MA10、MA20均线系统，判断趋势方向和强度：
    1. 均线金叉：短期均线上穿长期均线
    2. 趋势方向：均线排列顺序判断趋势
    3. 趋势强度：均线之间的间距和角度
    4. 拐点判断：股价刚突破均线系统
    
    返回：趋势因子得分，正值表示向上趋势，负值表示向下趋势
    """
    # 检查是否有均线数据
    if not all(key in stock for key in ['ma5', 'ma10', 'ma20']):
        return 0.0
    
    ma5 = stock.get('ma5', 0)
    ma10 = stock.get('ma10', 0)
    ma20 = stock.get('ma20', 0)
    current_price = stock.get('price', 0)
    
    # 检查数据有效性
    if ma5 <= 0 or ma10 <= 0 or ma20 <= 0 or current_price <= 0:
        return 0.0
    
    # 1. 均线金叉判断
    # MA5上穿MA10且MA10上穿MA20为金叉
    golden_cross_5_10 = ma5 > ma10
    golden_cross_10_20 = ma10 > ma20
    
    # 2. 均线排列顺序（多头排列：MA5 > MA10 > MA20）
    if golden_cross_5_10 and golden_cross_10_20:
        # 完美多头排列
        trend_strength = 1.0
    elif golden_cross_5_10 and not golden_cross_10_20:
        # 部分多头排列（MA5 > MA10但MA10 < MA20）
        trend_strength = 0.5
    elif not golden_cross_5_10 and golden_cross_10_20:
        # 部分多头排列（MA5 < MA10但MA10 > MA20）
        trend_strength = 0.3
    else:
        # 空头排列
        trend_strength = -0.5
    
    # 3. 趋势强度计算（基于均线间距）
    # MA5与MA10的间距
    gap_5_10 = (ma5 - ma10) / ma10 * 100
    # MA10与MA20的间距
    gap_10_20 = (ma10 - ma20) / ma20 * 100
    
    # 趋势强度因子
    gap_factor = min(2.0, max(-2.0, (gap_5_10 + gap_10_20) / 2))
    
    # 4. 拐点判断（股价刚突破均线系统）
    # 当前价格相对于均线的位置
    price_above_ma5 = current_price > ma5
    price_above_ma10 = current_price > ma10
    price_above_ma20 = current_price > ma20
    
    # 突破强度
    if price_above_ma5 and price_above_ma10 and price_above_ma20:
        # 完全突破
        breakthrough_strength = 1.0
    elif price_above_ma5 and price_above_ma10:
        # 部分突破
        breakthrough_strength = 0.6
    elif price_above_ma5:
        # 初步突破
        breakthrough_strength = 0.3
    else:
        # 未突破
        breakthrough_strength = -0.5
    
    # 5. 综合趋势因子计算
    trend_score = trend_strength * 40 + gap_factor * 20 + breakthrough_strength * 40
    
    # 归一化到合理范围
    trend_score = max(-100, min(100, trend_score))
    
    return trend_score

def select_stocks_with_15day_factor(stock_data, top_n=10):
    """
    使用短期15天动量反转因子从所有股票中选择top_n只
    """
    # 收集所有股票，添加错误处理
    if 'sector_stocks' in stock_data:
        all_stocks = collect_all_stocks(stock_data['sector_stocks'])
    else:
        # 尝试其他可能的数据结构格式
        all_stocks = []
        print("警告: 'sector_stocks' 键不存在，尝试查找其他格式的数据...")
        
        # 如果stock_data本身就是一个字典，尝试直接从中提取股票数据
        if isinstance(stock_data, dict):
            for key, value in stock_data.items():
                if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict) and 'code' in value[0]:
                    # 找到可能的股票列表
                    for stock in value:
                        stock['sector'] = key
                        all_stocks.append(stock)
    
    if not all_stocks:
        print("错误: 未能找到任何股票数据")
        return []
    
    print(f"总共收集到{len(all_stocks)}只股票")
    
    # 统计有历史价格数据的股票数量
    stocks_with_history = [s for s in all_stocks if 'history_prices' in s and len(s.get('history_prices', [])) >= 15]
    print(f"其中{len(stocks_with_history)}只股票有完整的15天历史价格数据")
    
    # 计算每只股票的15天动量反转因子得分
    for stock in all_stocks:
        stock['15day_momentum_score'] = calculate_15day_momentum_reversal_factor(stock)
        
        # 同时计算旧版因子用于对比
        change_rates = [s.get('change_rate', 0) for s in all_stocks]
        market_median_change = np.median(change_rates) if change_rates else 0
        stock['old_momentum_score'] = calculate_momentum_factor(stock, market_median_change)
    
    # 按15天动量反转因子得分排序，选择前top_n只股票
    selected_stocks = sorted(all_stocks, key=lambda x: x['15day_momentum_score'], reverse=True)[:top_n]
    
    return selected_stocks

def select_stocks(stock_data, top_n=10):
    """
    使用动态调整动量反转因子从所有股票中选择top_n只（兼容旧版本）
    """
    # 收集所有股票，添加错误处理
    if 'sector_stocks' in stock_data:
        all_stocks = collect_all_stocks(stock_data['sector_stocks'])
    else:
        # 尝试其他可能的数据结构格式
        all_stocks = []
        print("警告: 'sector_stocks' 键不存在，尝试查找其他格式的数据...")
        
        # 如果stock_data本身就是一个字典，尝试直接从中提取股票数据
        if isinstance(stock_data, dict):
            for key, value in stock_data.items():
                if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict) and 'code' in value[0]:
                    # 找到可能的股票列表
                    for stock in value:
                        stock['sector'] = key
                        all_stocks.append(stock)
    
    if not all_stocks:
        print("错误: 未能找到任何股票数据")
        return []
    
    print(f"总共收集到{len(all_stocks)}只股票")
    
    # 计算市场中位数涨跌幅，用于动态调整
    change_rates = [stock.get('change_rate', 0) for stock in all_stocks]
    market_median_change = np.median(change_rates) if change_rates else 0
    
    # 计算每只股票的动量反转因子得分
    for stock in all_stocks:
        stock['momentum_score'] = calculate_momentum_factor(stock, market_median_change)
    
    # 按得分排序，选择前top_n只股票
    selected_stocks = sorted(all_stocks, key=lambda x: x['momentum_score'], reverse=True)[:top_n]
    
    return selected_stocks

def select_stocks_with_phase(stock_data, phase_type="上涨阶段", top_n=10):
    """
    使用阶段类型配置的选股策略
    
    Args:
        stock_data: 股票数据
        phase_type: 阶段类型（上涨阶段/震荡阶段/下跌阶段）
        top_n: 选择前n只股票
    """
    # 检查阶段类型是否有效
    if phase_type not in PHASE_CONFIG:
        print(f"错误: 无效的阶段类型 '{phase_type}'，请使用以下类型之一: {list(PHASE_CONFIG.keys())}")
        return []
    
    # 获取阶段配置
    phase_config = PHASE_CONFIG[phase_type]
    weights = phase_config['weights']
    
    print(f"=== 使用 {phase_type} 选股策略 ===")
    print(f"策略描述: {phase_config['description']}")
    print(f"因子权重配置: {weights}")
    
    # 收集所有股票，添加错误处理
    if 'sector_stocks' in stock_data:
        all_stocks = collect_all_stocks(stock_data['sector_stocks'])
    else:
        # 尝试其他可能的数据结构格式
        all_stocks = []
        print("警告: 'sector_stocks' 键不存在，尝试查找其他格式的数据...")
        
        # 如果stock_data本身就是一个字典，尝试直接从中提取股票数据
        if isinstance(stock_data, dict):
            for key, value in stock_data.items():
                if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict) and 'code' in value[0]:
                    # 找到可能的股票列表
                    for stock in value:
                        stock['sector'] = key
                        all_stocks.append(stock)
    
    if not all_stocks:
        print("错误: 未能找到任何股票数据")
        return []
    
    print(f"总共收集到{len(all_stocks)}只股票")
    
    # 计算市场中位数涨跌幅，用于动态调整
    change_rates = [stock.get('change_rate', 0) for stock in all_stocks]
    market_median_change = np.median(change_rates) if change_rates else 0
    
    # 计算每只股票的综合得分
    for stock in all_stocks:
        # 计算各个因子得分
        momentum_score = calculate_momentum_factor(stock, market_median_change)
        trend_score = calculate_trend_factor(stock)
        
        # 计算成交量因子（基于量比）
        volume_ratio = stock.get('volume_ratio', 1.0)
        volume_factor = min(3.0, volume_ratio) - 1.0
        if volume_ratio > 3.0:
            volume_factor = 2.0 + (volume_ratio - 3.0) * 0.2
        
        # 计算资金流向因子
        main_inflow = stock.get('main_inflow', 0)
        main_ratio = stock.get('main_ratio', 0)
        fund_flow_factor = 0.6 * (main_inflow / 1e8) + 0.4 * main_ratio
        
        # 应用阶段权重计算综合得分
        composite_score = (
            momentum_score * weights['momentum_factor'] +
            trend_score * weights['trend_factor'] +
            volume_factor * 20 * weights['volume_factor'] +  # 放大成交量因子
            fund_flow_factor * 20 * weights['fund_flow_factor']  # 放大资金流向因子
        )
        
        # 存储各个因子得分和综合得分
        stock['phase_momentum_score'] = momentum_score
        stock['phase_trend_score'] = trend_score
        stock['phase_volume_factor'] = volume_factor
        stock['phase_fund_flow_factor'] = fund_flow_factor
        stock['phase_composite_score'] = composite_score
        stock['phase_type'] = phase_type
    
    # 按综合得分排序，选择前top_n只股票
    selected_stocks = sorted(all_stocks, key=lambda x: x['phase_composite_score'], reverse=True)[:top_n]
    
    return selected_stocks

def generate_selection_report(selected_stocks, use_15day_factor=False, phase_type=None):
    """
    生成选股报告
    
    Args:
        selected_stocks: 选中的股票列表
        use_15day_factor: 是否使用15天动量反转因子
        phase_type: 阶段类型（上涨阶段/震荡阶段/下跌阶段）
    """
    if phase_type:
        factor_type = f'phase_{phase_type}'
    else:
        factor_type = '15day_momentum_reversal' if use_15day_factor else 'original_momentum'
    
    report = {
        'selection_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_selected': len(selected_stocks),
        'factor_type': factor_type,
        'selected_stocks': []
    }
    
    for i, stock in enumerate(selected_stocks, 1):
        # 确保main_inflow保持原始单位（元），不进行单位转换
        stock_report = {
            'rank': i,
            'code': stock.get('code', ''),
            'name': stock.get('name', ''),
            'sector': stock.get('sector', ''),
            'price': stock.get('price', 0),
            'change_rate': stock.get('change_rate', 0),
            'main_inflow': stock.get('main_inflow', 0),  # 保持原始单位
            'main_ratio': stock.get('main_ratio', 0),
            'super_large_inflow': stock.get('super_large_inflow', 0),
            'super_large_ratio': stock.get('super_large_ratio', 0),
            'large_inflow': stock.get('large_inflow', 0),
            'large_ratio': stock.get('large_ratio', 0)
        }
        
        if phase_type:
            # 阶段类型选股报告
            stock_report['phase_type'] = phase_type
            stock_report['phase_composite_score'] = stock.get('phase_composite_score', 0)
            stock_report['phase_momentum_score'] = stock.get('phase_momentum_score', 0)
            stock_report['phase_trend_score'] = stock.get('phase_trend_score', 0)
            stock_report['phase_volume_factor'] = stock.get('phase_volume_factor', 0)
            stock_report['phase_fund_flow_factor'] = stock.get('phase_fund_flow_factor', 0)
        elif use_15day_factor:
            stock_report['15day_momentum_score'] = stock.get('15day_momentum_score', 0)
            stock_report['old_momentum_score'] = stock.get('old_momentum_score', 0)
        else:
            stock_report['momentum_score'] = stock.get('momentum_score', 0)
        
        report['selected_stocks'].append(stock_report)
    
    return report

def save_selection_result(report, output_file='selected_stocks.json'):
    """
    保存选股结果到JSON文件
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"选股结果已保存到 {output_file}")
    except Exception as e:
        print(f"保存选股结果失败: {e}")

def save_combined_selection_result(report_old, report_new, phase_reports=None, output_file='selected_stocks_combined.json'):
    """
    保存合并的选股结果到单个JSON文件
    """
    try:
        combined_report = {
            'selection_time': report_new.get('selection_time', ''),
            'total_selected': {
                'original_momentum': report_old.get('total_selected', 0),
                '15day_momentum_reversal': report_new.get('total_selected', 0)
            },
            'original_momentum_stocks': report_old.get('selected_stocks', []),
            '15day_momentum_reversal_stocks': report_new.get('selected_stocks', [])
        }
        
        # 添加阶段选股结果
        if phase_reports:
            combined_report['total_selected']['phase_selection'] = sum(len(report.get('selected_stocks', [])) for report in phase_reports.values())
            for phase_type, report in phase_reports.items():
                combined_report[f'{phase_type}_stocks'] = report.get('selected_stocks', [])
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(combined_report, f, ensure_ascii=False, indent=2)
        print(f"合并选股结果已保存到 {output_file}")
    except Exception as e:
        print(f"保存合并选股结果失败: {e}")

def print_selection_summary(selected_stocks, use_15day_factor=False, phase_type=None):
    """
    打印选股结果摘要
    
    Args:
        selected_stocks: 选中的股票列表
        use_15day_factor: 是否使用15天动量反转因子
        phase_type: 阶段类型（上涨阶段/震荡阶段/下跌阶段）
    """
    if phase_type:
        factor_type = f"阶段类型选股 ({phase_type})"
    else:
        factor_type = "15天动量反转因子" if use_15day_factor else "原动量因子"
    
    print(f"\n=== 选股结果摘要 ({factor_type}) ===")
    print(f"选股时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"共选出{len(selected_stocks)}只股票")
    
    if phase_type:
        print("\n排名  股票代码  股票名称      行业      价格    涨跌幅(%)  综合得分  动量得分  趋势得分  成交量因子  资金流向因子")
        print("-" * 150)
        
        for i, stock in enumerate(selected_stocks, 1):
            print(f"{i:<4}  {stock.get('code', ''):<8}  {stock.get('name', ''):<10}  {stock.get('sector', ''):<8}  {stock.get('price', 0):<8.2f}  {stock.get('change_rate', 0):<9.2f}  {stock.get('phase_composite_score', 0):<8.2f}  {stock.get('phase_momentum_score', 0):<8.2f}  {stock.get('phase_trend_score', 0):<8.2f}  {stock.get('phase_volume_factor', 0):<10.2f}  {stock.get('phase_fund_flow_factor', 0):<12.2f}")
    elif use_15day_factor:
        print("\n排名  股票代码  股票名称      行业      价格    涨跌幅(%)  15天动量得分  原动量得分")
        print("-" * 120)
        
        for i, stock in enumerate(selected_stocks, 1):
            print(f"{i:<4}  {stock.get('code', ''):<8}  {stock.get('name', ''):<10}  {stock.get('sector', ''):<8}  {stock.get('price', 0):<8.2f}  {stock.get('change_rate', 0):<9.2f}  {stock.get('15day_momentum_score', 0):<12.2f}  {stock.get('old_momentum_score', 0):<10.2f}")
    else:
        print("\n排名  股票代码  股票名称      行业      价格    涨跌幅(%)  主力净流入(亿)  主力净占比(%)  超大单净流入(亿) 超大单净占比(%) 大单净流入(亿) 大单净占比(%) 动量得分")
        print("-" * 150)
        
        for i, stock in enumerate(selected_stocks, 1):
            main_inflow_yi = stock.get('main_inflow', 0) / 1e8
            super_large_inflow_yi = stock.get('super_large_inflow', 0) / 1e8
            large_inflow_yi = stock.get('large_inflow', 0) / 1e8
            print(f"{i:<4}  {stock.get('code', ''):<8}  {stock.get('name', ''):<10}  {stock.get('sector', ''):<8}  {stock.get('price', 0):<8.2f}  {stock.get('change_rate', 0):<9.2f}  {main_inflow_yi:<12.2f}  {stock.get('main_ratio', 0):<11.2f}  {super_large_inflow_yi:<14.2f} {stock.get('super_large_ratio', 0):<12.2f} {large_inflow_yi:<12.2f} {stock.get('large_ratio', 0):<11.2f} {stock.get('momentum_score', 0):<8.2f}")

def main():
    """
    主函数
    """
    print("开始执行选股策略...")
    
    # 配置当前使用的阶段类型（可修改为'上涨阶段'、'震荡阶段'或'下跌阶段'）
    CURRENT_PHASE_TYPE = '震荡阶段'  # 当前配置为下跌阶段
    
    # 获取当前目录下的eastmoney_crawl_data.json文件
    json_file = 'eastmoney_crawl_data.json'
    
    if not os.path.exists(json_file):
        print(f"错误: 找不到文件 {json_file}")
        return
    
    # 加载股票数据
    data = load_stock_data(json_file)
    if not data:
        print("无法加载股票数据，程序退出")
        return
    
    print(f"\n=== 使用{CURRENT_PHASE_TYPE}阶段选股策略 ===")
    print(f"阶段配置: {PHASE_CONFIG[CURRENT_PHASE_TYPE]['description']}")
    print(f"因子权重: {PHASE_CONFIG[CURRENT_PHASE_TYPE]['weights']}")
    
    # 执行阶段类型选股（只使用配置的阶段）
    selected_stocks_phase = select_stocks_with_phase(data, phase_type=CURRENT_PHASE_TYPE, top_n=10)
    
    # 生成报告
    phase_report = generate_selection_report(selected_stocks_phase, phase_type=CURRENT_PHASE_TYPE)
    
    # 打印摘要
    print_selection_summary(selected_stocks_phase, phase_type=CURRENT_PHASE_TYPE)
    
    # 保存选股结果到文件（只保存配置阶段的选股结果）
    save_selection_result(phase_report, 'selected_stocks.json')
    
    print("\n选股策略执行完成！")
    print(f"选股结果已保存到: selected_stocks.json")
    print(f"当前使用阶段: {CURRENT_PHASE_TYPE}")

if __name__ == "__main__":
    main()