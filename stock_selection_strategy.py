import json
import os
import pandas as pd
import numpy as np
from datetime import datetime

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

def calculate_momentum_factor(stock, market_median_change=None):
    """
    计算动态调整动量反转因子
    
    逻辑说明：
    1. 基于股票的涨跌幅、主力资金流入和市场中位数涨跌幅进行动态调整
    2. 当股票涨幅过大时，倾向于反转策略
    3. 当股票涨幅适中时，倾向于动量策略
    4. 结合主力资金流向作为重要参考
    """
    # 基础动量因子（基于涨跌幅）
    change_rate = stock.get('change_rate', 0)
    
    # 资金流向因子
    main_inflow = stock.get('main_inflow', 0)
    main_ratio = stock.get('main_ratio', 0)
    
    # 超大单流入因子
    super_large_inflow = stock.get('super_large_inflow', 0)
    super_large_ratio = stock.get('super_large_ratio', 0)
    
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
    
    # 根据涨跌幅的不同区间应用不同策略
    if change_rate > reversal_threshold:
        # 涨幅过大，应用反转策略，得分降低
        momentum_score = -0.5 * change_rate + 1.5 * fund_strength + 1.0 * super_large_strength
    elif change_rate < -2.0:
        # 跌幅较大，应用反转策略，得分提高
        momentum_score = 0.8 * abs(change_rate) + 1.0 * fund_strength + 0.8 * super_large_strength
    else:
        # 涨幅适中，应用动量策略
        momentum_score = 1.2 * change_rate + 1.5 * fund_strength + 1.2 * super_large_strength
    
    # 添加价格因素的调整（价格较低的股票可能有更大的上涨空间）
    price = stock.get('price', 100)
    price_factor = 100 / (price + 50)  # 非线性价格调整因子
    momentum_score *= (1 + 0.2 * price_factor)
    
    # 确保得分不会过高或过低
    momentum_score = max(-100, min(100, momentum_score))
    
    return momentum_score

def select_stocks(stock_data, top_n=10):
    """
    使用动态调整动量反转因子从所有股票中选择top_n只
    """
    # 收集所有股票
    all_stocks = collect_all_stocks(stock_data['sector_stocks'])
    
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

def generate_selection_report(selected_stocks):
    """
    生成选股报告
    """
    report = {
        'selection_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_selected': len(selected_stocks),
        'selected_stocks': []
    }
    
    for i, stock in enumerate(selected_stocks, 1):
        stock_report = {
            'rank': i,
            'code': stock.get('code', ''),
            'name': stock.get('name', ''),
            'sector': stock.get('sector', ''),
            'price': stock.get('price', 0),
            'change_rate': stock.get('change_rate', 0),
            'main_inflow': stock.get('main_inflow', 0),
            'main_ratio': stock.get('main_ratio', 0),
            'momentum_score': stock.get('momentum_score', 0)
        }
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

def print_selection_summary(selected_stocks):
    """
    打印选股结果摘要
    """
    print("\n=== 选股结果摘要 ===")
    print(f"选股时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"共选出{len(selected_stocks)}只股票")
    print("\n排名  股票代码  股票名称      行业      价格    涨跌幅(%)  主力净流入(亿)  主力净占比(%)  动量得分")
    print("-" * 100)
    
    for i, stock in enumerate(selected_stocks, 1):
        main_inflow_yi = stock.get('main_inflow', 0) / 1e8
        print(f"{i:<4}  {stock.get('code', ''):<8}  {stock.get('name', ''):<10}  {stock.get('sector', ''):<8}  {stock.get('price', 0):<8.2f}  {stock.get('change_rate', 0):<9.2f}  {main_inflow_yi:<12.2f}  {stock.get('main_ratio', 0):<11.2f}  {stock.get('momentum_score', 0):<8.2f}")

def main():
    """
    主函数
    """
    print("开始执行选股策略...")
    
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
    
    # 执行选股
    selected_stocks = select_stocks(data, top_n=10)
    
    # 生成报告
    report = generate_selection_report(selected_stocks)
    
    # 保存结果
    save_selection_result(report)
    
    # 打印摘要
    print_selection_summary(selected_stocks)
    
    print("\n选股策略执行完成！")

if __name__ == "__main__":
    main()