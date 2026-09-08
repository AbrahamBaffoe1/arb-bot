"""Configuration validation and stable identity for persisted evidence."""
import hashlib
import copy
import json
import math
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parent.parent


def fingerprint(cfg):
    relevant = {k: copy.deepcopy(cfg[k]) for k in ('venues', 'scanner', 'risk', 'paper', 'arb_groups', 'triangles','economics','portfolio') if k in cfg}
    for venue in relevant['venues'].values():
        venue.pop('fees_verified_at', None)
    return hashlib.sha256(json.dumps(relevant, sort_keys=True).encode()).hexdigest()


def load_config(path=None):
    with open(path or ROOT / 'config/config.yaml') as f:
        cfg = yaml.safe_load(f)
    validate(cfg)
    cfg['storage'] = cfg.get('storage', {})
    cfg['storage']['db_path'] = str(ROOT / cfg['storage'].get('db_path', 'data/engine.db'))
    cfg['storage']['live_db_path'] = str(ROOT / cfg['storage'].get('live_db_path', 'data/live.db'))
    cfg['risk']['kill_switch_file'] = str(ROOT / cfg['risk']['kill_switch_file'])
    for section,key in (('recording','db_path'),('operations','heartbeat_file'),('operations','backup_dir')):
        if key in cfg.get(section,{}):
            cfg[section][key]=str(ROOT / cfg[section][key])
    return cfg


def validate(cfg):
    if cfg['mode'] not in ('paper', 'live'):
        raise ValueError('mode must be paper or live')
    for section, keys in {'scanner': ['interval_ms', 'stale_book_ms', 'min_net_edge', 'min_profit_usdt',
                                     'confirmation_ms', 'max_book_skew_ms', 'window_gap_s'],
                          'risk': ['trade_notional_usdt', 'max_daily_loss_usdt', 'max_open_skew_usdt'],
                          'paper': ['starting_balance_usdt'],
                          'live': ['max_trade_usdt', 'max_quote_age_ms'],
                          'funding': ['min_observation_hours', 'min_paper_fills', 'min_days_with_fills',
                                      'min_net_pnl_usdt', 'fee_max_age_hours', 'evidence_max_age_hours',
                                      'pilot_cash_reserve_usdt']}.items():
        for key in keys:
            value = cfg[section][key]
            if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f'{section}.{key} must be finite and positive')
    for value in cfg['scanner'].get('sizes_usdt', []):
        if not math.isfinite(value) or value <= 0:
            raise ValueError('scanner.sizes_usdt must contain positive finite budgets')
    for name, v in cfg['venues'].items():
        for key in ('taker_fee', 'maker_fee'):
            if not math.isfinite(v[key]) or not 0 <= v[key] < 0.1:
                raise ValueError(f'{name}.{key} must be a fractional fee under 0.1')
    for key in ('slippage_buffer', 'rebalance_buffer'):
        if not math.isfinite(cfg['scanner'][key]) or not 0 <= cfg['scanner'][key] < 0.1:
            raise ValueError(f'{key} must be a nonnegative fraction under 0.1')
    if not 0 < cfg['paper']['inventory_fraction'] < 1:
        raise ValueError('paper.inventory_fraction must be between zero and one')
    if cfg['scanner']['book_depth'] not in (10, 25, 100, 500, 1000):
        raise ValueError('choose a supported Kraken book depth')
    if cfg['risk']['cooldown_s'] < 0 or not math.isfinite(cfg['risk']['cooldown_s']):
        raise ValueError('risk.cooldown_s must be finite and nonnegative')
    enabled = {v for v, settings in cfg['venues'].items() if settings.get('enabled')}
    if enabled != {'coinbase', 'kraken'}:
        raise ValueError('This product requires Coinbase and Kraken enabled')
    if cfg['mode'] == 'live' and cfg['scanner'].get('triangular_enabled'):
        raise ValueError('Live triangular execution is not supported; disable triangular_enabled')
    if cfg['live']['max_trade_usdt'] > cfg['risk']['trade_notional_usdt']:
        raise ValueError('live.max_trade_usdt cannot exceed risk.trade_notional_usdt')
    if cfg['storage']['db_path'] == cfg['storage']['live_db_path']:
        raise ValueError('Paper and live databases must be different')
    for section,keys in {'recording':['queue_capacity','max_disk_gb','min_free_disk_gb'],
                         'operations':['watchdog_max_age_s','max_event_loop_lag_ms','backup_interval_hours','max_backup_disk_gb'],
                         'portfolio':['max_drawdown_fraction','max_asset_exposure_usdt','max_venue_exposure_usdt','min_cash_fraction']}.items():
        for key in keys:
            value=cfg.get(section,{}).get(key,1)
            if not isinstance(value,(int,float)) or not math.isfinite(value) or value<=0:
                raise ValueError(f'{section}.{key} must be finite and positive')
    operating=cfg.get('economics',{}).get('operating_cost_usdt_per_day',0)
    if not isinstance(operating,(int,float)) or not math.isfinite(operating) or operating<0:
        raise ValueError('Operating costs must be finite and nonnegative')
    for key in ('max_drawdown_fraction','min_cash_fraction'):
        value=cfg.get('portfolio',{}).get(key,.2)
        if value>1: raise ValueError(f'portfolio.{key} cannot exceed one')
    for section,key,default in (('recording','queue_capacity',20000),('research','max_discovery_markets',100),('research','discovery_depth_markets',10)):
        value=cfg.get(section,{}).get(key,default)
        if not isinstance(value,int) or isinstance(value,bool) or value<=0:
            raise ValueError(f'{section}.{key} must be a positive integer')
    unmatched=cfg.get('research',{}).get('max_unhedged_usdt',.01)
    if not isinstance(unmatched,(int,float)) or not math.isfinite(unmatched) or unmatched<0:
        raise ValueError('research.max_unhedged_usdt must be finite and nonnegative')
