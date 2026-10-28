#!/usr/bin/env python3
"""Мониторинг частоты ошибок по моделям (issue #335).

Собирает метрики ошибок из D1 (ladder_calls), сравнивает с предыдущим периодом,
обнаруживает изменения и генерирует отчет.

Env: CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID
Usage:
    python3 scripts/monitor-error-rates.py --threshold 0.1 --period-hours 24 --format markdown
    python3 scripts/monitor-error-rates.py --threshold 0.1 --period-hours 24 --format json
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

DB_NAME = 'trained-assist-llm-ladder-trace'

# Бакеты по размеру запроса (в tokens), согласованы с src/error-monitor.js
SIZE_BUCKETS = [
    ('small', 0, 4096),
    ('medium', 4097, 16384),
    ('large', 16385, 65536),
    ('xlarge', 65537, float('inf')),
]


def since_ms(hours):
    hours = max(1, min(int(hours), 24 * 30))  # Max 30 days
    return int((time.time() - hours * 3600) * 1000), hours


def query_d1(sql, params, token, account, opener=None):
    """POST one statement via the D1 REST API."""
    base = f'https://api.cloudflare.com/client/v4/accounts/{account}'
    req = urllib.request.Request(
        f'{base}/d1/database', headers={'Authorization': f'Bearer {token}'})
    with (opener or urllib.request.urlopen)(req, timeout=30) as r:
        dbs = json.loads(r.read().decode()).get('result') or []
    db = next((d for d in dbs if d.get('name') == DB_NAME), None)
    if not db:
        sys.exit(f'D1 database {DB_NAME} not found')
    req = urllib.request.Request(
        f'{base}/d1/database/{db["uuid"]}/query',
        data=json.dumps({'sql': sql, 'params': params}).encode(),
        headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
        method='POST')
    with (opener or urllib.request.urlopen)(req, timeout=60) as r:
        res = json.loads(r.read().decode())
    if res.get('errors'):
        sys.exit(f'D1 error: {res["errors"]}')
    return (res.get('result') or [{}])[0].get('results') or []


def collect_error_metrics(since, until, token, account):
    """Сбор метрик ошибок по моделям за период."""
    sql = """
    SELECT
      model,
      COUNT(*) as total_calls,
      SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END) as failed_calls,
      AVG(estimated_tokens_in) as avg_estimated_tokens,
      -- Распределение по размерам (вызовы)
      SUM(CASE WHEN estimated_tokens_in <= 4096 THEN 1 ELSE 0 END) as small_calls,
      SUM(CASE WHEN estimated_tokens_in > 4096 AND estimated_tokens_in <= 16384 THEN 1 ELSE 0 END) as medium_calls,
      SUM(CASE WHEN estimated_tokens_in > 16384 AND estimated_tokens_in <= 65536 THEN 1 ELSE 0 END) as large_calls,
      SUM(CASE WHEN estimated_tokens_in > 65536 THEN 1 ELSE 0 END) as xlarge_calls,
      -- Распределение по размерам (ошибки)
      SUM(CASE WHEN ok = 0 AND estimated_tokens_in <= 4096 THEN 1 ELSE 0 END) as small_failures,
      SUM(CASE WHEN ok = 0 AND estimated_tokens_in > 4096 AND estimated_tokens_in <= 16384 THEN 1 ELSE 0 END) as medium_failures,
      SUM(CASE WHEN ok = 0 AND estimated_tokens_in > 16384 AND estimated_tokens_in <= 65536 THEN 1 ELSE 0 END) as large_failures,
      SUM(CASE WHEN ok = 0 AND estimated_tokens_in > 65536 THEN 1 ELSE 0 END) as xlarge_failures
    FROM ladder_calls
    WHERE ts >= ?1 AND ts <= ?2 AND model IS NOT NULL
    GROUP BY model
    ORDER BY total_calls DESC
    """
    rows = query_d1(sql, [since, until], token, account)
    
    metrics = []
    for row in rows:
        total = int(row.get('total_calls') or 0)
        failed = int(row.get('failed_calls') or 0)
        
        # Распределение по размерам
        size_dist = {}
        for bucket_name, _, _ in SIZE_BUCKETS:
            calls = int(row.get(f'{bucket_name}_calls') or 0)
            failures = int(row.get(f'{bucket_name}_failures') or 0)
            size_dist[bucket_name] = {
                'calls': calls,
                'failures': failures,
                'error_rate': failures / calls if calls > 0 else 0
            }
        
        metrics.append({
            'model': row.get('model'),
            'totalCalls': total,
            'failedCalls': failed,
            'errorRate': failed / total if total > 0 else 0,
            'avgEstimatedTokens': float(row.get('avg_estimated_tokens') or 0),
            'sizeDistribution': size_dist,
        })
    
    return metrics


def detect_changes(current, previous, threshold):
    """Детект изменений в частоте ошибок и распределении по размерам."""
    changes = []
    prev_by_model = {p['model']: p for p in previous}
    
    for curr in current:
        prev = prev_by_model.get(curr['model'])
        
        # Новая модель
        if not prev:
            if curr['totalCalls'] >= 10:
                changes.append({
                    'model': curr['model'],
                    'type': 'new_model',
                    'severity': 'info',
                    'currentErrorRate': curr['errorRate'],
                    'totalCalls': curr['totalCalls'],
                })
            continue
        
        # Изменение общей частоты ошибок
        error_rate_diff = abs(curr['errorRate'] - prev['errorRate'])
        if error_rate_diff > threshold and curr['totalCalls'] >= 10:
            severity = 'critical' if error_rate_diff > threshold * 2 else 'warning'
            changes.append({
                'model': curr['model'],
                'type': 'error_rate_change',
                'severity': severity,
                'previousErrorRate': prev['errorRate'],
                'currentErrorRate': curr['errorRate'],
                'diff': error_rate_diff,
                'direction': 'worse' if curr['errorRate'] > prev['errorRate'] else 'better',
            })
        
        # Изменение распределения по размерам
        for bucket_name, _, _ in SIZE_BUCKETS:
            curr_data = curr['sizeDistribution'].get(bucket_name, {'calls': 0, 'failures': 0})
            prev_data = prev['sizeDistribution'].get(bucket_name, {'calls': 0, 'failures': 0})
            
            if curr_data['calls'] < 5 or prev_data['calls'] < 5:
                continue
            
            curr_rate = curr_data['error_rate']
            prev_rate = prev_data['error_rate']
            rate_diff = abs(curr_rate - prev_rate)
            
            if rate_diff > threshold:
                severity = 'critical' if rate_diff > threshold * 2 else 'warning'
                changes.append({
                    'model': curr['model'],
                    'type': 'size_distribution_change',
                    'severity': severity,
                    'bucket': bucket_name,
                    'previousRate': prev_rate,
                    'currentRate': curr_rate,
                    'diff': rate_diff,
                    'direction': 'worse' if curr_rate > prev_rate else 'better',
                    'calls': curr_data['calls'],
                    'failures': curr_data['failures'],
                })
    
    return changes


def read_previous_metrics(before, token, account, limit=100):
    """Чтение предыдущих метрик из error_metrics_history."""
    try:
        sql = """
        SELECT model_id, total_calls, failed_calls, error_rate, avg_estimated_tokens, size_distribution
        FROM error_metrics_history
        WHERE collected_at < ?1
        ORDER BY collected_at DESC
        LIMIT ?2
        """
        rows = query_d1(sql, [before, limit], token, account)
        
        metrics = []
        for row in rows:
            size_dist = json.loads(row.get('size_distribution') or '{}')
            metrics.append({
                'model': row.get('model_id'),
                'totalCalls': int(row.get('total_calls') or 0),
                'failedCalls': int(row.get('failed_calls') or 0),
                'errorRate': float(row.get('error_rate') or 0),
                'avgEstimatedTokens': float(row.get('avg_estimated_tokens') or 0),
                'sizeDistribution': size_dist,
            })
        return metrics
    except Exception as e:
        # Таблица может не существовать yet
        print(f'Warning: could not read previous metrics: {e}', file=sys.stderr)
        return []


def write_metrics(metrics, since, until, token, account):
    """Запись метрик в error_metrics_history."""
    now = int(time.time() * 1000)
    
    for m in metrics:
        try:
            sql = """
            INSERT INTO error_metrics_history
            (collected_at, period_start, period_end, model_id, total_calls, failed_calls,
             error_rate, avg_estimated_tokens, size_distribution, raw_metrics)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            """
            params = [
                now, since, until, m['model'], m['totalCalls'], m['failedCalls'],
                m['errorRate'], int(m['avgEstimatedTokens']),
                json.dumps(m['sizeDistribution']), json.dumps(m),
            ]
            query_d1(sql, params, token, account)
        except Exception as e:
            print(f'Warning: could not write metrics for {m["model"]}: {e}', file=sys.stderr)


def build_report(metrics, changes, hours, threshold):
    """Построение отчета."""
    total_calls = sum(m['totalCalls'] for m in metrics)
    total_failures = sum(m['failedCalls'] for m in metrics)
    overall_error_rate = total_failures / total_calls if total_calls > 0 else 0
    
    # Группировка изменений по severity
    critical_changes = [c for c in changes if c.get('severity') == 'critical']
    warning_changes = [c for c in changes if c.get('severity') == 'warning']
    info_changes = [c for c in changes if c.get('severity') == 'info']
    
    return {
        'generated_at': time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime()),
        'period_hours': hours,
        'threshold': threshold,
        'summary': {
            'models': len(metrics),
            'total_calls': total_calls,
            'total_failures': total_failures,
            'overall_error_rate': overall_error_rate,
        },
        'changes': {
            'critical': critical_changes,
            'warning': warning_changes,
            'info': info_changes,
            'total': len(changes),
        },
        'metrics': metrics,
    }


def render_markdown(report):
    """Рендеринг отчета в Markdown."""
    lines = [
        f'# Ladder Error Monitor — {report["period_hours"]}h',
        '',
        f'**Generated:** {report["generated_at"]}',
        f'**Threshold:** {report["threshold"]}',
        '',
        '## Summary',
        '',
        f'- **Models analyzed:** {report["summary"]["models"]}',
        f'- **Total calls:** {report["summary"]["total_calls"]}',
        f'- **Total failures:** {report["summary"]["total_failures"]}',
        f'- **Overall error rate:** {report["summary"]["overall_error_rate"] * 100:.1f}%',
        '',
    ]
    
    # Изменения
    changes = report['changes']
    if changes['total'] > 0:
        lines.append('## Changes Detected')
        lines.append('')
        
        if changes['critical']:
            lines.append(f'### 🔴 Critical ({len(changes["critical"])})')
            lines.append('')
            for c in changes['critical']:
                if c['type'] == 'error_rate_change':
                    lines.append(f'- **{c["model"]}**: error rate {c["previousErrorRate"]*100:.1f}% → {c["currentErrorRate"]*100:.1f}% ({c["direction"]})')
                elif c['type'] == 'size_distribution_change':
                    lines.append(f'- **{c["model"]}** ({c["bucket"]}): error rate {c["previousRate"]*100:.1f}% → {c["currentRate"]*100:.1f}% ({c["direction"]}, {c["calls"]} calls)')
            lines.append('')
        
        if changes['warning']:
            lines.append(f'### 🟡 Warning ({len(changes["warning"])})')
            lines.append('')
            for c in changes['warning']:
                if c['type'] == 'error_rate_change':
                    lines.append(f'- **{c["model"]}**: error rate {c["previousErrorRate"]*100:.1f}% → {c["currentErrorRate"]*100:.1f}% ({c["direction"]})')
                elif c['type'] == 'size_distribution_change':
                    lines.append(f'- **{c["model"]}** ({c["bucket"]}): error rate {c["previousRate"]*100:.1f}% → {c["currentRate"]*100:.1f}% ({c["direction"]}, {c["calls"]} calls)')
            lines.append('')
        
        if changes['info']:
            lines.append(f'### ℹ️ Info ({len(changes["info"])})')
            lines.append('')
            for c in changes['info']:
                if c['type'] == 'new_model':
                    lines.append(f'- **{c["model"]}**: new model ({c["totalCalls"]} calls, error rate {c["currentErrorRate"]*100:.1f}%)')
            lines.append('')
    else:
        lines.append('## No Changes Detected')
        lines.append('')
        lines.append('No significant changes in error rates or size distributions.')
        lines.append('')
    
    # Топ моделей по ошибкам
    lines.append('## Top Models by Error Rate')
    lines.append('')
    lines.append('| Model | Calls | Failures | Error Rate | Avg Tokens |')
    lines.append('|---|---:|---:|---:|---:|')
    
    sorted_metrics = sorted(report['metrics'], key=lambda m: m['errorRate'], reverse=True)
    for m in sorted_metrics[:10]:
        lines.append(f'| `{m["model"]}` | {m["totalCalls"]} | {m["failedCalls"]} | {m["errorRate"]*100:.1f}% | {m["avgEstimatedTokens"]:.0f} |')
    
    lines.append('')
    return '\n'.join(lines)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--threshold', type=float, default=0.1, help='Threshold for change detection (0.0-1.0)')
    ap.add_argument('--period-hours', type=int, default=24, help='Period in hours to analyze')
    ap.add_argument('--format', choices=['md', 'json'], default='md', help='Output format')
    ap.add_argument('--write', action='store_true', help='Write metrics to error_metrics_history')
    args = ap.parse_args()
    
    token = os.environ.get('CLOUDFLARE_API_TOKEN')
    account = os.environ.get('CLOUDFLARE_ACCOUNT_ID')
    if not token or not account:
        sys.exit('CLOUDFLARE_API_TOKEN and CLOUDFLARE_ACCOUNT_ID are required')
    
    since, hours = since_ms(args.period_hours)
    until = int(time.time() * 1000)
    
    # Сбор текущих метрик
    current = collect_error_metrics(since, until, token, account)
    
    # Чтение предыдущих метрик
    previous = read_previous_metrics(since, token, account)
    
    # Детект изменений
    changes = detect_changes(current, previous, args.threshold)
    
    # Запись метрик (опционально)
    if args.write and current:
        write_metrics(current, since, until, token, account)
    
    # Построение отчета
    report = build_report(current, changes, hours, args.threshold)
    
    # Вывод
    if args.format == 'json':
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(render_markdown(report), end='')


if __name__ == '__main__':
    main()
