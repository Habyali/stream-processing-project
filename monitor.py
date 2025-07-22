#!/usr/bin/env python3

import asyncio
import time
import json
import subprocess
import requests
from datetime import datetime
from typing import Dict, Tuple, Optional
from dataclasses import dataclass

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.columns import Columns
from rich.text import Text
from rich.layout import Layout
from rich.progress import Progress, BarColumn, TextColumn

#=========
# Data Classes

@dataclass
class Metrics:
    pg_count: int = 0
    bq_count: int = 0
    es_count: int = 0
    kafka_offset: int = 0
    pg_rate: int = 0
    bq_rate: int = 0
    es_rate: int = 0
    redis_engagement: Dict[str, float] = None
    redis_access: Dict[str, int] = None
    timestamp: float = 0

#=========
# Data Fetchers

class StreamingMonitor:
    def __init__(self):
        self.console = Console()
        self.previous_metrics = Metrics()
        self.metrics_history = []
        
    async def get_postgres_count(self) -> int:
        try:
            cmd = ['docker-compose', 'exec', '-T', 'postgresql', 'psql', 
                   '-U', 'streaming_user', '-d', 'streaming_db', '-c', 
                   'SELECT COUNT(*) FROM engagement_events;']
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            
            for line in result.stdout.split('\n'):
                line = line.strip()
                if line.isdigit():
                    return int(line)
            return 0
        except Exception:
            return 0

    async def get_bigquery_count(self) -> int:
        try:
            url = "http://localhost:9050/projects/streaming_project/queries"
            payload = {"query": "SELECT COUNT(*) as total_events FROM engagement_data.events"}
            
            response = requests.post(url, json=payload, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if 'rows' in data and data['rows']:
                    return int(data['rows'][0]['f'][0]['v'])
            return 0
        except Exception:
            return 0

    async def get_elasticsearch_count(self) -> int:
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            url = f"http://localhost:9200/engagement-events-{today}/_count"
            
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                return data.get('count', 0)
            return 0
        except Exception:
            return 0

    async def get_kafka_offset(self) -> int:
        try:
            cmd = ['docker-compose', 'exec', '-T', 'kafka', 'kafka-run-class', 
                   'kafka.tools.ConsumerGroupCommand', '--bootstrap-server', 
                   'localhost:9092', '--group', 'flink-datastream-processor', '--describe']
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            
            for line in result.stdout.split('\n'):
                if 'streaming.public.engagement_events' in line:
                    parts = line.split()
                    if len(parts) > 4 and parts[4].isdigit():
                        return int(parts[4])
            return 0
        except Exception:
            return 0

    async def get_redis_stats(self) -> Tuple[Dict[str, float], Dict[str, int]]:
        try:
            # Get top 5 engagement
            cmd_engagement = ['docker-compose', 'exec', '-T', 'redis', 'redis-cli', 
                            'ZREVRANGE', 'stats:top_by_engagement', '0', '4', 'WITHSCORES']
            result_eng = subprocess.run(cmd_engagement, capture_output=True, text=True, timeout=5)
            
            # Get top 7 access  
            cmd_access = ['docker-compose', 'exec', '-T', 'redis', 'redis-cli', 
                         'ZREVRANGE', 'stats:top_by_access', '0', '6', 'WITHSCORES']
            result_acc = subprocess.run(cmd_access, capture_output=True, text=True, timeout=5)
            
            # Debug print
            print(f"DEBUG - Engagement output: {result_eng.stdout.strip()}")
            print(f"DEBUG - Access output: {result_acc.stdout.strip()}")
            
            # Parse engagement
            engagement = {}
            eng_lines = [line.strip() for line in result_eng.stdout.strip().split('\n') if line.strip()]
            for i in range(0, len(eng_lines), 2):
                if i + 1 < len(eng_lines):
                    content_type = eng_lines[i]
                    try:
                        score = float(eng_lines[i + 1])
                        engagement[content_type] = score
                    except ValueError:
                        continue
            
            # Parse access
            access = {}
            acc_lines = [line.strip() for line in result_acc.stdout.strip().split('\n') if line.strip()]
            for i in range(0, len(acc_lines), 2):
                if i + 1 < len(acc_lines):
                    content_type = acc_lines[i]
                    try:
                        count = int(float(acc_lines[i + 1]))
                        access[content_type] = count
                    except ValueError:
                        continue
            
            return engagement, access
        except Exception as e:
            print(f"DEBUG - Redis error: {e}")
            return {}, {}

    async def collect_metrics(self) -> Metrics:
        # Collect all metrics concurrently
        pg_task = asyncio.create_task(self.get_postgres_count())
        bq_task = asyncio.create_task(self.get_bigquery_count())
        es_task = asyncio.create_task(self.get_elasticsearch_count())
        kafka_task = asyncio.create_task(self.get_kafka_offset())
        redis_task = asyncio.create_task(self.get_redis_stats())
        
        pg_count = await pg_task
        bq_count = await bq_task
        es_count = await es_task
        kafka_offset = await kafka_task
        redis_engagement, redis_access = await redis_task
        
        current_time = time.time()
        
        # Calculate rates
        pg_rate = bq_rate = es_rate = 0
        if self.previous_metrics.timestamp > 0:
            time_diff = current_time - self.previous_metrics.timestamp
            if time_diff > 0:
                pg_rate = int((pg_count - self.previous_metrics.pg_count) / time_diff)
                bq_rate = int((bq_count - self.previous_metrics.bq_count) / time_diff)
                es_rate = int((es_count - self.previous_metrics.es_count) / time_diff)
        
        metrics = Metrics(
            pg_count=pg_count,
            bq_count=bq_count,
            es_count=es_count,
            kafka_offset=kafka_offset,
            pg_rate=max(0, pg_rate),
            bq_rate=max(0, bq_rate),
            es_rate=max(0, es_rate),
            redis_engagement=redis_engagement,
            redis_access=redis_access,
            timestamp=current_time
        )
        
        self.previous_metrics = metrics
        return metrics

    def create_header_panel(self) -> Panel:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        header_text = Text(f"🚀 STREAMING PIPELINE MONITOR 🚀\n{current_time}", 
                          style="bold white", justify="center")
        return Panel(header_text, style="bold blue")

    def create_pipeline_flow_panel(self) -> Panel:
        flow_text = Text("Generator → PostgreSQL → Kafka → Flink → [Redis|BigQuery|Elasticsearch]", 
                        style="cyan", justify="center")
        return Panel(flow_text, title="Pipeline Flow", style="yellow")

    def create_metrics_table(self, metrics: Metrics) -> Table:
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Component", style="cyan", width=20)
        table.add_column("Events", justify="right", style="green", width=15)
        table.add_column("Rate/sec", justify="right", style="yellow", width=12)
        table.add_column("Processing Lag", justify="right", style="red", width=15)
        
        # Calculate lags (how many events behind PostgreSQL each sink is)
        bq_lag = metrics.pg_count - metrics.bq_count
        es_lag = metrics.pg_count - metrics.es_count
        
        table.add_row("📝 PostgreSQL", f"{metrics.pg_count:,}", f"{metrics.pg_rate}", "Source")
        table.add_row("🏢 BigQuery", f"{metrics.bq_count:,}", f"{metrics.bq_rate}", 
                     f"{bq_lag:,} behind" if bq_lag > 0 else "✓ Caught up")
        table.add_row("🔍 Elasticsearch", f"{metrics.es_count:,}", f"{metrics.es_rate}", 
                     f"{es_lag:,} behind" if es_lag > 0 else "✓ Caught up")
        table.add_row("🔄 Kafka", f"{metrics.kafka_offset:,}", "-", "Message queue")
        
        return table

    def create_explanation_panel(self) -> Panel:
        explanation = """[bold yellow]📖 METRICS EXPLAINED:[/bold yellow]

[cyan]Events[/cyan]: Total number of events processed by each component
[yellow]Rate/sec[/yellow]: Current processing rate (events per second)
[red]Processing Lag[/red]: How many events each sink is behind PostgreSQL
  • [green]✓ Caught up[/green]: Sink has processed all available events  
  • [red]X behind[/red]: Sink is lagging by X events (backlog)

[bold cyan]Pipeline Flow:[/bold cyan]
1. Generator creates fake events → PostgreSQL
2. Debezium captures changes → Kafka  
3. Flink processes events → [Redis + BigQuery + Elasticsearch]

[bold red]Redis Analytics:[/bold red] Shows sliding window stats (last 10 minutes)
"""
        return Panel(Text.from_markup(explanation), title="📚 Help", style="dim")

    def create_redis_panel(self, metrics: Metrics) -> Panel:
        if not metrics.redis_engagement and not metrics.redis_access:
            return Panel("No Redis data available", title="🔥 Redis Analytics", style="red")
        
        content_lines = []
        
        # Add engagement section (top 3)
        if metrics.redis_engagement:
            content_lines.append("🎯 [bold yellow]Top 3 Engagement (10min window):[/bold yellow]")
            for i, (content_type, score) in enumerate(list(metrics.redis_engagement.items())[:3], 1):
                content_lines.append(f"   {i}. [cyan]{content_type:<12}[/cyan]: [green]{score:6.2f}%[/green]")
            content_lines.append("")
        
        # Add access section (top 5)
        if metrics.redis_access:
            content_lines.append("📊 [bold yellow]Top 5 Access Count:[/bold yellow]")
            for i, (content_type, count) in enumerate(list(metrics.redis_access.items())[:5], 1):
                content_lines.append(f"   {i}. [cyan]{content_type:<12}[/cyan]: [yellow]{count:>8,}[/yellow] events")
        
        content_text = "\n".join(content_lines) if content_lines else "No Redis data available"
        
        return Panel(Text.from_markup(content_text), 
                    title="🔥 Redis Analytics (Last 10 minutes)", 
                    style="red")

    def create_layout(self, metrics: Metrics) -> Layout:
        layout = Layout()
        
        layout.split_column(
            Layout(self.create_header_panel(), size=4),
            Layout(self.create_pipeline_flow_panel(), size=3),
            Layout(self.create_metrics_table(metrics), size=8),
            Layout(self.create_redis_panel(metrics), size=12),
            Layout(self.create_explanation_panel(), size=6),
            Layout(Panel(Text("Press Ctrl+C to exit", justify="center"), style="dim"), size=3)
        )
        
        return layout

    async def run_monitor(self):
        with Live(self.create_layout(Metrics()), refresh_per_second=0.5, screen=True) as live:
            try:
                while True:
                    metrics = await self.collect_metrics()
                    layout = self.create_layout(metrics)
                    live.update(layout)
                    await asyncio.sleep(2)
            except KeyboardInterrupt:
                pass

#=========
# Main Entry Point

async def main():
    monitor = StreamingMonitor()
    await monitor.run_monitor()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nMonitor stopped.")