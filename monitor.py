#!/usr/bin/env python3

import asyncio
import time
import json
import subprocess
import requests
from datetime import datetime
from typing import Dict, Tuple, Optional
from dataclasses import dataclass
import redis.asyncio as redis
import aiohttp

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.columns import Columns
from rich.text import Text
from rich.layout import Layout
from rich.progress import Progress, BarColumn, TextColumn

#=========
# Configuration

UPDATE_INTERVAL = 1  
REFRESH_RATE = 1       
TIMEOUT = 2          

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
    errors: Dict[str, str] = None

#=========
# Optimized Data Fetchers

class StreamingMonitor:
    def __init__(self):
        self.console = Console()
        self.previous_metrics = Metrics()
        self.metrics_history = []
        self.redis_client = None
        self.session = None
        self.last_bq_check = 0
        
    async def setup_connections(self):
        """Setup persistent connections for better performance"""
        try:
            self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
            await self.redis_client.ping()
        except Exception as e:
            print(f"Redis connection failed: {e}")
            self.redis_client = None
            
        # HTTP session for BigQuery/Elasticsearch
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=TIMEOUT))

    async def cleanup_connections(self):
        """Clean up connections"""
        if self.redis_client:
            await self.redis_client.close()
        if self.session:
            await self.session.close()

    async def get_postgres_count(self) -> Tuple[int, Optional[str]]:
        try:
            cmd = ['docker-compose', 'exec', '-T', 'postgresql', 'psql', 
                   '-U', 'streaming_user', '-d', 'streaming_db', '-t', '-c', 
                   'SELECT COUNT(*) FROM engagement_events;']
            
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=TIMEOUT)
            
            for line in stdout.decode().split('\n'):
                line = line.strip()
                if line.isdigit():
                    return int(line), None
            return 0, "No valid count found"
        except asyncio.TimeoutError:
            return 0, "Timeout"
        except Exception as e:
            return 0, str(e)

    async def get_bigquery_count(self) -> Tuple[int, Optional[str]]:
        try:
            current_time = time.time()
            if current_time - self.last_bq_check < 5.0:  # Only check every 5 seconds
                return self.previous_metrics.bq_count if self.previous_metrics else 0, None
            self.last_bq_check = current_time
            if not self.session:
                return 0, "No session"
                
            url = "http://localhost:9050/projects/streaming_project/queries"
            payload = {"query": "SELECT COUNT(*) as total_events FROM engagement_data.events"}
            
            async with self.session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'rows' in data and data['rows']:
                        return int(data['rows'][0]['f'][0]['v']), None
                return 0, f"Status: {response.status}"
        except Exception as e:
            return 0, str(e)

    async def get_elasticsearch_count(self) -> Tuple[int, Optional[str]]:
        try:
            if not self.session:
                return 0, "No session"
                
            today = datetime.now().strftime('%Y-%m-%d')
            url = f"http://localhost:9200/engagement-events-{today}/_count"
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('count', 0), None
                return 0, f"Status: {response.status}"
        except Exception as e:
            return 0, str(e)

    async def get_kafka_offset(self) -> Tuple[int, Optional[str]]:
        try:
            cmd = ['docker-compose', 'exec', '-T', 'kafka', 'kafka-consumer-groups', 
                '--bootstrap-server', 'localhost:9092', '--group', 'flink-datastream-processor', '--describe']
            
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=TIMEOUT)
            print(f"DEBUG KAFKA OUTPUT:\n{stdout.decode()}")
            for line in stdout.decode().split('\n'):
                if 'streaming.public.engagement_events' in line and 'flink-datastream-processor' in line:
                    parts = line.split()
                    if len(parts) > 3 and parts[3].isdigit():
                        return int(parts[3]), None
            return 0, "No offset found"
        except asyncio.TimeoutError:
            return 0, "Timeout"
        except Exception as e:
            return 0, str(e)

    async def get_redis_stats(self) -> Tuple[Dict[str, float], Dict[str, int], Optional[str]]:
        try:
            if not self.redis_client:
                return {}, {}, "No Redis connection"
            
            # Use individual commands instead of pipeline for better compatibility
            eng_data = await self.redis_client.zrevrange('stats:top_by_engagement', 0, 4, withscores=True)
            acc_data = await self.redis_client.zrevrange('stats:top_by_access', 0, 6, withscores=True)
            # Parse engagement (top 3)
            engagement = {}
            if eng_data:
                # eng_data is a list of tuples: [('content_type', score), ...]
                for item in eng_data[:3]:  # Top 3 only
                    if isinstance(item, tuple) and len(item) == 2:
                        content_type, score = item
                        try:
                            engagement[content_type] = float(score)
                        except (ValueError, TypeError):
                            continue
            
            # Parse access (top 5) 
            access = {}
            if acc_data:
                # acc_data is a list of tuples: [('content_type', count), ...]
                for item in acc_data[:5]:  # Top 5 only
                    if isinstance(item, tuple) and len(item) == 2:
                        content_type, count = item
                        try:
                            access[content_type] = int(float(count))
                        except (ValueError, TypeError):
                            continue
            
            return engagement, access, None
        except Exception as e:
            return {}, {}, str(e)

    async def collect_metrics(self) -> Metrics:
        """Collect all metrics concurrently with error handling"""
        tasks = [
            self.get_postgres_count(),
            self.get_bigquery_count(), 
            self.get_elasticsearch_count(),
            self.get_kafka_offset(),
            self.get_redis_stats()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Unpack results with error handling
        pg_count, pg_error = results[0] if not isinstance(results[0], Exception) else (0, str(results[0]))
        bq_count, bq_error = results[1] if not isinstance(results[1], Exception) else (0, str(results[1]))
        es_count, es_error = results[2] if not isinstance(results[2], Exception) else (0, str(results[2]))
        kafka_offset, kafka_error = results[3] if not isinstance(results[3], Exception) else (0, str(results[3]))
        
        if isinstance(results[4], Exception):
            redis_engagement, redis_access, redis_error = {}, {}, str(results[4])
        else:
            redis_engagement, redis_access, redis_error = results[4]
        
        current_time = time.time()
        
        # Calculate rates with better smoothing
        pg_rate = bq_rate = es_rate = 0
        if self.previous_metrics.timestamp > 0:
            time_diff = current_time - self.previous_metrics.timestamp
            if time_diff > 0:
                pg_rate = max(0, int((pg_count - self.previous_metrics.pg_count) / time_diff))
                bq_rate = max(0, int((bq_count - self.previous_metrics.bq_count) / time_diff))
                es_rate = max(0, int((es_count - self.previous_metrics.es_count) / time_diff))
        
        # Collect errors
        errors = {}
        if pg_error: errors['PostgreSQL'] = pg_error
        if bq_error: errors['BigQuery'] = bq_error  
        if es_error: errors['Elasticsearch'] = es_error
        if kafka_error: errors['Kafka'] = kafka_error
        if redis_error: errors['Redis'] = redis_error
        
        metrics = Metrics(
            pg_count=pg_count,
            bq_count=bq_count,
            es_count=es_count,
            kafka_offset=kafka_offset,
            pg_rate=pg_rate,
            bq_rate=bq_rate,
            es_rate=es_rate,
            redis_engagement=redis_engagement,
            redis_access=redis_access,
            timestamp=current_time,
            errors=errors if errors else None
        )
        
        self.previous_metrics = metrics
        return metrics

    def create_header_panel(self) -> Panel:
        current_time = datetime.now().strftime('%H:%M:%S')
        header_text = Text(f"STREAMING PIPELINE MONITOR - FARHAN\n{current_time} | Refreshing every {UPDATE_INTERVAL}s", 
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
        
        bq_lag = metrics.pg_count - metrics.bq_count
        es_lag = metrics.pg_count - metrics.es_count
        
        # Add status indicators
        pg_status = "📝 PostgreSQL"
        bq_status = "🏢 BigQuery" 
        es_status = "🔍 Elasticsearch"
        kafka_status = "🔄 Kafka"
        
        if metrics.errors:
            if 'PostgreSQL' in metrics.errors: pg_status += " ⚠️"
            if 'BigQuery' in metrics.errors: bq_status += " ⚠️"  
            if 'Elasticsearch' in metrics.errors: es_status += " ⚠️"
            if 'Kafka' in metrics.errors: kafka_status += " ⚠️"
        
        table.add_row(pg_status, f"{metrics.pg_count:,}", f"{metrics.pg_rate}", "Source")
        table.add_row(bq_status, f"{metrics.bq_count:,}", f"{metrics.bq_rate}", 
                     f"{bq_lag:,} behind" if bq_lag > 0 else "✓ Caught up")
        table.add_row(es_status, f"{metrics.es_count:,}", f"{metrics.es_rate}", 
                     f"{es_lag:,} behind" if es_lag > 0 else "✓ Caught up")
        table.add_row(kafka_status, f"{metrics.kafka_offset}", "-", "Message queue")
        
        return table

    def create_redis_panel(self, metrics: Metrics) -> Panel:
        debug_text = f"DEBUG: eng={len(metrics.redis_engagement) if metrics.redis_engagement else 0}, acc={len(metrics.redis_access) if metrics.redis_access else 0}"
        if not metrics.redis_engagement and not metrics.redis_access:
            error_msg = "No Redis data available"
            if metrics.errors and 'Redis' in metrics.errors:
                error_msg += f" ({metrics.errors['Redis']})"
            return Panel(error_msg, title="🔥 Redis Analytics", style="red")
        
        content_lines = []
        
        if metrics.redis_engagement:
            content_lines.append("🎯 [bold yellow]Top 3 Engagement (10min window):[/bold yellow]")
            for i, (content_type, score) in enumerate(list(metrics.redis_engagement.items())[:3], 1):
                content_lines.append(f"   {i}. [cyan]{content_type:<12}[/cyan]: [green]{score:6.2f}%[/green]")
            content_lines.append("")
        
        if metrics.redis_access:
            content_lines.append("📊 [bold yellow]Top 5 Access Count:[/bold yellow]")
            for i, (content_type, count) in enumerate(list(metrics.redis_access.items())[:5], 1):
                content_lines.append(f"   {i}. [cyan]{content_type:<12}[/cyan]: [yellow]{count:>8,}[/yellow] events")
        
        content_text = "\n".join(content_lines) if content_lines else "No Redis data available"
        
        return Panel(Text.from_markup(content_text), 
                    title="Redis Analytics (Last 10 minutes)", 
                    style="red")

    def create_errors_panel(self, metrics: Metrics) -> Optional[Panel]:
        if not metrics.errors:
            return None
            
        error_lines = []
        for component, error in metrics.errors.items():
            error_lines.append(f"[red]{component}[/red]: {error}")
        
        return Panel(Text.from_markup("\n".join(error_lines)), 
                    title="Errors", style="red")

    def create_layout(self, metrics: Metrics) -> Layout:
        layout = Layout()
        
        # Base layout
        panels = [
            Layout(self.create_header_panel(), size=4),
            Layout(self.create_pipeline_flow_panel(), size=3),
            Layout(self.create_metrics_table(metrics), size=8),
            Layout(self.create_redis_panel(metrics), size=15),
        ]
        
        # Add errors panel if there are errors
        errors_panel = self.create_errors_panel(metrics)
        if errors_panel:
            panels.append(Layout(errors_panel, size=4))
        
        panels.append(Layout(Panel(Text("Press Ctrl+C to exit Monitor", justify="center"), style="dim"), size=3))
        
        layout.split_column(*panels)
        return layout

    async def run_monitor(self):
        await self.setup_connections()
        
        with Live(self.create_layout(Metrics()), refresh_per_second=REFRESH_RATE, screen=True) as live:
            try:
                while True:
                    start_time = time.time()
                    metrics = await self.collect_metrics()
                    layout = self.create_layout(metrics)
                    live.update(layout)
                    
                    # Ensure consistent update interval
                    elapsed = time.time() - start_time
                    sleep_time = max(0, UPDATE_INTERVAL - elapsed)
                    await asyncio.sleep(sleep_time)
                    
            except KeyboardInterrupt:
                pass
            finally:
                await self.cleanup_connections()

#=========
# Main Entry Point

async def main():
    monitor = StreamingMonitor()
    await monitor.run_monitor()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n monitor stopped.")