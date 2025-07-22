#!/usr/bin/env python3

import asyncio
import time
import subprocess
import requests
from datetime import datetime
from dataclasses import dataclass

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn
from rich.text import Text
from rich.layout import Layout

#=========
# Test Stages (2 minutes each)

STRESS_STAGES = [
    {"name": "Baseline", "interval": 1.0, "batch_size": 100},      
    {"name": "Low Load", "interval": 0.5, "batch_size": 500},       
    {"name": "Medium Load", "interval": 0.2, "batch_size": 500}, 
    {"name": "High Load", "interval": 0.1, "batch_size": 500},   
    {"name": "Extreme Load", "interval": 0.05, "batch_size": 500}, # 
]

STAGE_DURATION = 15 

#=========
# Stress Test Engine

class SimpleStressTester:
    def __init__(self):
        self.console = Console()
        self.current_stage = 0
        self.current_metrics = {"pg": 0, "bq": 0, "es": 0, "pg_rate": 0, "bq_rate": 0, "es_rate": 0}
        
    async def update_env_and_restart(self, interval: float, batch_size: int):
        """Update .env file and restart generator"""
        try:
            # Update .env file
            with open('.env', 'r') as f:
                lines = f.readlines()
            
            updated_lines = []
            for line in lines:
                if line.startswith('EVENT_INTERVAL_SECONDS='):
                    updated_lines.append(f'EVENT_INTERVAL_SECONDS={interval}\n')
                elif line.startswith('BATCH_SIZE='):
                    updated_lines.append(f'BATCH_SIZE={batch_size}\n')
                else:
                    updated_lines.append(line)
            
            with open('.env', 'w') as f:
                f.writelines(updated_lines)
            
            # Force rebuild and restart generator
            self.console.print(f"[yellow]Updating to {interval}s interval, {batch_size} batch size...[/yellow]")
            
            process = await asyncio.create_subprocess_exec(
                'docker-compose', 'stop', 'generator',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await process.wait()
            
            process = await asyncio.create_subprocess_exec(
                'docker-compose', 'up', '-d', '--build', 'generator',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await process.wait()
            
            # Give generator more time to start
            await asyncio.sleep(10)
            
        except Exception as e:
            self.console.print(f"[red]Error updating config: {e}[/red]")

    async def get_metrics(self):
        """Get current metrics from all sinks"""
        # PostgreSQL
        pg_count = 0
        try:
            process = await asyncio.create_subprocess_exec(
                'docker-compose', 'exec', '-T', 'postgresql', 'psql',
                '-U', 'streaming_user', '-d', 'streaming_db', '-c',
                'SELECT COUNT(*) FROM engagement_events;',
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await process.communicate()
            for line in stdout.decode().split('\n'):
                if line.strip().isdigit():
                    pg_count = int(line.strip())
                    break
        except Exception:
            pass

        # BigQuery
        bq_count = 0
        try:
            response = requests.post(
                "http://localhost:9050/projects/streaming_project/queries",
                json={"query": "SELECT COUNT(*) as total_events FROM engagement_data.events"},
                timeout=5
            )
            if response.status_code == 200:
                data = response.json()
                if 'rows' in data and data['rows']:
                    bq_count = int(data['rows'][0]['f'][0]['v'])
        except Exception:
            pass

        # Elasticsearch
        es_count = 0
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            response = requests.get(f"http://localhost:9200/engagement-events-{today}/_count", timeout=5)
            if response.status_code == 200:
                es_count = response.json().get('count', 0)
        except Exception:
            pass

        return pg_count, bq_count, es_count

    def create_header_panel(self, elapsed_total: int) -> Panel:
        header_text = Text(f"🔥 SIMPLE STRESS TEST 🔥\nTotal Elapsed: {elapsed_total//60:02d}:{elapsed_total%60:02d}", 
                          style="bold white", justify="center")
        return Panel(header_text, style="bold red")

    def create_stage_panel(self, stage_info: dict, stage_elapsed: int) -> Panel:
        stage_text = f"""[bold yellow]Stage {self.current_stage + 1}/5: {stage_info['name']}[/bold yellow]
Interval: [green]{stage_info['interval']}s[/green]
Batch Size: [green]{stage_info['batch_size']}[/green]
Stage Time: [cyan]{stage_elapsed//60:02d}:{stage_elapsed%60:02d} / 02:00[/cyan]

[bold cyan]Current Metrics:[/bold cyan]
PostgreSQL: [green]{self.current_metrics['pg']:,}[/green] ([yellow]{self.current_metrics['pg_rate']}/s[/yellow])
BigQuery: [green]{self.current_metrics['bq']:,}[/green] ([yellow]{self.current_metrics['bq_rate']}/s[/yellow])
Elasticsearch: [green]{self.current_metrics['es']:,}[/green] ([yellow]{self.current_metrics['es_rate']}/s[/yellow])

[bold red]Lag:[/bold red]
BQ: [red]{self.current_metrics['pg'] - self.current_metrics['bq']:,}[/red] | ES: [red]{self.current_metrics['pg'] - self.current_metrics['es']:,}[/red]
"""
        return Panel(Text.from_markup(stage_text), title=f"Stage {self.current_stage + 1}", style="green")

    def create_stages_panel(self) -> Panel:
        stages_text = ""
        for i, stage in enumerate(STRESS_STAGES):
            if i < self.current_stage:
                status = "[green]✓ Complete[/green]"
            elif i == self.current_stage:
                status = "[yellow]🔄 Running[/yellow]"
            else:
                status = "[dim]⏳ Pending[/dim]"
            
            stages_text += f"{i+1}. [cyan]{stage['name']}[/cyan] ({stage['interval']}s/{stage['batch_size']}) - {status}\n"
        
        return Panel(Text.from_markup(stages_text), title="Test Stages", style="blue")

    def create_layout(self, progress: Progress, elapsed_total: int, stage_elapsed: int) -> Layout:
        layout = Layout()
        
        current_stage_info = STRESS_STAGES[self.current_stage]
        
        layout.split_column(
            Layout(self.create_header_panel(elapsed_total), size=4),
            Layout(
                Layout.split_row(
                    Layout(self.create_stages_panel()),
                    Layout(self.create_stage_panel(current_stage_info, stage_elapsed))
                ), size=12
            ),
            Layout(Panel(progress, title="Stage Progress", style="cyan"), size=4)
        )
        
        return layout

    async def run_test(self):
        """Run the complete stress test"""
        test_start_time = time.time()
        
        self.console.clear()
        self.console.print(Panel(Text("🔥 STARTING SIMPLE STRESS TEST 🔥", style="bold white", justify="center"), style="bold red"))
        
        for stage_idx, stage_info in enumerate(STRESS_STAGES):
            self.current_stage = stage_idx
            
            self.console.print(f"\n[bold yellow]Starting Stage {stage_idx + 1}/5: {stage_info['name']}[/bold yellow]")
            self.console.print(f"Settings: {stage_info['interval']}s interval, {stage_info['batch_size']} batch size")
            
            # Update generator settings
            await self.update_env_and_restart(stage_info['interval'], stage_info['batch_size'])
            
            stage_start = time.time()
            prev_pg = prev_bq = prev_es = 0
            
            # Run stage for 2 minutes
            while time.time() - stage_start < STAGE_DURATION:
                current_time = time.time()
                stage_elapsed = int(current_time - stage_start)
                remaining = STAGE_DURATION - stage_elapsed
                
                # Get metrics
                pg_count, bq_count, es_count = await self.get_metrics()
                
                # Calculate rates
                time_diff = max(1, stage_elapsed)
                if stage_elapsed > 10:  # Only calculate rates after 10 seconds
                    pg_rate = max(0, int((pg_count - prev_pg) / time_diff)) if prev_pg > 0 else 0
                    bq_rate = max(0, int((bq_count - prev_bq) / time_diff)) if prev_bq > 0 else 0
                    es_rate = max(0, int((es_count - prev_es) / time_diff)) if prev_es > 0 else 0
                else:
                    pg_rate = bq_rate = es_rate = 0
                
                # Store previous counts for rate calculation
                if prev_pg == 0:
                    prev_pg, prev_bq, prev_es = pg_count, bq_count, es_count
                
                # Progress bar
                progress_pct = (stage_elapsed / STAGE_DURATION) * 100
                bar_length = 30
                filled_length = int(bar_length * stage_elapsed / STAGE_DURATION)
                bar = '█' * filled_length + '░' * (bar_length - filled_length)
                
                # Clear and update display
                self.console.clear()
                self.console.print(Panel(Text(f"🔥 STRESS TEST - Stage {stage_idx + 1}/5 🔥", style="bold white", justify="center"), style="bold red"))
                
                self.console.print(f"\n[bold cyan]Current Stage: {stage_info['name']}[/bold cyan]")
                self.console.print(f"Time: {stage_elapsed//60:02d}:{stage_elapsed%60:02d} / 01:00 (remaining: {remaining//60:02d}:{remaining%60:02d})")
                self.console.print(f"Progress: [{bar}] {progress_pct:.1f}%")
                
                self.console.print(f"\n[bold green]Metrics:[/bold green]")
                self.console.print(f"PostgreSQL:   {pg_count:>8,} events ({pg_rate:>4}/s)")
                self.console.print(f"BigQuery:     {bq_count:>8,} events ({bq_rate:>4}/s) - {pg_count - bq_count:,} behind")
                self.console.print(f"Elasticsearch: {es_count:>8,} events ({es_rate:>4}/s) - {pg_count - es_count:,} behind")
                
                # Show all stages
                self.console.print(f"\n[bold blue]Test Progress:[/bold blue]")
                for i, stage in enumerate(STRESS_STAGES):
                    if i < stage_idx:
                        status = "[green]✓ Complete[/green]"
                    elif i == stage_idx:
                        status = f"[yellow]🔄 Running ({progress_pct:.0f}%)[/yellow]"
                    else:
                        status = "[dim]⏳ Pending[/dim]"
                    
                    self.console.print(f"{i+1}. {stage['name']} ({stage['interval']}s/{stage['batch_size']}) - {status}")
                
                await asyncio.sleep(5)  # Update every 5 seconds
            
            self.console.print(f"\n[green]✓ Stage {stage_idx + 1} completed![/green]")
        
        # Final summary
        self.console.clear()
        self.console.print(Panel(Text("🎉 STRESS TEST COMPLETED! 🎉", style="bold green", justify="center"), style="bold green"))
        
        final_pg, final_bq, final_es = await self.get_metrics()
        self.console.print(f"\n[bold cyan]Final Results:[/bold cyan]")
        self.console.print(f"PostgreSQL:    {final_pg:>8,} events")
        self.console.print(f"BigQuery:      {final_bq:>8,} events")
        self.console.print(f"Elasticsearch: {final_es:>8,} events")
        self.console.print(f"\n[yellow]Total Test Duration: {len(STRESS_STAGES)} minutes[/yellow]")

#=========
# Main Entry Point

async def main():
    tester = SimpleStressTester()
    await tester.run_test()
    print("\n✅ Simple stress test completed!")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️  Stress test interrupted.")