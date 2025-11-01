#!/usr/bin/env python3
"""
Docker Management Script for Elasticsearch

This script helps you manage the Elasticsearch Docker container
with easy commands.

Usage:
    python docker_setup.py start   # Start Elasticsearch
    python docker_setup.py stop    # Stop Elasticsearch
    python docker_setup.py restart # Restart Elasticsearch
    python docker_setup.py status  # Check status
    python docker_setup.py logs    # View logs
    python docker_setup.py clean   # Remove container completely
"""

import sys
import subprocess
import time


class ElasticsearchDocker:
    """Manages Elasticsearch Docker container"""
    
    def __init__(self):
        self.container_name = 'elasticsearch-search-index'
        self.port = 9200
        self.image = 'docker.elastic.co/elasticsearch/elasticsearch:8.11.0'
    
    def check_docker_installed(self):
        """Check if Docker is installed"""
        try:
            subprocess.run(['docker', '--version'], 
                         capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("❌ Docker is not installed or not in PATH")
            print("\nInstall Docker from: https://docs.docker.com/get-docker/")
            return False
    
    def check_docker_running(self):
        """Check if Docker daemon is running"""
        try:
            subprocess.run(['docker', 'info'], 
                         capture_output=True, 
                         stderr=subprocess.DEVNULL,
                         check=True)
            return True
        except subprocess.CalledProcessError:
            print("❌ Docker daemon is not running")
            print("\nStart Docker Desktop or run: sudo systemctl start docker")
            return False
    
    def is_container_running(self):
        """Check if our Elasticsearch container is running"""
        try:
            result = subprocess.run(
                ['docker', 'ps', '--filter', f'name={self.container_name}', 
                 '--format', '{{.Names}}'],
                capture_output=True, text=True, check=True
            )
            return self.container_name in result.stdout
        except subprocess.CalledProcessError:
            return False
    
    def is_container_exists(self):
        """Check if container exists (running or stopped)"""
        try:
            result = subprocess.run(
                ['docker', 'ps', '-a', '--filter', f'name={self.container_name}',
                 '--format', '{{.Names}}'],
                capture_output=True, text=True, check=True
            )
            return self.container_name in result.stdout
        except subprocess.CalledProcessError:
            return False
    
    def start(self):
        """Start Elasticsearch container"""
        print("="*60)
        print("Starting Elasticsearch in Docker")
        print("="*60)
        
        if not self.check_docker_installed():
            print('1 hi')
            return False
        
        if not self.check_docker_running():
            print('2 hi')
            return False
        
        if self.is_container_running():
            print('3 hi')
            print(f"✓ Container '{self.container_name}' is already running")
            self._show_connection_info()
            return True
        
        if self.is_container_exists():
            print(f"📦 Starting existing container '{self.container_name}'...")
            try:
                subprocess.run(['docker', 'start', self.container_name], check=True)
            except subprocess.CalledProcessError as e:
                print(f"❌ Failed to start container: {e}")
                return False
        else:
            print("📦 Creating new Elasticsearch container...")
            print(f"  Image: {self.image}")
            print(f"  Port: {self.port}")
            print(f"  Memory: 512MB")
            print(f"  Container: {self.container_name}")
            print()
            print("This may take a few minutes on first run (downloading image)...")
            
            cmd = [
                'docker', 'run', '-d',
                '--name', self.container_name,
                '-p', f'{self.port}:9200',
                '-e', 'discovery.type=single-node',
                '-e', 'xpack.security.enabled=false',
                '-e', 'ES_JAVA_OPTS=-Xms512m -Xmx512m',
                self.image
            ]
            
            try:
                subprocess.run(cmd, check=True)
                print("✓ Container created")
            except subprocess.CalledProcessError as e:
                print(f"❌ Failed to create container: {e}")
                return False
        
        # Wait for Elasticsearch to be ready
        print("\n⏳ Waiting for Elasticsearch to start", end='', flush=True)
        max_retries = 30
        
        for i in range(max_retries):
            time.sleep(2)
            try:
                result = subprocess.run(
                    ['curl', '-s', f'http://localhost:{self.port}'],
                    capture_output=True,
                    timeout=2
                )
                if result.returncode == 0:
                    print(" ✓")
                    print("\n" + "="*60)
                    print("✓ Elasticsearch is ready!")
                    print("="*60)
                    self._show_connection_info()
                    return True
            except:
                pass
            
            print(".", end='', flush=True)
        
        print("\n❌ Elasticsearch failed to start within timeout")
        print("Check logs with: python docker_setup.py logs")
        return False
    
    def stop(self):
        """Stop the container"""
        print("="*60)
        print("Stopping Elasticsearch")
        print("="*60)
        
        if not self.is_container_running():
            print(f"⚠️  Container '{self.container_name}' is not running")
            return
        
        print(f"⏹  Stopping container '{self.container_name}'...")
        subprocess.run(['docker', 'stop', self.container_name], check=True)
        print("✓ Container stopped")
    
    def restart(self):
        """Restart the container"""
        print("="*60)
        print("Restarting Elasticsearch")
        print("="*60)
        
        self.stop()
        print()
        self.start()
    
    def status(self):
        """Show container status"""
        print("="*60)
        print("Elasticsearch Docker Status")
        print("="*60)
        
        if not self.check_docker_installed():
            return
        
        if not self.check_docker_running():
            return
        
        if not self.is_container_exists():
            print(f"⚪ Container '{self.container_name}' does not exist")
            print("\nCreate it with: python docker_setup.py start")
            return
        
        if self.is_container_running():
            print(f"✓ Container '{self.container_name}' is RUNNING")
            
            # Get container details
            result = subprocess.run(
                ['docker', 'ps', '--filter', f'name={self.container_name}',
                 '--format', 'table {{.Status}}\t{{.Ports}}'],
                capture_output=True, text=True
            )
            print("\nDetails:")
            print(result.stdout)
            
            self._show_connection_info()
        else:
            print(f"⚪ Container '{self.container_name}' exists but is STOPPED")
            print("\nStart it with: python docker_setup.py start")
    
    def logs(self, lines=50):
        """Show container logs"""
        print("="*60)
        print(f"Elasticsearch Logs (last {lines} lines)")
        print("="*60)
        
        if not self.is_container_exists():
            print(f"❌ Container '{self.container_name}' does not exist")
            return
        
        subprocess.run(['docker', 'logs', '--tail', str(lines), self.container_name])
    
    def clean(self):
        """Remove container completely"""
        print("="*60)
        print("Cleaning Up Elasticsearch Container")
        print("="*60)
        
        if self.is_container_running():
            print("⏹  Stopping container...")
            self.stop()
            print()
        
        if self.is_container_exists():
            print(f"🗑️  Removing container '{self.container_name}'...")
            subprocess.run(['docker', 'rm', self.container_name], check=True)
            print("✓ Container removed")
            print("\n💡 Tip: To remove the Docker image as well, run:")
            print(f"   docker rmi {self.image}")
        else:
            print(f"⚪ Container '{self.container_name}' does not exist")
    
    def _show_connection_info(self):
        """Show connection information"""
        print("\n📡 Connection Information:")
        print(f"  URL: http://localhost:{self.port}")
        print(f"  Health: http://localhost:{self.port}/_cluster/health")
        print(f"\n💡 Test connection:")
        print(f"  curl http://localhost:{self.port}")
        print(f"\n💡 In Python:")
        print(f"  from elasticsearch import Elasticsearch")
        print(f"  es = Elasticsearch(['http://localhost:{self.port}'])")


def main():
    """Main entry point"""
    
    docker = ElasticsearchDocker()
    
    if len(sys.argv) < 2:
        print("Elasticsearch Docker Management")
        print("\nUsage:")
        print("  python docker_setup.py start   # Start Elasticsearch")
        print("  python docker_setup.py stop    # Stop Elasticsearch")
        print("  python docker_setup.py restart # Restart Elasticsearch")
        print("  python docker_setup.py status  # Check status")
        print("  python docker_setup.py logs    # View logs")
        print("  python docker_setup.py clean   # Remove container")
        return
    
    command = sys.argv[1].lower()
    
    commands = {
        'start': docker.start,
        'stop': docker.stop,
        'restart': docker.restart,
        'status': docker.status,
        'logs': docker.logs,
        'clean': docker.clean
    }
    
    if command in commands:
        commands[command]()
    else:
        print(f"❌ Unknown command: {command}")
        print("\nAvailable commands: start, stop, restart, status, logs, clean")


if __name__ == "__main__":
    main()