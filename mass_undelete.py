import os
import time
import asyncio
from datetime import datetime
from typing import Dict, List
import threading
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureNamedKeyCredential
from tqdm import tqdm
from collections import deque
from statistics import mean

class RestoreManager:
    def __init__(self, storage_uri: str, container: str, access_key: str = None):
        self.storage_uri = storage_uri
        self.container = container
        self.access_key = access_key
        if access_key:
            account_name = self.storage_uri.split('//')[1].split('.')[0]
            self.credential = AzureNamedKeyCredential(account_name, access_key)
        else:
            self.credential = DefaultAzureCredential()
        self.restored_count = 0
        self.failed_count = 0
        self.lock = threading.Lock()
        self.initial_concurrency = 100 
        self.sem = asyncio.Semaphore(self.initial_concurrency)
        self.error_window = deque(maxlen=100)
        self.success_window = deque(maxlen=100)  

        self.last_adjustment = time.time()
        self.current_concurrency = self.initial_concurrency
        self.min_concurrency = 10
        self.max_concurrency = 600 

        # Error tracking with timestamps
        self.error_window = deque(maxlen=100)  
        self.window_duration = 10 

        # Backoff and recovery settings
        self.backoff_factor = 0.5  # How much to reduce concurrency on errors
        self.recovery_factor = 1.2  # How much to increase concurrency during recovery
        self.recovery_interval = 5  # Seconds to wait before attempting recovery
        self.last_error_time = 0
        
    def format_elapsed_time(self, seconds: float) -> str:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

    def is_5xx_error(self, error_str: str) -> bool:
        error_patterns = [
            "500 Server Error",
            "502 Bad Gateway",
            "503 Service Unavailable",
            "504 Gateway Timeout",
            "ServerBusy",  
            "ThrottlingError"  
        ]
        return any(pattern in error_str for pattern in error_patterns)

    def get_error_rate(self) -> float:
        current_time = time.time()
        # Clean old errors outside the window
        while self.error_window and (current_time - self.error_window[0][0]) > self.window_duration:
            self.error_window.popleft()
        
        if not self.error_window:
            return 0.0
        
        # Count recent 5xx errors
        recent_errors = sum(1 for t, is_5xx in self.error_window if is_5xx and 
                          (current_time - t) <= self.window_duration)
        return recent_errors / len(self.error_window)

    async def adjust_concurrency(self):
        while True:
            await asyncio.sleep(1)
            current_time = time.time()
            
            # Skip if we've adjusted recently
            if current_time - self.last_adjustment < 1:
                continue
                
            error_rate = self.get_error_rate()
            time_since_last_error = current_time - self.last_error_time
            
            if error_rate > 0.1:  # More than 10% 5xx errors
                new_concurrency = max(
                    int(self.current_concurrency * self.backoff_factor),
                    self.min_concurrency
                )
                if new_concurrency != self.current_concurrency:
                    print(f"\nReducing concurrency to {new_concurrency} due to 5xx errors (error rate: {error_rate:.2%})")
                    self.current_concurrency = new_concurrency
                    self.sem = asyncio.Semaphore(self.current_concurrency)
                    
            elif time_since_last_error > self.recovery_interval and self.current_concurrency < self.max_concurrency:
                # No recent errors, try to recover
                new_concurrency = min(
                    int(self.current_concurrency * self.recovery_factor),
                    self.max_concurrency
                )
                if new_concurrency != self.current_concurrency:
                    print(f"\nIncreasing concurrency to {new_concurrency} (no errors for {time_since_last_error:.1f}s)")
                    self.current_concurrency = new_concurrency
                    self.sem = asyncio.Semaphore(self.current_concurrency)
            
            self.last_adjustment = current_time

    async def restore_item(self, item: Dict, service_client: DataLakeServiceClient) -> bool:
        async with self.sem:
            try:
                file_system_client = service_client.get_file_system_client(self.container)
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None, 
                    lambda: file_system_client._undelete_path(item['path'], item['deletion_id'])
                )
                with self.lock:
                    self.restored_count += 1
                return True
            except Exception as e:
                error_str = str(e)
                if "BlobAlreadyExists" not in error_str:
                    with self.lock:
                        self.failed_count += 1
                        current_time = time.time()
                        is_5xx = self.is_5xx_error(error_str)
                        self.error_window.append((current_time, is_5xx))
                        if is_5xx:
                            self.last_error_time = current_time
                    print(f"\nFailed to restore: {item['path']}")
                    print(f"Error: {error_str}")
                return False

    async def process_batch(self, items: List[Dict], service_client: DataLakeServiceClient, pbar: tqdm) -> None:
        tasks = []
        for item in items:
            task = asyncio.create_task(self.restore_item(item, service_client))
            tasks.append(task)
        
        for task in asyncio.as_completed(tasks):
            await task
            pbar.update(1)

            pbar.refresh()
            current_time = time.time()
            if pbar.start_t != current_time:
                current_speed = pbar.n / (current_time - pbar.start_t)
                remaining_items = pbar.total - pbar.n
                remaining_time = remaining_items / current_speed if current_speed > 0 else 0
                error_rate = self.get_error_rate()
                pbar.set_postfix({
                    'Speed': f'{current_speed:.2f} it/s',
                    'ETA': time.strftime('%H:%M:%S', time.gmtime(remaining_time)),
                    'Error Rate': f'{error_rate:.1%}'
                }, refresh=True)

    async def run_async(self):
        auth_method = "Access Key" if self.access_key else "DefaultAzureCredential"
        print(f"Authenticating to storage account using {auth_method}...")
        service_client = DataLakeServiceClient(
            account_url=self.storage_uri,
            credential=self.credential
        )

        print("\nGetting list of deleted items...")
        file_system_client = service_client.get_file_system_client(self.container)
        deleted_items = []
        
        for item in file_system_client.list_deleted_paths():
            deleted_items.append({
                'path': item.name,
                'deletion_id': item.deletion_id,
                'depth': len(item.name.split('/'))
            })

        if not deleted_items:
            print("No deleted items found.")
            return

        deleted_items.sort(key=lambda x: x['depth'])
        
        total_items = len(deleted_items)
        # Dynamic batch sizing - use smaller batches for small remaining items
        batch_size = min(1000, max(10, total_items // 3))
        batches = [deleted_items[i:i + batch_size] 
                  for i in range(0, len(deleted_items), batch_size)]
        
        if not batches and deleted_items:  # Ensure single small batch is processed
            batches = [deleted_items]
        
        print(f"Processing {total_items} items in {len(batches)} batches...")
        script_start = time.time()

        # Start concurrency adjustment task
        adjustment_task = asyncio.create_task(self.adjust_concurrency())
        
        with tqdm(total=total_items, desc="Restoring items", unit="items") as pbar:
            for batch_num, batch in enumerate(batches, 1):
                await self.process_batch(batch, service_client, pbar)
                
                elapsed = time.time() - script_start
                ops_per_second = self.restored_count / elapsed if elapsed > 0 else 0
                error_rate = self.get_error_rate()
                print(f"\rBatch {batch_num}/{len(batches)} - "
                      f"Restored: {self.restored_count} - "
                      f"Failed: {self.failed_count} - "
                      f"Ops/sec: {ops_per_second:.2f} - "
                      f"Concurrency: {self.current_concurrency} - "
                      f"Error Rate: {error_rate:.1%} - "
                      f"Elapsed: {self.format_elapsed_time(elapsed)}", 
                      end="")

        adjustment_task.cancel()
        total_time = time.time() - script_start
        print("\nScript Complete!")
        print(f"Total time: {self.format_elapsed_time(total_time)}")
        print(f"Successfully restored: {self.restored_count} items")
        print(f"Failed to restore: {self.failed_count} items")
        print(f"Average throughput: {self.restored_count/total_time:.2f} items/second")


    def run(self):
        asyncio.run(self.run_async())

if __name__ == "__main__":
    import argparse
    import re
    
    def validate_storage_uri(uri):
        """Validate Azure Storage Account URI format"""
        pattern = r"^https:\/\/[a-z0-9]{3,24}\.(blob|dfs)\.core\.windows\.net$"
        if not re.match(pattern, uri):
            raise argparse.ArgumentTypeError(
                "Invalid storage account URI format. Expected format: "
                "https://<account-name>.(blob|dfs).core.windows.net"
            )
        return uri

    def validate_container_name(name):
        """Validate Azure Storage container name format"""
        if not (3 <= len(name) <= 63):
            raise argparse.ArgumentTypeError(
                "Container name must be between 3 and 63 characters"
            )
        if not re.match(r"^[a-z0-9-]+$", name):
            raise argparse.ArgumentTypeError(
                "Container name can only contain lowercase letters, numbers and hyphens"
            )
        if name.startswith("-") or name.endswith("-"):
            raise argparse.ArgumentTypeError(
                "Container name cannot start or end with a hyphen"
            )
        return name

    parser = argparse.ArgumentParser(
        description="Mass undelete tool for Azure Data Lake Storage",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-u", "--storage-uri",
        required=True,
        type=validate_storage_uri,
        help="Storage account URI (e.g. https://<account-name>.dfs.core.windows.net)"
    )
    parser.add_argument(
        "-c", "--container",
        required=True,
        type=validate_container_name,
        help="Container name to restore deleted items from"
    )
    parser.add_argument(
        "-k", "--access-key",
        help="Storage account access key. If not provided, DefaultAzureCredential will be used"
    )
    
    args = parser.parse_args()

    manager = RestoreManager(
        storage_uri=args.storage_uri,
        container=args.container,
        access_key=args.access_key
    )
    manager.run()
